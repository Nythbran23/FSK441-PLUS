// src/fsk441rx/geo.rs
//
// Geographic validation for decoded callsigns.
// Uses cty.dat (the standard Amateur Radio country/prefix database) to look up
// the approximate location of a callsign prefix, then checks it's within the
// maximum great-circle range for 2m meteor scatter from the operator's QTH.
//
// cty.dat longitude convention: POSITIVE = WEST (inverted from geographic standard).
// We convert to standard East-positive on load.

use std::path::Path;

// ─── QTH ─────────────────────────────────────────────────────────────────────

/// Operator QTH — set from --qth flag or compiled default
#[derive(Debug, Clone, Copy)]
pub struct Qth {
    pub lat: f64,  // degrees North
    pub lon: f64,  // degrees East (negative = West)
}

impl Qth {
    /// Parse a Maidenhead locator (4 or 6 chars) into lat/lon
    pub fn from_maidenhead(grid: &str) -> Option<Self> {
        let g = grid.to_uppercase();
        let b = g.as_bytes();
        if b.len() < 4 { return None; }

        // Field (2 letters)
        let lon = (b[0].wrapping_sub(b'A')) as f64 * 20.0 - 180.0;
        let lat = (b[1].wrapping_sub(b'A')) as f64 * 10.0 - 90.0;

        // Square (2 digits)
        let lon = lon + (b[2].wrapping_sub(b'0')) as f64 * 2.0;
        let lat = lat + (b[3].wrapping_sub(b'0')) as f64;

        // Subsquare (2 letters, optional)
        let (lon, lat) = if b.len() >= 6 {
            let lo = lon + (b[4].wrapping_sub(b'A')) as f64 * (2.0 / 24.0);
            let la = lat + (b[5].wrapping_sub(b'A')) as f64 * (1.0 / 24.0);
            // Centre of subsquare
            (lo + 1.0 / 24.0, la + 0.5 / 24.0)
        } else {
            // Centre of square
            (lon + 1.0, lat + 0.5)
        };

        Some(Self { lat, lon })
    }
}

impl Default for Qth {
    /// Default QTH: IO82KM (Roger GW4WND, Wales)
    fn default() -> Self {
        Self::from_maidenhead("IO82KM").unwrap()
    }
}

// ─── Haversine great-circle distance ─────────────────────────────────────────

/// Returns distance in kilometres between two lat/lon points
pub fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6371.0_f64;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    r * 2.0 * a.sqrt().asin()
}

// ─── cty.dat parser ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CountryEntry {
    pub name: String,
    pub lat:  f64,   // degrees North
    pub lon:  f64,   // degrees East (converted from cty.dat West-positive)
}

/// Prefix → CountryEntry lookup table
pub struct PrefixDb {
    /// Sorted list of (prefix, entry) — longest prefix match wins
    prefixes: Vec<(String, CountryEntry)>,
}

impl PrefixDb {
    /// Load from cty.dat file. Returns empty db on failure (no crash).
    pub fn load(path: &Path) -> Self {
        let text = match std::fs::read_to_string(path) {
            Ok(t)  => t,
            Err(e) => {
                log::warn!("[GEO] Cannot read cty.dat: {} — geo filtering disabled", e);
                return Self { prefixes: vec![] };
            }
        };
        Self::parse(&text)
    }

    fn parse(text: &str) -> Self {
        let mut prefixes: Vec<(String, CountryEntry)> = Vec::new();
        let mut current: Option<CountryEntry> = None;

        for line in text.lines() {
            let line = line.trim().trim_end_matches('\r');
            if line.is_empty() { continue; }

            // Header line: fields separated by colon, ends with main prefix + colon
            // e.g. "England:  14:  27:  EU:   52.77:     1.47:     0.0:  G:"
            if !line.starts_with(' ') && line.contains(':') {
                let parts: Vec<&str> = line.split(':').collect();
                if parts.len() >= 8 {
                    let name = parts[0].trim().to_string();
                    let lat  = parts[4].trim().parse::<f64>().unwrap_or(0.0);
                    // cty.dat lon: positive = West → convert to East-positive
                    let lon  = -parts[5].trim().parse::<f64>().unwrap_or(0.0);
                    let main_pfx = parts[7].trim().to_string();
                    current = Some(CountryEntry { name, lat, lon });
                    // Register the main prefix immediately
                    if let Some(ref entry) = current {
                        if !main_pfx.is_empty() {
                            prefixes.push((main_pfx, entry.clone()));
                        }
                    }
                }
            } else if let Some(ref entry) = current {
                // Continuation line: comma-separated prefixes, ends with ; or ,
                let clean = line.trim_end_matches(';').trim_end_matches(',');
                for pfx in clean.split(',') {
                    let pfx = pfx.trim();
                    if pfx.is_empty() { continue; }
                    // Skip exact-match overrides (start with =)
                    if pfx.starts_with('=') { continue; }
                    prefixes.push((pfx.to_string(), entry.clone()));
                }
                // End of entry
                if line.ends_with(';') { current = None; }
            }
        }

        // Sort longest prefix first for correct longest-match lookup
        prefixes.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

        log::info!("[GEO] Loaded {} prefixes from cty.dat", prefixes.len());
        Self { prefixes }
    }

    /// Look up a callsign — returns the best matching CountryEntry
    pub fn lookup(&self, callsign: &str) -> Option<&CountryEntry> {
        let call = callsign.to_uppercase();
        // Try longest prefix first
        for (pfx, entry) in &self.prefixes {
            if call.starts_with(pfx.as_str()) {
                return Some(entry);
            }
        }
        None
    }

    pub fn is_empty(&self) -> bool { self.prefixes.is_empty() }
}

// ─── Geographic validator ─────────────────────────────────────────────────────

pub struct GeoValidator {
    db:          PrefixDb,
    qth:         Qth,
    max_dist_km: f64,
}

impl GeoValidator {
    pub fn new(db: PrefixDb, qth: Qth, max_dist_km: f64) -> Self {
        Self { db, qth, max_dist_km }
    }

    /// Returns true if the callsign is geographically plausible
    /// (within max_dist_km of QTH, or unknown prefix → assume plausible)
    pub fn is_plausible(&self, callsign: &str) -> bool {
        if self.db.is_empty() { return true; } // no db = no filter

        match self.db.lookup(callsign) {
            None => {
                // Unknown prefix — reject if DB loaded, pass if DB empty
                log::debug!("[GEO] {} — prefix not in cty.dat, rejecting", callsign);
                false
            }
            Some(entry) => {
                let dist = haversine_km(
                    self.qth.lat, self.qth.lon,
                    entry.lat,    entry.lon,
                );
                if dist > self.max_dist_km {
                    log::debug!(
                        "[GEO] {} ({}) is {:.0}km away — rejected (max {}km)",
                        callsign, entry.name, dist, self.max_dist_km
                    );
                }
                dist <= self.max_dist_km
            }
        }
    }

    /// Filter a list of callsigns, returning only plausible ones
    pub fn filter_callsigns<'a>(&self, calls: &'a [String]) -> Vec<&'a String> {
        calls.iter().filter(|c| self.is_plausible(c)).collect()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io82km_coords() {
        let q = Qth::from_maidenhead("IO82KM").unwrap();
        assert!((q.lat - 52.5).abs() < 0.1, "lat={}", q.lat);
        assert!((q.lon - (-3.1)).abs() < 0.2, "lon={}", q.lon);
    }

    #[test]
    fn haversine_wales_to_finland() {
        // IO82KM to OH (Finland ~64N, 26E) should be ~2100km
        let d = haversine_km(52.5, -3.1, 64.0, 26.0);
        assert!(d > 1900.0 && d < 2300.0, "d={}", d);
    }

    #[test]
    fn haversine_wales_to_usa() {
        // IO82KM to W (USA ~38N, 95W) should be ~6800km
        let d = haversine_km(52.5, -3.1, 38.0, -95.0);
        assert!(d > 6000.0, "d={}", d);
    }
}
