//! ADIF log writer for FSK441+
//! Appends one <EOR>-terminated record per QSO to ~/.fsk441/fsk441.adi
//! Format: ADIF 3.x — compatible with WSJT-X, HAMRS, LOTW, ClubLog imports.

use std::path::{Path, PathBuf};
use std::fs::{OpenOptions, File};
use std::io::{Write, BufReader, BufRead};
use anyhow::Result;

/// One logged QSO
#[derive(Clone, Debug, Default)]
pub struct QsoRecord {
    pub station_callsign: String,
    pub callsign:         String,
    pub qso_date:         String,   // YYYYMMDD
    pub time_on:          String,   // HHMMSS
    pub time_off:         String,   // HHMMSS
    pub band:             String,   // e.g. "2M"
    pub freq_mhz:         Option<f64>,
    pub mode:             String,   // "FSK441"
    pub rst_sent:         String,
    pub rst_rcvd:         String,
    pub gridsquare:       String,   // their locator
    pub my_gridsquare:    String,   // my locator
    pub prop_mode:        String,   // "MS"
    pub nr_pings:         u32,
    #[allow(dead_code)]
    pub peak_ccf:         u64,
    pub comment:          String,
}

fn adif_field(tag: &str, val: &str) -> String {
    if val.is_empty() { return String::new(); }
    format!("<{}:{}>{}", tag, val.len(), val)
}

impl QsoRecord {
    pub fn to_adif(&self) -> String {
        let mut r = String::new();
        r.push_str(&adif_field("STATION_CALLSIGN", &self.station_callsign));
        r.push_str(&adif_field("CALL",             &self.callsign));
        r.push_str(&adif_field("QSO_DATE",         &self.qso_date));
        r.push_str(&adif_field("TIME_ON",          &self.time_on));
        if !self.time_off.is_empty() {
            r.push_str(&adif_field("TIME_OFF",     &self.time_off));
        }
        r.push_str(&adif_field("BAND",  &self.band));
        r.push_str(&adif_field("MODE",  &self.mode));
        r.push_str(&adif_field("SUBMODE", "FSK441"));
        if let Some(f) = self.freq_mhz {
            r.push_str(&adif_field("FREQ", &format!("{:.4}", f)));
        }
        r.push_str(&adif_field("RST_SENT",      &self.rst_sent));
        r.push_str(&adif_field("RST_RCVD",      &self.rst_rcvd));
        r.push_str(&adif_field("GRIDSQUARE",    &self.gridsquare));
        r.push_str(&adif_field("MY_GRIDSQUARE", &self.my_gridsquare));
        r.push_str(&adif_field("PROP_MODE",     &self.prop_mode));
        if self.nr_pings > 0 {
            r.push_str(&adif_field("NR_PINGS", &self.nr_pings.to_string()));
        }
        if !self.comment.is_empty() {
            r.push_str(&adif_field("COMMENT", &self.comment));
        }
        r.push_str("<EOR>\n");
        r
    }
}

pub struct AdifLogger {
    path: PathBuf,
}

impl AdifLogger {
    pub fn default_path() -> PathBuf {
        let mut p = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        p.push(".fsk441");
        p.push("fsk441.adi");
        p
    }

    pub fn new(path: &Path) -> Self {
        // Ensure the directory and header exist
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        // Write ADIF header if file is new/empty
        if !path.exists() || path.metadata().map(|m| m.len() == 0).unwrap_or(true) {
            if let Ok(mut f) = File::create(path) {
                let _ = writeln!(f, "FSK441+ ADIF Log");
                let _ = writeln!(f, "<adif_ver:5>3.1.4");
                let _ = writeln!(f, "<programid:7>FSK441+");
                let _ = writeln!(f, "<EOH>");
            }
        }
        Self { path: path.to_path_buf() }
    }

    pub fn append(&self, record: &QsoRecord) -> Result<()> {
        let mut f = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)?;
        writeln!(f, "{}", record.to_adif())?;
        log::info!("[ADIF] Logged {} at {}", record.callsign, record.time_on);
        Ok(())
    }

    /// Read all records back (for display) — parses CALL and TIME_ON only
    #[allow(dead_code)]
    pub fn read_all(&self) -> Result<Vec<QsoRecord>> {
        let f = File::open(&self.path)?;
        let reader = BufReader::new(f);
        let mut records = Vec::new();
        let mut current = String::new();
        for line in reader.lines().flatten() {
            current.push_str(&line);
            current.push(' ');
            if line.contains("<EOR>") || line.contains("<eor>") {
                // Parse CALL and TIME_ON for display
                let call  = parse_adif_field(&current, "CALL").unwrap_or_default();
                let date  = parse_adif_field(&current, "QSO_DATE").unwrap_or_default();
                let ton   = parse_adif_field(&current, "TIME_ON").unwrap_or_default();
                let rst_s = parse_adif_field(&current, "RST_SENT").unwrap_or_default();
                let rst_r = parse_adif_field(&current, "RST_RCVD").unwrap_or_default();
                let grid  = parse_adif_field(&current, "GRIDSQUARE").unwrap_or_default();
                if !call.is_empty() {
                    records.push(QsoRecord {
                        callsign: call, qso_date: date, time_on: ton,
                        rst_sent: rst_s, rst_rcvd: rst_r, gridsquare: grid,
                        ..Default::default()
                    });
                }
                current.clear();
            }
        }
        Ok(records)
    }
}

#[allow(dead_code)]
fn parse_adif_field(s: &str, tag: &str) -> Option<String> {
    let target = format!("<{}:", tag.to_uppercase());
    let pos = s.to_uppercase().find(&target)?;
    let rest = &s[pos + target.len()..];
    let colon = rest.find('>')?;
    let len: usize = rest[..colon].parse().ok()?;
    let val_start = pos + target.len() + colon + 1;
    if val_start + len <= s.len() {
        Some(s[val_start..val_start + len].to_string())
    } else { None }
}
