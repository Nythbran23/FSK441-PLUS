/// Meteor scatter geometry — E-layer shell at 110 km.
///
/// Computes the azimuth arc from station A within which scatter points
/// exist on the 110 km shell that are mutually visible from both A and B
/// (i.e. elevation ≥ MIN_EL_DEG from each end).
///
/// Also computes the optimal beam headings given the antenna horizontal
/// beamwidth — inset from the arc edges by half-beamwidth so the main
/// lobe stays inside the scatter zone.

const R_EARTH: f64 = 6_371.0;   // km
const SHELL_KM: f64 = 110.0;    // underdense E-layer
const MIN_EL_DEG: f64 = 1.0;    // minimum elevation accepted (°)
const AZ_STEP: f64 = 0.1;       // azimuth sweep resolution (°)

#[derive(Debug, Clone)]
pub struct ScatterArc {
    /// Direct great-circle bearing A→B (°)
    pub gc_bearing: f64,
    /// Great-circle distance A→B (km)
    pub gc_distance_km: f64,
    /// Leftmost (CCW) bearing in scatter zone (°, 0-360)
    pub arc_min: f64,
    /// Rightmost (CW) bearing in scatter zone (°, 0-360)
    pub arc_max: f64,
    /// Arc half-width (°) — symmetric offset either side of GC
    pub arc_half_width: f64,
    /// Elevation to GC midpoint scatter point (°)
    pub midpoint_el: f64,
    /// Optimal left beam heading given antenna H-beamwidth (°)
    pub beam_left: Option<f64>,
    /// Optimal right beam heading given antenna H-beamwidth (°)
    pub beam_right: Option<f64>,
}

/// Great-circle distance (km).
pub fn gc_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let (lat1, lon1, lat2, lon2) = (
        lat1.to_radians(), lon1.to_radians(),
        lat2.to_radians(), lon2.to_radians(),
    );
    let dlat = lat2 - lat1;
    let dlon = lon2 - lon1;
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    R_EARTH * 2.0 * a.sqrt().asin()
}

/// Initial bearing A→B (°, 0-360).
pub fn gc_bearing(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let (lat1, lon1, lat2, lon2) = (
        lat1.to_radians(), lon1.to_radians(),
        lat2.to_radians(), lon2.to_radians(),
    );
    let dlon = lon2 - lon1;
    let x = dlon.sin() * lat2.cos();
    let y = lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * dlon.cos();
    (x.atan2(y).to_degrees() + 360.0) % 360.0
}

/// Great-circle destination given start, bearing (°), distance (km).
fn destination(lat_deg: f64, lon_deg: f64, bearing_deg: f64, dist_km: f64) -> (f64, f64) {
    let lat = lat_deg.to_radians();
    let lon = lon_deg.to_radians();
    let brg = bearing_deg.to_radians();
    let d = dist_km / R_EARTH;
    let lat2 = (lat.sin() * d.cos() + lat.cos() * d.sin() * brg.cos()).asin();
    let lon2 = lon + (brg.sin() * d.sin() * lat.cos())
        .atan2(d.cos() - lat.sin() * lat2.sin());
    (lat2.to_degrees(), lon2.to_degrees())
}

/// Elevation angle (°) from station A (ground) to scatter point P at alt_km.
///
/// Uses spherical-Earth law of cosines. Positive = above horizon.
/// In triangle O(earth-centre)–A–P:
///   angle OAP > 90° ⟹ P is above A's horizon, elevation = OAP − 90°.
fn elevation_angle(lat_a: f64, lon_a: f64, lat_p: f64, lon_p: f64, alt_km: f64) -> f64 {
    let d = gc_distance(lat_a, lon_a, lat_p, lon_p);
    if d < 0.1 {
        return 90.0;
    }
    let rh = R_EARTH + alt_km;
    let central = d / R_EARTH; // radians
    // Slant range via cosine rule
    let ap2 = R_EARTH.powi(2) + rh.powi(2) - 2.0 * R_EARTH * rh * central.cos();
    if ap2 <= 0.0 {
        return 90.0;
    }
    let ap = ap2.sqrt();
    // Angle at A
    let cos_oap = (R_EARTH.powi(2) + ap2 - rh.powi(2)) / (2.0 * R_EARTH * ap);
    let cos_oap = cos_oap.clamp(-1.0, 1.0);
    let oap_deg = cos_oap.acos().to_degrees();
    oap_deg - 90.0 // positive ⟹ above horizon
}

/// Scatter point on the shell at (az_deg, MIN_EL_DEG) from station A.
/// Returns (lat, lon) or None if geometry is degenerate.
fn shell_point_at_az(lat_a: f64, lon_a: f64, az_deg: f64) -> Option<(f64, f64)> {
    let el = MIN_EL_DEG.to_radians();
    let rh = R_EARTH + SHELL_KM;
    // Quadratic: r² + 2R·sin(el)·r − (Rh² − R²) = 0
    let b = 2.0 * R_EARTH * el.sin();
    let c = -(rh.powi(2) - R_EARTH.powi(2));
    let disc = b * b - 4.0 * c;
    if disc < 0.0 {
        return None;
    }
    let r_slant = (-b + disc.sqrt()) / 2.0;
    if r_slant <= 0.0 {
        return None;
    }
    // Central angle via sine rule in O-A-P triangle
    let sin_angle_p = (R_EARTH * el.cos() / rh).clamp(-1.0, 1.0);
    let angle_p = sin_angle_p.asin();
    let central = std::f64::consts::FRAC_PI_2 - el - angle_p;
    if central < 0.0 {
        return None;
    }
    let ground_dist = R_EARTH * central;
    Some(destination(lat_a, lon_a, az_deg, ground_dist))
}

/// Compute the scatter arc and optimal beam headings.
///
/// `bw_horiz_deg` — antenna 3 dB horizontal beamwidth (°). Used to inset
/// the two optimal beam headings from the arc edges.
pub fn compute_scatter_arc(
    lat_a: f64, lon_a: f64,
    lat_b: f64, lon_b: f64,
    bw_horiz_deg: f64,
) -> Option<ScatterArc> {
    let gc_brg = gc_bearing(lat_a, lon_a, lat_b, lon_b);
    let d_ab = gc_distance(lat_a, lon_a, lat_b, lon_b);

    // Sweep all azimuths from A at MIN_EL_DEG to the 110 km shell;
    // keep those where B also has elevation ≥ MIN_EL_DEG to the shell point.
    let mut valid_offsets: Vec<f64> = Vec::new();
    let steps = (360.0 / AZ_STEP) as usize;

    for i in 0..steps {
        let az = i as f64 * AZ_STEP;
        let Some((lat_p, lon_p)) = shell_point_at_az(lat_a, lon_a, az) else {
            continue;
        };
        let el_b = elevation_angle(lat_b, lon_b, lat_p, lon_p, SHELL_KM);
        if el_b < MIN_EL_DEG {
            continue;
        }
        // Angular offset from GC bearing, wrapped to [−180, +180]
        let offset = ((az - gc_brg + 540.0) % 360.0) - 180.0;
        valid_offsets.push(offset);
    }

    if valid_offsets.is_empty() {
        return None;
    }

    // The valid set may wrap through ±180°. Find the contiguous arc
    // that straddles the GC bearing (offset ≈ 0).
    valid_offsets.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let arc_left  = *valid_offsets.first().unwrap();
    let arc_right = *valid_offsets.last().unwrap();
    let arc_half  = (arc_right - arc_left) / 2.0;

    let arc_min = (gc_brg + arc_left  + 360.0) % 360.0;
    let arc_max = (gc_brg + arc_right + 360.0) % 360.0;

    // Elevation to GC midpoint at 110 km
    let (mid_lat, mid_lon) = destination(lat_a, lon_a, gc_brg, d_ab / 2.0);
    let mid_el = elevation_angle(lat_a, lon_a, mid_lat, mid_lon, SHELL_KM);

    // Optimal beam headings: inset from arc edges by half the H-beamwidth
    let half_bw = bw_horiz_deg / 2.0;
    let (beam_left, beam_right) = if arc_half <= half_bw {
        // Arc narrower than beamwidth — one centred heading
        let centre = (gc_brg + (arc_left + arc_right) / 2.0 + 360.0) % 360.0;
        (Some(centre), None)
    } else {
        let bl = (gc_brg + arc_left  + half_bw + 360.0) % 360.0;
        let br = (gc_brg + arc_right - half_bw + 360.0) % 360.0;
        (Some(bl), Some(br))
    };

    Some(ScatterArc {
        gc_bearing: gc_brg,
        gc_distance_km: d_ab,
        arc_min,
        arc_max,
        arc_half_width: arc_half,
        midpoint_el: mid_el,
        beam_left,
        beam_right,
    })
}
