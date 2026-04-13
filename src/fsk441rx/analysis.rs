#![allow(dead_code)]
//! Background threshold optimiser for FSK441+
//!
//! WINDOWING STRATEGY
//! ──────────────────
//! Uses a 6-hour window (not 24h) to keep signal/noise ratio sensible.
//! If fewer than 5 confirmed signals found in 6h, falls back to 24h.
//!
//! NOISE BALANCE
//! ─────────────
//! Caps noise sample at 200× confirmed signal count. This prevents the
//! grid search from degrading when noise pool is orders of magnitude
//! larger than signal pool (which causes optimiser to just suggest
//! "threshold everything out" to maximise precision at the cost of recall).
//!
//! LABELLING
//! ─────────
//! A ping is "confirmed signal" if its callsign appears in ≥2 pings
//! within ±150Hz DF and 300s — temporal+spatial clustering.

use anyhow::Result;
use rusqlite::Connection;

#[derive(Debug, Clone)]
pub struct ThresholdSuggestion {
    pub analysed_at:         String,
    pub window_hours:        f32,
    pub n_noise_pings:       usize,
    pub n_signal_pings:      usize,
    pub n_confirmed_qsos:    usize,
    pub noise_ccf_p95:       f32,
    pub noise_ccf_p99:       f32,
    pub noise_conf_p95:      f32,
    pub noise_conf_p99:      f32,
    pub current_arm1_ccf:    f32,
    pub current_arm1_conf:   f32,
    pub current_arm2_ccf:    f32,
    pub current_arm2_conf:   f32,
    pub current_recall:      f32,
    pub current_precision:   f32,
    pub current_f1:          f32,
    pub suggested_arm1_ccf:  f32,
    pub suggested_arm1_conf: f32,
    pub suggested_arm2_ccf:  f32,
    pub suggested_arm2_conf: f32,
    pub suggested_recall:    f32,
    pub suggested_precision: f32,
    pub suggested_f1:        f32,
    pub summary:             String,
}

impl ThresholdSuggestion {
    pub fn is_improvement(&self) -> bool {
        self.suggested_f1 > self.current_f1 + 0.02
    }
}

pub fn run_optimiser(
    db_path: &str,
    _window_hours: f32,   // ignored — we pick window adaptively
    current_arm1_ccf:  f32,
    current_arm1_conf: f32,
    current_arm2_ccf:  f32,
    current_arm2_conf: f32,
) -> Result<ThresholdSuggestion> {
    let conn = Connection::open(db_path)?;

    conn.execute_batch("
        CREATE TABLE IF NOT EXISTS threshold_history (
            id              INTEGER PRIMARY KEY,
            analysed_at     TEXT NOT NULL,
            window_hours    REAL,
            n_noise         INTEGER,
            n_signal        INTEGER,
            noise_conf_p95  REAL,
            noise_conf_p99  REAL,
            noise_ccf_p95   REAL,
            noise_ccf_p99   REAL,
            cur_arm1_ccf    REAL,
            cur_arm1_conf   REAL,
            cur_f1          REAL,
            sug_arm1_ccf    REAL,
            sug_arm1_conf   REAL,
            sug_arm2_ccf    REAL,
            sug_arm2_conf   REAL,
            sug_f1          REAL,
            summary         TEXT
        );
    ")?;

    // Try 6h window first, fall back to 24h if not enough signals
    let (pings, labels, window_hours) = {
        let r6  = fetch_and_label(&conn, 6.0)?;
        let sig6 = r6.1.iter().filter(|&&b| b).count();
        if sig6 >= 5 {
            (r6.0, r6.1, 6.0f32)
        } else {
            let r24 = fetch_and_label(&conn, 24.0)?;
            (r24.0, r24.1, 24.0f32)
        }
    };

    let n_signal = labels.iter().filter(|&&b| b).count();
    let n_noise_total = labels.len() - n_signal;

    if n_signal < 3 {
        return Err(anyhow::anyhow!(
            "Only {} confirmed signals in 24h — need ≥3 for fitting", n_signal
        ));
    }

    // Split into signal and noise vecs
    let signal_pings: Vec<&Ping> = pings.iter().zip(&labels)
        .filter(|(_, &s)| s).map(|(p,_)| p).collect();
    let all_noise: Vec<&Ping> = pings.iter().zip(&labels)
        .filter(|(_, &s)| !s).map(|(p,_)| p).collect();

    // Cap noise at 200× signal to keep ratio sensible
    let max_noise = (n_signal * 200).max(500);
    let noise_pings: Vec<&Ping> = if all_noise.len() > max_noise {
        let step = (all_noise.len() / max_noise).max(1);
        all_noise.iter().step_by(step).copied().take(max_noise).collect()
    } else {
        all_noise.clone()
    };
    let n_noise = noise_pings.len();

    // Noise distribution from subsampled noise
    let (noise_conf_p95, noise_conf_p99) = percentiles(
        &noise_pings.iter().map(|p| p.conf).collect::<Vec<_>>()
    );
    let (noise_ccf_p95, noise_ccf_p99) = percentiles(
        &noise_pings.iter().map(|p| p.ccf).collect::<Vec<_>>()
    );

    // Build balanced evaluation set: all signals + capped noise
    let eval_pings: Vec<&Ping> = signal_pings.iter().copied()
        .chain(noise_pings.iter().copied()).collect();
    let eval_labels: Vec<bool> = std::iter::repeat(true).take(n_signal)
        .chain(std::iter::repeat(false).take(n_noise)).collect();

    // F1 scorer on balanced set
    let score = |a1c: f32, a1f: f32, a2c: f32, a2f: f32| -> (f32, f32, f32) {
        let mut tp = 0usize; let mut fp = 0usize; let mut fn_ = 0usize;
        for (p, &sig) in eval_pings.iter().zip(&eval_labels) {
            let passes = (p.ccf >= a1c && p.conf >= a1f)
                      || (p.ccf >= a2c && p.conf >= a2f);
            match (passes, sig) {
                (true,  true)  => tp += 1,
                (true,  false) => fp += 1,
                (false, true)  => fn_ += 1,
                _              => {},
            }
        }
        let prec = if tp+fp == 0 { 0.0 } else { tp as f32/(tp+fp) as f32 };
        let rec  = if tp+fn_== 0 { 0.0 } else { tp as f32/(tp+fn_) as f32 };
        let f1   = if prec+rec == 0.0 { 0.0 } else { 2.0*prec*rec/(prec+rec) };
        (prec, rec, f1)
    };

    let (cur_prec, cur_rec, cur_f1) = score(
        current_arm1_ccf, current_arm1_conf,
        current_arm2_ccf, current_arm2_conf,
    );

    // Grid search
    // arm1 conf floor: must be at least noise_conf_p99 + 0.08 margin.
    // This prevents the optimiser recommending a conf threshold that sits
    // inside the measured noise distribution. With p99≈0.477, the floor is
    // ~0.557, so the minimum arm1 conf the grid can ever suggest is 0.58.
    let arm1_conf_floor = (noise_conf_p99 + 0.08).max(0.50);

    let mut best_f1 = 0.0f32;
    let mut best = (current_arm1_ccf, current_arm1_conf,
                    current_arm2_ccf, current_arm2_conf);

    for &a1c in &[50.0f32, 75.0, 100.0, 125.0, 150.0, 200.0] {
        for &a1f in &[0.50f32, 0.52, 0.54, 0.55, 0.56, 0.58, 0.60, 0.62, 0.65] {
            if a1f < arm1_conf_floor { continue; }  // below noise floor — skip
            for &a2c in &[15.0f32, 20.0, 25.0, 30.0, 40.0, 50.0] {
                for &a2f in &[0.65f32, 0.70, 0.72, 0.75, 0.78, 0.80, 0.85] {
                    if a1c < 25.0 || a1f < 0.43 { continue; }
                    if a2c < 15.0 || a2f < 0.65 { continue; }
                    let (_, _, f1) = score(a1c, a1f, a2c, a2f);
                    if f1 > best_f1 { best_f1 = f1; best = (a1c, a1f, a2c, a2f); }
                }
            }
        }
    }

    let (sug_prec, sug_rec, sug_f1) = score(best.0, best.1, best.2, best.3);

    let summary = format!(
        "{:.0}h window: {} signal / {} noise (balanced from {})\n\
         Noise conf: 95th={:.3} 99th={:.3}  arm1 floor={:.3}\n\
         Current  (CCF≥{:.0}+conf≥{:.2} | CCF≥{:.0}+conf≥{:.2}): F1={:.3} P={:.2} R={:.2}\n\
         Suggested(CCF≥{:.0}+conf≥{:.2} | CCF≥{:.0}+conf≥{:.2}): F1={:.3} P={:.2} R={:.2}{}",
        window_hours, n_signal, n_noise, n_noise_total,
        noise_conf_p95, noise_conf_p99, arm1_conf_floor,
        current_arm1_ccf, current_arm1_conf, current_arm2_ccf, current_arm2_conf,
        cur_f1, cur_prec, cur_rec,
        best.0, best.1, best.2, best.3,
        sug_f1, sug_prec, sug_rec,
        if sug_f1 > cur_f1 + 0.02 { " ← IMPROVEMENT" } else { "" },
    );

    let analysed_at = chrono::Utc::now().to_rfc3339();

    conn.execute(
        "INSERT INTO threshold_history
         (analysed_at, window_hours, n_noise, n_signal,
          noise_conf_p95, noise_conf_p99, noise_ccf_p95, noise_ccf_p99,
          cur_arm1_ccf, cur_arm1_conf, cur_f1,
          sug_arm1_ccf, sug_arm1_conf, sug_arm2_ccf, sug_arm2_conf, sug_f1,
          summary)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
        rusqlite::params![
            analysed_at, window_hours as f64,
            n_noise as i64, n_signal as i64,
            noise_conf_p95, noise_conf_p99, noise_ccf_p95, noise_ccf_p99,
            current_arm1_ccf, current_arm1_conf, cur_f1,
            best.0, best.1, best.2, best.3, sug_f1,
            summary,
        ],
    )?;

    log::info!("[ANALYSIS] {}", summary);

    Ok(ThresholdSuggestion {
        analysed_at, window_hours, n_noise_pings: n_noise,
        n_signal_pings: n_signal, n_confirmed_qsos: 0,
        noise_ccf_p95, noise_ccf_p99, noise_conf_p95, noise_conf_p99,
        current_arm1_ccf, current_arm1_conf,
        current_arm2_ccf, current_arm2_conf,
        current_recall: cur_rec, current_precision: cur_prec, current_f1: cur_f1,
        suggested_arm1_ccf: best.0, suggested_arm1_conf: best.1,
        suggested_arm2_ccf: best.2, suggested_arm2_conf: best.3,
        suggested_recall: sug_rec, suggested_precision: sug_prec, suggested_f1: sug_f1,
        summary,
    })
}

#[derive(Clone)]
struct Ping { ts_secs: i64, df_hz: f32, ccf: f32, conf: f32, calls: Vec<String> }

fn fetch_and_label(conn: &Connection, window_hours: f32) -> Result<(Vec<Ping>, Vec<bool>)> {
    let callsign_re = regex::Regex::new(r"\b([A-Z]{1,2}[0-9][A-Z]{2,4})\b").unwrap();

    let mut stmt = conn.prepare(&format!(
        "SELECT detected_at, df_hz, ccf_ratio, mean_confidence, raw_decode
         FROM pings
         WHERE detected_at > datetime('now', '-{} hours')
           AND ccf_ratio < 50000
           AND CAST(SUBSTR(detected_at,18,2) AS INTEGER) < 30
         ORDER BY detected_at",
        window_hours as i64
    ))?;

    let pings: Vec<Ping> = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, f32>(1)?,
            row.get::<_, f32>(2)?,
            row.get::<_, f32>(3)?,
            row.get::<_, Option<String>>(4)?.unwrap_or_default(),
        ))
    })?
    .filter_map(|r| r.ok())
    .filter_map(|(ts, df, ccf, conf, raw)| {
        let ts_secs = chrono::DateTime::parse_from_rfc3339(&ts)
            .ok().map(|dt| dt.timestamp())?;
        let calls: Vec<String> = callsign_re.find_iter(&raw.to_uppercase())
            .map(|m| m.as_str().to_string()).collect();
        Some(Ping { ts_secs, df_hz: df, ccf, conf, calls })
    })
    .collect();

    if pings.is_empty() {
        return Ok((vec![], vec![]));
    }

    // Label: callsign confirmed if ≥2 pings within 300s and ±150Hz
    use std::collections::HashMap;
    let mut call_pings: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, p) in pings.iter().enumerate() {
        for c in &p.calls {
            call_pings.entry(c.clone()).or_default().push(i);
        }
    }

    let mut confirmed: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (call, indices) in &call_pings {
        if indices.len() < 2 { continue; }
        'outer: for &i in indices {
            for &j in indices {
                if i == j { continue; }
                let dt  = (pings[i].ts_secs - pings[j].ts_secs).abs();
                let ddf = (pings[i].df_hz  - pings[j].df_hz).abs();
                if dt <= 300 && ddf <= 150.0 {
                    confirmed.insert(call.clone());
                    break 'outer;
                }
            }
        }
    }

    let labels: Vec<bool> = pings.iter()
        .map(|p| p.calls.iter().any(|c| confirmed.contains(c)))
        .collect();

    Ok((pings, labels))
}

fn percentiles(values: &[f32]) -> (f32, f32) {
    if values.is_empty() { return (0.0, 0.0); }
    let mut v = values.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p95 = v[(v.len() as f32 * 0.95) as usize];
    let p99 = v[((v.len() as f32 * 0.99) as usize).min(v.len()-1)];
    (p95, p99)
}
