// src/fsk441rx/second_pass.rs
//
// Second-pass decoder for FSK441+
//
// Runs during the TX period, scoring the soft character probability data
// (char_probs blobs from analysis_pings) against known QSO message
// hypotheses. Returns confirmed character recoveries without inventing data.
//
// PRINCIPLE: A character is ONLY confirmed if the blob shows genuine energy
// at that character's slot above the measured noise floor. The hypothesis
// constrains WHICH slot to check — the energy must be independently present
// in the received signal. Nothing is ever invented.
//
// WORKFLOW:
//   1. TX starts → engine auto-saves ring buffer → gets capture_id
//   2. spawn_blocking(|| second_pass::run(db_path, capture_id, &ctx))
//   3. run() scores all pings, then deletes capture from DB (self-cleaning)
//   4. Results sent as EngineEvent::SecondPassDecodes(Vec<SecondPassResult>)
//
// TIMING: 30s TX period >> time to score ~300 pings × ~15 hypotheses.
// Completes in well under 1 second on any modern CPU.
//
// CHARSET MAPPING
// ───────────────
// analysis_pings.char_probs stores Vec<[f32;48]> — one 48-float probability
// distribution per decoded character position. Slots are ordered:
//   [0-9]   '0'-'9'
//   [10]    '+'  [11] '-'  [12] '.'  [13] '/'  [14] '?'
//   [15]    ' '  (space — FSK441 sync character)
//   [16]    '$'
//   [17-42] 'A'-'Z'
//   [43-47] (unused)
// Derived empirically 2026-04-12, confirmed against 8 known characters.

use anyhow::Result;
use rusqlite::Connection;

// ─── Charset ──────────────────────────────────────────────────────────────────

/// Map a character to its slot index in the char_probs blob.
/// Returns None for characters not in the FSK441 charset.
fn char_to_slot(c: char) -> Option<usize> {
    match c.to_ascii_uppercase() {
        '0'..='9' => Some(c as usize - '0' as usize),
        '+'       => Some(10),
        '-'       => Some(11),
        '.'       => Some(12),
        '/'       => Some(13),
        '?'       => Some(14),
        ' '       => Some(15),
        '$'       => Some(16),
        'A'..='Z' => Some(17 + c.to_ascii_uppercase() as usize - 'A' as usize),
        _         => None,
    }
}

// ─── QSO context ──────────────────────────────────────────────────────────────

/// Current QSO state passed to the second-pass scorer.
/// Drives hypothesis generation — tighter state = fewer hypotheses = fewer FPs.
#[derive(Debug, Clone)]
pub struct QsoContext {
    /// Our callsign
    pub my_call:     String,
    /// Their callsign if known (SET was pressed)
    pub their_call:  Option<String>,
    /// Their locator if known
    pub their_loc:   Option<String>,
    /// Report we sent them (e.g. "26")
    pub report_sent: Option<String>,
    /// Report they sent us (e.g. "26")
    pub report_rcvd: Option<String>,
    /// Which TX message we just sent
    pub stage:       QsoStage,
    /// noise_conf_p99 + 0.08 — from threshold_history, same floor used everywhere
    pub noise_floor: f32,
    /// When true, do NOT delete analysis_pings after scoring — retained for off-line analysis.
    /// Set from Settings::save_max_data.
    pub retain_after_run: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QsoStage {
    /// No active QSO — look for CQ calls and unsolicited callsigns
    Idle,
    /// We transmitted CQ — expect their first response
    CallingCq,
    /// We sent TX1 "[THEIR] [MY]" — expect their report
    CallingStation,
    /// We sent TX2 (our report) — expect their R+report or RRR
    SentReport,
    /// We sent TX3 RRRR — expect their RRR or 73
    SentRReport,
    /// We sent TX4/TX5 RR73/73 — expect their 73
    SentRR73,
}

// ─── Results ──────────────────────────────────────────────────────────────────

/// One recovered ping from the second-pass decoder.
#[derive(Debug, Clone)]
pub struct SecondPassResult {
    /// Timestamp of the original ping
    pub detected_at:     String,
    pub df_hz:           f32,
    pub ccf_ratio:       f32,
    pub mean_confidence: f32,
    /// The hypothesis message that matched
    pub hypothesis:      String,
    /// Chars confirmed above noise floor — these are REAL, not invented
    pub n_confirmed:     usize,
    /// Chars that could be checked (in our charset map)
    pub n_checked:       usize,
    /// (char, energy) pairs — only entries where energy > noise_floor
    pub confirmed_chars: Vec<(char, f32)>,
    /// Original primary decoder output (partial/empty)
    pub raw_decode:      String,
}

// Minimum independently-confirmed characters required to promote a ping.
// Set to 3: prevents 2-char coincidence false positives (e.g. '4' + ' '
// appearing in an unrelated OZ7UV ping scored against SM4SJY hypothesis).
const MIN_CONFIRMED: usize = 3;

// ─── Hypothesis generation ────────────────────────────────────────────────────

/// Standard FSK441 signal reports used in QSO exchanges.
const REPORTS: &[&str] = &["26","27","28","29","36","37","38","47","48","57","58","59"];

/// Generate candidate message hypotheses for the current QSO state.
///
/// Hypotheses are base messages — the scorer tries all repeating offsets
/// so the exact phase within the 30s period doesn't matter.
///
/// Ordered from most to least specific: specific hypotheses score higher
/// and will be preferred when the context is well-known.
pub fn generate_hypotheses(ctx: &QsoContext) -> Vec<String> {
    let mut h: Vec<String> = Vec::new();
    let my = ctx.my_call.to_uppercase();

    match &ctx.their_call {
        // ── No callsign known — broad search ──────────────────────────────
        None => {
            // Only CQ patterns — without a known call we can't constrain further
            h.push(format!("CQ {} ", my));
            h.push(format!("CQ DE {} ", my));
            // Generic CQ from unknown station — too broad to be useful
            // without a full charset map of likely callsign prefixes
        }

        // ── Known callsign — targeted hypotheses ──────────────────────────
        Some(their_raw) => {
            let their = their_raw.to_uppercase();

            // CQ: always possible regardless of stage (they might still be CQing)
            h.push(format!("CQ {} ", their));
            if let Some(loc) = &ctx.their_loc {
                h.push(format!("CQ {} {} ", their, loc.to_uppercase()));
            }

            match ctx.stage {
                // We sent CQ or haven't sent yet — expect their first contact
                QsoStage::Idle | QsoStage::CallingCq => {
                    // TX1 style: their call + our call (+ optional report)
                    h.push(format!("{} {} ", their, my));
                    h.push(format!("{} {} ", my, their));
                    // With report — they might include report in first TX
                    for rpt in REPORTS {
                        h.push(format!("{} {} {} ", their, my, rpt));
                        h.push(format!("{} {} {} ", my, their, rpt));
                    }
                }

                // We sent TX1 "[THEIR] [MY]" — expect their report back
                QsoStage::CallingStation => {
                    // Standard exchange: "[MY] [THEIR] [RPT]"
                    for rpt in REPORTS {
                        h.push(format!("{} {} {} ", my, their, rpt));
                        h.push(format!("{} {} {} ", their, my, rpt));
                        h.push(format!("{} {} R{} ", my, their, rpt));
                    }
                    // Just the callsigns — short ping at start of their TX
                    h.push(format!("{} {} ", my, their));
                }

                // We sent TX2 (report) — expect their R+report or RRR
                QsoStage::SentReport => {
                    // R+report: confirming they received our report
                    for rpt in REPORTS {
                        h.push(format!("{} {} R{} ", my, their, rpt));
                        h.push(format!("R{} {} {} R{} ", rpt, my, their, rpt));
                    }
                    // If we know what report we sent, more specific hypotheses
                    if let Some(sent) = &ctx.report_sent {
                        h.push(format!("{} {} R{} ", my, their, sent));
                        h.push(format!("R{} R{} {} ", sent, sent, my));
                    }
                    // RRR variants — they might go straight to roger
                    h.push(format!("{} RRRR RRRR {} RRRR ", my, their));
                    h.push(format!("RRRR RRRR {} RRRR ", my));
                }

                // We sent RRRR — expect their RRR or 73
                QsoStage::SentRReport => {
                    h.push(format!("RRRR RRRR {} RRRR ", my));
                    h.push(format!("{} RRRR RRRR {} ", my, their));
                    h.push(format!("{} RRRR {} RRRR ", their, my));
                    h.push(format!("73 73 73 {} 73 ", my));
                    h.push(format!("{} 73 73 {} 73 ", their, my));
                }

                // We sent RR73 or 73 — expect their 73
                QsoStage::SentRR73 => {
                    h.push(format!("73 73 73 {} 73 ", my));
                    h.push(format!("{} 73 73 {} 73 ", their, my));
                    h.push(format!("73 {} 73 73 {} ", my, their));
                }
            }
        }
    }

    // Deduplicate while preserving order
    let mut seen = std::collections::HashSet::new();
    h.retain(|s| seen.insert(s.clone()));
    h
}

// ─── Blob scoring ─────────────────────────────────────────────────────────────

/// Score a char_probs blob against one hypothesis message.
///
/// Tries all alignment offsets of the repeating hypothesis (accounts for
/// the decoder starting mid-message). Returns the best-scoring alignment.
///
/// For each character position i in the blob:
///   - Predict the character from the hypothesis at offset+i
///   - Look up that character's slot in the blob
///   - Read the energy value
///   - ONLY accept if energy >= noise_floor — never invent
///
/// Returns (n_confirmed, n_checked, confirmed_chars, best_offset).
fn score_blob(
    blob:        &[u8],
    hypothesis:  &str,
    noise_floor: f32,
) -> (usize, usize, Vec<(char, f32)>, usize) {
    let n_pos = blob.len() / (48 * 4);
    if n_pos == 0 { return (0, 0, vec![], 0); }

    // Repeat hypothesis to cover any alignment within a full message cycle
    let rep = format!("{}{}{}", hypothesis, hypothesis, hypothesis);
    let rep_chars: Vec<char> = rep.chars().collect();

    let mut best_confirmed = 0usize;
    let mut best_checked   = 0usize;
    let mut best_chars:  Vec<(char, f32)> = Vec::new();
    let mut best_offset  = 0usize;

    let hyp_len = hypothesis.chars().count();

    for offset in 0..hyp_len {
        let mut confirmed = 0usize;
        let mut checked   = 0usize;
        let mut chars: Vec<(char, f32)> = Vec::new();

        for i in 0..n_pos {
            let t_idx = offset + i;
            if t_idx >= rep_chars.len() { break; }
            let c = rep_chars[t_idx];

            let slot = match char_to_slot(c) {
                Some(s) => s,
                None    => continue,  // Character not in our map — skip, don't penalise
            };

            // Read the probability value for this slot at position i
            let byte_offset = i * 48 * 4 + slot * 4;
            if byte_offset + 4 > blob.len() { break; }
            let energy = f32::from_le_bytes([
                blob[byte_offset],
                blob[byte_offset + 1],
                blob[byte_offset + 2],
                blob[byte_offset + 3],
            ]);

            checked += 1;

            // ONLY accept if energy is genuinely above the noise floor
            if energy >= noise_floor {
                confirmed += 1;
                chars.push((c, energy));
            } else {
                // Don't immediately stop — the fade profile isn't perfectly
                // monotonic. A single weak position mid-message shouldn't
                // discard genuine edge characters either side.
            }
        }

        if confirmed > best_confirmed
            || (confirmed == best_confirmed && checked > best_checked)
        {
            best_confirmed = confirmed;
            best_checked   = checked;
            best_chars     = chars;
            best_offset    = offset;
        }
    }

    (best_confirmed, best_checked, best_chars, best_offset)
}

// ─── Main entry point ─────────────────────────────────────────────────────────

/// Raw row from analysis_pings
struct AnalysisRow {
    detected_at:     String,
    df_hz:           f32,
    ccf_ratio:       f32,
    mean_confidence: f32,
    validity_score:  i32,
    raw_decode:      String,
    char_probs:      Vec<u8>,
}

/// Run the second-pass decoder against a saved capture window.
///
/// Call from `tokio::task::spawn_blocking` at the start of the TX period.
/// This function opens its own DB connection, scores all pings in the capture
/// window, deletes the capture from the database when done (self-cleaning),
/// and returns confirmed recoveries sorted by n_confirmed descending.
///
/// Only pings with >= MIN_CONFIRMED independently-verified characters are returned.
///
/// # Arguments
/// * `db_path`     — Path to fsk441.db
/// * `capture_id`  — ID returned by `store.save_analysis()`
/// * `ctx`         — Current QSO context (calls, reports, stage, noise floor)
pub fn run(
    db_path:    &str,
    capture_id: i64,
    ctx:        &QsoContext,
) -> Result<Vec<SecondPassResult>> {
    let conn = Connection::open(db_path)?;

    // Load all analysis pings for this capture window
    let mut stmt = conn.prepare(
        "SELECT detected_at, df_hz, ccf_ratio, mean_confidence,
                validity_score, COALESCE(raw_decode,''), char_probs
         FROM analysis_pings
         WHERE capture_id = ?1
           AND char_probs IS NOT NULL
           AND length(char_probs) >= 192  -- at least 1 char × 48 floats × 4 bytes
         ORDER BY mean_confidence DESC"
    )?;

    let rows: Vec<AnalysisRow> = stmt.query_map([capture_id], |row| {
        Ok(AnalysisRow {
            detected_at:     row.get(0)?,
            df_hz:           row.get::<_, f32>(1).unwrap_or(0.0),
            ccf_ratio:       row.get::<_, f32>(2).unwrap_or(0.0),
            mean_confidence: row.get::<_, f32>(3).unwrap_or(0.0),
            validity_score:  row.get::<_, i32>(4).unwrap_or(0),
            raw_decode:      row.get(5)?,
            char_probs:      row.get::<_, Vec<u8>>(6)?,
        })
    })?.filter_map(|r| r.ok()).collect();

    log::info!("[2PASS] capture_id={} loaded {} pings, stage={:?}",
        capture_id, rows.len(), ctx.stage);

    if rows.is_empty() {
        return Ok(vec![]);
    }

    // Deduplicate: same (detected_at, ccf_ratio) = same physical ping saved
    // in multiple overlapping 30s windows. Keep highest mean_confidence copy.
    let mut seen: std::collections::HashMap<String, f32> = std::collections::HashMap::new();
    let unique_rows: Vec<&AnalysisRow> = rows.iter().filter(|r| {
        let key = format!("{:.1}_{:.0}", r.df_hz, r.ccf_ratio);
        if let Some(&prev_conf) = seen.get(&key) {
            if r.mean_confidence > prev_conf {
                seen.insert(key, r.mean_confidence);
                true
            } else {
                false
            }
        } else {
            seen.insert(key, r.mean_confidence);
            true
        }
    }).collect();

    log::info!("[2PASS] {} unique pings after deduplication", unique_rows.len());

    // Generate hypotheses for current QSO state
    let hypotheses = generate_hypotheses(ctx);
    log::info!("[2PASS] {} hypotheses: {:?}", hypotheses.len(),
        hypotheses.iter().take(3).collect::<Vec<_>>());

    let mut results: Vec<SecondPassResult> = Vec::new();

    for row in &unique_rows {
        // Primary decoder already produced a high-quality decode — skip.
        // Second pass is only for pings the primary decoder couldn't confirm.
        // validity_score >= 70 means a recognisable callsign was decoded.
        if row.validity_score >= 70 { continue; }

        let mut best_confirmed = 0usize;
        let mut best_result: Option<SecondPassResult> = None;

        for hyp in &hypotheses {
            let (n_confirmed, n_checked, chars, _offset) =
                score_blob(&row.char_probs, hyp, ctx.noise_floor);

            if n_confirmed >= MIN_CONFIRMED && n_confirmed > best_confirmed {
                best_confirmed = n_confirmed;
                best_result = Some(SecondPassResult {
                    detected_at:     row.detected_at.clone(),
                    df_hz:           row.df_hz,
                    ccf_ratio:       row.ccf_ratio,
                    mean_confidence: row.mean_confidence,
                    hypothesis:      hyp.trim().to_string(),
                    n_confirmed,
                    n_checked,
                    confirmed_chars: chars,
                    raw_decode:      row.raw_decode.clone(),
                });
            }
        }

        if let Some(r) = best_result {
            log::info!("[2PASS] Recovered: conf={:.3} ccf={:.1} hyp='{}' n_confirmed={}/{}",
                r.mean_confidence, r.ccf_ratio, r.hypothesis, r.n_confirmed, r.n_checked);
            results.push(r);
        }
    }

    // Sort: most confirmed characters first, then by confidence
    results.sort_by(|a, b| {
        b.n_confirmed.cmp(&a.n_confirmed)
            .then(b.mean_confidence.partial_cmp(&a.mean_confidence).unwrap())
    });

    log::info!("[2PASS] Complete: {} recoveries from {} unique pings",
        results.len(), unique_rows.len());

    // Delete the capture from DB unless the operator has enabled Save Max Data,
    // in which case analysis_pings is retained for off-line analysis.
    if !ctx.retain_after_run {
        let deleted = conn.execute(
            "DELETE FROM analysis_pings WHERE capture_id = ?1",
            [capture_id],
        );
        match deleted {
            Ok(n) => log::info!("[2PASS] Deleted capture_id={} ({} rows)", capture_id, n),
            Err(e) => log::warn!("[2PASS] Failed to delete capture_id={}: {}", capture_id, e),
        }
    } else {
        log::info!("[2PASS] Save Max Data enabled — retaining capture_id={}", capture_id);
    }

    Ok(results)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn char_slot_spot_checks() {
        assert_eq!(char_to_slot('4'), Some(4));
        assert_eq!(char_to_slot('S'), Some(35));
        assert_eq!(char_to_slot('M'), Some(29));
        assert_eq!(char_to_slot('J'), Some(26));
        assert_eq!(char_to_slot('C'), Some(19));
        assert_eq!(char_to_slot('Q'), Some(33));
        assert_eq!(char_to_slot('Y'), Some(41));
        assert_eq!(char_to_slot(' '), Some(15));
        assert_eq!(char_to_slot('0'), Some(0));
        assert_eq!(char_to_slot('9'), Some(9));
        assert_eq!(char_to_slot('A'), Some(17));
        assert_eq!(char_to_slot('Z'), Some(42));
        assert_eq!(char_to_slot('+'), Some(10));
        assert_eq!(char_to_slot('$'), Some(16));
        assert_eq!(char_to_slot('!'), None);  // Not in FSK441 charset
    }

    #[test]
    fn char_slot_case_insensitive() {
        assert_eq!(char_to_slot('s'), char_to_slot('S'));
        assert_eq!(char_to_slot('m'), char_to_slot('M'));
    }

    #[test]
    fn hypotheses_idle_with_call() {
        let ctx = QsoContext {
            my_call:     "GW4WND".into(),
            their_call:  Some("SM4SJY".into()),
            their_loc:   Some("IO82".into()),
            report_sent: None,
            report_rcvd: None,
            stage:       QsoStage::Idle,
            noise_floor: 0.557,
        };
        let h = generate_hypotheses(&ctx);
        assert!(!h.is_empty());
        assert!(h.iter().any(|s| s.contains("SM4SJY")));
        assert!(h.iter().any(|s| s.contains("CQ")));
        // No duplicates
        let mut seen = std::collections::HashSet::new();
        for hyp in &h { assert!(seen.insert(hyp.clone()), "Duplicate: {}", hyp); }
    }

    #[test]
    fn hypotheses_sent_report_specific() {
        let ctx = QsoContext {
            my_call:     "GW4WND".into(),
            their_call:  Some("SM4SJY".into()),
            their_loc:   None,
            report_sent: Some("26".into()),
            report_rcvd: None,
            stage:       QsoStage::SentReport,
            noise_floor: 0.557,
        };
        let h = generate_hypotheses(&ctx);
        // Should include R26 response hypothesis
        assert!(h.iter().any(|s| s.contains("R26")),
            "Expected R26 hypothesis, got: {:?}", h);
    }

    #[test]
    fn score_blob_empty() {
        let (n, _, _, _) = score_blob(&[], "CQ SM4SJY", 0.55);
        assert_eq!(n, 0);
    }

    #[test]
    fn score_blob_all_zeros_gives_zero_confirmed() {
        // A zero blob should never confirm anything
        let blob = vec![0u8; 22 * 48 * 4];
        let (n, _, _, _) = score_blob(&blob, "CQ SM4SJY IO82", 0.55);
        assert_eq!(n, 0, "Zero blob should confirm nothing");
    }

    #[test]
    fn score_blob_synthetic_signal() {
        // Build a synthetic blob with known energy above noise floor
        // for the message "SM4SJY" at positions 0-5
        let n_chars = 6;
        let mut blob = vec![0u8; n_chars * 48 * 4];
        let message = "SM4SJY";
        for (i, c) in message.chars().enumerate() {
            if let Some(slot) = char_to_slot(c) {
                let offset = i * 48 * 4 + slot * 4;
                let energy: f32 = 0.80;
                blob[offset..offset+4].copy_from_slice(&energy.to_le_bytes());
            }
        }
        // Score against a hypothesis containing SM4SJY
        let (n, checked, chars, _) = score_blob(&blob, "CQ SM4SJY IO82", 0.55);
        assert!(n >= 4, "Expected ≥4 confirmed, got {}", n);
        assert!(checked > 0);
        for (c, e) in &chars {
            assert!(*e >= 0.55, "Energy {:.3} below floor for char '{}'", e, c);
        }
    }
}
