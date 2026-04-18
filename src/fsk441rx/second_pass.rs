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
    /// When true, load ALL analysis_pings regardless of capture_id —
    /// accumulates evidence across all RX periods since QSO start.
    /// When false (Idle), only the current capture window is scored.
    pub qso_active: bool,
    /// RFC3339 timestamp of when the current QSO started (SET pressed / first TX).
    /// Used to filter analysis_pings to only this QSO's data.
    /// None = no filter (load all).
    pub qso_started_at: Option<String>,
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

                // We sent TX2 (report) — expect their R+report or RRR.
                // Their report is independent of ours — cover all standard values.
                QsoStage::SentReport => {
                    for rpt in REPORTS {
                        h.push(format!("{} {} R{} ", my, their, rpt));
                        h.push(format!("{} {} R{} ", their, my, rpt));
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

// ─── Accumulated probability vector ───────────────────────────────────────────

/// Per-position accumulated char_probs summed across multiple pings.
/// Shape: [n_chars][48] — values are SUM of probabilities across all pings,
/// NOT normalised. Larger values = more total energy received at that slot.
struct AccumulatedProbs {
    /// Summed probabilities: n_chars × 48 f32 values
    data:    Vec<f32>,
    /// Number of character positions accumulated
    n_chars: usize,
    /// Number of pings summed into this accumulation
    n_pings: usize,
}

impl AccumulatedProbs {
    fn new(n_chars: usize) -> Self {
        Self { data: vec![0.0; n_chars * 48], n_chars, n_pings: 0 }
    }

    /// Add one ping's char_probs blob (raw bytes, n × 48 × 4) into accumulation.
    /// Blobs shorter than n_chars are zero-extended; longer blobs are truncated.
    fn accumulate(&mut self, blob: &[u8]) {
        let n_from_blob = blob.len() / (48 * 4);
        let n = n_from_blob.min(self.n_chars);
        for i in 0..n {
            for slot in 0..48 {
                let byte_off = i * 48 * 4 + slot * 4;
                if byte_off + 4 > blob.len() { break; }
                let v = f32::from_le_bytes([
                    blob[byte_off], blob[byte_off+1],
                    blob[byte_off+2], blob[byte_off+3],
                ]);
                self.data[i * 48 + slot] += v;
            }
        }
        self.n_pings += 1;
    }

    /// Read accumulated (summed) probability for character position i, slot s.
    fn get(&self, i: usize, slot: usize) -> f32 {
        if i >= self.n_chars || slot >= 48 { return 0.0; }
        self.data[i * 48 + slot]
    }

    /// Mean accumulated probability across all slots at position i.
    /// Used as the noise reference: a slot well above this is carrying signal.
    fn mean_at(&self, i: usize) -> f32 {
        if i >= self.n_chars { return 0.0; }
        let base = i * 48;
        self.data[base..base+48].iter().sum::<f32>() / 48.0
    }
}

// ─── Accumulated blob scoring ──────────────────────────────────────────────────

/// Score an AccumulatedProbs vector against one hypothesis message.
///
/// Two-level gate:
///   1. Character gate: accumulated energy at hypothesis[i]'s slot must exceed
///      `signal_floor` × n_pings (i.e. average energy per ping above threshold).
///      This is energy derived from received signal — never invented.
///
///   2. Space constraint (trellis framing): the space character (slot 15) acts
///      as a sync signal in FSK441. At space positions in the hypothesis the
///      space slot should be elevated; at non-space positions it should not
///      dominate. This validates alignment independently of character identity.
///
/// Tries all alignment offsets of the repeating hypothesis and returns the
/// best-scoring one.
///
/// Returns (n_confirmed, n_checked, confirmed_chars, space_score, best_offset).
fn score_accumulated(
    acc:          &AccumulatedProbs,
    hypothesis:   &str,
    signal_floor: f32,  // per-ping threshold (e.g. adaptive_conf_threshold)
) -> (usize, usize, Vec<(char, f32)>, f32, usize) {
    if acc.n_chars == 0 || acc.n_pings == 0 {
        return (0, 0, vec![], 0.0, 0);
    }

    // The effective gate scales with number of pings accumulated:
    // if signal_floor=0.51 and we have 10 pings, a character needs
    // total accumulated energy > 0.51 × 10 = 5.1 to be confirmed.
    let effective_floor = signal_floor * acc.n_pings as f32;

    // Space slot index
    const SPACE_SLOT: usize = 15;

    // Repeat hypothesis 3× to cover any phase alignment
    let rep = format!("{}{}{}", hypothesis, hypothesis, hypothesis);
    let rep_chars: Vec<char> = rep.chars().collect();
    let hyp_len = hypothesis.chars().count();

    let mut best_confirmed = 0usize;
    let mut best_checked   = 0usize;
    let mut best_chars:    Vec<(char, f32)> = Vec::new();
    let mut best_space_score = 0.0f32;
    let mut best_offset    = 0usize;

    for offset in 0..hyp_len {
        let mut confirmed = 0usize;
        let mut checked   = 0usize;
        let mut chars: Vec<(char, f32)> = Vec::new();
        let mut space_score = 0.0f32;
        let mut space_checked = 0usize;

        for i in 0..acc.n_chars {
            let t_idx = offset + i;
            if t_idx >= rep_chars.len() { break; }
            let c = rep_chars[t_idx];

            let slot = match char_to_slot(c) {
                Some(s) => s,
                None    => continue,
            };

            let energy = acc.get(i, slot);

            // ── Space constraint (trellis framing) ────────────────────────
            // At each position, the space slot energy tells us whether this
            // position carries a space character. This is independent of what
            // character we're predicting — it validates the hypothesis framing.
            let space_energy = acc.get(i, SPACE_SLOT);
            let mean_energy  = acc.mean_at(i);
            if mean_energy > 0.0 {
                let space_relative = space_energy / (mean_energy * 48.0);
                if c == ' ' {
                    // Space expected: reward if space slot is elevated
                    space_score += space_relative;
                } else {
                    // Non-space expected: reward if space slot is suppressed
                    space_score += 1.0 - space_relative;
                }
                space_checked += 1;
            }

            // ── Character gate ─────────────────────────────────────────────
            // Only accept if accumulated energy exceeds scaled floor.
            // This is received signal energy — never invented.
            checked += 1;
            if energy >= effective_floor {
                confirmed += 1;
                // Report mean energy per ping for display
                chars.push((c, energy / acc.n_pings as f32));
            }
        }

        // Normalise space score to [0,1]
        let norm_space = if space_checked > 0 {
            space_score / space_checked as f32
        } else { 0.0 };

        // Primary sort: confirmed characters (real energy)
        // Secondary: space framing score (alignment validation)
        if confirmed > best_confirmed
            || (confirmed == best_confirmed && norm_space > best_space_score)
        {
            best_confirmed   = confirmed;
            best_checked     = checked;
            best_chars       = chars;
            best_space_score = norm_space;
            best_offset      = offset;
        }
    }

    (best_confirmed, best_checked, best_chars, best_space_score, best_offset)
}

/// Legacy single-blob scorer — kept for tests and fallback use.
fn score_blob(
    blob:        &[u8],
    hypothesis:  &str,
    noise_floor: f32,
) -> (usize, usize, Vec<(char, f32)>, usize) {
    let n_pos = blob.len() / (48 * 4);
    if n_pos == 0 { return (0, 0, vec![], 0); }
    let mut acc = AccumulatedProbs::new(n_pos);
    acc.accumulate(blob);
    let (nc, nch, chars, _, off) = score_accumulated(&acc, hypothesis, noise_floor);
    (nc, nch, chars, off)
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
///
/// KEY IMPROVEMENT over single-ping scoring:
/// Pings are grouped by DF bucket (43Hz resolution). Within each bucket,
/// char_probs are ACCUMULATED (summed) across all pings. This means a
/// character appearing at probability 0.55 across 8 independent pings
/// accumulates to 4.4 total energy — well above the effective floor of
/// 0.51 × 8 = 4.08. Uncorrelated noise averages out across pings.
///
/// The space constraint validates hypothesis framing: the space character
/// (slot 15) acts as a sync signal. Its energy distribution across positions
/// tells us where word boundaries fall, independently of character identity.
pub fn run(
    db_path:    &str,
    capture_id: i64,
    ctx:        &QsoContext,
) -> Result<Vec<SecondPassResult>> {
    let conn = Connection::open(db_path)?;

    // During an active QSO, load ALL analysis_pings from this QSO only —
    // filtered by captured_at >= qso_started_at so previous QSOs in the
    // same session don't pollute the accumulation.
    // Outside QSO (Idle), only score the current capture window.
    let mut stmt = if ctx.qso_active {
        match &ctx.qso_started_at {
            Some(started) => conn.prepare(&format!(
                "SELECT detected_at, df_hz, ccf_ratio, mean_confidence,
                        validity_score, COALESCE(raw_decode,''), char_probs
                 FROM analysis_pings
                 WHERE char_probs IS NOT NULL
                   AND length(char_probs) >= 192
                   AND captured_at >= '{}'
                 ORDER BY mean_confidence DESC",
                started
            ))?,
            None => conn.prepare(
                "SELECT detected_at, df_hz, ccf_ratio, mean_confidence,
                        validity_score, COALESCE(raw_decode,''), char_probs
                 FROM analysis_pings
                 WHERE char_probs IS NOT NULL
                   AND length(char_probs) >= 192
                 ORDER BY mean_confidence DESC"
            )?,
        }
    } else {
        conn.prepare(
            "SELECT detected_at, df_hz, ccf_ratio, mean_confidence,
                    validity_score, COALESCE(raw_decode,''), char_probs
             FROM analysis_pings
             WHERE capture_id = ?1
               AND char_probs IS NOT NULL
               AND length(char_probs) >= 192
             ORDER BY mean_confidence DESC"
        )?
    };

    let rows: Vec<AnalysisRow> = if ctx.qso_active {
        stmt.query_map([], |row| {
            Ok(AnalysisRow {
                detected_at:     row.get(0)?,
                df_hz:           row.get::<_, f32>(1).unwrap_or(0.0),
                ccf_ratio:       row.get::<_, f32>(2).unwrap_or(0.0),
                mean_confidence: row.get::<_, f32>(3).unwrap_or(0.0),
                validity_score:  row.get::<_, i32>(4).unwrap_or(0),
                raw_decode:      row.get(5)?,
                char_probs:      row.get::<_, Vec<u8>>(6)?,
            })
        })?.filter_map(|r| r.ok()).collect()
    } else {
        stmt.query_map([capture_id], |row| {
            Ok(AnalysisRow {
                detected_at:     row.get(0)?,
                df_hz:           row.get::<_, f32>(1).unwrap_or(0.0),
                ccf_ratio:       row.get::<_, f32>(2).unwrap_or(0.0),
                mean_confidence: row.get::<_, f32>(3).unwrap_or(0.0),
                validity_score:  row.get::<_, i32>(4).unwrap_or(0),
                raw_decode:      row.get(5)?,
                char_probs:      row.get::<_, Vec<u8>>(6)?,
            })
        })?.filter_map(|r| r.ok()).collect()
    };

    log::info!("[2PASS] capture_id={} qso_active={} loaded {} pings, stage={:?}",
        capture_id, ctx.qso_active, rows.len(), ctx.stage);

    if rows.is_empty() {
        return Ok(vec![]);
    }

    // ── Group pings by DF bucket (43Hz resolution) ────────────────────────
    // Pings at the same DF are from the same station/trail. Accumulating
    // their char_probs reinforces the signal while averaging out noise.
    let mut df_groups: std::collections::HashMap<i32, Vec<&AnalysisRow>> =
        std::collections::HashMap::new();
    for row in &rows {
        let bucket = (row.df_hz / 43.0).round() as i32;
        df_groups.entry(bucket).or_default().push(row);
    }

    log::info!("[2PASS] {} distinct DF buckets", df_groups.len());

    let hypotheses = generate_hypotheses(ctx);
    log::info!("[2PASS] {} hypotheses for stage={:?}", hypotheses.len(), ctx.stage);

    let mut results: Vec<SecondPassResult> = Vec::new();

    for (bucket, group) in &df_groups {
        if group.is_empty() { continue; }

        // ── Accumulate char_probs across all pings in this DF bucket ──────
        let max_chars = group.iter()
            .map(|r| r.char_probs.len() / (48 * 4))
            .max()
            .unwrap_or(0);
        if max_chars == 0 { continue; }

        let mut acc = AccumulatedProbs::new(max_chars);
        let mut best_confidence = 0.0f32;
        let mut best_ccf = 0.0f32;
        let mut best_detected_at = String::new();
        let mut best_raw = String::new();

        for row in group {
            // Skip pings already well-decoded by primary decoder
            if row.validity_score >= 70 { continue; }
            acc.accumulate(&row.char_probs);
            if row.mean_confidence > best_confidence {
                best_confidence  = row.mean_confidence;
                best_ccf         = row.ccf_ratio;
                best_detected_at = row.detected_at.clone();
                best_raw         = row.raw_decode.clone();
            }
        }

        if acc.n_pings == 0 { continue; }

        log::debug!("[2PASS] DF={:+}Hz: {} pings accumulated, max_chars={}",
            bucket * 43, acc.n_pings, max_chars);

        // ── Score accumulated vector against all hypotheses ───────────────
        let mut best_confirmed = 0usize;
        let mut best_result: Option<SecondPassResult> = None;

        for hyp in &hypotheses {
            let (n_confirmed, n_checked, chars, space_score, _offset) =
                score_accumulated(&acc, hyp, ctx.noise_floor);

            // Require confirmed characters from received energy AND
            // reasonable space framing (>0.55 = spaces broadly agree with hypothesis)
            if n_confirmed >= MIN_CONFIRMED
                && n_confirmed > best_confirmed
                && space_score >= 0.55
            {
                best_confirmed = n_confirmed;
                best_result = Some(SecondPassResult {
                    detected_at:     best_detected_at.clone(),
                    df_hz:           *bucket as f32 * 43.0,
                    ccf_ratio:       best_ccf,
                    mean_confidence: best_confidence,
                    hypothesis:      hyp.trim().to_string(),
                    n_confirmed,
                    n_checked,
                    confirmed_chars: chars,
                    raw_decode:      best_raw.clone(),
                });
            }
        }

        if let Some(r) = best_result {
            log::info!(
                "[2PASS] DF={:+.0}Hz n_pings={} hyp='{}' confirmed={}/{} space_ok",
                r.df_hz, acc.n_pings, r.hypothesis, r.n_confirmed, r.n_checked
            );
            results.push(r);
        }
    }

    results.sort_by(|a, b| {
        b.n_confirmed.cmp(&a.n_confirmed)
            .then(b.mean_confidence.partial_cmp(&a.mean_confidence).unwrap())
    });

    log::info!("[2PASS] Complete: {} recoveries from {} DF buckets ({} total pings)",
        results.len(), df_groups.len(), rows.len());

    // During an active QSO: retain analysis_pings so next period can accumulate.
    // At QSO end (qso_active=false): delete only this capture window.
    // The app-level Clear handler deletes all pings from the QSO period.
    if !ctx.qso_active && !ctx.retain_after_run {
        let deleted = conn.execute(
            "DELETE FROM analysis_pings WHERE capture_id = ?1",
            [capture_id],
        );
        match deleted {
            Ok(n) => log::info!("[2PASS] Deleted capture_id={} ({} rows)", capture_id, n),
            Err(e) => log::warn!("[2PASS] Failed to delete capture_id={}: {}", capture_id, e),
        }
    } else if ctx.qso_active {
        log::info!("[2PASS] QSO active — retaining analysis_pings for cross-period accumulation");
    } else {
        log::info!("[2PASS] Save Max Data — retaining capture_id={}", capture_id);
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
            my_call:        "GW4WND".into(),
            their_call:     Some("SM4SJY".into()),
            their_loc:      Some("IO82".into()),
            report_sent:    None,
            report_rcvd:    None,
            stage:          QsoStage::Idle,
            noise_floor:    0.557,
            retain_after_run: false,
            qso_active:     false,
            qso_started_at: None,
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
            my_call:        "GW4WND".into(),
            their_call:     Some("SM4SJY".into()),
            their_loc:      None,
            report_sent:    Some("26".into()),
            report_rcvd:    None,
            stage:          QsoStage::SentReport,
            noise_floor:    0.557,
            retain_after_run: false,
            qso_active:     false,
            qso_started_at: None,
        };
        let h = generate_hypotheses(&ctx);
        // R26 must appear — it's in REPORTS, covered regardless of what report_sent is
        assert!(h.iter().any(|s| s.contains("R26")),
            "Expected R26 hypothesis, got: {:?}", h);
        // R28 must also appear — their report is independent of ours
        assert!(h.iter().any(|s| s.contains("R28")),
            "Expected R28 hypothesis (their report independent of ours), got: {:?}", h);
    }

    #[test]
    fn score_blob_empty() {
        let (n, _, _, _) = score_blob(&[], "CQ SM4SJY", 0.55);
        assert_eq!(n, 0);
    }

    #[test]
    fn score_blob_all_zeros_gives_zero_confirmed() {
        let blob = vec![0u8; 22 * 48 * 4];
        let (n, _, _, _) = score_blob(&blob, "CQ SM4SJY IO82", 0.55);
        assert_eq!(n, 0, "Zero blob should confirm nothing");
    }

    #[test]
    fn accumulated_probs_sums_correctly() {
        let mut acc = AccumulatedProbs::new(4);
        let mut blob = vec![0u8; 4 * 48 * 4];
        let e: f32 = 0.6;
        blob[0..4].copy_from_slice(&e.to_le_bytes());
        acc.accumulate(&blob);
        acc.accumulate(&blob);
        assert_eq!(acc.n_pings, 2);
        let total = acc.get(0, 0);
        assert!((total - 1.2).abs() < 1e-5, "Expected 1.2, got {}", total);
    }

    #[test]
    fn accumulation_confirms_weak_signal() {
        // Signal per ping at 0.53 — just above floor 0.51
        // After 5 pings: accumulated = 2.65 > effective_floor 0.51×5 = 2.55
        let floor = 0.51f32;
        let signal = 0.53f32;
        let message = "GW4WND";
        let n_chars = message.len();
        let noise: f32 = (1.0 - signal) / 47.0;
        let mut blob = vec![0u8; n_chars * 48 * 4];
        for (i, c) in message.chars().enumerate() {
            let slot = char_to_slot(c).unwrap();
            for s in 0..48 {
                let v: f32 = if s == slot { signal } else { noise };
                let off = i * 48 * 4 + s * 4;
                blob[off..off+4].copy_from_slice(&v.to_le_bytes());
            }
        }
        let mut acc = AccumulatedProbs::new(n_chars);
        for _ in 0..5 { acc.accumulate(&blob); }
        let (n, _, _, _, _) = score_accumulated(&acc, message, floor);
        assert_eq!(n, n_chars, "Accumulated should confirm all {} chars", n_chars);
    }

    #[test]
    fn space_constraint_validates_framing() {
        let message = "SM4SJY GW4WND";
        let n_chars = message.len();
        let mut blob = vec![0u8; n_chars * 48 * 4];
        let noise: f32 = 0.01;
        let signal: f32 = 0.70;
        for (i, c) in message.chars().enumerate() {
            let slot = char_to_slot(c).unwrap();
            for s in 0..48 {
                let off = i * 48 * 4 + s * 4;
                let v = if s == slot { signal } else { noise };
                blob[off..off+4].copy_from_slice(&v.to_le_bytes());
            }
        }
        let mut acc = AccumulatedProbs::new(n_chars);
        acc.accumulate(&blob);
        let (_, _, _, space_score, _) = score_accumulated(&acc, message, 0.51);
        assert!(space_score > 0.5,
            "Space framing should score >0.5 for correct hypothesis, got {}", space_score);
    }
}
