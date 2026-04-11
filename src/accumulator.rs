#![allow(dead_code)]
// src/fsk441rx/accumulator.rs
//
// Multi-burst soft-decision accumulator for FSK441.
//
// Design mirrors MSK2K's accumulator.rs but adapted for FSK441's character
// ═══════════════════════════════════════════════════════════════════════════
// ACCUMULATOR — what it is and what it does
// ═══════════════════════════════════════════════════════════════════════════
//
// PROBLEM: A 30-second FSK441 slot may contain several SHORT meteor pings,
// each too brief to decode alone. A ping lasting 50ms might decode only
// 5 chars of a 14-char message, at uncertain confidence. But multiple pings
// from the same station, arriving at the same DF (Doppler frequency), all
// carry fragments of the SAME repeating message. If we could combine their
// soft-bit evidence, we'd get a much cleaner decode.
//
// SOLUTION: The accumulator collects ALL pings from the same DF bin within
// a slot and does a WEIGHTED SUM of their char_probs arrays:
//   - Each ping contributes its probability distribution over 48 chars,
//     at each message position, weighted by its CCF² (energy squared)
//   - Pings are phase-aligned: a short ping saying "CEW R37" at offset 3
//     aligns with "SM2CEW R37 GW" from another ping
//   - After summing N pings, the correct character has probability N×p
//     while random noise chars each get ~N×(1/48) — SNR improves √N
//
// WHEN IT FIRES: Once ≥2 fragments in the same DF bin have been collected
// (even partially), process() runs and may emit an AccumulatedDecode with
// a ★ star marker in the UI. This happens even if individual pings were
// below the display threshold.
//
// QSO CONSTRAINT: When Their Call is Set, the constraint steers decodes
// toward chars that are valid in a QSO message (CALL CALL RST etc.).
// Boost ×2.5 for valid chars, suppress ×0.15 for impossible chars.
// This narrows the alphabet from 48 to ~18 chars per position.
//
// RING BUFFER: All pings with conf≥0.45 are stored in an in-memory ring
// buffer (last 300 pings) with their full char_probs. Press "💾 Save 30s"
// to commit to analysis_pings table for offline constraint simulation.
// ═══════════════════════════════════════════════════════════════════════════
//
// structure. Instead of 258-bit LLR packets, we work with char_probs:
// Vec<[f32; 48]> — a 48-element probability distribution per character.
//
// Algorithm:
//   1. Collect all ping fragments (char_probs + CCF weight) from one RX slot.
//   2. Find best pairwise phase alignment between fragments using a sliding
//      dot-product score on the probability vectors (soft SW alignment).
//   3. Mask out low-confidence positions (where the ping was silent).
//   4. Weighted-sum char_probs at each aligned message position.
//   5. Hard-decode accumulated probs → characters.
//   6. Run through existing filter/geo pipeline.
//
// For A/B comparison: SinglePing decode (standard path) and Accumulated decode
// (new path) are both emitted — the UI shows them in different colours.

use crate::demod::DemodResult;
use crate::params::CHARSET;


// ─── QSO Context Constraint ───────────────────────────────────────────────────
// During an active QSO (TX1+), we know both callsigns and the valid message set.
// This lets us restrict each accumulator output position to a subset of the 48
// FSK441 characters, boosting the correct char and suppressing noise chars.
//
// The constraint is SOFT: valid chars get ×BOOST, invalid get ×SUPPRESS.
// If the signal strongly disagrees with the constraint, it still wins.
// Applied only when both calls are known (their_call set, my_call known).

const CONSTRAINT_BOOST:    f32 = 2.5;   // multiply valid-char probs by this
const CONSTRAINT_SUPPRESS: f32 = 0.15;  // multiply invalid-char probs by this

/// Per-position valid character index sets for a specific QSO exchange.
/// Position i contains the union of valid char indices across all possible
/// QSO messages (TX1-TX5) at all possible phase offsets.
#[derive(Debug, Clone)]
pub struct QsoConstraint {
    /// valid_chars[i] = sorted list of valid char indices at position i
    pub valid_chars: Vec<Vec<usize>>,
}

impl QsoConstraint {
    /// Build constraint from the two callsigns. `max_len` should match
    /// the accumulator's max message length (typ. 22).
    pub fn build(my_call: &str, their_call: &str, max_len: usize) -> Self {
        use crate::params::CHARSET;

        // Map char → first index in CHARSET
        let mut char_to_idx = std::collections::HashMap::new();
        for (i, &b) in CHARSET.iter().enumerate() {
            char_to_idx.entry(b as char).or_insert(i);
        }

        let mc = my_call.trim().to_uppercase();
        let tc = their_call.trim().to_uppercase();

        // All messages that could arrive from the other station
        let mut templates: Vec<String> = Vec::new();
        templates.push(format!("CQ {} ", tc));
        templates.push(format!("{} {}", tc, mc));
        for r in &["26","27","28","29","56","57","58","59"] {
            templates.push(format!("{} {} {}", tc, mc, r));
            templates.push(format!("{} {} R{}", tc, mc, r));
            templates.push(format!("{} {} R{} R{}", tc, mc, r, r));
        }
        templates.push(format!("{} {} RRR RRR", tc, mc));
        templates.push(format!("{} {} RRR", tc, mc));
        templates.push(format!("{} {} 73", tc, mc));

        // Union of valid chars at each position across all templates × all phases
        let mut valid: Vec<std::collections::HashSet<usize>> = vec![
            std::collections::HashSet::new(); max_len
        ];

        for tmpl in &templates {
            let chars: Vec<char> = tmpl.chars().collect();
            let tlen = chars.len();
            for phase in 0..tlen {
                for pos in 0..max_len {
                    let ch = chars[(phase + pos) % tlen];
                    if let Some(&idx) = char_to_idx.get(&ch) {
                        valid[pos].insert(idx);
                    }
                }
            }
        }

        Self {
            valid_chars: valid.into_iter().map(|s| {
                let mut v: Vec<usize> = s.into_iter().collect();
                v.sort_unstable();
                v
            }).collect(),
        }
    }

    /// Apply soft constraint to accumulated probs at position `pos`.
    /// Boosts valid chars, suppresses invalid. Does NOT renormalise —
    /// that happens in the hard-decode step.
    #[inline]
    pub fn apply(&self, pos: usize, probs: &mut [f32; 48]) {
        let valid = &self.valid_chars[pos.min(self.valid_chars.len() - 1)];
        if valid.len() >= 47 { return; } // no constraint available
        // Use binary search on the sorted valid list — O(log n) per char
        for k in 0..48usize {
            let is_valid = valid.binary_search(&k).is_ok();
            probs[k] *= if is_valid { CONSTRAINT_BOOST } else { CONSTRAINT_SUPPRESS };
        }
    }
}

/// One fragment from a single decoded ping
#[derive(Debug, Clone)]
pub struct Fragment {
    pub char_probs: Vec<[f32; 48]>,
    pub char_conf:  Vec<f32>,
    pub ccf_weight: f32,
    pub df_hz:      f32,
    pub timestamp:  std::time::Instant,
}

impl Fragment {
    pub fn from_result(result: &DemodResult, ccf_ratio: f32) -> Option<Self> {
        if result.char_probs.is_empty() { return None; }
        let char_conf: Vec<f32> = result.char_probs.iter()
            .map(|p| p.iter().cloned().fold(0.0f32, f32::max))
            .collect();
        Some(Self {
            char_probs: result.char_probs.clone(),
            char_conf,
            ccf_weight: ccf_ratio,
            df_hz: result.df_hz,
            timestamp: std::time::Instant::now(),
        })
    }

    pub fn len(&self) -> usize { self.char_probs.len() }

    /// Dot-product similarity between position i in self and position j in other.
    /// High value = both fragments agree on the same character distribution.
    pub fn similarity(&self, i: usize, other: &Fragment, j: usize) -> f32 {
        let a = &self.char_probs[i];
        let b = &other.char_probs[j];
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }
}

/// Result of accumulation — the merged soft decode
#[derive(Debug, Clone)]
pub struct AccumulatedDecode {
    pub text:           String,
    pub n_fragments:    usize,
    pub mean_conf:      f32,
    pub char_conf:      Vec<f32>,
    #[allow(dead_code)]
    pub char_probs:     Vec<[f32; 48]>,
    /// DF bin this came from — used to update existing row in place
    pub df_bin_hz:      i32,
}

/// Phase alignment between two fragments using sliding window soft alignment.
/// Returns the offset (in characters) to shift `other` relative to `anchor`
/// so their overlapping regions have maximum probability agreement.
/// Equivalent to the virtual_hist phase search in MSK2K accumulator.
fn find_alignment(anchor: &Fragment, other: &Fragment, max_shift: usize) -> (i32, f32) {
    let na = anchor.len();
    let nb = other.len();
    if na == 0 || nb == 0 { return (0, 0.0); }

    let mut best_score  = f32::NEG_INFINITY;
    let mut best_offset = 0i32;

    // Try all shifts in [-max_shift, +max_shift]
    let max_shift = max_shift.min(na.max(nb));
    for shift in -(max_shift as i32)..=(max_shift as i32) {
        let mut score = 0.0f32;
        let mut count = 0u32;

        for i in 0..na as i32 {
            let j = i - shift;
            if j < 0 || j >= nb as i32 { continue; }
            // Weight by product of confidence — only count positions where
            // both fragments have meaningful signal
            let conf_a = anchor.char_conf[i as usize];
            let conf_b = other.char_conf[j as usize];
            let weight = conf_a * conf_b;
            if weight < 0.01 { continue; }  // both positions must have signal
            score += weight * anchor.similarity(i as usize, other, j as usize);
            count += 1;
        }

        // Normalise by overlap length so shorter overlaps don't dominate
        if count > 2 {
            let norm_score = score / count as f32;
            if norm_score > best_score {
                best_score  = norm_score;
                best_offset = shift;
            }
        }
    }

    (best_offset, best_score.max(0.0))
}

/// Estimate the repeating message length from character probability entropy.
/// A periodic message will have lower average entropy at the repeat period.
/// Returns estimated message length in characters, or None if unclear.
fn estimate_message_length(fragments: &[Fragment]) -> Option<usize> {
    // Use the longest high-confidence fragment
    let anchor = fragments.iter()
        .max_by(|a, b| {
            let sa = a.char_conf.iter().filter(|&&c| c > 0.4).count();
            let sb = b.char_conf.iter().filter(|&&c| c > 0.4).count();
            sa.cmp(&sb)
        })?;

    let n = anchor.len();
    if n < 4 { return None; }

    // Look for the minimum average cross-entropy at period lengths 4..=n/2
    // A true repeating message will show dip in entropy at the period
    let mut best_period = None;
    let mut best_score  = f32::NEG_INFINITY;

    // Try shorter periods first — FSK441 messages are typically 8-20 chars
    for period in 4..=(n / 2).max(4).min(25) {
        let mut score = 0.0f32;
        let mut count = 0u32;
        for i in 0..(n - period) {
            let dot = anchor.similarity(i, anchor, i + period);
            let ca  = anchor.char_conf[i];
            let cb  = anchor.char_conf[i + period];
            if ca > 0.3 && cb > 0.3 {
                score += dot;
                count += 1;
            }
        }
        if count > 2 {
            let norm = score / count as f32;
            if norm > best_score {
                best_score  = norm;
                best_period = Some(period);
            }
        }
    }

    best_period
}

pub struct FragmentAccumulator {
    fragments:      Vec<Fragment>,
    df_tolerance:   f32,
    conf_threshold: f32,
    their_call:     Option<String>,
    #[allow(dead_code)]
    my_call:        Option<String>,   // excluded from accumulation
    /// QSO context constraint — set when both calls are known (TX1+)
    pub constraint: Option<QsoConstraint>,
}

impl FragmentAccumulator {
    pub fn new() -> Self {
        Self {
            fragments:      Vec::new(),
            df_tolerance:   40.0,
            conf_threshold: 0.15,
            their_call:     None,
            my_call:        None,
            constraint:     None,
        }
    }

    #[allow(dead_code)]
    pub fn set_my_call(&mut self, call: Option<String>) {
        self.my_call = call.map(|c| c.to_uppercase());
    }

    #[allow(dead_code)]
    pub fn set_their_call(&mut self, call: Option<String>) {
        self.their_call = call.map(|c| c.to_uppercase());
    }

    /// Set QSO context constraint from known callsigns.
    /// Call when QSO enters TX1+ state with both calls known.
    pub fn set_constraint(&mut self, my_call: &str, their_call: &str) {
        self.constraint = Some(QsoConstraint::build(my_call, their_call, 22));
        log::info!("[ACCUM] QSO constraint set: {} ↔ {}", my_call, their_call);
    }

    pub fn clear_constraint(&mut self) {
        self.constraint = None;
        log::debug!("[ACCUM] QSO constraint cleared");
    }


    pub fn add(&mut self, frag: Fragment) {
        // If we know who we're working, only keep fragments containing that callsign
        if let Some(ref call) = self.their_call {
            // Check if any high-confidence character sequence in the fragment suggests this call
            // Use a loose check: if the fragment has low max confidence, admit it anyway
            // (weak signals won't have the full call but may still be useful)
            let max_conf = frag.char_conf.iter().cloned().fold(0.0f32, f32::max);
            if max_conf > 0.6 {
                // High confidence fragment — check it contains expected callsign chars
                // Build best-guess hard decode of this fragment
                let hard: String = frag.char_probs.iter().map(|p| {
                    let best = p.iter().enumerate()
                        .max_by(|a,b| a.1.partial_cmp(b.1).unwrap())
                        .map(|(i,_)| i).unwrap_or(0);
                    crate::params::CHARSET.get(best)
                        .and_then(|&b| if b == 0 { None } else { char::from_u32(b as u32) })
                        .unwrap_or('?')
                }).collect();
                // Only reject if confident AND clearly doesn't match
                if !hard.contains(call.as_str()) && !call.chars().all(|c| hard.contains(c)) {
                    log::debug!("[ACCUM] Rejected fragment (not from {}): {}", call, &hard[..hard.len().min(20)]);
                    return;
                }
            }
        }
        self.fragments.push(frag);
        log::debug!("[ACCUM] {} fragments collected", self.fragments.len());
    }

    pub fn clear(&mut self) { self.fragments.clear(); }

    /// Remove fragments older than `secs` seconds — prevents unbounded growth
    pub fn prune_older_than(&mut self, secs: u64) {
        let cutoff = std::time::Duration::from_secs(secs);
        self.fragments.retain(|f| f.timestamp.elapsed() < cutoff);
    }

    pub fn fragment_count(&self) -> usize { self.fragments.len() }

    /// Group fragments by DF (same station) and process each group.
    /// Returns one AccumulatedDecode per DF group that has ≥2 fragments.
    pub fn process(&self) -> Vec<AccumulatedDecode> {
        if self.fragments.is_empty() { return vec![]; }

        // Group by DF: sort by df_hz, cluster within df_tolerance
        let mut sorted = self.fragments.clone();
        sorted.sort_by(|a, b| a.df_hz.partial_cmp(&b.df_hz).unwrap());

        let mut groups: Vec<Vec<&Fragment>> = Vec::new();
        let mut current: Vec<&Fragment> = Vec::new();

        for frag in &sorted {
            if current.is_empty()
                || (frag.df_hz - current[0].df_hz).abs() <= self.df_tolerance
            {
                current.push(frag);
            } else {
                if !current.is_empty() { groups.push(current); }
                current = vec![frag];
            }
        }
        if !current.is_empty() { groups.push(current); }

        groups.into_iter()
            .filter_map(|g| self.accumulate_group(&g))
            .collect()
    }

    fn accumulate_group(&self, group: &[&Fragment]) -> Option<AccumulatedDecode> {
        // Use highest-CCF fragment as alignment anchor
        let anchor = group.iter()
            .max_by(|a, b| a.ccf_weight.partial_cmp(&b.ccf_weight).unwrap())?;
        let df_bin = (anchor.df_hz / 43.0).round() as i32 * 43;

        let msg_len_hint = estimate_message_length(&group.iter().map(|f| (*f).clone()).collect::<Vec<_>>())
            .unwrap_or(anchor.len().min(20));

        let msg_len = msg_len_hint.max(4).min(22);  // FSK441 max practical message length

        // Accumulate: weighted sum of char_probs at each message position
        let mut accum_probs  = vec![[0.0f32; 48]; msg_len];
        let mut accum_weight = vec![0.0f32;       msg_len];

        for frag in group {
            let w = frag.ccf_weight.powi(2);  // CCF² as weight (like MSK2K corr²)

            // Align this fragment to the anchor
            let (shift, align_score) = find_alignment(anchor, frag, msg_len);
            if align_score < 0.001 && std::ptr::eq(*frag, *anchor) == false {
                log::debug!("[ACCUM] Fragment alignment too weak ({:.4}), skipping", align_score);
                // Still include anchor even if alignment score is 0
                if !std::ptr::eq(*frag, *anchor) { continue; }
            }

            log::debug!("[ACCUM] Fragment df={:.0}Hz shift={} score={:.4} weight={:.1}",
                frag.df_hz, shift, align_score, w);

            for i in 0..msg_len as i32 {
                // Position in this fragment after applying shift
                let j = (i + shift).rem_euclid(frag.len() as i32) as usize;
                if j >= frag.len() { continue; }

                // Only use positions where this fragment has signal
                let conf = frag.char_conf[j];
                if conf < self.conf_threshold { continue; }

                let pos_weight = w * conf;
                for k in 0..48 {
                    accum_probs[i as usize][k] += frag.char_probs[j][k] * pos_weight;
                }
                accum_weight[i as usize] += pos_weight;
            }
        }

        // ── QSO context constraint ───────────────────────────────────────────
        // If both calls are known, boost chars that can appear in a valid QSO
        // message and suppress everything else. Applied before normalisation so
        // the hard-decode argmax sees boosted probs. Soft: strong signal wins.
        if let Some(ref constraint) = self.constraint {
            for i in 0..msg_len {
                if accum_weight[i] < 1e-10 { continue; }
                constraint.apply(i, &mut accum_probs[i]);
            }
        }

        // Normalise accumulated probs and hard-decode
        let mut text      = String::with_capacity(msg_len);
        let mut char_conf = Vec::with_capacity(msg_len);
        let mut char_probs_out = Vec::with_capacity(msg_len);
        let mut total_conf = 0.0f32;
        let mut n_decoded  = 0usize;

        for i in 0..msg_len {
            if accum_weight[i] < 1e-10 {
                text.push('?');
                char_conf.push(0.0);
                char_probs_out.push([0.0f32; 48]);
                continue;
            }

            // Normalise
            let _wt = accum_weight[i];
            let mut p = accum_probs[i];
            let tot: f32 = p.iter().sum::<f32>().max(1e-30);
            p.iter_mut().for_each(|x| *x /= tot);

            // Hard decode: argmax
            let (best_idx, best_prob) = p.iter().enumerate()
                .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
                .unwrap_or((0, &0.0));

            // Second best for confidence
            let second = p.iter().enumerate()
                .filter(|(i, _)| *i != best_idx)
                .map(|(_, &v)| v)
                .fold(0.0f32, f32::max);

            let conf = (best_prob - second).clamp(0.0, 1.0);

            let ch = CHARSET.get(best_idx)
                .and_then(|&b| if b == 0 { None } else { char::from_u32(b as u32) })
                .unwrap_or('?');

            text.push(ch);
            char_conf.push(conf);
            char_probs_out.push(p);
            total_conf += conf;
            n_decoded  += 1;
        }

        let text = text.trim().to_string();
        if text.len() < 3 { return None; }

        let mean_conf = if n_decoded > 0 { total_conf / n_decoded as f32 } else { 0.0 };

        // Quality gate: reject low confidence and garbage-heavy results
        let q_marks   = text.chars().filter(|&c| c == '?').count();
        let garbage   = text.chars().filter(|&c| !c.is_alphanumeric() && c != ' ').count();
        let non_space = text.chars().filter(|&c| c != ' ').count().max(1);
        let q_ratio   = q_marks  as f32 / non_space as f32;
        let g_ratio   = garbage  as f32 / non_space as f32;
        if mean_conf < 0.45 {
            log::debug!("[ACCUM] Rejected — mean_conf {:.2} < 0.45", mean_conf);
            return None;
        }
        if q_ratio > 0.35 {
            log::debug!("[ACCUM] Rejected — {:.0}% uncertain chars", q_ratio * 100.0);
            return None;
        }
        if g_ratio > 0.25 {
            log::debug!("[ACCUM] Rejected — {:.0}% garbage chars", g_ratio * 100.0);
            return None;
        }
        // Require at least 2 fragments for a meaningful accumulation
        if group.len() < 2 {
            // Single fragment: only emit if it has high confidence on its own
            if mean_conf < 0.55 { return None; }
        }

        log::info!("[ACCUM] ✅ Accumulated ({} frags, df_group, len={}): '{}' conf={:.2}",
            group.len(), msg_len, text, mean_conf);

        Some(AccumulatedDecode {
            text,
            n_fragments: group.len(),
            mean_conf,
            char_conf,
            char_probs: char_probs_out,
            df_bin_hz: df_bin,
        })
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_probs_for_char(ch_idx: usize, confidence: f32) -> [f32; 48] {
        let mut p = [0.001f32; 48];
        let noise = (1.0 - confidence) / 48.0;
        for v in &mut p { *v = noise; }
        p[ch_idx] = confidence + noise * 47.0;
        let tot: f32 = p.iter().sum();
        p.iter_mut().for_each(|x| *x /= tot);
        p
    }

    fn make_fragment(chars: &[usize], confidence: f32, ccf: f32, df: f32) -> Fragment {
        let char_probs: Vec<[f32; 48]> = chars.iter()
            .map(|&c| make_probs_for_char(c, confidence))
            .collect();
        let char_conf = char_probs.iter()
            .map(|p| p.iter().cloned().fold(0.0f32, f32::max))
            .collect();
        Fragment { char_probs, char_conf, ccf_weight: ccf, df_hz: df, timestamp: std::time::Instant::now() }
    }

    #[test]
    fn alignment_finds_correct_shift() {
        // Fragment A: chars [1,2,3,4,5]
        // Fragment B: chars [3,4,5,1,2] — shifted by 2
        let a = make_fragment(&[1,2,3,4,5], 0.9, 500.0, 0.0);
        let b = make_fragment(&[3,4,5,1,2], 0.9, 400.0, 0.0);
        let (shift, score) = find_alignment(&a, &b, 5);
        assert!(score > 0.1, "alignment score should be positive, got {}", score);
        // shift=-2 means b starts 2 chars later in the message cycle
        assert!(shift.abs() <= 3, "shift {} should be small", shift);
    }

    #[test]
    fn accumulator_merges_two_fragments() {
        // Two fragments encoding the same 5-char message
        let chars = [17usize, 6, 22, 4, 22]; // "GW4WN" roughly
        let f1 = make_fragment(&chars, 0.85, 600.0, -5.0);
        let f2 = make_fragment(&chars, 0.75, 450.0, 10.0);
        let mut acc = FragmentAccumulator::new();
        acc.add(f1); acc.add(f2);
        let results = acc.process();
        assert!(!results.is_empty(), "should produce an accumulated decode");
        let r = &results[0];
        assert_eq!(r.n_fragments, 2);
        assert!(r.mean_conf > 0.5, "confidence should be high: {}", r.mean_conf);
    }

    #[test]
    fn accumulator_groups_by_df() {
        // Two fragments from station A (df≈0) and two from station B (df≈500)
        let chars_a = [1usize,2,3,4,5];
        let chars_b = [6usize,7,8,9,10];
        let mut acc = FragmentAccumulator::new();
        acc.add(make_fragment(&chars_a, 0.8, 400.0,   0.0));
        acc.add(make_fragment(&chars_a, 0.8, 350.0,  20.0));
        acc.add(make_fragment(&chars_b, 0.8, 400.0, 500.0));
        acc.add(make_fragment(&chars_b, 0.8, 350.0, 480.0));
        let results = acc.process();
        // Should produce 2 groups
        assert_eq!(results.len(), 2, "expected 2 DF groups, got {}", results.len());
    }
}
