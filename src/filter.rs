// src/fsk441rx/filter.rs

use regex::Regex;
use std::sync::OnceLock;

fn callsign_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(
        r"\b([A-Z]{1,2}[0-9][A-Z]{1,4})\b"
    ).unwrap())
}

fn locator_re_6() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b([A-R]{2}[0-9]{2}[A-X]{2})\b").unwrap())
}

fn locator_re_raw() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    // Matches any 4-char Maidenhead candidate — post-processing validates trailing chars
    RE.get_or_init(|| Regex::new(r"\b([A-R]{2}[0-9]{2})").unwrap())
}

fn report_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(
        r"\b(RR73|RRR|73|OOO|-[0-2][0-9]|[1-5][1-9])"
    ).unwrap())
}

/// Extract a Maidenhead locator from a string.
///
/// Priority: 6-char (IO82KM) > 4-char (IO82).
/// For 4-char, we accept the match even if followed by a single letter (noise),
/// as long as it's NOT followed by two letters [A-X] (which would be a valid 6-char
/// subsquare already caught above).
///
/// Examples:
///   "IO82"     → Some("IO82")   ✓
///   "IO82KM"   → Some("IO82KM") ✓ (6-char wins)
///   "IO82X,"   → Some("IO82")   ✓ (X alone = noise, not subsquare)
///   "IO82XZ"   → Some("IO82XZ") ✓ (valid 6-char, caught by locator_re_6)
fn extract_locator(s: &str) -> Option<String> {
    // Try 6-char first
    if let Some(m) = locator_re_6().find(s) {
        return Some(m.as_str().to_string());
    }

    // Try 4-char with post-processing
    let bytes = s.as_bytes();
    for cap in locator_re_raw().captures_iter(s) {
        let m = cap.get(1)?;
        let loc = m.as_str().to_string();
        let end = m.end();

        // Check what follows the 4-char match
        let next1 = bytes.get(end).copied();
        let next2 = bytes.get(end + 1).copied();

        let is_alpha = |c: u8| c.is_ascii_alphabetic();

        match (next1, next2) {
            // End of string — valid 4-char
            (None, _) => return Some(loc),
            // Followed by non-alpha — valid 4-char (e.g. "IO82 ", "IO82,")
            (Some(c1), _) if !is_alpha(c1) => return Some(loc),
            // Followed by one letter then non-alpha or end — noise after locator
            // e.g. "IO82X," — accept IO82, discard the X
            (Some(c1), None) if is_alpha(c1) => return Some(loc),
            (Some(c1), Some(c2)) if is_alpha(c1) && !is_alpha(c2) => return Some(loc),
            // Followed by two alpha chars — this is a 6-char locator candidate
            // but locator_re_6 would have caught it if valid; skip this 4-char match
            _ => continue,
        }
    }
    None
}

#[derive(Debug, Clone, PartialEq)]
pub enum MessageType {
    StandardExchange,
    CqCall,
    Roger,
    TwoCallsigns,
    PartialWithLocator,
    OneCallsign,
    Garbage,
}

#[derive(Debug, Clone)]
pub struct ParsedMessage {
    pub raw:             String,
    pub callsigns:       Vec<String>,
    pub locator:         Option<String>,
    pub report:          Option<String>,
    pub is_cq:           bool,
    pub message_type:    MessageType,
    pub validity_score:  u8,
    pub valid_callsigns: Vec<String>,
}

impl ParsedMessage {
    pub fn parse(raw: &str) -> Self {
        Self::parse_geo(raw, None)
    }

    pub fn parse_geo(raw: &str, geo: Option<&crate::geo::GeoValidator>) -> Self {
        let upper = raw.trim().to_uppercase();

        // Extract all unique callsigns
        let mut callsigns: Vec<String> = Vec::new();
        for m in callsign_re().find_iter(&upper) {
            let s = m.as_str().to_string();
            if s.len() >= 4
                && s != "OOO" && s != "RRR"
                && !callsigns.contains(&s)
            {
                callsigns.push(s);
            }
        }

        // FSK441 CQ detection:
        //   1. Starts with "CQ "
        //   2. Contains "CQ" token anywhere with a callsign (e.g. "SM4SJY CQ" or "CQ SM4SJY")
        //   3. Same callsign appears twice: "GW4WND GW4WND IO82"
        let has_cq_token = upper.split_whitespace().any(|w| w == "CQ");
        let starts_cq = upper.starts_with("CQ ");
        let is_fsk_cq = callsigns.len() == 1 && {
            let c = &callsigns[0];
            callsign_re().find_iter(&upper)
                .filter(|m| m.as_str() == c.as_str())
                .count() >= 2
        };
        // CQ if: starts with CQ, OR has CQ token + callsign, OR same callsign repeated
        let is_cq = starts_cq || (has_cq_token && !callsigns.is_empty()) || is_fsk_cq;

        // Geographic validation
        let valid_callsigns: Vec<String> = match geo {
            None    => callsigns.clone(),
            Some(g) => callsigns.iter()
                .filter(|c| g.is_plausible(c))
                .cloned()
                .collect(),
        };

        let locator = extract_locator(&upper);
        let report  = report_re().find(&upper).map(|m| m.as_str().to_string());

        let (message_type, validity_score) = classify_full(
            &upper, valid_callsigns.len(), &valid_callsigns,
            locator.is_some(), report.is_some(), is_cq,
        );

        ParsedMessage {
            raw: raw.to_string(),
            callsigns,
            locator,
            report,
            is_cq,
            message_type,
            validity_score,
            valid_callsigns,
        }
    }

    pub fn is_valid(&self)           -> bool { self.validity_score >= 40 }
    pub fn is_high_confidence(&self) -> bool { self.validity_score >= 75 }
    pub fn callsign_a(&self) -> Option<&str> { self.valid_callsigns.get(0).map(|s| s.as_str()) }
    pub fn callsign_b(&self) -> Option<&str> { self.valid_callsigns.get(1).map(|s| s.as_str()) }
}

fn count_rrr(s: &str) -> usize {
    // Count RRR occurrences — RRRR counts as one, RRRR RRRR as two
    let mut count = 0;
    let mut i = 0;
    let bytes = s.as_bytes();
    while i < bytes.len() {
        if bytes[i] == b'R' {
            let mut j = i;
            while j < bytes.len() && bytes[j] == b'R' { j += 1; }
            if j - i >= 3 { count += 1; i = j; continue; }
        }
        i += 1;
    }
    count
}

fn count_repeating_call(s: &str, calls: &[String]) -> usize {
    calls.iter().map(|c| {
        let mut count = 0;
        let mut pos = 0;
        while let Some(idx) = s[pos..].find(c.as_str()) {
            count += 1;
            pos += idx + c.len();
        }
        count
    }).max().unwrap_or(0)
}

fn classify_full(raw: &str, n_calls: usize, calls: &[String], has_loc: bool, has_rpt: bool, is_cq: bool)
    -> (MessageType, u8)
{
    let upper = raw.to_uppercase();
    let rrr_count = count_rrr(&upper);
    let call_repeats = count_repeating_call(&upper, calls);

    // Base score from structure
    let (msg_type, base_score) = match (n_calls, has_loc, has_rpt, is_cq) {
        (1.., true,  _,     true)  => (MessageType::CqCall,             82u8),
        (1.., false, _,     true)  => (MessageType::CqCall,             72),
        (2.., true,  true,  false) => (MessageType::StandardExchange,   95),
        (2.., false, true,  false) => (MessageType::Roger,              88),
        (2.., true,  false, false) => (MessageType::TwoCallsigns,       78),
        (2.., false, false, false) => (MessageType::TwoCallsigns,       62),
        (1,   true,  _,     false) => (MessageType::PartialWithLocator, 62),
        (1,   false, true,  false) => (MessageType::OneCallsign,        52),
        (1,   false, false, false) => (MessageType::OneCallsign,        32),
        _                          => (MessageType::Garbage,             0),
    };

    // Bonus: RRR/RRRR pattern is a strong QSO indicator
    // "RRRR G4DCV RRRR RRRR" — 1 call + 3×RRR should score like Roger
    let rrr_bonus: u8 = if rrr_count >= 2 { 20 }
                        else if rrr_count == 1 { 8 }
                        else { 0 };

    // Bonus: callsign repeating multiple times confirms real signal
    let repeat_bonus: u8 = if call_repeats >= 3 { 15 }
                           else if call_repeats == 2 { 8 }
                           else { 0 };

    let score = base_score.saturating_add(rrr_bonus).saturating_add(repeat_bonus).min(95);
    (msg_type, score)
}

fn classify(n_calls: usize, has_loc: bool, has_rpt: bool, is_cq: bool)
    -> (MessageType, u8)
{
    // Legacy path — used without raw text context
    classify_full("", n_calls, &[], has_loc, has_rpt, is_cq)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_exchange() {
        let p = ParsedMessage::parse("G4ABC GW4WND IO82 57");
        assert_eq!(p.message_type, MessageType::StandardExchange);
        assert!(p.validity_score >= 90);
        assert_eq!(p.report.as_deref(), Some("57"));
    }

    #[test]
    fn fsk441_cq_format() {
        let p = ParsedMessage::parse("GW4WND GW4WND IO82");
        assert!(p.is_cq);
        assert_eq!(p.message_type, MessageType::CqCall);
        assert!(p.validity_score >= 80);
        assert_eq!(p.locator.as_deref(), Some("IO82"));
    }

    #[test]
    fn fsk441_cq_with_trailing_noise() {
        // IO82X, — single letter after 4-char = noise, accept IO82
        let p = ParsedMessage::parse("GW4WND GW4WND IO82X,");
        assert!(p.is_cq);
        assert_eq!(p.locator.as_deref(), Some("IO82"));
        assert_eq!(p.message_type, MessageType::CqCall);
        assert!(p.validity_score >= 80);
    }

    #[test]
    fn fsk441_cq_6char_locator() {
        let p = ParsedMessage::parse("GW4WND GW4WND IO82KM");
        assert!(p.is_cq);
        assert_eq!(p.locator.as_deref(), Some("IO82KM"));
        assert!(p.validity_score >= 80);
    }

    #[test]
    fn locator_4char_space_separated() {
        let p = ParsedMessage::parse("G4ABC GW4WND IO82 57");
        assert_eq!(p.locator.as_deref(), Some("IO82"));
    }

    #[test]
    fn region1_report() {
        let p = ParsedMessage::parse("GW4WND I5YDI IO82 57");
        assert_eq!(p.report.as_deref(), Some("57"));
    }

    #[test]
    fn roger() {
        let p = ParsedMessage::parse("GW4WND G4ABC RRR");
        assert_eq!(p.message_type, MessageType::Roger);
    }

    #[test]
    fn cq_classic() {
        let p = ParsedMessage::parse("CQ G4ABC IO91");
        assert_eq!(p.message_type, MessageType::CqCall);
        assert!(p.validity_score >= 80);
    }

    #[test]
    fn garbage() {
        let p = ParsedMessage::parse("3K?Z /1 XQ#2 KP$");
        assert_eq!(p.message_type, MessageType::Garbage);
    }
}
