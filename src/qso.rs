// src/fsk441rx/qso.rs
//
// FSK441 QSO state machine — implements the three scenarios from QSO_scenarios_MSK2K.pdf
//
// Scenario 1: A calls CQ, B answers with both calls + report
// Scenario 2: A receives unsolicited call, responds with both calls + report  
// Scenario 3: A calls specific station B (sked)
//
// All three converge at the Exchange state and follow the same path to completion.

use crate::filter::ParsedMessage;

// ─── State ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum QsoState {
    /// No active QSO
    Idle,

    /// Scenario 1: We are calling CQ
    /// TX: "GW4WND GW4WND IO82"
    CallingCq {
        my_call: String,
        my_loc:  String,
    },

    /// Scenario 3: We are calling a specific station (sked)
    /// TX: "GW4WND DF2ZC IO82"  (no report yet)
    CallingStation {
        my_call:    String,
        my_loc:     String,
        their_call: String,
        their_loc:  String,
    },

    /// We have heard both calls — sending both calls + report
    /// Scenario 1 (A): triggered by hearing both calls + report from B
    /// Scenario 2 (A): triggered by hearing both calls, no report  
    /// TX: "GW4WND I5YDI IO82 57"
    SendingReport {
        my_call:     String,
        my_loc:      String,
        their_call:  String,
        their_loc:   String,
        report_sent: String,
    },

    /// We have heard both calls + report — sending R + our report
    /// TX: "GW4WND I5YDI R 37"
    SendingRReport {
        my_call:     String,
        their_call:  String,
        report_sent: String,   // "R 57" — the R prefix confirms receipt
        report_rcvd: String,   // their report e.g. "37"
        their_loc:   String,
    },

    /// We have heard R report — sending RR
    /// TX: "GW4WND I5YDI RR"
    SendingRR {
        my_call:     String,
        their_call:  String,
        their_loc:   String,
        report_sent: String,
        report_rcvd: String,
    },

    /// We have heard RR — sending 73 × 5 then stop
    /// TX: "GW4WND I5YDI 73"
    Sending73 {
        my_call:     String,
        their_call:  String,
        their_loc:   String,
        report_sent: String,
        report_rcvd: String,
        remaining:   u8,       // count down from 5
    },

    /// QSO complete — ready to log
    Complete {
        their_call:  String,
        their_loc:   String,
        report_sent: String,
        report_rcvd: String,
    },
}

impl QsoState {
    /// Generate the TX message for the current state
    pub fn tx_message(&self) -> Option<String> {
        match self {
            QsoState::Idle     => None,
            QsoState::Complete { .. } => None,

            QsoState::CallingCq { my_call, my_loc } => {
                let loc4 = &my_loc[..my_loc.len().min(4)];
                Some(format!("CQ {} {}", my_call, loc4))
            }

            QsoState::CallingStation { my_call, my_loc, their_call, .. } => {
                let loc4 = &my_loc[..my_loc.len().min(4)];
                Some(format!("{} {} {}", their_call, my_call, loc4))
            }

            QsoState::SendingReport { my_call, my_loc, their_call, report_sent, .. } => {
                let loc4 = &my_loc[..my_loc.len().min(4)];
                Some(format!("{} {} {} {}", their_call, my_call, loc4, report_sent))
            }

            QsoState::SendingRReport { my_call, their_call, report_sent, .. } =>
                Some(format!("{} {} {}", their_call, my_call, report_sent)),

            QsoState::SendingRR { my_call, their_call, .. } =>
                Some(format!("{} {} RR", my_call, their_call)),

            QsoState::Sending73 { my_call, their_call, .. } =>
                Some(format!("{} {} 73", their_call, my_call)),
        }
    }

    /// Label for the TX button in the UI
    #[allow(dead_code)]
    pub fn tx_button_label(&self) -> &str {
        match self {
            QsoState::Idle              => "CQ",
            QsoState::CallingCq { .. }  => "CQ",
            QsoState::CallingStation {..}=> "TX1",
            QsoState::SendingReport {..} => "TX2",
            QsoState::SendingRReport {..}=> "TX3 (R Rpt)",
            QsoState::SendingRR { .. }  => "TX4 (RR)",
            QsoState::Sending73 { .. }  => "TX5 (73)",
            QsoState::Complete { .. }   => "LOG",
        }
    }

    /// True if we should be transmitting in the current state
    pub fn is_active(&self) -> bool {
        !matches!(self, QsoState::Idle | QsoState::Complete { .. })
    }

    /// Their callsign if known
    #[allow(dead_code)]
    pub fn their_call(&self) -> Option<&str> {
        match self {
            QsoState::CallingStation { their_call, .. } => Some(their_call),
            QsoState::SendingReport  { their_call, .. } => Some(their_call),
            QsoState::SendingRReport { their_call, .. } => Some(their_call),
            QsoState::SendingRR      { their_call, .. } => Some(their_call),
            QsoState::Sending73      { their_call, .. } => Some(their_call),
            QsoState::Complete       { their_call, .. } => Some(their_call),
            _ => None,
        }
    }

    /// Their locator if known
    #[allow(dead_code)]
    pub fn their_loc(&self) -> Option<&str> {
        match self {
            QsoState::CallingStation { their_loc, .. } => Some(their_loc),
            QsoState::SendingReport  { their_loc, .. } => Some(their_loc),
            QsoState::SendingRReport { their_loc, .. } => Some(their_loc),
            QsoState::SendingRR      { their_loc, .. } => Some(their_loc),
            QsoState::Sending73      { their_loc, .. } => Some(their_loc),
            QsoState::Complete       { their_loc, .. } => Some(their_loc),
            _ => None,
        }
    }
}

// ─── State transitions ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Transition {
    /// State advanced automatically based on decode
    Auto(String),  // description for the UI log
    /// 73 count decremented
    Count73(u8),
    /// QSO complete
    Complete,
    /// No change
    None,
}

/// Process a new decode against the current QSO state.
/// Returns the transition that occurred (if any).
/// The caller is responsible for updating the state.
pub fn on_decode(
    state:      &mut QsoState,
    decoded:    &ParsedMessage,
    my_call:    &str,
    report_gen: impl Fn() -> String,  // closure that generates a report
) -> Transition {
    let heard_my_call = decoded.valid_callsigns.iter()
        .any(|c| c.eq_ignore_ascii_case(my_call));

    let their_report = decoded.report.clone();

    // Did we hear both calls (ours + theirs)?
    let heard_both = |their: &str| {
        let heard_theirs = decoded.valid_callsigns.iter()
            .any(|c| c.eq_ignore_ascii_case(their));
        heard_my_call && heard_theirs
    };

    match state.clone() {

        // ── Scenario 1: Calling CQ ─────────────────────────────────────────
        // Trigger: hear both calls + report → jump straight to SendingRReport
        // Trigger: hear both calls, no report → SendingReport
        QsoState::CallingCq { my_call: mc, my_loc } => {
            if !heard_my_call { return Transition::None; }

            let their_call = decoded.valid_callsigns.iter()
                .find(|c| !c.eq_ignore_ascii_case(&mc))
                .cloned();

            if let Some(their_call) = their_call {
                let their_loc = decoded.locator.clone().unwrap_or_default();

                if let Some(rpt) = their_report {
                    // They sent both calls + report → we reply with R + report
                    let my_report = format!("R {}", report_gen());
                    *state = QsoState::SendingRReport {
                        my_call: mc, their_call: their_call.clone(),
                        report_sent: my_report, report_rcvd: rpt.clone(),
                        their_loc,
                    };
                    return Transition::Auto(
                        format!("Heard {} + report {} → sending R report", their_call, rpt)
                    );
                } else {
                    // They sent both calls, no report → we reply with both calls + report
                    let my_report = report_gen();
                    *state = QsoState::SendingReport {
                        my_call: mc, my_loc, their_call: their_call.clone(),
                        their_loc, report_sent: my_report.clone(),
                    };
                    return Transition::Auto(
                        format!("Heard {} → sending report {}", their_call, my_report)
                    );
                }
            }
            Transition::None
        }

        // ── Scenario 3: Calling specific station ──────────────────────────
        // Same triggers as CallingCq once we hear them respond
        QsoState::CallingStation { my_call: mc, my_loc, their_call: tc, their_loc: tl } => {
            if !heard_both(&tc) { return Transition::None; }

            if let Some(rpt) = their_report {
                let my_report = format!("R {}", report_gen());
                *state = QsoState::SendingRReport {
                    my_call: mc, their_call: tc.clone(),
                    report_sent: my_report, report_rcvd: rpt.clone(),
                    their_loc: tl,
                };
                return Transition::Auto(
                    format!("Heard {} + {} → sending R report", tc, rpt)
                );
            } else {
                let my_report = report_gen();
                *state = QsoState::SendingReport {
                    my_call: mc, my_loc, their_call: tc.clone(),
                    their_loc: tl, report_sent: my_report.clone(),
                };
                return Transition::Auto(
                    format!("Heard {} → sending report {}", tc, my_report)
                );
            }
        }

        // ── Sending report → hear R report → send RR ──────────────────────
        QsoState::SendingReport { my_call: mc, their_call: tc, their_loc: tl,
                                   report_sent: rs, .. } => {
            if !heard_both(&tc) { return Transition::None; }

            // Their R report starts with "R " or is just a report
            let is_r_report = their_report.as_deref()
                .map(|r| r.starts_with('R') || r.chars().all(|c| c.is_ascii_digit()))
                .unwrap_or(false);

            if is_r_report {
                let rcvd = their_report.unwrap_or_default();
                *state = QsoState::SendingRR {
                    my_call: mc, their_call: tc.clone(),
                    their_loc: tl, report_sent: rs,
                    report_rcvd: rcvd.clone(),
                };
                return Transition::Auto(
                    format!("Heard R report {} → sending RR", rcvd)
                );
            }
            Transition::None
        }

        // ── Sending R report → hear RR → send 73×5 ────────────────────────
        QsoState::SendingRReport { my_call: mc, their_call: tc, their_loc: tl,
                                    report_sent: rs, report_rcvd: rr } => {
            if !heard_both(&tc) { return Transition::None; }

            let is_rr = decoded.report.as_deref()
                .map(|r| r == "RR" || r == "RRR" || r == "RR73")
                .unwrap_or(false);

            if is_rr {
                *state = QsoState::Sending73 {
                    my_call: mc, their_call: tc.clone(),
                    their_loc: tl, report_sent: rs, report_rcvd: rr,
                    remaining: 5,
                };
                return Transition::Auto("Heard RR → sending 73×5".to_string());
            }
            Transition::None
        }

        // ── Sending RR → hear 73 → send 73×5 ─────────────────────────────
        QsoState::SendingRR { my_call: mc, their_call: tc, their_loc: tl,
                               report_sent: rs, report_rcvd: rr } => {
            if !heard_both(&tc) { return Transition::None; }

            let is_73 = decoded.report.as_deref()
                .map(|r| r == "73" || r == "RR73")
                .unwrap_or(false);

            if is_73 {
                *state = QsoState::Sending73 {
                    my_call: mc, their_call: tc,
                    their_loc: tl, report_sent: rs, report_rcvd: rr,
                    remaining: 5,
                };
                return Transition::Auto("Heard 73 → sending 73×5".to_string());
            }
            Transition::None
        }

        // ── Sending 73 — decrement counter ───────────────────────────────
        QsoState::Sending73 { my_call: mc, their_call: tc, their_loc: tl,
                               report_sent: rs, report_rcvd: rr, remaining } => {
            if remaining == 0 {
                *state = QsoState::Complete {
                    their_call: tc, their_loc: tl,
                    report_sent: rs, report_rcvd: rr,
                };
                return Transition::Complete;
            }
            // Decrement on each TX
            *state = QsoState::Sending73 {
                my_call: mc, their_call: tc, their_loc: tl,
                report_sent: rs, report_rcvd: rr,
                remaining: remaining - 1,
            };
            Transition::Count73(remaining - 1)
        }

        _ => Transition::None,
    }
}

// ─── Constructor helpers ──────────────────────────────────────────────────────

impl QsoState {
    /// Start calling CQ (Scenario 1)
    #[allow(dead_code)]
    pub fn start_cq(my_call: String, my_loc: String) -> Self {
        QsoState::CallingCq { my_call, my_loc }
    }

    /// Start calling a specific station (Scenario 3)
    #[allow(dead_code)]
    pub fn start_sked(
        my_call: String, my_loc: String,
        their_call: String, their_loc: String,
    ) -> Self {
        QsoState::CallingStation { my_call, my_loc, their_call, their_loc }
    }

    /// Answer a CQ (Scenario 2) — immediately enter SendingReport
    pub fn answer_cq(
        my_call: String, my_loc: String,
        their_call: String, their_loc: String,
        report_sent: String,
    ) -> Self {
        QsoState::SendingReport { my_call, my_loc, their_call, their_loc, report_sent }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::ParsedMessage;

    fn make_decode(raw: &str) -> ParsedMessage {
        ParsedMessage::parse(raw)
    }

    #[test]
    fn cq_to_sending_report() {
        let mut state = QsoState::start_cq("GW4WND".into(), "IO82".into());

        // Hear both calls, no report (Scenario 2 trigger)
        let decoded = make_decode("GW4WND I5YDI IO82");
        let t = on_decode(&mut state, &decoded, "GW4WND", || "57".to_string());

        assert!(matches!(t, Transition::Auto(_)));
        assert!(matches!(state, QsoState::SendingReport { .. }));
        assert_eq!(state.tx_message().unwrap(), "GW4WND I5YDI IO82 57");
    }

    #[test]
    fn cq_to_r_report_direct() {
        let mut state = QsoState::start_cq("GW4WND".into(), "IO82".into());

        // Hear both calls + report (Scenario 1 trigger — B sent report)
        let decoded = make_decode("GW4WND I5YDI IO82 37");
        let t = on_decode(&mut state, &decoded, "GW4WND", || "57".to_string());

        assert!(matches!(t, Transition::Auto(_)));
        assert!(matches!(state, QsoState::SendingRReport { .. }));
        assert_eq!(state.tx_message().unwrap(), "GW4WND I5YDI R 57");
    }

    #[test]
    fn tx_messages_correct() {
        assert_eq!(
            QsoState::start_cq("GW4WND".into(), "IO82".into()).tx_message().unwrap(),
            "GW4WND GW4WND IO82"
        );
        assert_eq!(
            QsoState::answer_cq(
                "GW4WND".into(), "IO82".into(),
                "I5YDI".into(), "JN53".into(),
                "57".into()
            ).tx_message().unwrap(),
            "GW4WND I5YDI IO82 57"
        );
    }
}
