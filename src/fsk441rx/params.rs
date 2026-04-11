#![allow(dead_code)]
// src/fsk441rx/params.rs
//
// FSK441 signal constants.
// Key identity: 441 baud × 25 samples/dit = 11025 Hz exactly.
// Both WSJT and MSHV capture audio natively at 11025 Hz — no resampling.
// We do the same: configure cpal to open the soundcard at 11025 Hz directly.

// ─── Sample rate ──────────────────────────────────────────────────────────────

/// DSP sample rate AND capture rate — same value, no resampling needed.
/// The IC-9700 USB audio codec (Burr-Brown PCM2902) supports 11025 Hz natively.
pub const SAMPLE_RATE:   u32 = 11025;
pub const SAMPLE_RATE_F: f32 = 11025.0;

/// Hardware capture rate — same as DSP rate, no conversion required.
/// Previously 48000 with rubato resampling; that introduced a ~-195 Hz
/// systematic frequency offset due to non-integer ratio errors.
#[allow(dead_code)]
pub const CAPTURE_RATE:  u32 = 11025;

// ─── FSK441 modulation ────────────────────────────────────────────────────────

pub const BAUD:      f32   = 441.0;
pub const NSPD:      usize = 25;       // samples per dit = 11025/441 exactly
pub const N_TONES:   usize = 4;
pub const TONE_BASE: f32   = 882.0;   // lowest tone = 2×441 Hz
pub const TONE_STEP: f32   = 441.0;   // spacing between tones

/// The four tone centre frequencies (Hz)
pub const TONES: [f32; 4] = [882.0, 1323.0, 1764.0, 2205.0];

/// Tone 3 (2205 Hz) is the sync tone — absent at char-boundary positions
pub const SYNC_TONE_IDX: usize = 3;

#[inline(always)]
pub fn tone_freq(idx: usize) -> f32 {
    TONE_BASE + idx as f32 * TONE_STEP
}

// ─── Character set ────────────────────────────────────────────────────────────
//
// 48 symbols. nc = 16×d0 + 4×d1 + d2
// Source: MSHV config_msg_all.h c_FSK441_RX[48]

pub const CHARSET: &[u8; 48] = b" 123456789.,?/# $ABCD FGHIJKLMNOPQRSTUVWXY 0EZ*!";

#[inline]
pub fn dits_to_char(d0: u8, d1: u8, d2: u8) -> Option<char> {
    let nc = 16 * d0 as usize + 4 * d1 as usize + d2 as usize;
    CHARSET.get(nc).map(|&b| b as char)
}

// ─── Buffer and decode limits ─────────────────────────────────────────────────

pub const RING_LEN:      usize = (SAMPLE_RATE_F * 2.0) as usize; // 22050 (2 s)
pub const MAX_NDITS:     usize = 441;
pub const MAX_MSG_CHARS: usize = 46;
pub const SPEC_NFFT:     usize = 256;

// ─── Detector defaults ────────────────────────────────────────────────────────

#[allow(dead_code)]
pub const DEFAULT_THRESHOLD:     f32      = 2.1;
pub const DEFAULT_DFTOL:         i32      = 200;
pub const DEFAULT_BP_CORRECTION: [f32; 4] = [1.0; 4];

// ─── Audio buffer size ────────────────────────────────────────────────────────

/// cpal buffer size in samples at 11025 Hz.
/// ~93ms per chunk — low enough latency for responsive detection.
pub const AUDIO_BUFFER_SIZE: u32 = 1024;

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn charset_len()   { assert_eq!(CHARSET.len(), 48); }
    #[test] fn nspd_exact()    { assert_eq!(SAMPLE_RATE / BAUD as u32, NSPD as u32); }
    #[test] fn tone_0()        { assert_eq!(tone_freq(0), 882.0); }
    #[test] fn tone_3()        { assert_eq!(tone_freq(3), 2205.0); }
    #[test] fn decode_space()  { assert_eq!(dits_to_char(0,0,0), Some(' ')); }
    #[test] fn decode_a()      { assert_eq!(dits_to_char(1,0,1), Some('A')); }
    #[test] fn decode_g()      { assert_eq!(dits_to_char(1,1,3), Some('G')); }
    #[test] fn decode_w()      { assert_eq!(dits_to_char(2,1,3), Some('W')); }
    #[test] fn no_resampling() { assert_eq!(CAPTURE_RATE, SAMPLE_RATE); }
}
