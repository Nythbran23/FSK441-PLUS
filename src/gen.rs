// src/fsk441rx/gen.rs
//
// FSK441 transmit audio generator — port of MSHV gen441() / abc441().
//
// Given a message string, produces a Vec<f32> of audio samples at 11025 Hz
// that can be fed directly to cpal for playback through the IC-9700 USB audio.
//
// FSK441 message format: continuous-phase FSK, 4 tones, 441 baud.
// Each character encodes to 3 dits (base-4 trits), each dit = NSPD samples.
// The message is repeated continuously until TX is stopped.

use std::f32::consts::TAU;
use crate::fsk441rx::params::*;

// ─── Character → dit encoding ─────────────────────────────────────────────────
//
// Port of MSHV abc441(). Maps each character to 3 tone indices (0-3).
// Uses the same CHARSET lookup as the decoder for consistency.

/// Encode a single character to 3 dit indices (d0, d1, d2).
/// Returns None if character is not in the FSK441 alphabet.
fn char_to_dits(c: char) -> Option<(u8, u8, u8)> {
    let uc = c.to_ascii_uppercase();
    let pos = CHARSET.iter().position(|&b| b as char == uc)?;
    let d0 = (pos / 16) as u8;
    let d1 = ((pos / 4) % 4) as u8;
    let d2 = (pos % 4) as u8;
    Some((d0, d1, d2))
}

/// Encode a message string to a sequence of tone indices (0-3).
/// Unknown characters are encoded as space (nc=0 → 0,0,0).
pub fn encode_message(msg: &str) -> Vec<u8> {
    let mut tones = Vec::with_capacity(msg.len() * 3);
    for c in msg.chars() {
        let (d0, d1, d2) = char_to_dits(c).unwrap_or((0, 0, 0));
        tones.push(d0);
        tones.push(d1);
        tones.push(d2);
    }
    tones
}

// ─── Tone sequence → audio samples ───────────────────────────────────────────
//
// Port of MSHV gen441(). Generates continuous-phase FSK audio.
// Phase is maintained across dit boundaries to avoid clicks.
// The four tone frequencies are TONE_BASE + n*TONE_STEP for n in 0..4.

/// Generate one pass of FSK441 audio for the given tone sequence.
/// Returns samples at SAMPLE_RATE (11025 Hz), normalised to ±1.0.
/// Phase is continuous across dit boundaries (no phase discontinuities).
pub fn generate_audio(tones: &[u8]) -> Vec<f32> {
    let n_samples = tones.len() * NSPD;
    let mut samples = Vec::with_capacity(n_samples);

    let dt = 1.0_f32 / SAMPLE_RATE_F;
    let mut phase = 0.0f32;

    for &tone_idx in tones {
        let freq = tone_freq(tone_idx as usize);
        let dpha = TAU * freq * dt;
        for _ in 0..NSPD {
            samples.push(phase.sin());
            phase += dpha;
            // Keep phase in [0, TAU) to prevent float precision drift
            if phase >= TAU { phase -= TAU; }
        }
    }

    samples
}

// ─── Public API ───────────────────────────────────────────────────────────────

/// A ready-to-play FSK441 transmission.
/// The audio repeats the message continuously — the TX engine slices it
/// into period-length chunks and stops at the period boundary.
#[derive(Debug, Clone)]
pub struct Fsk441Tx {
    /// The message being transmitted (uppercase, trimmed)
    pub message: String,
    /// One full pass of audio samples at 11025 Hz
    pub audio_one_pass: Vec<f32>,
    /// Duration of one pass in seconds
    pub pass_duration_s: f32,
    /// Duration of one pass in samples
    pub pass_samples: usize,
}

impl Fsk441Tx {
    /// Create a new transmission from a message string.
    ///
    /// The message is uppercased and validated against the FSK441 alphabet.
    /// Characters not in the alphabet are replaced with space.
    pub fn new(message: &str) -> Self {
        let msg = message.trim().to_uppercase();
        let tones = encode_message(&msg);
        let audio = generate_audio(&tones);
        let n = audio.len();
        let dur = n as f32 / SAMPLE_RATE_F;

        Self {
            message: msg,
            audio_one_pass: audio,
            pass_duration_s: dur,
            pass_samples: n,
        }
    }

    /// Get audio for a specific number of complete passes.
    pub fn audio_n_passes(&self, n: usize) -> Vec<f32> {
        self.audio_one_pass.iter().cloned()
            .cycle()
            .take(self.pass_samples * n)
            .collect()
    }

    /// Get audio for a fixed duration (seconds), repeating as needed.
    pub fn audio_for_duration(&self, duration_s: f32) -> Vec<f32> {
        let n_samples = (duration_s * SAMPLE_RATE_F) as usize;
        self.audio_one_pass.iter().cloned()
            .cycle()
            .take(n_samples)
            .collect()
    }

    /// How many complete passes fit in a 30-second TX period?
    pub fn passes_per_period(&self) -> usize {
        (30.0 / self.pass_duration_s) as usize
    }

    /// Estimated passes per period (for display)
    pub fn repeats_info(&self) -> String {
        format!(
            "{} chars, {:.1}ms/pass, ~{} repeats in 30s",
            self.message.len(),
            self.pass_duration_s * 1000.0,
            self.passes_per_period(),
        )
    }
}

// ─── WAV export (for testing) ─────────────────────────────────────────────────

/// Write a transmission to a WAV file for off-air testing.
/// Writes `n_passes` repetitions of the message.
pub fn write_wav(tx: &Fsk441Tx, path: &str, n_passes: usize) -> Result<(), hound::Error> {
    let spec = hound::WavSpec {
        channels:        1,
        sample_rate:     SAMPLE_RATE,
        bits_per_sample: 16,
        sample_format:   hound::SampleFormat::Int,
    };
    let mut writer = hound::WavWriter::create(path, spec)?;
    for &s in tx.audio_n_passes(n_passes).iter() {
        let s16 = (s.clamp(-1.0, 1.0) * 32767.0) as i16;
        writer.write_sample(s16)?;
    }
    writer.finalize()?;
    Ok(())
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        // Encode a known message and verify the tone sequence
        // G = nc 23 = (1,1,3), space = nc 0 = (0,0,0)
        let tones = encode_message("G");
        assert_eq!(tones, vec![1, 1, 3], "G should encode to dits (1,1,3)");

        let tones = encode_message(" ");
        assert_eq!(tones, vec![0, 0, 0], "space should encode to dits (0,0,0)");
    }

    #[test]
    fn audio_length_correct() {
        let tx = Fsk441Tx::new("GW4WND GW4WND IO82");
        // 18 chars × 3 dits × 25 samples = 1350 samples
        assert_eq!(tx.pass_samples, 18 * 3 * NSPD);
        assert_eq!(tx.audio_one_pass.len(), tx.pass_samples);
    }

    #[test]
    fn audio_amplitude_normalised() {
        let tx = Fsk441Tx::new("GW4WND GW4WND IO82");
        let max_amp = tx.audio_one_pass.iter().copied()
            .fold(0.0f32, f32::max);
        assert!(max_amp <= 1.0, "amplitude must not exceed 1.0, got {}", max_amp);
        assert!(max_amp > 0.9, "amplitude should be near 1.0, got {}", max_amp);
    }

    #[test]
    fn phase_continuous() {
        // Check there are no large phase jumps between dits
        let tx = Fsk441Tx::new("AB");
        let audio = &tx.audio_one_pass;
        let mut max_jump = 0.0f32;
        for i in 1..audio.len() {
            let jump = (audio[i] - audio[i-1]).abs();
            max_jump = max_jump.max(jump);
        }
        // Maximum sample-to-sample jump for a 441 Hz tone at 11025 Hz is
        // sin(TAU*2205/11025) - sin(0) ≈ 0.97 at most (highest tone)
        // A phase discontinuity would cause a much larger jump
        assert!(max_jump < 1.5, "phase discontinuity detected: max jump {}", max_jump);
    }

    #[test]
    fn cq_message_format() {
        let tx = Fsk441Tx::new("GW4WND GW4WND IO82");
        assert_eq!(tx.message, "GW4WND GW4WND IO82");
        // 18 chars at 147 chars/sec = ~122ms per pass, ~245 repeats in 30s
        assert!(tx.passes_per_period() > 200);
        println!("{}", tx.repeats_info());
    }
}
