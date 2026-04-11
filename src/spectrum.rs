// src/fsk441rx/spectrum.rs
// WSJT-style spectrogram: X=time (0..30s per period), Y=frequency (0..3kHz)
// Each audio chunk produces one vertical column; display resets each period.

use rustfft::{FftPlanner, num_complex::Complex};

pub const FFT_SIZE:     usize = 1024;
pub const DISPLAY_BINS: usize = 300; // bins 0..300 → 0..3228 Hz at 11025/1024 Hz/bin

/// Compute one column of spectrum magnitudes (normalised 0..1) from an audio chunk.
pub fn compute_column(samples: &[f32], planner: &mut FftPlanner<f32>) -> Vec<f32> {
    let fft = planner.plan_fft_forward(FFT_SIZE);
    let n   = samples.len().min(FFT_SIZE);

    let mut buf: Vec<Complex<f32>> = (0..FFT_SIZE)
        .map(|i| {
            if i < n {
                let w = 0.5 * (1.0 - (2.0 * std::f32::consts::PI * i as f32
                    / (FFT_SIZE as f32 - 1.0)).cos());
                Complex::new(samples[i] * w, 0.0)
            } else {
                Complex::new(0.0, 0.0)
            }
        })
        .collect();

    fft.process(&mut buf);

    let mags: Vec<f32> = buf[..DISPLAY_BINS]
        .iter()
        .map(|c| { let m = c.norm(); if m > 1e-10 { 20.0 * m.log10() } else { -80.0 } })
        .collect();

    // Auto-scale: use median as noise floor, 25 dB dynamic range
    let mut sorted = mags.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let floor = sorted[sorted.len() / 2];
    let ceil  = floor + 25.0;

    mags.iter().map(|&v| ((v - floor) / (ceil - floor)).clamp(0.0, 1.0)).collect()
}

/// WSJT-style colour: dark blue background, cyan/yellow for signals
pub fn heat_color(v: f32) -> egui::Color32 {
    let v = v.clamp(0.0, 1.0);
    let (r, g, b) = if v < 0.2 {
        let t = v / 0.2;
        (0.0, 0.0, t * 0.5)
    } else if v < 0.5 {
        let t = (v - 0.2) / 0.3;
        (0.0, t * 0.7, 0.5 + t * 0.3)
    } else if v < 0.75 {
        let t = (v - 0.5) / 0.25;
        (t * 0.9, 0.7 + t * 0.3, 0.8 - t * 0.8)
    } else {
        let t = (v - 0.75) / 0.25;
        (0.9 + t * 0.1, 1.0, t * 0.3)
    };
    egui::Color32::from_rgb((r*255.0) as u8, (g*255.0) as u8, (b*255.0) as u8)
}

/// FSK441 tone frequencies in Hz (shown as horizontal marker lines)
pub const FSK441_TONES_HZ: [f32; 4] = [882.0, 1323.0, 1764.0, 2205.0];

/// Convert Hz to bin index
pub fn hz_to_bin(hz: f32) -> usize {
    ((hz * FFT_SIZE as f32 / 11025.0) as usize).min(DISPLAY_BINS - 1)
}
