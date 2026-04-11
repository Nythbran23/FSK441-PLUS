// src/fsk441rx/demod.rs

use std::f32::consts::TAU;
use num_complex::Complex32;
use rustfft::FftPlanner;

use crate::params::*;

#[derive(Debug, Clone)]
pub struct SoftDit {
    pub energies:   [f32; 4],
    pub hard:       u8,
    pub confidence: f32,
}

#[derive(Debug, Clone)]
pub struct DemodResult {
    pub raw_decode:      String,
    pub soft_dits:       Vec<SoftDit>,
    pub char_probs:      Vec<[f32; 48]>,
    pub mean_confidence: f32,
    pub min_confidence:  f32,
    pub n_ambiguous:     usize,
    pub df_hz:           f32,
}

impl DemodResult {
    pub fn empty(df_hz: f32) -> Self {
        Self {
            raw_decode: String::new(), soft_dits: vec![], char_probs: vec![],
            mean_confidence: 0.0, min_confidence: 0.0, n_ambiguous: 0, df_hz,
        }
    }
    pub fn is_empty(&self) -> bool { self.raw_decode.trim().is_empty() }
}

pub fn detect(data: &[f32], freq: f32) -> Vec<f32> {
    let npts = data.len();
    if npts < NSPD { return vec![]; }

    let dpha = TAU * freq / SAMPLE_RATE_F;

    let c: Vec<Complex32> = data.iter().enumerate().map(|(i, &s)| {
        let a = dpha * i as f32;
        Complex32::new(s * a.cos(), -s * a.sin())
    }).collect();

    let mut csum: Complex32 = c[..NSPD].iter().sum();
    let mut y = Vec::with_capacity(npts - NSPD + 1);
    y.push(csum.norm_sqr());

    for i in 1..npts.saturating_sub(NSPD - 1) {
        csum = csum - c[i - 1] + c[i + NSPD - 1];
        y.push(csum.norm_sqr());
    }
    y
}

pub fn find_sync_phase(y: &[[f32; 4]]) -> usize {
    let mut zf = [0.0f32; NSPD];

    for (i, e) in y.iter().enumerate() {
        let best   = e.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let second = e.iter().copied()
            .filter(|&v| (v - best).abs() > 1e-30)
            .fold(f32::NEG_INFINITY, f32::max)
            .max(0.0);
        zf[i % NSPD] += 1e-6 * (best - second);
    }

    let csum: Complex32 = zf.iter().enumerate().map(|(j, &z)| {
        let a = TAU * j as f32 / NSPD as f32;
        z * Complex32::new(a.cos(), -a.sin())
    }).sum();

    let phase = -csum.im.atan2(csum.re);
    ((NSPD as f32 * phase / TAU).round() as i32).rem_euclid(NSPD as i32) as usize
}

pub fn longx(data: &[f32], df_hint: f32, dftol: i32) -> DemodResult {
    let npts = data.len().min(SAMPLE_RATE as usize);
    if npts < NSPD * 6 { return DemodResult::empty(df_hint); }

    let df_hz = refine_frequency(&data[..npts], df_hint, dftol);

    let raw: [Vec<f32>; 4] = [
        detect(&data[..npts], tone_freq(0) + df_hz),
        detect(&data[..npts], tone_freq(1) + df_hz),
        detect(&data[..npts], tone_freq(2) + df_hz),
        detect(&data[..npts], tone_freq(3) + df_hz),
    ];

    let n = raw.iter().map(|e| e.len()).min().unwrap_or(0);
    if n < NSPD * 3 { return DemodResult::empty(df_hz); }

    let bp = DEFAULT_BP_CORRECTION;
    let energy_mat: Vec<[f32; 4]> = (0..n).map(|i| [
        raw[0][i] * bp[0],
        raw[1][i] * bp[1],
        raw[2][i] * bp[2],
        raw[3][i] * bp[3],
    ]).collect();

    let jpk    = find_sync_phase(&energy_mat);
    let n_dits = ((n - jpk) / NSPD).min(MAX_NDITS);

    let dit_energies: Vec<[f32; 4]> = (0..n_dits)
        .map(|i| energy_mat[jpk + i * NSPD])
        .collect();

    let soft_dits: Vec<SoftDit> = dit_energies.iter().map(soft_dit_from).collect();

    // Find mod-3 framing: tone 3 absent at positions i%3 == jsync
    let mut n4 = [0u32; 3];
    for (i, e) in dit_energies.iter().enumerate() {
        let best = e.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        if (e[SYNC_TONE_IDX] - best).abs() < 1e-30 { n4[i % 3] += 1; }
    }
    let jsync = n4.iter().enumerate()
        .min_by_key(|(_, &c)| c)
        .map(|(i, _)| i)
        .unwrap_or(0);

    let n_chars = ((n_dits.saturating_sub(jsync)) / 3).min(MAX_MSG_CHARS);
    let mut raw_decode = String::with_capacity(n_chars);
    let mut char_probs: Vec<[f32; 48]> = Vec::with_capacity(n_chars);

    for i in 0..n_chars {
        let j = jsync + i * 3;
        if j + 2 >= soft_dits.len() { break; }

        // Use space for uncertain dits instead of '?' — cleaner output,
        // real callsigns stand out visually against whitespace
        let ch = dits_to_char(
            soft_dits[j].hard,
            soft_dits[j+1].hard,
            soft_dits[j+2].hard,
        ).unwrap_or(' ');

        raw_decode.push(ch);
        char_probs.push(char_probs_at(
            &dit_energies[j],
            &dit_energies[j+1],
            &dit_energies[j+2],
        ));
    }

    let raw_decode = dedup_spaces(raw_decode.trim());

    let confs: Vec<f32> = soft_dits.iter().map(|d| d.confidence).collect();
    let mean_confidence = if confs.is_empty() { 0.0 }
        else { confs.iter().sum::<f32>() / confs.len() as f32 };
    let min_confidence  = confs.iter().copied().fold(f32::INFINITY, f32::min).min(1.0);
    let n_ambiguous     = confs.iter().filter(|&&c| c < 0.3).count();

    DemodResult { raw_decode, soft_dits, char_probs,
                  mean_confidence, min_confidence, n_ambiguous, df_hz }
}

fn refine_frequency(data: &[f32], df_hint: f32, dftol: i32) -> f32 {
    let nfft   = SPEC_NFFT;
    let nh     = nfft / 2;
    let df_bin = SAMPLE_RATE_F / nfft as f32;

    let mut planner = FftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(nfft);

    let mut s = vec![0.0f32; nh];
    let n_windows = data.len() / nfft;
    if n_windows == 0 { return df_hint; }

    for w in 0..n_windows {
        let start = w * nfft;
        let mut buf: Vec<rustfft::num_complex::Complex<f32>> = data[start..start+nfft]
            .iter()
            .map(|&x| rustfft::num_complex::Complex::<f32>::new(x, 0.0))
            .collect();
        fft.process(&mut buf);
        for i in 0..nh { s[i] += buf[i].norm_sqr(); }
    }
    let fac = 1.0 / (100.0 * nfft as f32 * n_windows as f32);
    s.iter_mut().for_each(|x| *x *= fac);

    let nbaud_bins = (BAUD / df_bin).round() as i32;
    let ltone      = (TONE_BASE / BAUD) as i32;
    let hint_bin   = (df_hint / df_bin).round() as i32;
    let tol_bins   = (dftol as f32 / df_bin).round() as i32;
    let wgt        = [1.0f32, 4.0, 6.0, 4.0, 1.0];

    let mut smax    = 0.0f32;
    let mut best_df = df_hint;

    let lo = (hint_bin - tol_bins).max(-(ltone * nbaud_bins));
    let hi =  hint_bin + tol_bins;

    for offset_bin in lo..=hi {
        let mut sum = 0.0f32;
        for tone in 0..N_TONES as i32 {
            let centre = (ltone + tone) * nbaud_bins + offset_bin;
            for (k, &w) in wgt.iter().enumerate() {
                let bin = (centre - 2 + k as i32).clamp(0, nh as i32 - 1) as usize;
                sum += w * s[bin];
            }
        }
        if sum > smax {
            smax    = sum;
            best_df = offset_bin as f32 * df_bin;
        }
    }
    best_df
}

fn soft_dit_from(e: &[f32; 4]) -> SoftDit {
    let best_idx = e.iter().enumerate()
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(i, _)| i)
        .unwrap_or(0);
    let best   = e[best_idx];
    let second = e.iter().copied().enumerate()
        .filter(|&(i, _)| i != best_idx)
        .map(|(_, v)| v)
        .fold(f32::NEG_INFINITY, f32::max)
        .max(0.0);
    let confidence = if best > 1e-30 {
        ((best - second) / best).clamp(0.0, 1.0)
    } else { 0.0 };
    SoftDit { energies: *e, hard: best_idx as u8, confidence }
}

fn char_probs_at(e0: &[f32; 4], e1: &[f32; 4], e2: &[f32; 4]) -> [f32; 48] {
    let norm = |e: &[f32; 4]| -> [f32; 4] {
        let t = e.iter().sum::<f32>().max(1e-30);
        [e[0]/t, e[1]/t, e[2]/t, e[3]/t]
    };
    let p0 = norm(e0); let p1 = norm(e1); let p2 = norm(e2);
    let mut probs = [0.0f32; 48];
    let mut total = 0.0f32;
    for c in 0..48usize {
        let p = p0[c/16] * p1[(c/4)%4] * p2[c%4];
        probs[c] = p; total += p;
    }
    if total > 1e-30 { probs.iter_mut().for_each(|p| *p /= total); }
    probs
}

fn dedup_spaces(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev = false;
    for c in s.chars() {
        if c == ' ' { if !prev { out.push(' '); } prev = true; }
        else        { out.push(c); prev = false; }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_pure_tone() {
        let npts = 1100usize;
        let data: Vec<f32> = (0..npts)
            .map(|i| (TAU * 882.0 * i as f32 / 11025.0).sin())
            .collect();
        let y0 = detect(&data, 882.0);
        let y1 = detect(&data, 1323.0);
        let e0: f32 = y0.iter().sum::<f32>() / y0.len() as f32;
        let e1: f32 = y1.iter().sum::<f32>() / y1.len() as f32;
        assert!(e0 > e1 * 5.0, "tone 0 energy {:.4} should dominate {:.4}", e0, e1);
    }

    #[test]
    fn char_probs_normalised() {
        let e = [1.0f32, 2.0, 0.5, 0.5];
        let p = char_probs_at(&e, &e, &e);
        assert!((p.iter().sum::<f32>() - 1.0).abs() < 1e-5);
    }
}
