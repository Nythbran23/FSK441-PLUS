// src/fsk441rx/detector.rs

use std::collections::VecDeque;
use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use rustfft::{FftPlanner, num_complex::Complex};

use crate::params::*;

#[derive(Debug, Clone)]
pub struct DetectedPing {
    pub timestamp:   DateTime<Utc>,
    pub audio:       Vec<f32>,
    pub n_samples:   usize,
    pub df_hz:       f32,
    pub ccf_ratio:   f32,
    pub duration_ms: f32,
}

struct RingBuffer {
    buf:      VecDeque<f32>,
    capacity: usize,
}

impl RingBuffer {
    fn new(capacity: usize) -> Self {
        Self { buf: VecDeque::with_capacity(capacity), capacity }
    }

    fn push(&mut self, samples: &[f32]) {
        for &s in samples {
            if self.buf.len() == self.capacity { self.buf.pop_front(); }
            self.buf.push_back(s);
        }
    }

    fn as_vec(&self) -> Vec<f32> {
        self.buf.iter().copied().collect()
    }

    /// Return the most recent `n` samples
    fn last_n(&self, n: usize) -> Vec<f32> {
        let start = self.buf.len().saturating_sub(n);
        self.buf.iter().skip(start).copied().collect()
    }

    fn len(&self) -> usize { self.buf.len() }
}

pub struct PingDetector {
    ring:       RingBuffer,
    threshold:  f32,
    dftol:      i32,
    planner:    FftPlanner<f32>,
    cooldown:   usize,
    scan_every: usize,
    since_scan: usize,
}

impl PingDetector {
    pub fn new(threshold: f32, dftol: i32) -> Self {
        Self {
            ring:       RingBuffer::new(RING_LEN),
            threshold,
            dftol,
            planner:    FftPlanner::new(),
            cooldown:   0,
            scan_every: SAMPLE_RATE as usize / 20,  // every 50 ms
            since_scan: 0,
        }
    }

    pub fn push_samples(&mut self, samples: &[f32]) -> Vec<DetectedPing> {
        self.ring.push(samples);

        if self.cooldown > 0 {
            self.cooldown = self.cooldown.saturating_sub(samples.len());
            return vec![];
        }

        self.since_scan += samples.len();
        if self.since_scan < self.scan_every { return vec![]; }
        self.since_scan = 0;

        // Need at least 0.5s of audio before scanning
        if self.ring.len() < SAMPLE_RATE as usize / 2 { return vec![]; }

        // Scan only the most recent 0.5s — real pings are short, using the
        // full 2s ring dilutes the CCF and buries weak pings in the noise floor
        let scan_len = (SAMPLE_RATE_F * 0.5) as usize;
        let data = self.ring.last_n(scan_len);

        match self.scan(&data) {
            Some(ping) => {
                // Short cooldown: 300ms — just enough to avoid re-triggering
                // on the same ping, but not long enough to miss the next one
                self.cooldown = (SAMPLE_RATE_F * 0.3) as usize;
                vec![ping]
            }
            None => vec![],
        }
    }

    fn scan(&mut self, data: &[f32]) -> Option<DetectedPing> {
        if is_bad_data(data) { return None; }

        let npts = data.len();
        let nfft = next_pow2(npts).max(4096);
        let df1  = SAMPLE_RATE_F / nfft as f32;

        let (analytic, spectrum) = analytic_and_spectrum(data, nfft, &mut self.planner);

        let tol_bins = (self.dftol as f32 / df1).round() as i32;
        let mut ccf_max = 0.0f32;
        let mut best_offset_bin = 0i32;

        for offset_bin in -tol_bins..=tol_bins {
            let mut sum = 0.0f32;
            for &tone_hz in &TONES {
                let bin = ((tone_hz + offset_bin as f32 * df1) / df1).round() as usize;
                if bin < spectrum.len() { sum += spectrum[bin]; }
            }
            if sum > ccf_max {
                ccf_max         = sum;
                best_offset_bin = offset_bin;
            }
        }

        let baseline = spectrum_median(&spectrum, best_offset_bin, 220.0, df1);
        if baseline <= 0.0 { return None; }

        let ccf_ratio = ccf_max / baseline;
        if ccf_ratio < self.threshold { return None; }

        let df_hz = best_offset_bin as f32 * df1;
        let (start, length) = ping_bounds(&analytic, npts);
        if length < NSPD * 3 { return None; }

        let audio       = data[start..start + length].to_vec();
        let duration_ms = length as f32 * 1000.0 / SAMPLE_RATE_F;

        log::debug!("[DET] CCF={:.2} DF={:+.0}Hz dur={:.0}ms", ccf_ratio, df_hz, duration_ms);

        Some(DetectedPing {
            timestamp: Utc::now(),
            n_samples: length,
            audio,
            df_hz,
            ccf_ratio,
            duration_ms,
        })
    }
}

fn analytic_and_spectrum(
    data:    &[f32],
    nfft:    usize,
    planner: &mut FftPlanner<f32>,
) -> (Vec<Complex<f32>>, Vec<f32>) {
    let nh  = nfft / 2;
    let fac = 2.0f32 / nfft as f32;

    let mut buf: Vec<Complex<f32>> = data.iter()
        .map(|&x| Complex::<f32>::new(fac * x, 0.0))
        .chain(std::iter::repeat(Complex::<f32>::new(0.0, 0.0)))
        .take(nfft)
        .collect();

    planner.plan_fft_forward(nfft).process(&mut buf);

    let spectrum: Vec<f32> = buf[..nh].iter()
        .map(|c: &Complex<f32>| c.norm_sqr())
        .collect();

    buf[0] = Complex::<f32>::new(0.0, 0.0);
    for c in &mut buf[nh + 1..] { *c = Complex::<f32>::new(0.0, 0.0); }

    planner.plan_fft_inverse(nfft).process(&mut buf);

    (buf[..data.len()].to_vec(), spectrum)
}

fn is_bad_data(data: &[f32]) -> bool {
    let block    = 1200usize;
    let n_blocks = data.len() / block;
    if n_blocks < 2 { return false; }

    let mut s_min = f32::INFINITY;
    let mut s_max = 0.0f32;

    for b in 0..n_blocks {
        let rms = (data[b*block..(b+1)*block]
            .iter().map(|&x| x * x).sum::<f32>() / block as f32).sqrt();
        s_min = s_min.min(rms);
        s_max = s_max.max(rms);
    }

    s_min <= 0.0 || (s_max / s_min) > 1e6
}

fn spectrum_median(spectrum: &[f32], peak_bin: i32, range_hz: f32, df1: f32) -> f32 {
    let ic = (range_hz / df1).round() as i32;
    let lo = (peak_bin - ic).max(0) as usize;
    let hi = ((peak_bin + ic + 1) as usize).min(spectrum.len());
    if lo >= hi { return 1.0; }

    let mut vals: Vec<f32> = spectrum[lo..hi].to_vec();
    vals.sort_by(|a: &f32, b: &f32| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    vals[vals.len() / 2]
}

fn ping_bounds(analytic: &[Complex<f32>], npts: usize) -> (usize, usize) {
    let block    = (SAMPLE_RATE_F * 0.010) as usize;
    if npts < block * 4 { return (0, npts); }

    let n_blocks = npts / block;
    let energies: Vec<f32> = (0..n_blocks).map(|b| {
        analytic[b*block..(b+1)*block]
            .iter()
            .map(|c: &Complex<f32>| c.norm_sqr())
            .sum::<f32>()
    }).collect();

    let mut sorted: Vec<f32> = energies.clone();
    sorted.sort_by(|a: &f32, b: &f32| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let thresh = sorted[sorted.len() / 2] * 3.0;

    let start_blk = energies.iter().position(|&e| e > thresh).unwrap_or(0);
    let end_blk   = energies.iter().rposition(|&e| e > thresh)
        .map(|i| i + 1).unwrap_or(n_blocks);

    let start = start_blk.saturating_sub(1) * block;
    let end   = ((end_blk + 2) * block).min(npts);
    (start, end - start)
}

fn next_pow2(n: usize) -> usize {
    let mut p = 1usize;
    while p < n { p <<= 1; }
    p
}

pub async fn run_detector(
    mut audio_rx: mpsc::UnboundedReceiver<Vec<f32>>,
    ping_tx:      mpsc::UnboundedSender<DetectedPing>,
    threshold:    f32,
    dftol:        i32,
) {
    let mut detector      = PingDetector::new(threshold, dftol);
    let mut total_samples = 0u64;

    while let Some(chunk) = audio_rx.recv().await {
        total_samples += chunk.len() as u64;
        for ping in detector.push_samples(&chunk) {
            if ping_tx.send(ping).is_err() { return; }
        }
        if total_samples % (SAMPLE_RATE as u64 * 60) < chunk.len() as u64 {
            log::info!("[DET] {:.0}s processed", total_samples as f32 / SAMPLE_RATE_F);
        }
    }
}
