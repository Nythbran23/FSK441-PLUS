// src/fsk441rx/tx.rs

use std::f32::consts::TAU;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::params::*;
use cpal::traits::{DeviceTrait, HostTrait};

/// Output sample rate — Windows USB devices reject 11025Hz, need 48000Hz.
/// macOS/Linux work fine at SAMPLE_RATE directly.
#[cfg(target_os = "windows")]
const OUTPUT_SAMPLE_RATE: u32 = 48_000;
#[cfg(not(target_os = "windows"))]
const OUTPUT_SAMPLE_RATE: u32 = SAMPLE_RATE;

/// Upsample from SAMPLE_RATE (11025) to OUTPUT_SAMPLE_RATE (48000) on Windows.
/// On macOS/Linux this is a no-op since OUTPUT_SAMPLE_RATE == SAMPLE_RATE.
fn upsample(samples: &[f32]) -> Vec<f32> {
    if OUTPUT_SAMPLE_RATE == SAMPLE_RATE { return samples.to_vec(); }
    let ratio = OUTPUT_SAMPLE_RATE as f64 / SAMPLE_RATE as f64;
    let out_len = (samples.len() as f64 * ratio).ceil() as usize;
    let mut out = Vec::with_capacity(out_len);
    for i in 0..out_len {
        let src = i as f64 / ratio;
        let idx = src as usize;
        let frac = (src - idx as f64) as f32;
        let a = samples.get(idx).copied().unwrap_or(0.0);
        let b = samples.get(idx + 1).copied().unwrap_or(0.0);
        out.push(a + frac * (b - a));
    }
    out
}

// ─── FSK441 encoding ─────────────────────────────────────────────────────────

fn char_to_dits(c: char) -> Option<(u8, u8, u8)> {
    let uc = c.to_ascii_uppercase() as u8;
    let nc = CHARSET.iter().position(|&b| b == uc)?;
    Some(((nc / 16) as u8, ((nc / 4) % 4) as u8, (nc % 4) as u8))
}

pub fn encode_message(msg: &str) -> Vec<u8> {
    let mut t = Vec::with_capacity(msg.len() * 3);
    for c in msg.chars() {
        let (d0, d1, d2) = char_to_dits(c).unwrap_or((0, 0, 0));
        t.push(d0); t.push(d1); t.push(d2);
    }
    t
}

/// Generate FSK441 audio at `device_rate` Hz with exact 441 Hz baud rate.
///
/// At 48000 Hz, 441 baud requires 108.843... samples per dit — not an integer.
/// Rounding to 109 gives 440.37 Hz baud (0.14% error). This is harmless for
/// informal use but MSHV's chk441 cross-correlates against an exact 441 Hz
/// reference; the phase drift over a message (~2 samples at 60 dits) reduces
/// the correlation score below the auto-decode threshold (0.75) while still
/// passing manual decode (0.60).
///
/// Fix: fractional sample accumulation. Each dit gets floor(carry) or
/// ceil(carry) samples so the average baud rate is exactly 441 Hz regardless
/// of device rate. The tones themselves are always at exact FSK441 frequencies
/// (882, 1323, 1764, 2205 Hz) via continuous phase accumulation.
pub fn gen441_at_rate(tones: &[u8], device_rate: u32) -> Vec<f32> {
    let dt = 1.0_f64 / device_rate as f64;
    // Exact dit duration at 441 baud
    let samples_per_dit = device_rate as f64 / 441.0_f64;
    const BASE_AMPLITUDE: f32 = 0.612;
    let mut s = Vec::with_capacity((tones.len() as f64 * samples_per_dit) as usize + 2);
    let mut phi   = 0.0_f64;
    let mut carry = 0.0_f64;  // accumulated fractional samples

    for &ti in tones {
        let freq = tone_freq(ti as usize) as f64;
        let dpha = std::f64::consts::TAU * freq * dt;

        // Accumulate and take the integer part — alternates between floor and
        // ceiling to keep average exactly at 441 baud
        carry += samples_per_dit;
        let n = carry as usize;   // floor
        carry -= n as f64;        // keep fractional remainder for next dit

        for _ in 0..n {
            phi += dpha;
            s.push((phi.sin() as f32) * BASE_AMPLITUDE);
        }
    }
    s
}

pub fn gen441(tones: &[u8]) -> Vec<f32> {
    let dt = 1.0_f32 / SAMPLE_RATE_F;
    // Baseline amplitude matches MSHV's default TX level (vol_win ≈ 0.612 at slider=95/100)
    // This ensures the slider's 100% position gives the same output level as MSHV defaults.
    // The tx_level slider (0.0–1.0) scales from silence up to this baseline.
    const BASE_AMPLITUDE: f32 = 0.612;
    let mut s = Vec::with_capacity(tones.len() * NSPD);
    let mut phi = 0.0f32;
    for &ti in tones {
        let dpha = TAU * tone_freq(ti as usize) * dt;
        for _ in 0..NSPD {
            phi += dpha;
            if phi > TAU { phi -= TAU; }
            s.push(phi.sin() * BASE_AMPLITUDE);
        }
    }
    s
}

pub fn message_to_audio(msg: &str) -> Vec<f32> { gen441(&encode_message(msg)) }

// ─── Device name helpers — copied from MSK2K parse_device_suffix ─────────────

fn parse_device_suffix(display_name: &str) -> (String, String) {
    if let Some(pos) = display_name.rfind(" (") {
        if display_name.ends_with(')') {
            let s = display_name[pos+2..display_name.len()-1].trim();
            if s == "RX" || s == "TX" || s == "RX/TX" || s.chars().all(|c| c.is_ascii_digit()) {
                return (display_name[..pos].to_string(), s.to_string());
            }
        }
    }
    (display_name.to_string(), String::new())
}

/// Resolve input device for capture — substring match via cpal
#[allow(dead_code)]
pub fn find_input_device(display_name: &str) -> Option<cpal::Device> {
    use cpal::traits::HostTrait;
    let (base, _) = parse_device_suffix(display_name);
    let host = cpal::default_host();
    if let Ok(devs) = host.input_devices() {
        for d in devs {
            if let Ok(name) = d.name() {
                if name.contains(&base) || name.contains(display_name) { return Some(d); }
            }
        }
    }
    host.default_input_device()
}

/// Find output device by name.
///
/// On macOS the IC-9700 USB Audio CODEC TX device reports has_out=false via cpal
/// capability checks, but build_output_stream() succeeds. We use host.devices()
/// WITHOUT capability filtering as the fallback so the correct device is found.
/// Then msk2k_audio::AudioOutput::start() tries build_output_stream() directly.
/// Query the actual output sample rate of the named device.
/// Prefers 44100Hz (exact 4× multiple of 11025Hz FSK441 base rate, zero baud error).
/// Falls back to device default, then OUTPUT_SAMPLE_RATE.
/// Log all supported sample rates and formats for a named device.
/// Called at startup so the log shows exactly what the codec supports.
pub fn probe_device(display_name: &str) {
    use cpal::traits::{DeviceTrait, HostTrait};
    let host = cpal::default_host();
    log::info!("[PROBE] Probing audio device: '{}'", display_name);
    let (base, _) = parse_device_suffix(display_name);
    let mut found = false;
    if let Ok(devs) = host.devices() {
        for d in devs {
            let name = d.name().unwrap_or_default();
            if !name.contains(&base) && !base.contains(&name) { continue; }
            found = true;
            log::info!("[PROBE] Device: '{}'", name);
            if let Ok(cfgs) = d.supported_output_configs() {
                let cfgs: Vec<_> = cfgs.collect();
                if cfgs.is_empty() {
                    log::info!("[PROBE]   OUTPUT: none");
                }
                for c in &cfgs {
                    log::info!("[PROBE]   OUTPUT ch={} rate={}-{} fmt={:?} buf={:?}",
                        c.channels(), c.min_sample_rate().0,
                        c.max_sample_rate().0, c.sample_format(),
                        c.buffer_size());
                }
            }
            if let Ok(cfgs) = d.supported_input_configs() {
                let cfgs: Vec<_> = cfgs.collect();
                if cfgs.is_empty() {
                    log::info!("[PROBE]   INPUT: none");
                }
                for c in &cfgs {
                    log::info!("[PROBE]   INPUT  ch={} rate={}-{} fmt={:?} buf={:?}",
                        c.channels(), c.min_sample_rate().0,
                        c.max_sample_rate().0, c.sample_format(),
                        c.buffer_size());
                }
            }
            if let Ok(c) = d.default_output_config() {
                log::info!("[PROBE]   Default OUT: ch={} rate={} fmt={:?}",
                    c.channels(), c.sample_rate().0, c.sample_format());
            }
            if let Ok(c) = d.default_input_config() {
                log::info!("[PROBE]   Default IN:  ch={} rate={} fmt={:?}",
                    c.channels(), c.sample_rate().0, c.sample_format());
            }
        }
    }
    if !found {
        log::warn!("[PROBE] Device '{}' not found", display_name);
    }
}

fn get_device_rate(device_name: Option<&str>) -> u32 {
    use cpal::traits::{DeviceTrait, HostTrait};
    let device = if let Some(name) = device_name {
        find_output_device(name)
    } else {
        cpal::default_host().default_output_device()
    };

    let device = match device {
        Some(d) => d,
        None => {
            log::warn!("[TX] get_device_rate: no device — defaulting to {}", OUTPUT_SAMPLE_RATE);
            return OUTPUT_SAMPLE_RATE;
        }
    };

    // Preferred rates in order — first one that successfully opens a stream wins.
    // We do NOT rely on supported_output_configs() — it returns empty on macOS
    // CoreAudio and is unreliable on Windows WASAPI for USB audio devices.
    // CoreAudio and WASAPI both resample transparently to hardware rate, so
    // requesting 11025 or 44100 Hz works even if the hardware runs at 48000 Hz.
    let candidates: &[u32] = &[11025, 44100, 48000, 22050, 32000];

    for &rate in candidates {
        let config = cpal::StreamConfig {
            channels:    1,
            sample_rate: cpal::SampleRate(rate),
            buffer_size: cpal::BufferSize::Default,
        };
        let test = device.build_output_stream(
            &config,
            |out: &mut [f32], _| { out.fill(0.0); },
            |_| {},
            None,
        );
        match test {
            Ok(_stream) => {
                drop(_stream);
                log::info!("[TX] Device rate probe: {}Hz accepted ✓ (zero baud error: {})",
                    rate, rate == 11025 || rate == 44100);
                return rate;
            }
            Err(e) => {
                log::debug!("[TX] Device rate probe: {}Hz rejected ({})", rate, e);
            }
        }
    }

    log::warn!("[TX] All rate probes failed — defaulting to {}", OUTPUT_SAMPLE_RATE);
    OUTPUT_SAMPLE_RATE
}

fn find_output_device(display_name: &str) -> Option<cpal::Device> {
    #[allow(unused_imports)]
    let (base, _) = parse_device_suffix(display_name);
    let host = cpal::default_host();

    // 1. Standard path: output_devices() with substring match (MSK2K approach)
    if let Ok(devs) = host.output_devices() {
        for d in devs {
            let name_res: Result<String, _> = d.name();
            if let Ok(n) = name_res {
                if n.contains(base.as_str()) || base.contains(n.as_str()) {
                    log::info!("[TX] Output device via output_devices(): '{}'", n);
                    return Some(d);
                }
            }
        }
    }

    // 2. Fallback: host.devices() WITHOUT has_out check.
    //    IC-9700 TX shows has_out=false from cpal but build_output_stream works.
    if let Ok(devs) = host.devices() {
        for d in devs {
            let name_res: Result<String, _> = d.name();
            if let Ok(n) = name_res {
                if n.contains(base.as_str()) || base.contains(n.as_str()) {
                    log::warn!("[TX] Output device via host.devices() (no cap check): '{}'", n);
                    return Some(d);
                }
            }
        }
    }

    log::error!("[TX] Cannot find output device '{}'", display_name);
    None
}

// ─── Audio playback ───────────────────────────────────────────────────────────

/// Open a persistent CPAL output stream on a dedicated std::thread.
/// Mirrors the MSK2K pattern: stream lives for the engine lifetime, never closed between TX slots.
/// Eliminates CoreAudio/WASAPI teardown latency (~2s on USB devices).
fn open_persistent_output_stream(
    device_name: Option<String>,
) -> Option<std::sync::mpsc::SyncSender<Vec<f32>>> {
    let (cmd_tx, cmd_rx) = std::sync::mpsc::sync_channel::<Vec<f32>>(4);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<bool>();

    // Query device rate before spawning — persistent stream uses native rate
    let stream_rate = get_device_rate(device_name.as_deref());

    std::thread::Builder::new()
        .name("audio-output".into())
        .spawn(move || {
            use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
            let host = cpal::default_host();
            let device = if let Some(ref name) = device_name {
                find_output_device(name).or_else(|| host.default_output_device())
            } else {
                host.default_output_device()
            };
            let device = match device {
                Some(d) => d,
                None => {
                    log::error!("[AUDIO] Persistent stream: no output device");
                    let _ = ready_tx.send(false);
                    return;
                }
            };

            log::info!("[AUDIO] Persistent stream on '{}' @ {}Hz",
                device.name().unwrap_or_default(), stream_rate);

            let requested = cpal::StreamConfig {
                channels: 1,
                sample_rate: cpal::SampleRate(stream_rate),
                buffer_size: cpal::BufferSize::Fixed(AUDIO_BUFFER_SIZE as u32),
            };
            let (stream_config, channels) = {
                let test = device.build_output_stream(&requested, |_: &mut [f32], _| {}, |_| {}, None);
                if test.is_ok() {
                    drop(test);
                    log::info!("[AUDIO] Using 1ch Fixed at {}Hz", stream_rate);
                    (requested, 1usize)
                } else {
                    let def = device.default_output_config().unwrap();
                    let ch = def.channels() as usize;
                    let fallback = cpal::StreamConfig {
                        channels: ch as u16,
                        sample_rate: cpal::SampleRate(stream_rate),
                        buffer_size: cpal::BufferSize::Default,
                    };
                    log::info!("[AUDIO] Falling back to {}ch Default at {}Hz", ch, stream_rate);
                    (fallback, ch)
                }
            };

            let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::<f32>::new()));
            let buf2 = buf.clone();

            let stream = match device.build_output_stream(
                &stream_config,
                move |out: &mut [f32], _| {
                    let mut b = buf2.lock().unwrap();
                    let frames = out.len() / channels;
                    let copy = frames.min(b.len());
                    if channels == 1 {
                        out[..copy].copy_from_slice(&b[..copy]);
                        b.drain(..copy);
                        out[copy..].fill(0.0);
                    } else {
                        for i in 0..copy {
                            let s = b[i];
                            for ch in 0..channels {
                                out[i * channels + ch] = if ch == 0 { s } else { 0.0 };
                            }
                        }
                        b.drain(..copy);
                        out[copy * channels..].fill(0.0);
                    }
                },
                |e| log::error!("[AUDIO] Stream error: {}", e),
                None,
            ) {
                Ok(s) => s,
                Err(e) => {
                    log::error!("[AUDIO] build_output_stream failed: {}", e);
                    let _ = ready_tx.send(false);
                    return;
                }
            };

            if let Err(e) = stream.play() {
                log::error!("[AUDIO] stream.play failed: {}", e);
                let _ = ready_tx.send(false);
                return;
            }

            log::info!("[AUDIO] Persistent output stream running");
            let _ = ready_tx.send(true);

            loop {
                match cmd_rx.recv() {
                    Ok(samples) => {
                        if samples.is_empty() {
                            log::info!("[AUDIO] Clear command — draining buffer");
                            buf.lock().unwrap().clear();
                        } else {
                            let n = samples.len();
                            buf.lock().unwrap().extend_from_slice(&samples);
                            log::info!("[AUDIO] Waveform: {} samples queued", n);
                        }
                    }
                    Err(_) => {
                        log::info!("[AUDIO] Persistent stream shutting down");
                        break;
                    }
                }
            }
            drop(stream);
        })
        .ok()?;

    match ready_rx.recv() {
        Ok(true) => {
            log::info!("[AUDIO] Persistent output stream ready");
            Some(cmd_tx)
        }
        _ => {
            log::error!("[AUDIO] Persistent output stream failed to start");
            None
        }
    }
}

/// Toggle RTS or DTR line on a serial port for hardware PTT
fn set_serial_ptt(port_name: &str, method: &PttMethod, active: bool) {
    match serialport::new(port_name, 9600)
        .timeout(std::time::Duration::from_millis(100))
        .open()
    {
        Ok(mut port) => {
            let result = match method {
                PttMethod::RtsPort => port.write_request_to_send(active),
                PttMethod::DtrPort => port.write_data_terminal_ready(active),
                _ => Ok(()),
            };
            if let Err(e) = result {
                log::error!("[PTT] Serial PTT error on {}: {}", port_name, e);
            } else {
                log::info!("[PTT] Serial PTT {} on {} ({})",
                    if active { "ON" } else { "OFF" },
                    port_name,
                    match method { PttMethod::RtsPort => "RTS", _ => "DTR" });
            }
        }
        Err(e) => log::error!("[PTT] Cannot open serial port {}: {}", port_name, e),
    }
}

fn play_audio_blocking(samples: Vec<f32>, device_name: Option<String>, cancel: std::sync::Arc<std::sync::atomic::AtomicBool>) {
    let n = samples.len();

    let device = if let Some(ref name) = device_name {
        find_output_device(name)
    } else {
        use cpal::traits::HostTrait;
        cpal::default_host().default_output_device()
    };

    let device = match device {
        Some(d) => d,
        None => {
            use cpal::traits::HostTrait;
            log::warn!("[TX] Using system default output device");
            match cpal::default_host().default_output_device() {
                Some(d) => d,
                None => { log::error!("[TX] No output device"); return; }
            }
        }
    };

    log::info!("[TX] Playing on: '{}'", device.name().unwrap_or_default());

    use cpal::traits::{DeviceTrait, StreamTrait};

    // Get device native rate
    let device_rate = device.default_output_config()
        .map(|c| c.sample_rate().0)
        .unwrap_or(OUTPUT_SAMPLE_RATE);
    log::info!("[TX] Stream at {}Hz ({} samples = {:.1}s)", device_rate, n, n as f32 / device_rate as f32);

    // Build stream config — try 1ch Fixed first, fall back to device default
    let requested = cpal::StreamConfig {
        channels: 1,
        sample_rate: cpal::SampleRate(device_rate),
        buffer_size: cpal::BufferSize::Fixed(AUDIO_BUFFER_SIZE as u32),
    };
    let (stream_config, channels) = {
        let test = device.build_output_stream(&requested, |_: &mut [f32], _| {}, |_| {}, None);
        if test.is_ok() {
            drop(test);
            (requested, 1usize)
        } else {
            let def = device.default_output_config().unwrap();
            let ch = def.channels() as usize;
            let fallback = cpal::StreamConfig {
                channels: ch as u16,
                sample_rate: cpal::SampleRate(device_rate),
                buffer_size: cpal::BufferSize::Default,
            };
            log::info!("[TX] Falling back to {}ch Default at {}Hz", ch, device_rate);
            (fallback, ch)
        }
    };
    log::info!("[TX] play_audio_blocking: {}ch {}Hz {} samples", channels, device_rate, n);

    // Channel-fed callback — MSK2K pattern.
    // Sender holds the sample data; dropping sender signals end-of-audio.
    // Callback serves silence once channel is exhausted — no abrupt teardown.
    let (audio_tx, audio_rx) = std::sync::mpsc::sync_channel::<Vec<f32>>(2);

    // Keep a clone for the i16 fallback path — only used if f32 output fails
    let samples_for_i16 = samples.clone();

    // Send samples in chunks matching the buffer size so the channel stays responsive
    {
        let audio_tx2 = audio_tx.clone();
        std::thread::spawn(move || {
            for chunk in samples.chunks(AUDIO_BUFFER_SIZE as usize) {
                if audio_tx2.send(chunk.to_vec()).is_err() { break; }
            }
            // Drop audio_tx2 here — signals end of audio to callback
        });
    }
    // Drop our copy of sender — only the spawned thread holds one
    drop(audio_tx);

    let stream = match device.build_output_stream(
        &stream_config,
        {
            let mut remainder: Vec<f32> = Vec::new();
            move |out: &mut [f32], _| {
                let frames = out.len() / channels;
                let mut filled = 0;
                while filled < frames {
                    if remainder.is_empty() {
                        match audio_rx.try_recv() {
                            Ok(chunk) => remainder = chunk,
                            Err(_) => break,
                        }
                    }
                    let take = (frames - filled).min(remainder.len());
                    if channels == 1 {
                        out[filled..filled + take].copy_from_slice(&remainder[..take]);
                    } else {
                        for i in 0..take {
                            let s = remainder[i];
                            for ch in 0..channels {
                                out[(filled + i) * channels + ch] = if ch == 0 { s } else { 0.0 };
                            }
                        }
                    }
                    remainder.drain(..take);
                    filled += take;
                }
                out[filled * channels..].fill(0.0);
            }
        },
        |e| log::error!("[TX] Output error: {}", e),
        None,
    ) {
        Ok(s) => s,
        Err(e) => {
            // f32 output rejected — try i16 (IC-7300/Linux ALSA S16_LE only)
            log::warn!("[TX] f32 output failed ({}), trying i16 format", e);
            let (audio_tx2, audio_rx2) = std::sync::mpsc::sync_channel::<Vec<f32>>(2);
            {
                let samples2 = samples_for_i16;
                std::thread::spawn(move || {
                    for chunk in samples2.chunks(AUDIO_BUFFER_SIZE as usize) {
                        if audio_tx2.send(chunk.to_vec()).is_err() { break; }
                    }
                });
            }
            match device.build_output_stream(
                &stream_config,
                {
                    let mut remainder: Vec<f32> = Vec::new();
                    move |out: &mut [i16], _| {
                        let frames = out.len() / channels;
                        let mut filled = 0;
                        while filled < frames {
                            if remainder.is_empty() {
                                match audio_rx2.try_recv() {
                                    Ok(chunk) => remainder = chunk,
                                    Err(_) => break,
                                }
                            }
                            let take = (frames - filled).min(remainder.len());
                            for i in 0..take {
                                let s = (remainder[i].clamp(-1.0, 1.0) * 32767.0) as i16;
                                for ch in 0..channels {
                                    // Write same audio to all channels — IC-7300 may use
                                    // either or both channels for USB modulation input
                                    out[(filled + i) * channels + ch] = s;
                                }
                            }
                            remainder.drain(..take);
                            filled += take;
                        }
                        for i in filled * channels..out.len() { out[i] = 0; }
                    }
                },
                |e| log::error!("[TX] Output error (i16): {}", e),
                None,
            ) {
                Ok(s) => { log::info!("[TX] Output stream opened with i16 format"); s }
                Err(e2) => { log::error!("[TX] build_output_stream i16: {}", e2); return; }
            }
        }
    };

    if let Err(e) = stream.play() {
        log::error!("[TX] stream.play: {}", e); return;
    }

    // Wait for slot to complete or cancel
    let total_ms = n as u64 * 1000 / device_rate as u64 + 500;
    log::info!("[TX] Playing {} samples ({:.1}s)", n, n as f32 / device_rate as f32);
    let mut elapsed = 0u64;
    while elapsed < total_ms {
        if cancel.load(std::sync::atomic::Ordering::Relaxed) {
            log::info!("[TX] Cancelled after {}ms", elapsed);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        elapsed += 50;
    }
    // Stream drains naturally — channel sender already dropped, callback serves silence.
    // drop(stream) closes hardware after callback has finished serving remaining samples.
    drop(stream);
    log::info!("[TX] play_audio_blocking done");
}

// ─── Period timer ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Period { TxFirst, TxSecond, TxFirst15, TxSecond15 }

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SlotState { Tx, Rx }

fn utc_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

fn slot_len(period: Period) -> i64 {
    match period { Period::TxFirst15 | Period::TxSecond15 => 15_000, _ => 30_000 }
}
fn slot_idx_p(ms: i64, period: Period) -> i64 { ms / slot_len(period) }
fn slot_par_p(ms: i64, period: Period) -> u8  { (slot_idx_p(ms, period) % 2) as u8 }
#[allow(dead_code)]
fn slot_idx(ms: i64) -> i64 { ms / 30_000 }
#[allow(dead_code)]
fn slot_par(ms: i64) -> u8  { (slot_idx(ms) % 2) as u8 }

pub struct PeriodTimer { pub period: Period }

impl PeriodTimer {
    pub fn new(period: Period) -> Self { Self { period } }

    pub fn current_slot(&self) -> (SlotState, u32) {
        let now    = utc_ms();
        let slen   = slot_len(self.period) as u32;
        let par    = slot_par_p(now, self.period);
        let tx_par = match self.period {
            Period::TxFirst | Period::TxFirst15   => 0,
            Period::TxSecond | Period::TxSecond15 => 1,
        };
        let in_tx = par == tx_par;
        let ms_in = (now % slen as i64) as u32;
        let rem   = (slen - ms_in) / 1000 + 1;
        (if in_tx { SlotState::Tx } else { SlotState::Rx }, rem)
    }
}

// ─── TX commands ─────────────────────────────────────────────────────────────

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum PttMethod {
    CatHamlib,
    RtsPort,
    DtrPort,
    Vox,
}

impl PttMethod {
    pub fn label(&self) -> &str {
        match self {
            PttMethod::CatHamlib => "CAT (Hamlib)",
            PttMethod::RtsPort   => "RTS (Serial)",
            PttMethod::DtrPort   => "DTR (Serial)",
            PttMethod::Vox       => "VOX",
        }
    }
    pub fn to_str(&self) -> &str {
        match self {
            PttMethod::CatHamlib => "cat",
            PttMethod::RtsPort   => "rts",
            PttMethod::DtrPort   => "dtr",
            PttMethod::Vox       => "vox",
        }
    }
    pub fn from_str(s: &str) -> Self {
        match s {
            "rts" => PttMethod::RtsPort,
            "dtr" => PttMethod::DtrPort,
            "vox" => PttMethod::Vox,
            _     => PttMethod::CatHamlib,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TxCommand {
    Transmit      { message: String, output_device: Option<String> },
    #[allow(dead_code)]
    TransmitN     { message: String, times: u8, output_device: Option<String> },
    SetVolume     (f32),   // update TX level immediately — takes effect on next waveform build
    ReArm,                 // clear halted flag — sent by send_tx before Transmit
    Halt,
    SetPeriod     (Period),
    SetOutputDevice(Option<String>),
    SetHamlib     (Option<String>),
    SetPttMethod  (PttMethod, Option<String>),  // method + optional port
    #[allow(dead_code)]
    ClearAccumulator,
    SetFreq       (u64),
}

// ─── Hamlib client — full port of MSK2K src/engine/hamlib.rs ─────────────────

#[derive(Debug)]
pub enum HamlibCmd { Ptt(bool), GetFreq, SetFreq(u64) }

#[derive(Debug, Clone)]
pub struct HamlibUpdate {
    pub freq:         Option<u64>,
    pub connected:    Option<bool>,  // None = no change
    pub transmitting: bool,
}

pub struct HamlibClient { cmd_tx: mpsc::UnboundedSender<HamlibCmd> }

impl HamlibClient {
    pub fn new(address: String, update_tx: mpsc::UnboundedSender<HamlibUpdate>) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move { run_hamlib_client(address, cmd_rx, update_tx).await; });
        Self { cmd_tx }
    }
    pub fn set_ptt(&self, active: bool)  { let _ = self.cmd_tx.send(HamlibCmd::Ptt(active)); }
    pub fn get_freq(&self)               { let _ = self.cmd_tx.send(HamlibCmd::GetFreq); }
    pub fn set_freq(&self, hz: u64)      { let _ = self.cmd_tx.send(HamlibCmd::SetFreq(hz)); }
}

async fn run_hamlib_client(
    addr: String,
    mut cmd_rx: mpsc::UnboundedReceiver<HamlibCmd>,
    update_tx: mpsc::UnboundedSender<HamlibUpdate>,
) {
    loop {
        match TcpStream::connect(&addr).await {
            Ok(mut stream) => {
                log::info!("[Hamlib] Connected to rigctld at {}", addr);
                let _ = update_tx.send(HamlibUpdate { freq: None, connected: Some(true), transmitting: false });
                let (reader, mut writer) = stream.split();
                let mut reader = BufReader::new(reader);
                let mut buf = String::new();

                if let Err(_) = writer.write_all(b"f\n").await { continue; }

                loop {
                    tokio::select! {
                        cmd_opt = cmd_rx.recv() => {
                            let cmd = match cmd_opt {
                                Some(c) => c,
                                None => {
                                    log::info!("[Hamlib] Channel closed");
                                    return;
                                }
                            };

                            // SetFreq needs a formatted string — handle separately
                            if let HamlibCmd::SetFreq(hz) = cmd {
                                let cmd_str = format!("F {}\n", hz);
                                log::info!("[Hamlib] → SetFreq {}", hz);
                                if let Err(_) = writer.write_all(cmd_str.as_bytes()).await {
                                    let _ = update_tx.send(HamlibUpdate { freq: None, connected: Some(false), transmitting: false });
                                    break;
                                }
                                buf.clear();
                                let _ = reader.read_line(&mut buf).await;
                                // After set, read back freq to confirm
                                if let Err(_) = writer.write_all(b"f\n").await { break; }
                                buf.clear();
                                if let Ok(_) = reader.read_line(&mut buf).await {
                                    if let Ok(f) = buf.trim().parse::<u64>() {
                                        let _ = update_tx.send(HamlibUpdate { freq: Some(f), connected: Some(true), transmitting: false });
                                    }
                                }
                                continue;
                            }
                            let cmd_str = match cmd {
                                HamlibCmd::Ptt(true)  => "T 1\n",
                                HamlibCmd::Ptt(false) => "T 0\n",
                                HamlibCmd::GetFreq    => "f\n",
                                HamlibCmd::SetFreq(_) => unreachable!(),
                            };

                            log::debug!("[Hamlib] → {}", cmd_str.trim());
                            if let Err(_) = writer.write_all(cmd_str.as_bytes()).await {
                                let _ = update_tx.send(HamlibUpdate { freq: None, connected: Some(false), transmitting: false });
                                break;
                            }

                            if matches!(cmd, HamlibCmd::Ptt(_)) {
                                buf.clear();
                                let _ = reader.read_line(&mut buf).await;
                            } else if matches!(cmd, HamlibCmd::GetFreq) {
                                buf.clear();
                                match reader.read_line(&mut buf).await {
                                    Ok(0) | Err(_) => {
                                        let _ = update_tx.send(HamlibUpdate { freq: None, connected: Some(false), transmitting: false });
                                        break;
                                    }
                                    Ok(_) => {
                                        if let Ok(f) = buf.trim().parse::<u64>() {
                                            let _ = update_tx.send(HamlibUpdate { freq: Some(f), connected: Some(true), transmitting: false });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                log::warn!("[Hamlib] Disconnected — retrying in 5s");
            }
            Err(_) => {
                if cmd_rx.is_closed() { return; }
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }
}



// ─── TX Engine ────────────────────────────────────────────────────────────────

pub struct TxEngine {
    pub cmd_rx:            mpsc::UnboundedReceiver<TxCommand>,
    pub period:            Period,
    pub output_device:     Option<String>,
    pub hamlib_addr:       Option<String>,
    pub hamlib_update_tx:  mpsc::UnboundedSender<HamlibUpdate>,
    pub ptt_method:        PttMethod,
    pub ptt_port:          Option<String>,
    pub tx_active:         std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub current_volume:    f32,   // live TX level — updated by SetVolume command
    pub halted:            bool,  // set by Halt — blocks any new Transmit until cleared
    pub device_rate:       u32,   // cached actual output sample rate — queried once at startup
}

impl TxEngine {
    pub fn new(
        cmd_rx:           mpsc::UnboundedReceiver<TxCommand>,
        period:           Period,
        output_device:    Option<String>,
        hamlib_addr:      Option<String>,
        hamlib_update_tx: mpsc::UnboundedSender<HamlibUpdate>,
        tx_active:        std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        // Probe device capabilities at startup — logs all supported rates/formats
        if let Some(ref name) = output_device {
            probe_device(name);
        }
        // Query device rate once at startup — used for all waveform generation
        let device_rate = get_device_rate(output_device.as_deref());
        log::info!("[TX] Output device rate: {}Hz (koef={:.3}x)",
            device_rate, device_rate as f64 / SAMPLE_RATE as f64);
        Self { cmd_rx, period, output_device, hamlib_addr, hamlib_update_tx,
            ptt_method: PttMethod::CatHamlib, ptt_port: None, tx_active,
            current_volume: 1.0, halted: false, device_rate }
    }

    pub async fn run(mut self) {
        let mut tx_par: u8 = match self.period {
            Period::TxFirst | Period::TxFirst15   => 0,
            Period::TxSecond | Period::TxSecond15 => 1,
        };
        let cancel_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        log::info!("[TX] Engine started. Period={:?} tx_par={} device={:?} hamlib={:?}",
            self.period, tx_par, self.output_device, self.hamlib_addr);

        // Start persistent hamlib client (PTT + freq polling)
        let mut hamlib_enabled = self.hamlib_addr.is_some();
        let hamlib: Option<HamlibClient> = self.hamlib_addr.as_ref().map(|addr| {
            HamlibClient::new(addr.clone(), self.hamlib_update_tx.clone())
        });

        // ── Persistent output stream on dedicated thread (macOS + Windows) ────
        // On Linux, ALSA USB audio is half-duplex — input and output cannot both
        // be open on the same device simultaneously. The Linux path uses open/close
        // per-slot (play_audio_blocking fallback) with the input stream stopped
        // at TX start and restarted after TX ends (see run_engine Linux cfg blocks).
        // On macOS/Windows the persistent stream eliminates the ~2s teardown latency.
        #[cfg(not(target_os = "linux"))]
        let persistent_audio_tx: Option<std::sync::mpsc::SyncSender<Vec<f32>>> = {
            open_persistent_output_stream(self.output_device.clone())
        };
        #[cfg(target_os = "linux")]
        let persistent_audio_tx: Option<std::sync::mpsc::SyncSender<Vec<f32>>> = None;

        let mut current: Option<TxCommand> = None;
        let mut tx_count = 0u8;
        let mut last_tx_sidx: Option<i64> = None;
        // When a command arrives mid-slot, queue it for the NEXT slot boundary
        let mut cmd_queued_sidx: Option<i64> = None;
        let mut tick: u64 = 0;

        loop {
            tick += 1;

            // Drain command queue
            while let Ok(cmd) = self.cmd_rx.try_recv() {
                match cmd {
                    TxCommand::Halt => {
                        log::info!("[TX] HALT");
                        self.halted = true;
                        cancel_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                        // Drop PTT and ungate fanout immediately — audio will drain/stop
                        self.tx_active.store(false, std::sync::atomic::Ordering::Relaxed);
                        let _ = self.hamlib_update_tx.send(HamlibUpdate { freq: None, connected: None, transmitting: false });
                        match &self.ptt_method {
                            PttMethod::CatHamlib => {
                                if hamlib_enabled { if let Some(ref h) = hamlib { h.set_ptt(false); } }
                            }
                            PttMethod::RtsPort | PttMethod::DtrPort => {
                                if let Some(ref port) = self.ptt_port {
                                    set_serial_ptt(port, &self.ptt_method, false);
                                }
                            }
                            PttMethod::Vox => {}
                        }
                        current = None; tx_count = 0; last_tx_sidx = None; cmd_queued_sidx = None;
                    }
                    TxCommand::SetPeriod(p) => {
                        log::info!("[TX] Period changed to {:?}", p);
                        self.period = p;
                        tx_par = match p { Period::TxFirst | Period::TxFirst15 => 0, _ => 1 };
                        last_tx_sidx = None;
                    }
                    TxCommand::SetOutputDevice(dev) => {
                        log::info!("[TX] Output device changed to {:?}", dev);
                        self.output_device = dev;
                        if let Some(ref name) = self.output_device {
                            probe_device(name);
                        }
                        // Re-query device rate for new device
                        self.device_rate = get_device_rate(self.output_device.as_deref());
                        log::info!("[TX] New device rate: {}Hz", self.device_rate);
                    }
                    TxCommand::SetHamlib(addr) => {
                        log::info!("[TX] Hamlib {}", if addr.is_some() { "enabled" } else { "disabled" });
                        self.hamlib_addr = addr;
                        hamlib_enabled = self.hamlib_addr.is_some();
                    }
                    TxCommand::SetPttMethod(method, port) => {
                        log::info!("[TX] PTT method changed to {:?} port={:?}", method, port);
                        self.ptt_method = method;
                        self.ptt_port   = port;
                    }
                    // Immediate commands — no slot boundary needed
                    TxCommand::SetFreq(hz) => {
                        if hamlib_enabled {
                            if let Some(ref h) = hamlib { h.set_freq(hz); }
                            log::info!("[TX] SetFreq → {} Hz", hz);
                        }
                    }
                    TxCommand::ReArm => {
                        log::info!("[TX] ReArm — ready to transmit");
                        self.halted = false;
                    }
                    TxCommand::SetVolume(v) => {
                        log::info!("[TX] Volume updated to {:.4} ({:.1}%)", v, v * 100.0);
                        self.current_volume = v;
                    }
                    TxCommand::ClearAccumulator => {
                        // Handled in run_engine — ignore here
                    }
                    other => {
                        // If halted, discard any Transmit that sneaks in after STOP
                        // until the engine is explicitly re-armed by the next user TX press.
                        // SetVolume, SetPeriod etc. still pass through above.
                        if self.halted {
                            log::info!("[TX] Ignoring command after HALT — re-arm by pressing TX");
                            continue;
                        }
                        let arrival_sidx = slot_idx_p(utc_ms(), self.period);
                        current = Some(other);
                        tx_count = 0;
                        cmd_queued_sidx = Some(arrival_sidx);
                        log::info!("[TX] Command queued at sidx={} — waiting for next slot boundary", arrival_sidx);
                    }
                }
            }

            let now    = utc_ms();
            let sidx   = slot_idx_p(now, self.period);
            let par    = slot_par_p(now, self.period);
            let in_tx  = par == tx_par;

            if tick % 10 == 0 {
                let now_s = (utc_ms() / 1000) % 30;
                log::debug!("[TX] sidx={} par={}/{} in_tx={} queued={} last={:?} pos={}s",
                    sidx, par, tx_par, in_tx, current.is_some(), last_tx_sidx, now_s);
            }

            // Freq poll every 2s (tick increments every 200ms, so 10 ticks = 2s)
            if tick % 10 == 0 {
                if hamlib_enabled { if let Some(ref h) = hamlib { h.get_freq(); } }
            }

            // RX slot — wait
            if !in_tx {
                last_tx_sidx = None; // reset so we start fresh next TX slot
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                continue;
            }

            // New TX slot started — reset count so TransmitN works per-slot
            if last_tx_sidx != Some(sidx) {
                last_tx_sidx = Some(sidx);
                tx_count = 0;
            }

            // Don't transmit in the same slot the command arrived — wait for next boundary
            if let Some(queued_sidx) = cmd_queued_sidx {
                if sidx <= queued_sidx {
                    log::debug!("[TX] Waiting for slot boundary (queued={} current={})", queued_sidx, sidx);
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                    continue;
                }
            }

            // Nothing to transmit
            let (message, dev) = match &current {
                None => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                    continue;
                }
                Some(TxCommand::Transmit { message, output_device }) =>
                    (message.clone(), output_device.clone()),
                Some(TxCommand::TransmitN { message, times, output_device, .. }) => {
                    if tx_count >= *times {
                        current = None; tx_count = 0;
                        continue;
                    }
                    (message.clone(), output_device.clone())
                }
                Some(TxCommand::Halt) => {
                    current = None; tx_count = 0;
                    continue;
                }
                // Config/control commands handled in drain loop — clear if somehow queued here
                Some(TxCommand::SetPeriod(_))
                | Some(TxCommand::SetOutputDevice(_))
                | Some(TxCommand::SetHamlib(_))
                | Some(TxCommand::SetPttMethod(_, _))
                | Some(TxCommand::ClearAccumulator)
                | Some(TxCommand::SetFreq(_))
                | Some(TxCommand::SetVolume(_))
                | Some(TxCommand::ReArm) => {
                    current = None;
                    continue;
                }
            };

            // --- TX slot: generate full 30s waveform upfront, send as ONE buffer ---
            // This is exactly what MSK2K does in generate_transmission() — repeat the
            // message to fill the slot length, truncate, send as single Vec<f32>.
            // One stream open, one buffer send, one stream close. No gaps, no keying chatter.
            cancel_flag.store(false, std::sync::atomic::Ordering::Relaxed);
            log::info!("[TX] Building full-slot waveform for sidx={} msg='{}'", sidx, message);
            tx_count += 1;

            // Calculate how many samples remain in this TX slot
            // Always start from the slot boundary — trim to remaining time
            let slot_secs = match self.period {
                Period::TxFirst15 | Period::TxSecond15 => 15usize,
                _ => 30usize,
            };
            let slot_ms    = slot_secs as i64 * 1000;
            let now_ms     = utc_ms();
            let slot_start = (now_ms / slot_ms) * slot_ms;
            let elapsed_ms = (now_ms - slot_start).max(0);
            let remain_ms  = (slot_ms - elapsed_ms).max(500);
            // Use cached device rate — queried once at startup in TxEngine::new()
            let device_rate = self.device_rate;
            let slot_samples = (device_rate as i64 * remain_ms / 1000) as usize;
            log::info!("[TX] Slot: {}s period, {}ms elapsed, {}ms remaining → {} samples @ {}Hz",
                slot_secs, elapsed_ms, remain_ms, slot_samples, device_rate);
            let padded_msg   = format!("{}   ", message);
            let encoded      = encode_message(&padded_msg);
            let one_msg      = gen441_at_rate(&encoded, device_rate);
            let msg_len      = one_msg.len();
            let repeats      = (slot_samples + msg_len - 1) / msg_len;
            let mut waveform: Vec<f32> = Vec::with_capacity(slot_samples);
            for _ in 0..repeats { waveform.extend_from_slice(&one_msg); }
            waveform.truncate(slot_samples);
            // Use self.current_volume — may have been updated by SetVolume since Transmit arrived
            let volume = self.current_volume;
            // Apply TX level — scale samples by operator-configured amplitude (0.0–1.0)
            if (volume - 1.0f32).abs() > 1e-4 {
                for s in &mut waveform { *s *= volume; }
            }

            // ── TX level diagnostics ──────────────────────────────────────────
            let rms_out = {
                let sum: f32 = waveform.iter().map(|&s| s * s).sum();
                (sum / waveform.len() as f32).sqrt()
            };
            let peak_out = waveform.iter().map(|s| s.abs()).fold(0.0f32, f32::max);
            let rms_dbfs  = if rms_out  > 1e-9 { 20.0 * rms_out.log10()  } else { -999.0 };
            let peak_dbfs = if peak_out > 1e-9 { 20.0 * peak_out.log10() } else { -999.0 };
            log::info!(
                "[TX_LEVEL] volume={:.4} ({:.1}%)  rms={:.1}dBFS  peak={:.1}dBFS  samples={}",
                volume, volume * 100.0, rms_dbfs, peak_dbfs, waveform.len()
            );

            log::info!("[TX] Waveform: {} samples ({:.1}s @ {}Hz), {} reps of {}ms",
                waveform.len(), waveform.len() as f32 / device_rate as f32,
                device_rate, repeats, msg_len * 1000 / device_rate as usize);

            // PTT on — write tx_active immediately so run_engine sees it without UI roundtrip
            self.tx_active.store(true, std::sync::atomic::Ordering::Relaxed);
            match &self.ptt_method {
                PttMethod::CatHamlib => {
                    if hamlib_enabled { if let Some(ref h) = hamlib { h.set_ptt(true); } }
                }
                PttMethod::RtsPort | PttMethod::DtrPort => {
                    if let Some(ref port) = self.ptt_port {
                        set_serial_ptt(port, &self.ptt_method, true);
                    }
                }
                PttMethod::Vox => {}
            }
            let _ = self.hamlib_update_tx.send(HamlibUpdate { freq: None, connected: None, transmitting: true });

            // Linux ALSA half-duplex: the RX input stream holds the USB codec device handle.
            // run_engine receives the transmitting=true event above and stops the RX stream,
            // but needs time to process the event and for ALSA to release the handle.
            // Without this delay the TX output stream opens while RX still holds the device,
            // causing rapid open/close chatter and PTT glitches.
            // MSK2K uses the same 500ms settling delay (runtime.rs line ~475).
            #[cfg(target_os = "linux")]
            tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

            let duration_ms = waveform.len() as u64 * 1000 / device_rate as u64;
            let wait_ms = duration_ms + 200; // 200ms tail — samples reach DAC, then PTT drops

            // Play audio — always use persistent stream (all platforms).
            // Persistent stream avoids the ~2s CoreAudio/WASAPI teardown on every TX→RX.
            // Falls back to open/close only if the persistent stream failed to open at startup.
            if let Some(ref audio_tx) = persistent_audio_tx {
                let n_samples = waveform.len();
                let waveform: Vec<f32> = waveform;
                if let Err(e) = audio_tx.send(waveform) {
                    log::error!("[TX] Persistent stream send failed: {}", e);
                } else {
                    log::info!("[TX] Sent {} samples to persistent stream, waiting {}ms",
                        n_samples, wait_ms);
                    let deadline = tokio::time::Instant::now()
                        + tokio::time::Duration::from_millis(wait_ms);
                    tokio::time::sleep_until(deadline).await;
                }
            } else {
                // Fallback: open/close stream (introduces teardown latency)
                log::warn!("[TX] No persistent stream — falling back to open/close");
                let device = dev.or_else(|| self.output_device.clone());
                let cancel_clone = cancel_flag.clone();
                tokio::task::spawn_blocking(move || {
                    play_audio_blocking(waveform, device, cancel_clone);
                }).await.ok();
            }

            // PTT off — clear tx_active immediately after audio completes
            // Note: play_audio_blocking has already returned, meaning all samples
            // have been handed to the OS audio layer. PTT can safely drop now.
            self.tx_active.store(false, std::sync::atomic::Ordering::Relaxed);
            match &self.ptt_method {
                PttMethod::CatHamlib => {
                    if hamlib_enabled { if let Some(ref h) = hamlib { h.set_ptt(false); } }
                }
                PttMethod::RtsPort | PttMethod::DtrPort => {
                    if let Some(ref port) = self.ptt_port {
                        set_serial_ptt(port, &self.ptt_method, false);
                    }
                }
                PttMethod::Vox => {}
            }
            let _ = self.hamlib_update_tx.send(HamlibUpdate { freq: None, connected: None, transmitting: false });
            log::info!("[TX] Done sidx={}", sidx);
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_suffix() {
        assert_eq!(parse_device_suffix("USB Audio CODEC (TX)"),
            ("USB Audio CODEC".into(), "TX".into()));
        assert_eq!(parse_device_suffix("Speakers (2- USB Audio CODEC )"),
            ("Speakers (2- USB Audio CODEC )".into(), String::new()));
    }

    #[test]
    fn encode_roundtrip() {
        let msg = "GW4WND I5YDI IO82 57";
        let t = encode_message(msg);
        assert_eq!(t.len(), msg.len() * 3);
        for i in 0..msg.len() {
            let ch = dits_to_char(t[i*3], t[i*3+1], t[i*3+2]).unwrap_or('?');
            assert_eq!(ch, msg.chars().nth(i).unwrap().to_ascii_uppercase());
        }
    }
}
