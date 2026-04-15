// src/fsk441rx/audio.rs
//
// Audio input pipeline — NO RESAMPLING.
//
// Both WSJT and MSHV capture audio natively at 11025 Hz by opening the
// soundcard directly at that rate. The IC-9700 USB audio codec (Burr-Brown
// PCM2902) supports 11025 Hz natively.
//
// Previously we captured at 48000 Hz and resampled with rubato. That introduced
// a systematic ~-195 Hz frequency offset due to non-integer ratio errors in the
// resampler. Capturing directly at 11025 Hz eliminates this entirely.
//
// Two modes:
//   Live: cpal opens soundcard at 11025 Hz, chunks forwarded directly to detector
//   WAV:  hound reads file, resamples only if file is not already 11025 Hz

use std::path::Path;
use anyhow::{Context, Result};
use tokio::sync::mpsc;

use crate::params::{SAMPLE_RATE, AUDIO_BUFFER_SIZE};
use cpal::traits::{DeviceTrait, HostTrait};

pub type AudioChunk = Vec<f32>;

// ─── Live capture ─────────────────────────────────────────────────────────────

/// Start live audio capture.
/// Returns a stop handle — on Linux, send () to release the ALSA device before TX.
/// On macOS/Windows, the handle is a no-op (dropping it is harmless).
pub async fn start_live(
    device_name: Option<String>,
    tx: tokio::sync::mpsc::UnboundedSender<Vec<f32>>,
) -> anyhow::Result<std::sync::mpsc::SyncSender<()>> {
    let (stop_tx, stop_rx) = std::sync::mpsc::sync_channel::<()>(1);
    start_live_inner(device_name, tx, stop_rx).await?;
    Ok(stop_tx)
}

async fn start_live_inner(
    device_name: Option<String>,
    tx: tokio::sync::mpsc::UnboundedSender<Vec<f32>>,
    stop_rx: std::sync::mpsc::Receiver<()>,
) -> anyhow::Result<()> {

    let device = if let Some(ref name) = device_name {
        // Inline device lookup — avoids cross-module dependency
        {
            use cpal::traits::HostTrait;
            let host = cpal::default_host();
            let found = host.input_devices().ok()
                .and_then(|mut devs| devs.find(|d| {
                    d.name().map(|n| n.contains(name.as_str())).unwrap_or(false)
                }));
            found.or_else(|| host.default_input_device())
                .ok_or_else(|| anyhow::anyhow!("Input device '{}' not found", name))?
        }
    } else {
        cpal::default_host().default_input_device()
            .ok_or_else(|| anyhow::anyhow!("No default input device"))?
    };

    let device_name_str = device.name().unwrap_or_default();
    let native_rate = device.default_input_config()
        .map(|c: cpal::SupportedStreamConfig| c.sample_rate().0)
        .unwrap_or(SAMPLE_RATE);
    let n_channels = device.default_input_config()
        .map(|c: cpal::SupportedStreamConfig| c.channels() as usize)
        .unwrap_or(1);

    log::info!("[AUDIO] Device: {}", device_name_str);

    // ── Normal path: device natively supports 11025 Hz ───────────────────────
    // Used for IC-9700 USB Audio CODEC — no resampling, no changes to DSP chain.
    if native_rate == SAMPLE_RATE {
        log::info!("[AUDIO] Capturing at {} Hz (no resampling)", SAMPLE_RATE);
        let stream_config = cpal::StreamConfig {
            channels: 1,
            sample_rate: cpal::SampleRate(SAMPLE_RATE),
            buffer_size: cpal::BufferSize::Fixed(AUDIO_BUFFER_SIZE as u32),
        };
        let tx_clone = tx.clone();
        let stream = device.build_input_stream(
            &stream_config,
            move |data: &[f32], _| { let _ = tx_clone.send(data.to_vec()); },
            |e| log::error!("[AUDIO] Stream error: {}", e),
            None,
        ).or_else(|_| {
            // Fallback: try default config
            let _default_cfg = device.default_input_config()
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            let fallback = cpal::StreamConfig {
                channels: 1,
                sample_rate: cpal::SampleRate(SAMPLE_RATE),
                buffer_size: cpal::BufferSize::Default,
            };
            let tx2 = tx.clone();
            device.build_input_stream(
                &fallback,
                move |data: &[f32], _| { let _ = tx2.send(data.to_vec()); },
                |e| log::error!("[AUDIO] Stream error: {}", e),
                None,
            ).map_err(|e| anyhow::anyhow!("{}", e))
        }).map_err(|e| anyhow::anyhow!("Build stream: {}", e))?;
        use cpal::traits::StreamTrait;
        stream.play().context("Start stream")?;
        // cpal::Stream is !Send on macOS CoreAudio — wrap it to move into a thread
        struct StreamHolder(cpal::Stream);
        unsafe impl Send for StreamHolder {}
        let holder = StreamHolder(stream);
        std::thread::Builder::new()
            .name("fsk441-audio-in".into())
            .spawn(move || {
                let _h = holder; // keeps stream alive until thread exits
                // Linux: block until stop signal received, then exit to release ALSA handle
                #[cfg(target_os = "linux")]
                { let _ = stop_rx.recv(); log::info!("[AUDIO] Linux: input stream released"); }
                // macOS/Windows: park forever — stop_rx dropped silently
                #[cfg(not(target_os = "linux"))]
                { let _ = stop_rx; loop { std::thread::sleep(std::time::Duration::from_secs(3600)); } }
            })
            .ok();
        return Ok(());
    }

    // ── Fallback path: device has different rate (e.g. BlackHole at 44100/48000) ──
    // Only used for loopback testing — does NOT affect normal IC-9700 operation.
    log::info!("[AUDIO] Capturing at {} Hz, {} ch → resampling to {} Hz mono (test mode)",
        native_rate, n_channels, SAMPLE_RATE);

    let stream_cfg = cpal::StreamConfig {
        channels:    n_channels as u16,
        sample_rate: cpal::SampleRate(native_rate),
        buffer_size: cpal::BufferSize::Default,
    };

    let (raw_tx, mut raw_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<f32>>();

    // CoreAudio Stream is !Send+!Sync — must build and keep on the same thread.
    // Use a std channel to confirm the stream started, then block the thread.
    let (started_tx, started_rx) = std::sync::mpsc::channel::<anyhow::Result<()>>();
    let ch = n_channels;

    std::thread::spawn(move || {
        use cpal::traits::DeviceTrait;
        let raw_tx2 = raw_tx;
        let stream = device.build_input_stream(
            &stream_cfg,
            move |data: &[f32], _: &cpal::InputCallbackInfo| {
                let mono: Vec<f32> = if ch == 1 {
                    data.to_vec()
                } else {
                    data.chunks(ch).map(|f| f.iter().sum::<f32>() / ch as f32).collect()
                };
                let _ = raw_tx2.send(mono);
            },
            |e| log::error!("[AUDIO] Stream error: {}", e),
            None,
        );
        match stream {
            Ok(s) => {
                if let Err(e) = { use cpal::traits::StreamTrait; let s: &cpal::Stream = &s; s.play() } {
                    let _ = started_tx.send(Err(anyhow::anyhow!("play: {}", e)));
                    return;
                }
                let _ = started_tx.send(Ok(()));
                // Keep stream alive — block this thread
                loop { std::thread::sleep(std::time::Duration::from_secs(3600)); }
            }
            Err(e) => { let _ = started_tx.send(Err(anyhow::anyhow!("build: {}", e))); }
        }
    });

    started_rx.recv()
        .context("Stream thread died")?
        .context("Build/play input stream")?;

    tokio::spawn(async move {
        use rubato::{Resampler, SincFixedIn, SincInterpolationParameters,
                     SincInterpolationType, WindowFunction};
        let ratio = SAMPLE_RATE as f64 / native_rate as f64;
        let input_size = ((AUDIO_BUFFER_SIZE as f64 / ratio).ceil() as usize + 8).max(64);
        let params = SincInterpolationParameters {
            sinc_len: 64, f_cutoff: 0.95,
            interpolation: SincInterpolationType::Linear,
            oversampling_factor: 64,
            window: WindowFunction::BlackmanHarris2,
        };
        let mut resampler = match SincFixedIn::<f32>::new(ratio, 1.02, params, input_size, 1) {
            Ok(r) => r,
            Err(e) => { log::error!("[AUDIO] Resampler: {}", e); return; }
        };
        let mut buf = Vec::new();
        while let Some(chunk) = raw_rx.recv().await {
            buf.extend_from_slice(&chunk);
            while buf.len() >= input_size {
                let input = vec![buf.drain(..input_size).collect::<Vec<f32>>()];
                match resampler.process(&input, None) {
                    Ok(out) => {
                        if tx.send(out.into_iter().next().unwrap_or_default()).is_err() { return; }
                    }
                    Err(e) => log::warn!("[AUDIO] Resample: {}", e),
                }
            }
        }
    });

    Ok(())
}


// ─── WAV file processing ──────────────────────────────────────────────────────

pub async fn process_wav(
    path: &Path,
    tx: mpsc::UnboundedSender<AudioChunk>,
    realtime: bool,
) -> Result<()> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || process_wav_blocking(&path, tx, realtime))
        .await
        .context("WAV task panicked")??;
    Ok(())
}

fn process_wav_blocking(
    path: &Path,
    tx: mpsc::UnboundedSender<AudioChunk>,
    realtime: bool,
) -> Result<()> {
    let mut reader = hound::WavReader::open(path)
        .with_context(|| format!("Cannot open WAV: {}", path.display()))?;

    let spec     = reader.spec();
    let in_rate  = spec.sample_rate;
    let channels = spec.channels as usize;

    log::info!("[AUDIO] WAV: {} Hz {} ch {} bit",
        in_rate, channels, spec.bits_per_sample);

    let max_val = (1i64 << (spec.bits_per_sample.saturating_sub(1))) as f32;

    // Read all samples as f32, left channel only
    let samples: Vec<f32> = match spec.sample_format {
        hound::SampleFormat::Float =>
            reader.samples::<f32>()
                .step_by(channels)
                .map(|s: hound::Result<f32>| s.unwrap_or(0.0))
                .collect(),
        hound::SampleFormat::Int =>
            reader.samples::<i32>()
                .step_by(channels)
                .map(|s: hound::Result<i32>| s.unwrap_or(0) as f32 / max_val)
                .collect(),
    };

    // If WAV is already 11025 Hz, forward directly in chunks
    if in_rate == SAMPLE_RATE {
        log::info!("[AUDIO] WAV at native rate — no resampling");
        let chunk_size = AUDIO_BUFFER_SIZE as usize;
        let pace_us    = (chunk_size as u64 * 1_000_000) / in_rate as u64;

        for chunk in samples.chunks(chunk_size) {
            if tx.send(chunk.to_vec()).is_err() { break; }
            if realtime {
                std::thread::sleep(std::time::Duration::from_micros(pace_us));
            }
        }
        log::info!("[AUDIO] WAV done ({:.1}s)", samples.len() as f32 / in_rate as f32);
        return Ok(());
    }

    // WAV at different rate — resample with rubato
    log::info!("[AUDIO] WAV at {} Hz — resampling to {}", in_rate, SAMPLE_RATE);
    use rubato::{FftFixedIn, Resampler};

    let chunk_size = 4800usize; // input chunk
    let mut resampler = FftFixedIn::<f32>::new(
        in_rate as usize,
        SAMPLE_RATE as usize,
        chunk_size,
        2,
        1,
    ).context("Build WAV resampler")?;

    let pace_us = (chunk_size as u64 * 1_000_000) / in_rate as u64;
    let mut pos = 0usize;

    while pos + chunk_size <= samples.len() {
        let chunk = &samples[pos..pos + chunk_size];
        let out: Vec<Vec<f32>> = resampler
            .process(&[chunk], None)
            .map_err(|e| anyhow::anyhow!("Resample: {}", e))?;

        let resampled: Vec<f32> = out.into_iter().next().unwrap_or_default();
        if tx.send(resampled).is_err() { break; }

        if realtime {
            std::thread::sleep(std::time::Duration::from_micros(pace_us));
        }
        pos += chunk_size;
    }

    log::info!("[AUDIO] WAV done ({:.1}s)", samples.len() as f32 / in_rate as f32);
    Ok(())
}
