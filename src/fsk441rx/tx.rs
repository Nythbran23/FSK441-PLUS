// src/fsk441rx/tx.rs

use std::f32::consts::TAU;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::params::*;
use cpal::traits::{DeviceTrait, HostTrait};

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

pub fn gen441(tones: &[u8]) -> Vec<f32> {
    let dt = 1.0_f32 / SAMPLE_RATE_F;
    let mut s = Vec::with_capacity(tones.len() * NSPD);
    let mut phi = 0.0f32;
    for &ti in tones {
        let dpha = TAU * tone_freq(ti as usize) * dt;
        for _ in 0..NSPD { phi += dpha; if phi > TAU { phi -= TAU; } s.push(phi.sin()); }
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

// ─── Audio playback — uses msk2k_audio AudioOutputBuilder ────────────────────

/// Open a persistent CPAL output stream for Windows WASAPI.
/// Returns a shared buffer — push samples into it to play audio.
/// The stream plays silence when the buffer is empty.
/// Stream is leaked intentionally — must live for the engine lifetime.
#[cfg(target_os = "windows")]
fn open_persistent_output_stream(
    device_name: Option<String>,
) -> Option<std::sync::Arc<std::sync::Mutex<Vec<f32>>>> {
    use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
    let host = cpal::default_host();
    let device = if let Some(ref name) = device_name {
        find_output_device(name).or_else(|| host.default_output_device())
    } else {
        host.default_output_device()
    };
    let device = match device {
        Some(d) => d,
        None => { log::error!("[TX] No output device for persistent stream"); return None; }
    };
    let stream_config = cpal::StreamConfig {
        channels: 1,
        sample_rate: cpal::SampleRate(SAMPLE_RATE),
        buffer_size: cpal::BufferSize::Fixed(AUDIO_BUFFER_SIZE as u32),
    };
    let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::<f32>::new()));
    let buf2 = buf.clone();
    let stream = match device.build_output_stream(
        &stream_config,
        move |out: &mut [f32], _| {
            let mut b = buf2.lock().unwrap();
            let n = out.len().min(b.len());
            if n > 0 {
                out[..n].copy_from_slice(&b[..n]);
                b.drain(..n);
            }
            out[n..].fill(0.0);
        },
        |e| log::error!("[TX] Persistent stream error: {}", e),
        None,
    ) {
        Ok(s) => s,
        Err(e) => { log::error!("[TX] Failed to build persistent output stream: {}", e); return None; }
    };
    if let Err(e) = stream.play() {
        log::error!("[TX] Failed to start persistent stream: {}", e);
        return None;
    }
    std::mem::forget(stream);
    log::info!("[TX] Windows persistent output stream opened on '{}'",
        device_name.as_deref().unwrap_or("default"));
    Some(buf)
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
    #[allow(unused_imports)]

    let n = samples.len();

    let device = if let Some(ref name) = device_name {
        find_output_device(name)
    } else {
        use cpal::traits::HostTrait;
        cpal::default_host().default_output_device()
    };

    let device = match device {
        Some(d) => d,
        None    => {
            use cpal::traits::HostTrait;
            log::warn!("[TX] Using system default output device");
            match cpal::default_host().default_output_device() {
                Some(d) => d,
                None    => { log::error!("[TX] No output device"); return; }
            }
        }
    };

    log::info!("[TX] Playing on: '{}'", device.name().unwrap_or_default());

    // Build output stream directly with cpal
    use cpal::traits::{DeviceTrait, StreamTrait};
    let stream_config = cpal::StreamConfig {
        channels: 1,
        sample_rate: cpal::SampleRate(SAMPLE_RATE),
        buffer_size: cpal::BufferSize::Fixed(AUDIO_BUFFER_SIZE as u32),
    };
    // Put samples in a shared buffer
    let buf = std::sync::Arc::new(std::sync::Mutex::new(samples.to_vec()));
    let buf2 = buf.clone();
    let stream = match device.build_output_stream(
        &stream_config,
        move |out: &mut [f32], _| {
            let mut b = buf2.lock().unwrap();
            let n = out.len().min(b.len());
            out[..n].copy_from_slice(&b[..n]);
            b.drain(..n);
            if n < out.len() { out[n..].fill(0.0); }
        },
        |e| log::error!("[TX] Output error: {}", e),
        None,
    ) {
        Ok(s) => s,
        Err(e) => { log::error!("[TX] build_output_stream: {}", e); return; }
    };
    if let Err(e) = stream.play() {
        log::error!("[TX] stream.play: {}", e); return;
    }

    // Drop PTT 200ms before audio ends so rig switches to RX as last samples drain
    let _ptt_off_ms = (n as u64 * 1000 / SAMPLE_RATE as u64).saturating_sub(200);
    let total_ms    = (n as u64 * 1000 / SAMPLE_RATE as u64) + 500;
    log::info!("[TX] Playing {} samples ({:.1}s)", n, n as f32 / SAMPLE_RATE_F);
    let mut elapsed = 0u64;
    while elapsed < total_ms {
        if cancel.load(std::sync::atomic::Ordering::Relaxed) {
            log::info!("[TX] Cancelled after {}ms", elapsed);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        elapsed += 100;
    }
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
}

impl TxEngine {
    pub fn new(
        cmd_rx:           mpsc::UnboundedReceiver<TxCommand>,
        period:           Period,
        output_device:    Option<String>,
        hamlib_addr:      Option<String>,
        hamlib_update_tx: mpsc::UnboundedSender<HamlibUpdate>,
    ) -> Self { Self { cmd_rx, period, output_device, hamlib_addr, hamlib_update_tx,
        ptt_method: PttMethod::CatHamlib, ptt_port: None } }

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

        // ── Windows: persistent output stream ────────────────────────────────
        #[cfg(target_os = "windows")]
        let win_audio_buf: Option<std::sync::Arc<std::sync::Mutex<Vec<f32>>>> = {
            open_persistent_output_stream(self.output_device.clone())
        };

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
                        cancel_flag.store(true, std::sync::atomic::Ordering::Relaxed);
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
                    TxCommand::ClearAccumulator => {
                        // Handled in run_engine — ignore here
                    }
                    other => {
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
                Some(TxCommand::TransmitN { message, times, output_device }) => {
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
                | Some(TxCommand::SetFreq(_)) => {
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
            let slot_start = (now_ms / slot_ms) * slot_ms; // ms of current slot start
            let elapsed_ms = (now_ms - slot_start).max(0);
            let remain_ms  = (slot_ms - elapsed_ms).max(500); // at least 500ms
            let slot_samples = (SAMPLE_RATE as i64 * remain_ms / 1000) as usize;
            log::info!("[TX] Slot: {}s period, {}ms elapsed, {}ms remaining → {} samples",
                slot_secs, elapsed_ms, remain_ms, slot_samples);
            // Add 3 spaces between repeats so messages don't run together
            // "CQ GW4WND IO82   CQ GW4WND IO82   ..." — separators aid sync detection
            let padded_msg   = format!("{}   ", message);
            let one_msg      = message_to_audio(&padded_msg);
            let msg_len      = one_msg.len();
            let repeats      = (slot_samples + msg_len - 1) / msg_len;
            let mut waveform: Vec<f32> = Vec::with_capacity(slot_samples);
            for _ in 0..repeats { waveform.extend_from_slice(&one_msg); }
            waveform.truncate(slot_samples);

            log::info!("[TX] Waveform: {} samples ({:.1}s), {} reps of {}ms",
                waveform.len(), waveform.len() as f32 / SAMPLE_RATE_F,
                repeats, msg_len * 1000 / SAMPLE_RATE as usize);

            // PTT on
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

            let duration_ms = waveform.len() as u64 * 1000 / SAMPLE_RATE as u64;
            #[allow(unused_variables)]
            let wait_ms = duration_ms + 500;

            // Play audio — Windows uses persistent stream to avoid WASAPI timing issues
            #[cfg(target_os = "windows")]
            {
                if let Some(ref audio_buf) = win_audio_buf {
                    {
                        let mut b = audio_buf.lock().unwrap();
                        b.extend_from_slice(&waveform);
                    }
                    log::info!("[TX] Windows: pushed {} samples to persistent stream, sleeping {}ms",
                        waveform.len(), wait_ms);
                    tokio::time::sleep(tokio::time::Duration::from_millis(wait_ms)).await;
                } else {
                    // Fallback if persistent stream failed to open
                    let device = dev.or_else(|| self.output_device.clone());
                    let cancel_clone = cancel_flag.clone();
                    tokio::task::spawn_blocking(move || {
                        play_audio_blocking(waveform, device, cancel_clone);
                    }).await.ok();
                }
            }
            #[cfg(not(target_os = "windows"))]
            {
                let device = dev.or_else(|| self.output_device.clone());
                let cancel_clone = cancel_flag.clone();
                tokio::task::spawn_blocking(move || {
                    play_audio_blocking(waveform, device, cancel_clone);
                }).await.ok();
            }

            // PTT off
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

            // Wait for the slot to end before looping
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                let now_sidx = slot_idx_p(utc_ms(), self.period);
                if now_sidx != sidx { break; }
                if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) { break; }
            }
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
