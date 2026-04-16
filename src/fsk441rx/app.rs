// src/fsk441rx/app.rs — FSK441+ Transceiver GUI
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use eframe::egui;
use std::path::PathBuf;
use std::process::Command;
#[cfg(windows)]
use std::os::windows::process::CommandExt;
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x08000000;
use tokio::sync::mpsc;
use chrono::{DateTime, Utc};

mod params;
mod audio;
mod detector;
mod demod;
mod filter;
mod store;
mod geo;
mod tx;
mod qso;
mod spectrum;
mod accumulator;
mod adif;
mod scatter;
mod second_pass;

use detector::run_detector;
use demod::longx;
use filter::ParsedMessage;
use store::{Store, AnalysisPing};
use adif::{AdifLogger, QsoRecord};
use geo::{GeoValidator, PrefixDb, Qth};
use tx::{TxCommand, PeriodTimer, Period, SlotState, HamlibUpdate};
use spectrum::{compute_column, heat_color, FSK441_TONES_HZ, DISPLAY_BINS, hz_to_bin};
use accumulator::{FragmentAccumulator, Fragment, AccumulatedDecode};
use qso::{QsoState, on_decode, Transition};

// ─── Audio device enumeration (matches MSK2K) ─────────────────────────────────

fn enumerate_audio_devices() -> (Vec<String>, Vec<String>) {
    use cpal::traits::{DeviceTrait, HostTrait};
    use std::collections::HashMap;
    let host = cpal::default_host();

    // Use host.devices() — sees all devices including duplicate USB CODECs on macOS
    let device_list: Vec<cpal::Device> = {
        let from_all = host.devices().map(|d| d.collect::<Vec<_>>()).unwrap_or_default();
        if !from_all.is_empty() { from_all } else {
            let mut devs: Vec<cpal::Device> = Vec::new();
            if let Ok(d) = host.input_devices()  { devs.extend(d); }
            if let Ok(d) = host.output_devices() { devs.extend(d); }
            devs
        }
    };

    let mut all_devices: Vec<(String, bool, bool)> = Vec::new();
    for d in device_list {
        if let Ok(name) = d.name() {
            let has_in  = d.supported_input_configs().map(|mut c| c.next().is_some()).unwrap_or(false)
                       || d.default_input_config().is_ok();
            let has_out = d.supported_output_configs().map(|mut c| c.next().is_some()).unwrap_or(false)
                       || d.default_output_config().is_ok();
            all_devices.push((name, has_in, has_out));
        }
    }
    all_devices.sort_by(|a, b| a.0.cmp(&b.0));

    // Count duplicates — IC-9700 shows as two "USB Audio CODEC" entries, one RX one TX
    let mut name_counts: HashMap<String, usize> = HashMap::new();
    for (name, _, _) in &all_devices { *name_counts.entry(name.clone()).or_insert(0) += 1; }

    let mut group_caps: HashMap<String, Vec<(bool, bool)>> = HashMap::new();
    for (name, has_in, has_out) in &all_devices {
        if name_counts[name.as_str()] > 1 {
            group_caps.entry(name.clone()).or_default().push((*has_in, *has_out));
        }
    }

    let mut name_indices: HashMap<String, usize> = HashMap::new();
    let display_names: Vec<String> = all_devices.iter().map(|(name, has_in, has_out)| {
        if name_counts[name.as_str()] > 1 {
            let idx = name_indices.entry(name.clone()).or_insert(0);
            *idx += 1;
            let caps = &group_caps[name.as_str()];
            let has_rx_sib = caps.iter().any(|(i, _)| *i);
            let has_tx_sib = caps.iter().any(|(_, o)| *o);
            let label = match (*has_in, *has_out) {
                (true,  false) => "RX".to_string(),
                (false, true)  => "TX".to_string(),
                (true,  true)  => "RX/TX".to_string(),
                (false, false) => {
                    if has_rx_sib && !has_tx_sib { "TX".to_string() }
                    else if has_tx_sib && !has_rx_sib { "RX".to_string() }
                    else { format!("{}", idx) }
                }
            };
            format!("{} ({})", name, label)
        } else {
            name.clone()
        }
    }).collect();

    log::info!("[AUDIO] Devices: {:?}", display_names);
    // Same list for both input and output — user picks the (RX) instance for input
    // and the (TX) instance for output
    (display_names.clone(), display_names)
}

// ─── Rig list from rigctld -l (matches MSK2K) ────────────────────────────────

fn enumerate_rigs() -> Vec<(String, String)> {
    let mut list = Vec::new();

    // Build candidate paths — binary-adjacent first (works for bundled rigctld.exe),
    // then well-known install locations on each platform.
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();

    // Next to our own executable (covers bundled rigctld / rigctld.exe)
    // Also check tools/ subdirectory — matches MSK2K Windows release layout
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let name = if cfg!(windows) { "rigctld.exe" } else { "rigctld" };
            candidates.push(dir.join(name));
            candidates.push(dir.join("tools").join(name));
        }
    }

    // Platform-specific well-known locations
    if cfg!(windows) {
        for p in &[
            r"C:\Program Files\Hamlib\bin\rigctld.exe",
            r"C:\Program Files (x86)\Hamlib\bin\rigctld.exe",
            r"C:\hamlib\bin\rigctld.exe",
            r"C:\tools\hamlib\bin\rigctld.exe",
        ] {
            candidates.push(std::path::PathBuf::from(p));
        }
        // Also try bare name in PATH
        candidates.push(std::path::PathBuf::from("rigctld.exe"));
        candidates.push(std::path::PathBuf::from("rigctld"));
    } else {
        for p in &[
            "/usr/local/bin/rigctld",
            "/opt/homebrew/bin/rigctld",
            "/usr/bin/rigctld",
        ] {
            candidates.push(std::path::PathBuf::from(p));
        }
        candidates.push(std::path::PathBuf::from("rigctld"));
    }

    for cmd in &candidates {
        if let Ok(out) = Command::new(cmd).arg("-l").output() {
            if let Ok(text) = String::from_utf8(out.stdout) {
                for line in text.lines() {
                    if line.trim().is_empty() || line.starts_with(" Rig") { continue; }
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 3 {
                        let id   = parts[0].to_string();
                        let name = format!("{} {}", parts[1], parts[2]);
                        list.push((id, name));
                    }
                }
                if !list.is_empty() {
                    log::info!("[RIGS] Enumerated {} rigs from {:?}", list.len(), cmd);
                    break;
                }
            }
        }
    }

    if list.is_empty() {
        log::warn!("[RIGS] rigctld not found — rig list empty. Install Hamlib or bundle rigctld.");
    }
    list
}

fn enumerate_serial_ports() -> Vec<String> {
    match serialport::available_ports() {
        Ok(ports) => ports.iter().map(|p: &serialport::SerialPortInfo| p.port_name.clone()).collect::<Vec<_>>(),
        Err(_)    => vec![],
    }
}

// ─── Decode entry ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct DecodeEntry {
    timestamp:   DateTime<Utc>,
    df_hz:       f32,
    ccf_ratio:   f32,
    duration_ms: f32,
    confidence: f32,
    score:      u8,
    raw:        String,
    callsigns:  Vec<String>,
    locator:    Option<String>,
    report:     Option<String>,
    is_cq:      bool,
    char_confs: Vec<f32>,
    is_accumulated: bool,
    #[allow(dead_code)]
    my_call:        String,
    df_bin_hz:      Option<i32>,
    passed_threshold: bool,
    #[allow(dead_code)]
    is_second_pass:   bool,
    #[allow(dead_code)]
    second_pass_chars: Vec<(char, bool)>,
}

enum EngineEvent { Decode(DecodeEntry), Accumulated(AccumulatedDecode), Hamlib(HamlibUpdate), Spectrum(Vec<f32>, f32), AnalysisSaved(i64, usize), SecondPassDecodes(Vec<second_pass::SecondPassResult>), NoiseStats { mean: f32, sigma: f32, threshold: f32 }, ConfUpdate(f32, bool) }


/// Find cty.dat — checks next to binary first, then current working directory.
fn default_cty_path() -> String {
    // Next to the running binary (covers bundled app and standard install)
    if let Ok(exe) = std::env::current_exe() {
        let p = exe.parent().unwrap_or(std::path::Path::new(".")).join("cty.dat");
        if p.exists() { return p.to_string_lossy().into_owned(); }
    }
    // Relative fallback — current working directory
    "cty.dat".into()
}

/// PTT keying method — how the transmitter is keyed during TX
#[derive(Clone, PartialEq, Debug)]
pub enum PttMethod {
    HamlibCat,   // PTT via CAT command through hamlib (existing behaviour)
    Rts,          // Assert RTS on ptt_serial_port
    Dtr,          // Assert DTR on ptt_serial_port
    VoxOnly,      // No software PTT — rely on VOX or manual keying
}

impl Default for PttMethod {
    fn default() -> Self { PttMethod::HamlibCat }
}

// ─── Settings ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct Settings {
    #[allow(dead_code)]
    my_call:        String,
    my_loc:         String,
    sel_in:         Option<String>,
    sel_out:        Option<String>,
    hamlib_enabled: bool,
    their_call_hint: Option<String>,
    station_tracker_enabled: bool,
    rig_model:      String,
    rig_port:       String,
    rig_baud:       String,
    period:         Period,
    cty_path:       String,
    max_km:         f64,
    threshold:      f32,
    min_ccf:        f32,   // kept for config backwards compat only
    k_sigma:        f32,   // sensitivity: threshold = noise_mean + k_sigma * sigma
    clear_accumulator: bool,
    their_df_hz:      Option<f32>,
    tx_level:       f32,
    ant_bw_horiz:   f32,
    ant_bw_vert:    f32,
    save_max_data:  bool,
    qsy_reset:      bool,  // pulse: reset EMA on QSY
    ptt_method:     PttMethod,
    ptt_serial_port: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            my_call:        "NOCALL".into(),
            my_loc:         "NOLOC".into(),
            sel_in:         Some("USB Audio CODEC".into()),
            sel_out:        Some("USB Audio CODEC".into()),
            hamlib_enabled: true,
            their_call_hint: None,
            station_tracker_enabled: true,
            rig_model:      String::new(),
            rig_port:       String::new(),
            rig_baud:       "19200".into(),
            period:         Period::TxSecond,
            cty_path:       default_cty_path(),
            max_km:         3000.0,
            threshold:      3.0,
            min_ccf:        200.0,  // legacy — not used in gate
            k_sigma:        5.0,    // μ + 5σ default
            clear_accumulator: false,
            their_df_hz: None,
            tx_level:       1.0,
            ant_bw_horiz:   40.0,
            ant_bw_vert:    40.0,
            save_max_data:  false,
            qsy_reset:      false,
            ptt_method:     PttMethod::HamlibCat,
            ptt_serial_port: String::new(),
        }
    }
}

impl Settings {
    fn audio_in(&self) -> Option<String> { self.sel_in.clone() }
    fn audio_out(&self) -> Option<String> { self.sel_out.clone() }
    fn hamlib_addr(&self) -> Option<String> {
        if self.hamlib_enabled { Some("127.0.0.1:4532".into()) } else { None }
    }
}


// Ensures rigctld is killed cleanly when app exits — same as MSK2K
struct ProcessGuard(std::process::Child);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        log::info!("[LAUNCHER] Shutting down rigctld (pid={})", self.0.id());
        // Release PTT cleanly before killing
        if let Ok(mut stream) = std::net::TcpStream::connect_timeout(
            &"127.0.0.1:4532".parse().unwrap(),
            std::time::Duration::from_millis(500),
        ) {
            use std::io::Write;
            let _ = stream.write_all(b"T 0\n");
            let _ = stream.flush();
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}


// ─── Station Tracker ──────────────────────────────────────────────────────────
// Persistently accumulates soft evidence across ALL pings at a given DF bin,
// surviving slot boundaries. Shows running reconstruction in the callsign panel.
#[derive(Default, Clone)]
pub struct TrackedStation {
    pub df_bin:     i32,        // DF rounded to nearest 43Hz bin
    pub ping_count: usize,
    pub last_seen:  Option<chrono::DateTime<chrono::Utc>>,
    pub first_seen: Option<chrono::DateTime<chrono::Utc>>,
    pub best_decode: String,    // highest-confidence single decode seen
    pub best_conf:  f32,
    pub callsigns:  Vec<(String, usize)>, // call → count
}

#[derive(Default)]
pub struct StationTracker {
    pub stations: Vec<TrackedStation>,
}

impl StationTracker {
    pub fn add_ping(&mut self, entry: &DecodeEntry) {
        let df_bin = (entry.df_hz / 43.0).round() as i32 * 43;
        let st = if let Some(s) = self.stations.iter_mut().find(|s| s.df_bin == df_bin) {
            s
        } else {
            self.stations.push(TrackedStation {
                df_bin, ..Default::default()
            });
            self.stations.last_mut().unwrap()
        };
        st.ping_count += 1;
        st.last_seen  = Some(entry.timestamp);
        if st.first_seen.is_none() { st.first_seen = Some(entry.timestamp); }
        // Keep best single-ping decode
        if entry.confidence > st.best_conf && !entry.raw.trim().is_empty() {
            st.best_conf   = entry.confidence;
            st.best_decode = entry.raw.trim().to_string();
        }
        // Accumulate callsigns — merge fragments into longest known form
        // e.g. I5Y + I5YD + I5YDI → all count under I5YDI
        for call in &entry.callsigns {
            // Check if this call is a prefix of an existing longer call
            let existing_longer = st.callsigns.iter_mut()
                .find(|(c,_)| c.starts_with(call.as_str()) && c.len() > call.len());
            if let Some(e) = existing_longer {
                e.1 += 1;
                continue;
            }
            // Check if this call extends a shorter existing call
            if let Some(e) = st.callsigns.iter_mut()
                .find(|(c,_)| call.starts_with(c.as_str()) && call.len() > c.len())
            {
                let count = e.1 + 1;
                let _ = std::mem::replace(e, (call.clone(), count));
                continue;
            }
            // New callsign
            if let Some(c) = st.callsigns.iter_mut().find(|(c,_)| c == call) {
                c.1 += 1;
            } else {
                st.callsigns.push((call.clone(), 1));
            }
        }
    }

    pub fn clear(&mut self) { self.stations.clear(); }

    // Stations active in last N seconds
    pub fn active(&self, secs: i64) -> Vec<&TrackedStation> {
        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(secs);
        let mut active: Vec<&TrackedStation> = self.stations.iter()
            .filter(|s| s.last_seen.map(|t| t > cutoff).unwrap_or(false))
            .collect();
        active.sort_by(|a, b| b.ping_count.cmp(&a.ping_count));
        active
    }
}

/// One entry in the DF scatter strip — a confirmed ping dot.
#[derive(Clone)]
struct DfDot {
    col_idx:  usize,   // X: waterfall column index
    df_hz:    f32,     // DF in Hz (signed)
    ccf:      f32,     // CCF ratio — drives dot size
    r: u8, g: u8, b: u8,
}

// ─── App ──────────────────────────────────────────────────────────────────────

/// Check if a raw decode contains content expected for the current QSO TX state.
/// Great-circle distance in km (Haversine)
fn great_circle_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6371.0f64;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat/2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon/2.0).sin().powi(2);
    2.0 * r * a.sqrt().asin()
}

/// True bearing in degrees
fn great_circle_bearing(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let dlon = (lon2 - lon1).to_radians();
    let (lat1r, lat2r) = (lat1.to_radians(), lat2.to_radians());
    let y = dlon.sin() * lat2r.cos();
    let x = lat1r.cos() * lat2r.sin() - lat1r.sin() * lat2r.cos() * dlon.cos();
    (y.atan2(x).to_degrees() + 360.0) % 360.0
}


fn qso_content_match(raw: &str, their_call: &str) -> bool {
    let upper = raw.to_uppercase();
    let c = their_call.to_uppercase();
    let min_len = 4.min(c.len());
    if c.len() < min_len { return false; }
    let has_them = (0..=c.len().saturating_sub(min_len))
        .any(|i| upper.contains(&c[i..i+min_len]));
    let has_rrr = upper.contains("RRR");
    let has_73  = upper.contains(" 73 ") || upper.ends_with(" 73")
                  || upper.starts_with("73 ");
    has_them || has_rrr || has_73
}


struct Fsk441App {
    settings:        Settings,
    settings_open:   bool,
    in_devs:         Vec<String>,
    out_devs:        Vec<String>,
    rig_list:        Vec<(String, String)>,
    rig_search:      String,
    serial_ports:    Vec<String>,
    decodes:         Vec<DecodeEntry>,
    selected:        Option<usize>,
    event_rx:        mpsc::UnboundedReceiver<EngineEvent>,
    tx_cmd_tx:       mpsc::UnboundedSender<TxCommand>,
    period_timer:    PeriodTimer,
    is_transmitting: bool,
    qso:             QsoState,
    their_call_edit: String,
    their_loc_edit:  String,
    report_sent:     String,
    report_rcvd:     String,
    // Editable TX message fields — shown in UI, user can override defaults
    tx_msgs:         [String; 6],  // [CQ, TX1, TX2, TX3, TX4, TX5]
    tx_active:       std::sync::Arc<std::sync::atomic::AtomicBool>,
    // Spectrogram: columns[i] = one FFT snapshot (vertical strip), X=time Y=freq
    #[allow(dead_code)]
    show_accumulated: bool,
    active_tx_idx:   Option<usize>,  // which TX button is active
    wf_columns:      Vec<Vec<f32>>,
    df_dots:         Vec<DfDot>,   // DF scatter strip history
    wf_amplitude:    Vec<f32>,  // RMS amplitude per column (0..1)
    last_conf:       f32,       // confidence of most recent decoded ping
    wf_period_idx:   i64,  // slot index when columns last cleared
    tx_msgs_key:     String,       // last key used to populate — repopulate only on change
    qso_log:         Vec<String>,
    seen_calls:      Vec<(String, chrono::DateTime<chrono::Utc>, usize)>,
    #[allow(dead_code)]
    station_tracker: StationTracker,
    qso_summary:     Option<String>,
    rig_freq_hz:     Option<u64>,
    base_freq_hz:    Option<u64>,   // freq at first CAT connect — used for ±250KHz clamp
    freq_edit:       String,         // KHz digits being typed (e.g. "385")
    freq_editing:    bool,           // true when KHz field is active
    cat_connected:   bool,
    last_cat_rx:     Option<std::time::Instant>,
    _runtime:        tokio::runtime::Runtime,
    _rigctld:        Option<ProcessGuard>,
    settings_watch_tx: tokio::sync::watch::Sender<Settings>,
    qso_time_on:       Option<chrono::DateTime<chrono::Utc>>,
    qso_logged:        bool,
    adif_logger:       AdifLogger,
    adif_log:          Vec<QsoRecord>,
    noise_mean:        f32,   // live noise floor mean conf
    noise_sigma:       f32,   // live noise floor sigma
    adaptive_threshold: f32,  // live adaptive conf threshold
    nf_settled:         bool, // EMA has converged — NF label goes green
    // Second-pass decoder
    event_tx:            mpsc::UnboundedSender<EngineEvent>,
    second_pass_db_path: String,
    ptt_port:            Option<Box<dyn serialport::SerialPort>>,
}

impl Fsk441App {
    fn new(_cc: &eframe::CreationContext) -> Self {
        let settings = load_config();
        let (in_devs, out_devs) = enumerate_audio_devices();
        let rig_list = enumerate_rigs();
        let serial_ports = enumerate_serial_ports();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all().build().expect("Tokio runtime");

        let (event_tx, event_rx) = mpsc::unbounded_channel::<EngineEvent>();
        let event_tx_ui = event_tx.clone();  // kept in App struct for second-pass spawning
        let (tx_cmd_tx, tx_cmd_rx) = mpsc::unbounded_channel::<TxCommand>();
        let (hamlib_tx, hamlib_rx)   = mpsc::unbounded_channel::<HamlibUpdate>();

        let s = settings.clone();
        let (settings_watch_tx, settings_watch_rx) = tokio::sync::watch::channel(s.clone());

        // Shared tx_active atomic — written by TxEngine AND UI, read by run_engine
        let tx_active_for_app    = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let tx_active_for_engine = tx_active_for_app.clone();
        let tx_active_for_tx     = tx_active_for_app.clone();

        let tx_engine = tx::TxEngine::new(
            tx_cmd_rx, settings.period,
            settings.audio_out(),
            settings.hamlib_addr(),
            hamlib_tx,
            tx_active_for_tx,
        );
        rt.spawn(async move { tx_engine.run().await; });

        // Forward HamlibUpdates from TxEngine into main event loop
        let htx = event_tx.clone();
        rt.spawn(async move {
            let mut hx = hamlib_rx;
            while let Some(upd) = hx.recv().await {
                let _ = htx.send(EngineEvent::Hamlib(upd));
            }
        });

        let et = event_tx.clone();
        rt.spawn(async move { run_engine(s, et, tx_active_for_engine, settings_watch_rx).await; });

        // Auto-launch rigctld if hamlib enabled and rig configured — same as MSK2K
        let mut rigctld_guard: Option<ProcessGuard> = None;
        if settings.hamlib_enabled
            && !settings.rig_model.is_empty()
            && !settings.rig_port.is_empty()
        {
            let baud: u32 = settings.rig_baud.parse().unwrap_or(19200);
            log::info!("[LAUNCHER] Auto-starting rigctld: model={} port={} baud={}",
                settings.rig_model, settings.rig_port, baud);

            // Kill any stale instance — platform-specific
            if cfg!(windows) {
                let _ = Command::new("taskkill").args(["/F", "/IM", "rigctld.exe"]).output();
            } else {
                let _ = Command::new("pkill").args(["-f", "rigctld"]).output();
            }
            std::thread::sleep(std::time::Duration::from_millis(300));

            // Find rigctld — tools/ subdir first (matches MSK2K bundling), then binary-adjacent, then PATH
            let rigctld_cmd = {
                let name = if cfg!(windows) { "rigctld.exe" } else { "rigctld" };
                let found = std::env::current_exe().ok()
                    .and_then(|e| e.parent().map(|d| d.to_path_buf()))
                    .and_then(|dir| {
                        let tools = dir.join("tools").join(name);
                        if tools.exists() { return Some(tools); }
                        let adj = dir.join(name);
                        if adj.exists() { return Some(adj); }
                        None
                    });
                if let Some(p) = found {
                    p.to_string_lossy().to_string()
                } else if cfg!(windows) {
                    "rigctld.exe".to_string()
                } else {
                    "rigctld".to_string()
                }
            };

            let mut cmd = Command::new(&rigctld_cmd);
            cmd.args(["-m", &settings.rig_model,
                      "-r", &settings.rig_port,
                      "-s", &baud.to_string(),
                      "-P", "RIG"]);
            #[cfg(windows)]
            cmd.creation_flags(CREATE_NO_WINDOW);
            match cmd.spawn() {
                Ok(c)  => {
                    log::info!("[LAUNCHER] rigctld started (pid={})", c.id());
                    rigctld_guard = Some(ProcessGuard(c));
                }
                Err(e) => log::error!("[LAUNCHER] rigctld failed ({}): {}", rigctld_cmd, e),
            }
            // Give rigctld time to bind port 4532
            std::thread::sleep(std::time::Duration::from_millis(800));
        }

        let period = settings.period;
        Self {
            settings_open: false, in_devs, out_devs, rig_list,
            rig_search: String::new(), serial_ports,
            decodes: Vec::new(), selected: None, event_rx, tx_cmd_tx,
            period_timer: PeriodTimer::new(period), is_transmitting: false,
            qso: QsoState::Idle,
            their_call_edit: String::new(), their_loc_edit: String::new(),
            report_sent: "26".into(), report_rcvd: String::new(),
            tx_active: tx_active_for_app,
        show_accumulated: true,
        active_tx_idx: None,
        wf_columns: Vec::new(),
        df_dots: Vec::new(),
        qso_time_on: None,
        qso_logged: false,
        adif_logger: AdifLogger::new(&AdifLogger::default_path()),
        adif_log: Vec::new(),
        noise_mean: 0.44,
        noise_sigma: 0.02,
        adaptive_threshold: 0.54, // prior: noise_mean(0.44) + k(5) × noise_sigma(0.02)
        nf_settled: false,
        wf_amplitude: Vec::new(),
        last_conf: 0.0,
        wf_period_idx: -1,
        tx_msgs: Default::default(), tx_msgs_key: String::new(), qso_log: Vec::new(), seen_calls: Vec::new(), station_tracker: StationTracker::default(), qso_summary: None, settings_watch_tx, rig_freq_hz: None,
            base_freq_hz: None,
            freq_edit: String::new(),
            freq_editing: false, cat_connected: false, last_cat_rx: None, _rigctld: rigctld_guard, settings, _runtime: rt,
        // Second-pass decoder
        event_tx: event_tx_ui,
        second_pass_db_path: {
            let mut p = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("."));
            p.push(".fsk441"); p.push("fsk441.db");
            p.to_string_lossy().to_string()
        },
        ptt_port: None,
        }
    }

    /// Open (or reopen) the PTT serial port based on current settings.
    /// Called when settings change. Safe to call when port is already open — closes first.
    fn open_ptt_port(&mut self) {
        self.ptt_port = None; // close existing
        if matches!(self.settings.ptt_method, PttMethod::Rts | PttMethod::Dtr)
            && !self.settings.ptt_serial_port.is_empty()
        {
            match serialport::new(&self.settings.ptt_serial_port, 9600)
                .timeout(std::time::Duration::from_millis(100))
                .open()
            {
                Ok(mut port) => {
                    // Ensure both lines start low (unkeyed)
                    let _ = port.write_request_to_send(false);
                    let _ = port.write_data_terminal_ready(false);
                    self.ptt_port = Some(port);
                    log::info!("[PTT] Opened serial PTT port: {}", self.settings.ptt_serial_port);
                }
                Err(e) => {
                    log::warn!("[PTT] Failed to open serial PTT port {}: {}",
                        self.settings.ptt_serial_port, e);
                }
            }
        }
    }

    /// Assert or deassert PTT on the configured serial pin (RTS or DTR).
    /// No-op if ptt_method is HamlibCat or VoxOnly.
    fn set_serial_ptt(&mut self, active: bool) {
        match self.settings.ptt_method {
            PttMethod::Rts => {
                if let Some(ref mut port) = self.ptt_port {
                    if let Err(e) = port.write_request_to_send(active) {
                        log::warn!("[PTT] RTS set {} failed: {}", active, e);
                    } else {
                        log::info!("[PTT] RTS → {}", if active { "TX" } else { "RX" });
                    }
                }
            }
            PttMethod::Dtr => {
                if let Some(ref mut port) = self.ptt_port {
                    if let Err(e) = port.write_data_terminal_ready(active) {
                        log::warn!("[PTT] DTR set {} failed: {}", active, e);
                    } else {
                        log::info!("[PTT] DTR → {}", if active { "TX" } else { "RX" });
                    }
                }
            }
            _ => {} // HamlibCat and VoxOnly handled elsewhere
        }
    }

    fn refresh_audio_devices(&mut self) {
        let (i, o) = enumerate_audio_devices();
        if let Some(ref sel) = self.settings.sel_in.clone() {
            if !i.contains(sel) { self.settings.sel_in = None; }
        }
        if let Some(ref sel) = self.settings.sel_out.clone() {
            if !o.contains(sel) { self.settings.sel_out = None; }
        }
        self.in_devs = i; self.out_devs = o;
    }

    fn poll_events(&mut self) {
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                EngineEvent::Accumulated(acc) => {
                    // Accumulated decode: prepend with ★ marker and different colour
                    let entry = DecodeEntry {
                        timestamp:   chrono::Utc::now(),
                        df_hz:       0.0,
                        duration_ms: 0.0,
                        ccf_ratio:   acc.n_fragments as f32 * 100.0,
                        confidence: acc.mean_conf,
                        score:      70u8,  // always show accumulated — confidence shown in text
                        raw:        format!("★ {}", acc.text),
                        callsigns:  vec![],
                        locator:    None,
                        report:     None,
                        is_cq:      acc.text.contains(" CQ ") || acc.text.starts_with("CQ "),
                        char_confs: acc.char_conf,
                        is_accumulated: true,
                        my_call: self.settings.my_call.clone(),
                        df_bin_hz: Some(acc.df_bin_hz),
                        passed_threshold: true,
                        is_second_pass: false,
                        second_pass_chars: vec![],
                    };
                    // Update seen callsigns — exclude MYCALL
                    let my = self.settings.my_call.to_uppercase();
                    for call in &entry.callsigns {
                        if call.eq_ignore_ascii_case(&my) { continue; }
                        if let Some(e) = self.seen_calls.iter_mut().find(|(c,_,_)| c == call) {
                            e.2 += 1;
                        } else {
                            self.seen_calls.push((call.clone(), entry.timestamp, 1));
                        }
                    }

                    // Update existing ★ row for this DF bin in place, or append
                    let df_key = entry.df_bin_hz;
                    if let Some(pos) = self.decodes.iter().position(|e|
                        e.is_accumulated && e.df_bin_hz.is_some() && e.df_bin_hz == df_key
                    ) {
                        self.decodes[pos] = entry.clone();
                    } else {
                        // Learn their DF from first threshold-passing ping in QSO
                        if self.settings.their_call_hint.is_some()
                            && self.settings.their_df_hz.is_none()
                            && entry.passed_threshold
                            && entry.confidence >= 0.50 {
                            self.settings.their_df_hz = Some(entry.df_hz);
                            log::info!("[QSO] Learned their DF: {:.0}Hz", entry.df_hz);
                            let _ = self.settings_watch_tx.send(self.settings.clone());
                        }
                        self.decodes.push(entry.clone());
                        if self.decodes.len() > 500 { self.decodes.remove(0); }
                    }
                    // DF scatter dot for accumulated entries
                    let (dr,dg,db) = if entry.confidence >= 0.75 { (80,220,80) }
                        else if entry.confidence >= 0.50 { (220,200,80) }
                        else { (120,120,120) };
                    self.df_dots.push(DfDot { col_idx: self.wf_columns.len(), df_hz: entry.df_hz, ccf: entry.ccf_ratio, r:dr,g:dg,b:db });
                    if self.df_dots.len() > 2000 { self.df_dots.remove(0); }
                    continue;
                }
                EngineEvent::NoiseStats { mean, sigma, threshold } => {
                    self.noise_mean = mean;
                    self.noise_sigma = sigma;
                    self.adaptive_threshold = threshold;
                    continue;
                }
                EngineEvent::ConfUpdate(c, settled) => {
                    self.last_conf = c;
                    self.nf_settled = settled;
                    continue;
                }
                EngineEvent::AnalysisSaved(capture_id, n) => {
                    log::info!("[UI] Analysis saved: capture={} n={} — spawning second pass", capture_id, n);
                    let my_call = self.settings.my_call.trim().to_uppercase();
                    let their_call = self.settings.their_call_hint.clone();
                    let their_loc = {
                        let l = self.their_loc_edit.trim().to_uppercase();
                        if l.len() >= 4 { Some(l) } else { None }
                    };
                    let report_sent = Some(self.report_sent.clone());
                    let report_rcvd = if !self.report_rcvd.is_empty() {
                        Some(self.report_rcvd.clone())
                    } else { None };
                    let stage = match self.active_tx_idx {
                        Some(0) => second_pass::QsoStage::CallingCq,
                        Some(1) => second_pass::QsoStage::CallingStation,
                        Some(2) => second_pass::QsoStage::SentReport,
                        Some(3) => second_pass::QsoStage::SentRReport,
                        _       => if their_call.is_some() {
                            second_pass::QsoStage::Idle
                        } else {
                            second_pass::QsoStage::CallingCq
                        },
                    };
                    let ctx = second_pass::QsoContext {
                        my_call, their_call, their_loc,
                        report_sent, report_rcvd, stage,
                        noise_floor: self.adaptive_threshold,
                        retain_after_run: self.settings.save_max_data,
                    };
                    let et = self.event_tx.clone();
                    let db = self.second_pass_db_path.clone();
                    self._runtime.spawn(async move {
                        let res = tokio::task::spawn_blocking(move || {
                            second_pass::run(&db, capture_id, &ctx)
                        }).await;
                        if let Ok(Ok(results)) = res {
                            if !results.is_empty() {
                                let _ = et.send(EngineEvent::SecondPassDecodes(results));
                            }
                        }
                    });
                    let _ = n;
                    continue;
                }
                EngineEvent::SecondPassDecodes(results) => {
                    log::info!("[UI] Second-pass: {} recoveries", results.len());
                    // Deduplicate by hypothesis — keep only the best (highest n_confirmed)
                    // result per unique hypothesis string. Multiple pings may confirm the
                    // same message; showing each one floods the display with identical rows.
                    let mut best_by_hyp: std::collections::HashMap<String, second_pass::SecondPassResult> =
                        std::collections::HashMap::new();
                    for r in results {
                        let entry = best_by_hyp.entry(r.hypothesis.clone()).or_insert_with(|| r.clone());
                        if r.n_confirmed > entry.n_confirmed
                            || (r.n_confirmed == entry.n_confirmed && r.mean_confidence > entry.mean_confidence)
                        {
                            *entry = r;
                        }
                    }
                    // Sort for stable insertion order: most confirmed first
                    let mut deduped: Vec<second_pass::SecondPassResult> =
                        best_by_hyp.into_values().collect();
                    deduped.sort_by(|a, b| b.n_confirmed.cmp(&a.n_confirmed)
                        .then(b.mean_confidence.partial_cmp(&a.mean_confidence).unwrap()));

                    for r in deduped {
                        let ts = chrono::DateTime::parse_from_rfc3339(&r.detected_at)
                            .map(|dt| dt.with_timezone(&chrono::Utc))
                            .unwrap_or_else(|_| chrono::Utc::now());
                        let hyp_chars: Vec<char> = r.hypothesis.chars().collect();
                        let mut second_pass_chars: Vec<(char, bool)> = Vec::new();
                        let mut conf_iter = r.confirmed_chars.iter().peekable();
                        for &hc in &hyp_chars {
                            let confirmed = if let Some((cc, _)) = conf_iter.peek() {
                                if *cc == hc.to_ascii_uppercase() {
                                    conf_iter.next(); true
                                } else { false }
                            } else { false };
                            // Show confirmed chars; replace unconfirmed with space
                            let display_ch = if confirmed { hc } else { ' ' };
                            second_pass_chars.push((display_ch, confirmed));
                        }
                        let entry = DecodeEntry {
                            timestamp: ts, df_hz: r.df_hz, ccf_ratio: r.ccf_ratio,
                            duration_ms: 0.0, confidence: r.mean_confidence, score: 0,
                            raw: r.hypothesis.clone(), callsigns: vec![], locator: None,
                            report: None, is_cq: false, char_confs: vec![],
                            is_accumulated: false, my_call: String::new(), df_bin_hz: None,
                            passed_threshold: true, is_second_pass: true, second_pass_chars,
                        };
                        let pos = self.decodes.partition_point(|d| d.timestamp <= ts);
                        self.decodes.insert(pos, entry);
                    }
                    continue;
                }
                EngineEvent::Spectrum(bins, rms) => {
                    // Only clear on slot boundary when in RX — avoids wiping the
                    // waterfall right at TX→RX transition due to wall-clock misalignment.
                    let is_tx = self.tx_active.load(std::sync::atomic::Ordering::Relaxed);
                    if !is_tx {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
                        let slot = now_ms / 30_000;
                        if slot != self.wf_period_idx {
                            self.wf_columns.clear();
                            self.wf_amplitude.clear();
                            self.wf_period_idx = slot;
                            self.df_dots.clear();
                        }
                    }
                    self.wf_columns.push(bins);
                    self.wf_amplitude.push(rms);
                    continue;
                }
                EngineEvent::Hamlib(upd) => {
                    if let Some(f) = upd.freq {
                        // Detect significant QSY (>10kHz) — reset EMA noise floor
                        if let Some(prev) = self.rig_freq_hz {
                            let delta_hz = (f as i64 - prev as i64).abs();
                            if delta_hz > 10_000 {
                                log::info!("[NOISE] QSY detected ({:.3}→{:.3} MHz) — resetting noise floor EMA",
                                    prev as f64 / 1e6, f as f64 / 1e6);
                                self.settings.qsy_reset = true;
                                let _ = self.settings_watch_tx.send(self.settings.clone());
                                self.settings.qsy_reset = false;
                            }
                        }
                        self.rig_freq_hz  = Some(f);
                        if self.base_freq_hz.is_none() { self.base_freq_hz = Some(f); }
                        self.cat_connected = true;
                        self.last_cat_rx  = Some(std::time::Instant::now());
                    }
                    if let Some(false) = upd.connected {
                        self.cat_connected = false;
                        self.rig_freq_hz   = None;
                    }
                    if upd.transmitting != self.is_transmitting {
                        self.is_transmitting = upd.transmitting;
                        if !upd.transmitting {
                            self.set_serial_ptt(false); // belt-and-braces deassert on TX end
                        }
                        // Only update tx_active from PTT events (freq=None), not freq polls (freq=Some)
                        // This prevents freq polls (which always send transmitting=false) from
                        // incorrectly clearing tx_active during transmission
                        if upd.freq.is_none() {
                            let prev = self.tx_active.load(std::sync::atomic::Ordering::Relaxed);
                            if prev != upd.transmitting {
                                log::info!("[TX_ACTIVE] PTT event: {} → {} at {}",
                                    prev, upd.transmitting,
                                    chrono::Utc::now().format("%H:%M:%S%.3f"));
                            }
                            self.tx_active.store(upd.transmitting, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    continue;
                }
                EngineEvent::Decode(entry) => {
                    // Discard loopback during TX
                    if self.is_transmitting { continue; }
            if self.qso.is_active() {
                let parsed = ParsedMessage {
                    raw: entry.raw.clone(), callsigns: entry.callsigns.clone(),
                    locator: entry.locator.clone(), report: entry.report.clone(),
                    is_cq: entry.is_cq, message_type: filter::MessageType::Garbage,
                    validity_score: entry.score, valid_callsigns: entry.callsigns.clone(),
                };
                let mc = self.settings.my_call.clone();
                let rs = self.report_sent.clone();
                log::info!("[APP] on_decode: qso={:?} raw={}", std::mem::discriminant(&self.qso), parsed.raw);
                let t  = on_decode(&mut self.qso, &parsed, &mc, move || rs.clone());
                log::info!("[APP] on_decode result: {:?}", t);
                match t {
                    Transition::Auto(msg) => {
                        self.qso_log.push(format!("AUTO: {}", msg));
                        if let Some(tx_msg) = self.qso.tx_message() {
                            let dev = self.settings.audio_out();
                            let _ = self.tx_cmd_tx.send(TxCommand::Transmit { message: tx_msg, output_device: dev });
                        }
                    }
                    Transition::Complete => {
                        log::info!("[APP] HALT from Transition::Complete");
                        self.qso_log.push("QSO COMPLETE — click LOG QSO".to_string());
                        let _ = self.tx_cmd_tx.send(TxCommand::Halt);
                        self.is_transmitting = false;
                    }
                    Transition::Count73(0) => {
                        log::info!("[APP] HALT from Count73(0)");
                        let _ = self.tx_cmd_tx.send(TxCommand::Halt);
                        self.is_transmitting = false;
                    }
                    _ => {}
                }
            }
            // Update seen callsigns
            for call in &entry.callsigns {
                // Merge fragments: I5Y / I5YD / I5YDI → longest form
                let longer = self.seen_calls.iter_mut()
                    .find(|(c,_,_)| c.starts_with(call.as_str()) && c.len() > call.len());
                if let Some(e) = longer { e.2 += 1; continue; }
                if let Some(e) = self.seen_calls.iter_mut()
                    .find(|(c,_,_)| call.starts_with(c.as_str()) && call.len() > c.len())
                {
                    let (_, ts, n) = e.clone();
                    *e = (call.clone(), ts, n + 1);
                    continue;
                }
                if let Some(e) = self.seen_calls.iter_mut().find(|(c,_,_)| c == call) {
                    e.2 += 1;
                } else {
                    self.seen_calls.push((call.clone(), entry.timestamp, 1));
                }
            }
            // Update station tracker
            if self.settings.station_tracker_enabled {
                self.station_tracker.add_ping(&entry);
            }
            self.decodes.push(entry.clone());
            if self.decodes.len() > 500 { self.decodes.remove(0); }
            // DF scatter dot
            let (dr,dg,db) = if entry.confidence >= 0.75 { (80,220,80) }
                else if entry.confidence >= 0.50 { (220,200,80) }
                else { (120,120,120) };
            self.df_dots.push(DfDot { col_idx: self.wf_columns.len(), df_hz: entry.df_hz, ccf: entry.ccf_ratio, r:dr,g:dg,b:db });
            if self.df_dots.len() > 2000 { self.df_dots.remove(0); }
            } // end Decode arm
            } // end match
        }
        // Filter — don't process decodes while transmitting (half duplex)
        if self.is_transmitting { self.decodes.retain(|_| true); }
    }

    fn populate_tx_msgs(&mut self) {
        let mc   = self.settings.my_call.clone();
        let ml   = self.settings.my_loc.clone();
        let tc   = self.their_call_edit.trim().to_uppercase();
        let rs   = self.report_sent.clone();
        let loc4 = ml[..ml.len().min(4)].to_string();

        // Only repopulate when callsign/locator/report changes — preserve user edits
        let key = format!("{},{},{},{},{}", mc, loc4, tc, rs, self.settings.my_loc);
        if key == self.tx_msgs_key { return; }
        self.tx_msgs_key = key;

        // CQ format: "CQ MY_CALL MY_GRID4" (CQ first — standard FSK441)
        self.tx_msgs[0] = format!("CQ {} {}", mc, loc4);
        // TX1: THEIR_CALL MY_CALL (no locator)
        self.tx_msgs[1] = if tc.is_empty() { format!("<CALL> {}", mc) }
                          else { format!("{} {}", tc, mc) };
        // TX2: THEIR_CALL MY_CALL REPORT REPORT
        self.tx_msgs[2] = if tc.is_empty() { format!("<CALL> {} {} {}", mc, rs, rs) }
                          else { format!("{} {} {} {}", tc, mc, rs, rs) };
        // TX3: THEIR_CALL MY_CALL R MY_REPORT
        self.tx_msgs[3] = if tc.is_empty() { format!("<CALL> {} R{} R{}", mc, rs, rs) }
                          else { format!("{} {} R{} R{}", tc, mc, rs, rs) };
        // TX4: RRRR RRRR MY_CALL (WSJT/MSHV standard)
        self.tx_msgs[4] = format!("RRRR RRRR {}", mc);
        // TX5: 73 73 73 MY_CALL
        self.tx_msgs[5] = format!("73 73 73 {}", mc);
    }

    fn send_tx(&mut self, msg: &str) {
        self.set_serial_ptt(true);
        let dev = self.settings.audio_out();
        // ReArm clears the halted guard set by STOP — must precede Transmit
        let _ = self.tx_cmd_tx.send(TxCommand::ReArm);
        let _ = self.tx_cmd_tx.send(TxCommand::SetVolume(self.settings.tx_level));
        let _ = self.tx_cmd_tx.send(TxCommand::Transmit {
            message: msg.to_string(),
            output_device: dev,
        });
        self.is_transmitting = true;
        // tx_active is controlled solely by hamlib PTT events — do NOT set here
        // Setting it here causes the spectrum to gate immediately on button press
        // even when transmitting won't start until the next TX slot
        // Second-pass is spawned from the AnalysisSaved handler.
    }


    fn clear_decodes(&mut self) {
        self.decodes.clear();
        self.seen_calls.clear();
        self.df_dots.clear();
        self.qso_summary = None;
        self.selected = None;
    }

    fn generate_qso_summary(&mut self) {
        let tc = self.their_call_edit.trim().to_uppercase();
        let tl = self.their_loc_edit.trim().to_uppercase();
        let mc = self.settings.my_call.trim().to_uppercase();
        let ml = self.settings.my_loc.trim().to_uppercase();
        let rs = self.report_sent.trim().to_string();
        let rr = self.report_rcvd.trim().to_string();
        let their_pings: Vec<&DecodeEntry> = self.decodes.iter()
            .filter(|e| e.callsigns.iter().any(|c| c.contains(&tc) || tc.contains(c.as_str())))
            .collect();
        let first_ping = their_pings.first()
            .map(|e| e.timestamp.format("%H:%M:%S").to_string())
            .unwrap_or_else(|| "?".to_string());
        let last_ping = their_pings.last()
            .map(|e| e.timestamp.format("%H:%M:%S").to_string())
            .unwrap_or_else(|| "?".to_string());
        let n_pings  = their_pings.len();
        let peak_ccf = their_pings.iter().map(|e| e.ccf_ratio as u64).max().unwrap_or(0);
        let now = chrono::Utc::now();
        let summary = format!(
            "QSO {}\n{} ↔ {}\nMy: {} Their: {}\nRpt: {} / {}\n{} pings {}-{}\nPeak CCF: {}",
            now.format("%Y-%m-%d %H:%M UTC"),
            mc, tc,
            ml, if tl.is_empty() { "?".to_string() } else { tl },
            if rs.is_empty() { "?".to_string() } else { rs },
            if rr.is_empty() { "?".to_string() } else { rr },
            n_pings, first_ping, last_ping,
            peak_ccf,
        );
        log::info!("[QSO] Summary: {}", summary);
        self.qso_summary = Some(summary);
    }

    fn export_qso_transcript(&self, rec: &QsoRecord) -> String {
        let div = "─".repeat(62);
        let mut out = String::new();
        out.push_str(&format!("FSK441+ QSO Transcript
"));
        out.push_str(&format!("{} ↔ {}  |  {}  |  {:.3} MHz
",
            rec.station_callsign, rec.callsign,
            if rec.qso_date.len() == 8 {
                format!("{}-{}-{}", &rec.qso_date[0..4], &rec.qso_date[4..6], &rec.qso_date[6..8])
            } else { rec.qso_date.clone() },
            rec.freq_mhz.unwrap_or(144.370),
        ));
        out.push_str(&format!("{}
", div));
        out.push_str(&format!("{:<10} {:>7} {:>7}  Decode
", "Time", "DF(Hz)", "Dur/S"));
        out.push_str(&format!("{}
", div));

        // Filter decodes to this QSO — their callsign, within QSO timeframe
        let their = rec.callsign.to_uppercase();
        // Parse time_on/time_off from HHMMSS
        let parse_t = |t: &str| -> Option<u32> {
            if t.len() >= 4 {
                let h: u32 = t[0..2].parse().ok()?;
                let m: u32 = t[2..4].parse().ok()?;
                let s: u32 = if t.len() >= 6 { t[4..6].parse().ok()? } else { 0 };
                Some(h * 3600 + m * 60 + s)
            } else { None }
        };
        let t_on  = parse_t(&rec.time_on).unwrap_or(0);
        let t_off = parse_t(&rec.time_off).unwrap_or(86400);

        let relevant: Vec<&DecodeEntry> = self.decodes.iter()
            .filter(|e| !e.is_accumulated)
            .filter(|e| {
                // Time match — within QSO window
                let es = e.timestamp.timestamp() % 86400;
                let et = es as u32;
                et >= t_on.saturating_sub(60) && et <= t_off + 60
            })
            .filter(|e|
                // Contains their callsign or partial match
                e.callsigns.iter().any(|c| c.contains(&their) || their.contains(c.as_str()))
                || e.raw.to_uppercase().contains(&their[their.len().min(4)-4.min(their.len())..])
            )
            .collect();

        // If no filtered results, include all pings in the time window
        let pings: Vec<&DecodeEntry> = if relevant.is_empty() {
            self.decodes.iter().filter(|e| !e.is_accumulated).filter(|e| {
                let et = (e.timestamp.timestamp() % 86400) as u32;
                et >= t_on.saturating_sub(60) && et <= t_off + 60
            }).collect()
        } else { relevant };

        for e in &pings {
            let strength = match e.ccf_ratio as u32 {
                0..=49    => 1u32, 50..=99   => 2, 100..=199  => 3,
                200..=399 => 4,   400..=799  => 5, 800..=1599  => 6,
                1600..=3199 => 7, 3200..=6399 => 8, _ => 9,
            };
            let dur_str = if e.duration_ms > 0.0 {
                format!("{:.0}/{}", e.duration_ms, strength)
            } else { String::new() };
            out.push_str(&format!("{:<10} {:>+7.0} {:>7}  {}
",
                e.timestamp.format("%H:%M:%S"), e.df_hz, dur_str, e.raw.trim()));
        }

        // Accumulated entries
        let acc: Vec<&DecodeEntry> = self.decodes.iter()
            .filter(|e| e.is_accumulated)
            .collect();
        if !acc.is_empty() {
            out.push_str(&format!("{}
", div));
            for e in acc {
                out.push_str(&format!("★ {:<10} {:>+7.0}  {}
",
                    e.timestamp.format("%H:%M:%S"), e.df_hz, e.raw.trim()));
            }
        }

        out.push_str(&format!("{}
", div));
        out.push_str(&format!(
            "QSO: {}Z → {}Z  |  Sent: {}  Rcvd: {}  |  {} pings  Peak CCF: {}
",
            if rec.time_on.len()>=4  { format!("{}:{}", &rec.time_on[0..2],  &rec.time_on[2..4])  } else { rec.time_on.clone() },
            if rec.time_off.len()>=4 { format!("{}:{}", &rec.time_off[0..2], &rec.time_off[2..4]) } else { rec.time_off.clone() },
            rec.rst_sent, rec.rst_rcvd, rec.nr_pings, rec.peak_ccf,
        ));
        if !rec.gridsquare.is_empty() {
            out.push_str(&format!("Their grid: {}  My grid: {}
",
                rec.gridsquare, rec.my_gridsquare));
        }
        out.push_str(&format!("{}
", div));
        out.push_str(&format!("Generated by FSK441+  {}  {}
",
            rec.station_callsign, rec.my_gridsquare));
        out
    }

    fn halt_tx(&mut self) {
        log::info!("[APP] HALT from halt_tx()");
        self.set_serial_ptt(false);
        let _ = self.tx_cmd_tx.send(TxCommand::Halt);
        self.is_transmitting = false;
        // tx_active is now controlled by TxEngine directly — it will set false
        // when PTT actually drops after audio finishes. Don't force it here or
        // we'll start decoding our own TX audio before it stops.
    }
}

impl eframe::App for Fsk441App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        ctx.set_visuals(egui::Visuals::dark());
        self.poll_events();
        // Click on empty space in central panel deselects
        if ctx.input(|i| i.pointer.any_click()) {
            if !ctx.is_pointer_over_area() {
                // pointer not over any egui widget — deselect
            }
        }
        ctx.request_repaint_after(std::time::Duration::from_millis(200));

        // ── Top bar ───────────────────────────────────────────────────────
        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                // FSK441 + callsign
                ui.label(egui::RichText::new("FSK441")
                    .strong().color(egui::Color32::from_rgb(0, 150, 255)));
                ui.separator();
                ui.label(egui::RichText::new(
                    format!("{} {}", self.settings.my_call, self.settings.my_loc))
                    .strong());
                ui.separator();

                // TX/RX slot countdown — always visible
                {
                    let (slot, remaining) = self.period_timer.current_slot();
                    ui.label(match slot {
                        SlotState::Tx => egui::RichText::new(format!("■ TX {:02}s", remaining))
                            .color(egui::Color32::RED).strong(),
                        SlotState::Rx => egui::RichText::new(format!("○ RX {:02}s", remaining))
                            .color(egui::Color32::from_rgb(0, 200, 100)),
                    });
                    ui.separator();
                }

                // Frequency display with KHz digit tuning
                // All elements in ONE horizontal line — ▲ digit ▼ side by side per digit
                // Clamped to ±250KHz from base frequency (band-agnostic)
                if self.cat_connected {
                    if let Some(freq) = self.rig_freq_hz {
                        let mhz     = freq / 1_000_000;
                        let khz     = (freq % 1_000_000) / 1_000;
                        let hz_part = (freq % 1_000) / 10;  // show as 2 digits
                        let col     = egui::Color32::from_rgb(100, 200, 130);
                        let col_dim = egui::Color32::from_rgb(60, 120, 80);
                        let col_amb = egui::Color32::from_rgb(200, 150, 50);

                        // ±250KHz clamp from base freq (set on first CAT connect)
                        let base = self.base_freq_hz.unwrap_or(freq);
                        let lo   = base.saturating_sub(250_000);
                        let hi   = base + 250_000;

                        let mut new_khz: Option<u64> = None;

                        // Freq: "144." [editable KHz] ".000 MHz"
                        // Click KHz field, type 3 digits, auto-QSY on 3rd digit or Enter
                        // Out of range: clears and shows original
                        ui.spacing_mut().item_spacing.x = 0.0;
                        ui.label(egui::RichText::new(format!("{}.", mhz))
                            .monospace().size(14.0).color(col));

                        if self.freq_editing {
                            // Active TextEdit — amber, accepts digit input
                            // Pre-populated with current kHz so user sees what they're changing
                            let resp = ui.add(
                                egui::TextEdit::singleline(&mut self.freq_edit)
                                    .desired_width(32.0)
                                    .font(egui::TextStyle::Monospace)
                                    .text_color(col_amb)
                                    .frame(true)
                            );
                            // Grab focus immediately on first render
                            if !resp.has_focus() { resp.request_focus(); }
                            // Keep only digits, max 3
                            self.freq_edit.retain(|c| c.is_ascii_digit());
                            if self.freq_edit.len() > 3 { self.freq_edit.truncate(3); }

                            // Commit on: 3rd digit entered OR Enter — no blank-box loop
                            let auto_commit = self.freq_edit.len() == 3 && resp.changed();
                            let enter_commit = ui.input(|i| i.key_pressed(egui::Key::Enter));
                            let focus_lost   = resp.lost_focus()
                                && !ui.input(|i| i.key_pressed(egui::Key::Escape));

                            if auto_commit || enter_commit || focus_lost {
                                if self.freq_edit.len() == 3 {
                                    if let Ok(new_k) = self.freq_edit.parse::<u64>() {
                                        let new_f = mhz * 1_000_000 + new_k * 1_000;
                                        if new_f >= lo && new_f <= hi {
                                            new_khz = Some(new_f);
                                        }
                                    }
                                }
                                self.freq_edit.clear();
                                self.freq_editing = false;
                                // Explicitly drop egui focus so no underline or cursor lingers
                                ui.memory_mut(|m| m.surrender_focus(resp.id));
                            }
                            // Escape cancels without sending
                            if ui.input(|i| i.key_pressed(egui::Key::Escape)) {
                                self.freq_edit.clear();
                                self.freq_editing = false;
                            }
                        } else {
                            // Idle — click to start editing.
                            // Drawn via painter (not egui::Label) to avoid the default
                            // clickable-label underline that egui adds to interactive labels.
                            let text = format!("{:03}", khz);
                            let galley = ui.fonts(|f| f.layout_no_wrap(
                                text.clone(),
                                egui::FontId::monospace(14.0),
                                col,
                            ));
                            let (rect, r) = ui.allocate_exact_size(
                                galley.size(), egui::Sense::click()
                            );
                            if r.hovered() {
                                ui.output_mut(|o| o.cursor_icon = egui::CursorIcon::Text);
                            }
                            let clicked = r.clicked();
                            ui.painter().galley(rect.min, galley, col);
                            r.on_hover_text(format!(
                                "Click to change — type 3 digits (e.g. {})", text
                            ));
                            if clicked {
                                // Pre-populate with current value so it's never a blank box
                                self.freq_edit = format!("{:03}", khz);
                                self.freq_editing = true;
                            }
                        }

                        // ".000 MHz" suffix
                        ui.label(egui::RichText::new(".")
                            .monospace().size(14.0).color(col_dim));
                        let hz_col = if hz_part == 0 { col_dim } else { col_amb };
                        ui.label(egui::RichText::new(format!("{:02}", hz_part))
                            .monospace().size(9.0).color(hz_col));
                        ui.label(egui::RichText::new(" MHz")
                            .monospace().size(14.0).color(col));

                        if let Some(f) = new_khz {
                            self.rig_freq_hz = Some(f);
                            let _ = self.tx_cmd_tx.send(TxCommand::SetFreq(f));
                        }
                    }
                } else {
                    ui.label(egui::RichText::new("No CAT")
                        .monospace().color(egui::Color32::from_gray(180)));
                }
                ui.separator();

                // Period selector inline
                egui::ComboBox::from_id_salt("period_top")
                    .width(80.0)
                    .selected_text(match self.settings.period {
                        Period::TxFirst    => "1st·30s",
                        Period::TxSecond   => "2nd·30s",
                        Period::TxFirst15  => "1st·15s",
                        Period::TxSecond15 => "2nd·15s",
                    })
                    .show_ui(ui, |ui| {
                        for (p, label) in [
                            (Period::TxFirst,    "TX 1st 30s"),
                            (Period::TxSecond,   "TX 2nd 30s"),
                            (Period::TxFirst15,  "TX 1st 15s"),
                            (Period::TxSecond15,  "TX 2nd 15s"),
                        ] {
                            if ui.selectable_value(&mut self.settings.period, p, label).clicked() {
                                self.period_timer = PeriodTimer::new(p);
                                let _ = self.tx_cmd_tx.send(TxCommand::SetPeriod(p));
                                save_config(&self.settings);
                            }
                        }
                    });
                ui.separator();

                // UTC clock
                let now_utc = chrono::Utc::now();
                ui.label(egui::RichText::new(now_utc.format("%H:%M:%S").to_string())
                    .monospace().color(egui::Color32::from_gray(200)));

                // SET state: their_call_hint is Some once SET is pressed.
                // Callsign shown from SET. ● IN QSO + elapsed only once TX starts.
                // GC distance, bearing, scatter arc live in the QSO panel row below.
                let call_set = self.settings.their_call_hint.is_some();
                let in_qso   = call_set && self.qso_time_on.is_some();

                if call_set {
                    ui.separator();
                    ui.add_space(4.0);

                    // ● IN QSO — only after TX starts
                    if in_qso {
                        ui.label(egui::RichText::new("● IN QSO")
                            .monospace().strong()
                            .color(egui::Color32::from_rgb(255, 180, 0)));
                        ui.add_space(8.0);
                    }

                    // Callsign
                    if let Some(ref call) = self.settings.their_call_hint {
                        ui.label(egui::RichText::new(call)
                            .monospace().strong()
                            .color(egui::Color32::from_rgb(100, 220, 100)));
                    }
                    ui.add_space(8.0);

                    // QSO start time and elapsed — only after TX starts
                    if let Some(t_on) = self.qso_time_on {
                        let elapsed = now_utc.signed_duration_since(t_on);
                        let mins = elapsed.num_minutes();
                        let secs = elapsed.num_seconds() % 60;
                        ui.label(egui::RichText::new(t_on.format("%H:%MZ").to_string())
                            .monospace()
                            .color(egui::Color32::from_gray(200)));
                        ui.add_space(4.0);
                        ui.label(egui::RichText::new(format!("+{}:{:02}", mins, secs))
                            .monospace()
                            .color(egui::Color32::from_gray(200)));
                    }
                }

                // Settings far right
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("⚙ Settings").clicked() {
                        self.settings_open = !self.settings_open;
                        if self.settings_open { self.refresh_audio_devices(); }
                    }
                });
            });
        });

        // ── QSO panel (bottom) ────────────────────────────────────────────
        egui::TopBottomPanel::bottom("qso").min_height(190.0).show(ctx, |ui| {
            ui.add_space(4.0);

            ui.horizontal(|ui| {
                ui.vertical(|ui| {
                    ui.label(egui::RichText::new("Rpt Sent").color(egui::Color32::from_gray(220)));
                    egui::ComboBox::from_id_salt("rpt_s")
                        .width(55.0).selected_text(&self.report_sent)
                        .show_ui(ui, |ui| {
                            for r in &["26","27","28","29","36","37","47","57","59"] {
                                if ui.selectable_value(&mut self.report_sent, r.to_string(), *r).clicked() {
                                    self.tx_msgs_key.clear();
                                }
                            }
                        });
                });
                ui.separator();
                ui.vertical(|ui| {
                    ui.label(egui::RichText::new("Their Call").color(egui::Color32::from_gray(220)));
                    // Lock fields when Their Call is set (QSO in progress)
                    // Field: free-form entry, locked only when QSO active
                    let field_locked = self.settings.their_call_hint.is_some();
                    let edit_resp = ui.add(egui::TextEdit::singleline(&mut self.their_call_edit)
                        .desired_width(80.0).hint_text("eg I5YDI")
                        .interactive(!field_locked));
                    if edit_resp.changed() {
                        self.tx_msgs_key.clear();
                        if self.their_call_edit.is_empty() {
                            self.settings.their_call_hint = None;
                            let _ = self.settings_watch_tx.send(self.settings.clone());
                        }
                    }
                    if field_locked {
                        // Show lock + click to unlock
                        if ui.label(egui::RichText::new("[locked]")
                            .color(egui::Color32::YELLOW))
                            .on_hover_text("Click ⟳ Clear to unlock")
                            .clicked() {}
                    } else {
                        // Set button — activates QSO mode
                        let call = self.their_call_edit.trim().to_uppercase();
                        let set_btn = ui.add_enabled(
                            !call.is_empty(),
                            egui::Button::new(egui::RichText::new("Set")
                                .color(egui::Color32::from_rgb(100,200,100)))
                        );
                        if set_btn.clicked() {
                            self.settings.their_call_hint = Some(call);
                            self.tx_msgs_key.clear();
                            let _ = self.settings_watch_tx.send(self.settings.clone());
                        }
                    }
                });
                ui.vertical(|ui| {
                    ui.label(egui::RichText::new("Their Loc").color(egui::Color32::from_gray(220)));
                    ui.add(egui::TextEdit::singleline(&mut self.their_loc_edit)
                        .desired_width(55.0).hint_text("JN53"));
                });
                ui.vertical(|ui| {
                    ui.label(egui::RichText::new("Rpt Rcvd").color(egui::Color32::from_gray(220)));
                    ui.add(egui::TextEdit::singleline(&mut self.report_rcvd)
                        .desired_width(40.0).hint_text("26"));
                });

                // Scatter arc — shown once SET pressed and locator ≥ 4 chars
                if self.settings.their_call_hint.is_some() {
                    let their_loc = self.their_loc_edit.trim().to_uppercase();
                    if their_loc.len() >= 4 {
                        if let (Some(my_qth), Some(their_qth)) = (
                            geo::Qth::from_maidenhead(&self.settings.my_loc),
                            geo::Qth::from_maidenhead(&their_loc),
                        ) {
                            let dist    = great_circle_km(my_qth.lat, my_qth.lon,
                                                          their_qth.lat, their_qth.lon);
                            let bearing = great_circle_bearing(my_qth.lat, my_qth.lon,
                                                               their_qth.lat, their_qth.lon);
                            ui.separator();
                            ui.vertical(|ui| {
                                ui.label(egui::RichText::new("Distance")
                                    .color(egui::Color32::from_gray(220)));
                                ui.label(egui::RichText::new(format!(
                                    "{:.0}km {:.0}°", dist, bearing))
                                    .monospace()
                                    .color(egui::Color32::from_rgb(180, 180, 255)));
                            });

                            if let Some(arc) = scatter::compute_scatter_arc(
                                my_qth.lat, my_qth.lon,
                                their_qth.lat, their_qth.lon,
                                self.settings.ant_bw_horiz as f64,
                            ) {
                                ui.separator();
                                ui.vertical(|ui| {
                                    ui.label(egui::RichText::new("Scatter Arc")
                                        .color(egui::Color32::from_gray(220)));
                                    ui.label(egui::RichText::new(format!(
                                        "{:.0}°–{:.0}°", arc.arc_min, arc.arc_max))
                                        .monospace()
                                        .color(egui::Color32::from_rgb(255, 200, 80)));
                                });
                                ui.vertical(|ui| {
                                    ui.label(egui::RichText::new("Beam")
                                        .color(egui::Color32::from_gray(220)));
                                    match (arc.beam_left, arc.beam_right) {
                                        (Some(bl), Some(br)) => {
                                            ui.label(egui::RichText::new(format!(
                                                "{:.0}° / {:.0}°", bl, br))
                                                .monospace()
                                                .color(egui::Color32::from_rgb(100, 255, 180)));
                                        }
                                        (Some(centre), None) => {
                                            ui.label(egui::RichText::new(format!(
                                                "{:.0}°", centre))
                                                .monospace()
                                                .color(egui::Color32::from_rgb(100, 255, 180)));
                                        }
                                        _ => {}
                                    }
                                });
                                ui.vertical(|ui| {
                                    ui.label(egui::RichText::new("El")
                                        .color(egui::Color32::from_gray(220)));
                                    let half_v = self.settings.ant_bw_vert as f64 / 2.0;
                                    let el_col = if arc.midpoint_el <= half_v {
                                        egui::Color32::from_gray(160)
                                    } else {
                                        egui::Color32::from_rgb(255, 120, 80)
                                    };
                                    ui.label(egui::RichText::new(format!(
                                        "{:.0}°", arc.midpoint_el))
                                        .monospace()
                                        .color(el_col));
                                });
                            }
                        }
                    }
                    // LOG QSO — right-justified, appears when report received
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let can_log = !self.their_call_edit.trim().is_empty()
                            && self.qso_time_on.is_some()
                            && !self.qso_logged;
                        let rpt_valid = {
                            let raw = self.report_rcvd.trim().to_uppercase();
                            let r = raw.trim_start_matches('R').trim();
                            r.len() == 2 && r.chars().all(|c| c.is_ascii_digit())
                                && r.parse::<u8>().map(|n| n >= 26 && n <= 59).unwrap_or(false)
                        };
                        if self.qso_logged {
                            ui.label(egui::RichText::new("✓ Logged")
                                .color(egui::Color32::from_rgb(80, 180, 80)).strong());
                        } else if can_log {
                            let btn_color = if rpt_valid {
                                egui::Color32::from_rgb(50, 200, 80)
                            } else {
                                egui::Color32::from_gray(170)
                            };
                            let btn = ui.add_enabled(
                                rpt_valid,
                                egui::Button::new(
                                    egui::RichText::new("📋 LOG QSO").color(btn_color).strong()
                                )
                            ).on_hover_text(if rpt_valid {
                                "Write QSO to ~/.fsk441/fsk441.adi"
                            } else {
                                "Enter received report (26-59) before logging"
                            });
                            if btn.clicked() {
                                // Inline log — duplicate of the full handler below
                                // to keep button in the right place
                                let now      = chrono::Utc::now();
                                let time_on  = self.qso_time_on.unwrap_or(now);
                                let callsign = self.their_call_edit.trim().to_uppercase();
                                let grid = {
                                    let from_edit = self.their_loc_edit.trim().to_uppercase();
                                    if !from_edit.is_empty() { from_edit }
                                    else {
                                        self.decodes.iter()
                                            .filter(|e| e.callsigns.iter().any(|c| c.contains(&callsign)))
                                            .filter_map(|e| e.locator.clone())
                                            .next().unwrap_or_default()
                                    }
                                };
                                let my_grid = {
                                    let loc = self.settings.my_loc.trim().to_uppercase();
                                    if loc.len() >= 4 { loc } else { format!("{}KM", loc) }
                                };
                                let rst_s = self.report_sent.trim().to_string();
                                let rst_r = {
                                    let r = self.report_rcvd.trim().to_uppercase();
                                    let r = r.trim_start_matches('R').trim().to_string();
                                    let valid = r.len() == 2
                                        && r.chars().all(|c| c.is_ascii_digit())
                                        && r.parse::<u8>().map(|n| n >= 26 && n <= 59).unwrap_or(false);
                                    if valid { r } else { String::new() }
                                };
                                let freq_mhz = self.rig_freq_hz.map(|h| h as f64 / 1_000_000.0);
                                let n_pings = self.decodes.iter()
                                    .filter(|e| e.callsigns.iter().any(|c| c.contains(&callsign)))
                                    .count() as u32;
                                let peak_ccf = self.decodes.iter()
                                    .filter(|e| e.callsigns.iter().any(|c| c.contains(&callsign)))
                                    .map(|e| e.ccf_ratio as u64).max().unwrap_or(0);
                                let rec = QsoRecord {
                                    station_callsign: self.settings.my_call.trim().to_uppercase(),
                                    callsign: callsign.clone(),
                                    qso_date: time_on.format("%Y%m%d").to_string(),
                                    time_on:  time_on.format("%H%M%S").to_string(),
                                    time_off: now.format("%H%M%S").to_string(),
                                    band: "2M".to_string(),
                                    freq_mhz,
                                    mode: "FSK441".to_string(),
                                    rst_sent: rst_s.clone(),
                                    rst_rcvd: rst_r.clone(),
                                    gridsquare: grid,
                                    my_gridsquare: my_grid,
                                    prop_mode: "MS".to_string(),
                                    nr_pings: n_pings,
                                    peak_ccf,
                                    comment: format!("FSK441+ peak CCF={}", peak_ccf),
                                };
                                match self.adif_logger.append(&rec) {
                                    Ok(()) => {
                                        self.adif_log.push(rec);
                                        self.qso_logged = true;
                                        self.qso_log.push(format!(
                                            "✓ LOGGED: {} S:{} R:{} → fsk441.adi",
                                            callsign, rst_s, rst_r));
                                    }
                                    Err(e) => { self.qso_log.push(format!("✗ Log failed: {}", e)); }
                                }
                                self.qso_time_on = None;
                                self.qso = QsoState::Idle;
                                self.settings.clear_accumulator = true;
                                let _ = self.settings_watch_tx.send(self.settings.clone());
                                self.settings.clear_accumulator = false;
                                self.decodes.retain(|e| !e.is_accumulated);
                            }
                        }
                    });
                }
            });

            ui.add_space(5.0);
            ui.separator();
            ui.add_space(3.0);

            // ── Editable TX message fields — WSJT style ───────────────────
            // Auto-populate defaults from callsign/locator/report fields.
            // User can edit any field freely before clicking its button.
            self.populate_tx_msgs();

            let labels = ["CQ", "TX1", "TX2", "TX3", "TX4", "TX5"];
            let _active_idx: Option<usize> = match &self.qso {
                QsoState::CallingCq { .. }     => Some(0),
                QsoState::CallingStation { .. } => Some(1),
                QsoState::SendingReport { .. }  => Some(2),
                QsoState::SendingRReport { .. } => Some(3),
                QsoState::SendingRR { .. }      => Some(4),
                QsoState::Sending73 { .. }      => Some(5),
                _                               => None,
            };

            ui.horizontal(|ui| {
            // ── Left: TX button rows ──────────────────────────────────────
            ui.vertical(|ui| {
            for i in 0..6usize {
                ui.horizontal(|ui| {
                    let is_queued = self.active_tx_idx == Some(i);
                    let is_txing  = self.is_transmitting && is_queued;
                    let btn_color = if is_txing {
                        egui::Color32::from_rgb(255, 80, 80)     // red = actively transmitting
                    } else if is_queued {
                        egui::Color32::from_rgb(255, 200, 0)     // amber = queued for next slot
                    } else {
                        egui::Color32::from_rgb(160, 160, 160)   // grey = idle
                    };
                    let btn = egui::Button::new(
                        egui::RichText::new(labels[i]).strong().color(btn_color)
                    ).min_size(egui::vec2(35.0, 0.0));

                    if ui.add(btn).clicked() {
                        let msg = self.tx_msgs[i].clone();
                        // Don't transmit if message still has placeholder
                        if msg.contains("<CALL>") {
                            self.qso_log.push("⚠ Enter Their Call first".to_string());
                        } else {
                            log::info!("[APP] {} clicked: {}", labels[i], msg);
                            self.qso_log.push(format!("{}: {}", labels[i], msg));
                            // Auto-set their_call_hint from field if not already set
                            let tc = self.their_call_edit.trim().to_uppercase();
                            if !tc.is_empty() && self.settings.their_call_hint.is_none() {
                                self.settings.their_call_hint = Some(tc);
                                let _ = self.settings_watch_tx.send(self.settings.clone());
                            }
                            // Start QSO timer on first TX press of any kind
                            if self.qso_time_on.is_none() {
                                self.qso_time_on = Some(chrono::Utc::now());
                                self.qso_logged = false;
                            }
                            self.active_tx_idx = Some(i);
                            self.send_tx(&msg);
                            // TX5 = 73 sent — generate QSO summary
                            if i == 5 {
                                self.generate_qso_summary();
                            }
                        }
                    }

                    ui.add(egui::TextEdit::singleline(&mut self.tx_msgs[i])
                        .desired_width(360.0)
                        .font(egui::TextStyle::Monospace));

                });
            } // end TX rows for loop
            }); // end left vertical

            // ── Right: STOP/Clear + Accumulated decodes ───────────────────
            ui.separator();
            ui.vertical(|ui| {
                ui.horizontal(|ui| {
                    let is_tx = self.is_transmitting || self.active_tx_idx.is_some();
                    let stop_text = if is_tx {
                        egui::RichText::new("■ STOP").color(egui::Color32::RED).strong()
                    } else {
                        egui::RichText::new("■ STOP").color(egui::Color32::from_gray(170))
                    };
                    if ui.add_enabled(is_tx, egui::Button::new(stop_text)).clicked() {
                        log::info!("[APP] STOP button clicked — reverting to RX");
                        self.halt_tx();
                        self.active_tx_idx = None;
                        self.qso = QsoState::Idle;
                        // Release QSO constraint so run_engine reverts to normal RX decoding
                        self.settings.their_call_hint = None;
                        self.settings.their_df_hz = None;
                        let _ = self.settings_watch_tx.send(self.settings.clone());
                        self.qso_time_on = None;
                        self.qso_log.push("STOPPED".to_string());
                    }
                    if ui.button(egui::RichText::new("⟳ Clear")
                        .color(egui::Color32::from_gray(180))).clicked()
                    {
                        self.their_call_edit.clear();
                        self.their_loc_edit.clear();
                        self.report_rcvd = "26".to_string();
                        self.active_tx_idx = None;
                        self.tx_msgs_key.clear();
                        self.halt_tx();
                        self.qso = QsoState::Idle;
                        self.settings.their_call_hint = None;
                        self.settings.their_df_hz = None;
                        let _ = self.settings_watch_tx.send(self.settings.clone());
                        self.qso_time_on = None;
                        self.qso_logged = false;
                        self.active_tx_idx = None;
                        self.clear_decodes();
                        self.settings.clear_accumulator = true;
                        let _ = self.settings_watch_tx.send(self.settings.clone());
                        self.settings.clear_accumulator = false;
                        self.qso_log.push("--- Cleared ---".to_string());
                    }

                    // NF, conf and k slider — right justified in this row
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let nf_col = if self.nf_settled {
                            egui::Color32::from_rgb(0, 200, 80)   // green — EMA converged
                        } else {
                            egui::Color32::from_rgb(180, 40, 40)  // dull red — still calibrating
                        };
                        ui.label(egui::RichText::new(format!("NF {:.2}", self.adaptive_threshold))
                            .color(nf_col)
                            .monospace())
                            .on_hover_text(format!(
                                "Adaptive noise floor gate\nμ={:.4}  σ={:.4}\n{}\nGreen = EMA settled  Red = still calibrating",
                                self.noise_mean, self.noise_sigma,
                                if self.nf_settled { "EMA converged" } else { "Warming up — needs more pings" }
                            ));
                        ui.separator();
                        let conf_col = if self.last_conf >= self.adaptive_threshold {
                            egui::Color32::from_rgb(0, 220, 100)
                        } else {
                            egui::Color32::from_gray(160)
                        };
                        ui.label(egui::RichText::new(format!("C {:.2}", self.last_conf))
                            .color(conf_col)
                            .monospace())
                            .on_hover_text("Confidence of last decoded ping. Green = above noise gate.");
                        ui.separator();
                        ui.label(egui::RichText::new("k").color(egui::Color32::from_gray(200)).monospace())
                            .on_hover_text(
                                "Detection sensitivity — sole threshold control.\n\
                                 Gate = noise_mean + k × sigma\n\
                                 Noise floor is measured automatically from band conditions.\n\n\
                                 k=5 (default): good balance for meteor scatter\n\
                                 Lower k: more sensitive, more noise visible\n\
                                 Higher k: cleaner display, may miss marginal pings"
                            );
                        let k_resp = ui.add(egui::Slider::new(&mut self.settings.k_sigma, 2.0..=10.0)
                            .fixed_decimals(1))
                            .on_hover_text(
                                "Detection sensitivity — sole threshold control.\n\
                                 Gate = noise_mean + k × sigma\n\
                                 Noise floor is measured automatically from band conditions.\n\n\
                                 k=5 (default): good balance for meteor scatter\n\
                                 Lower k: more sensitive, more noise visible\n\
                                 Higher k: cleaner display, may miss marginal pings"
                            );
                        if k_resp.changed() {
                            let _ = self.settings_watch_tx.send(self.settings.clone());
                        }
                        if k_resp.drag_stopped() {
                            save_config(&self.settings);
                        }
                    });
                });
                // ── Last Logged QSO ───────────────────────────────────────
                ui.add_space(4.0);
                ui.separator();
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("Last QSO").strong()
                        .color(egui::Color32::from_rgb(180, 200, 255)));
                    if let Some(rec) = self.adif_log.last() {
                        ui.add_space(6.0);
                        ui.label(egui::RichText::new(&rec.callsign)
                            .monospace().color(egui::Color32::from_rgb(100, 220, 100)).size(13.0));
                        ui.add_space(4.0);
                        let fmt = |t: &str| if t.len()>=4 { format!("{}:{}", &t[0..2], &t[2..4]) } else { t.to_string() };
                        ui.label(egui::RichText::new(format!("{}-{}",
                            fmt(&rec.time_on), fmt(&rec.time_off)))
                            .color(egui::Color32::from_gray(170)));
                        ui.add_space(4.0);
                        ui.label(egui::RichText::new(format!("S:{} R:{}",
                            rec.rst_sent, rec.rst_rcvd))
                            .monospace().color(egui::Color32::from_rgb(255, 200, 80)));
                        if !rec.gridsquare.is_empty() {
                            ui.add_space(4.0);
                            ui.label(egui::RichText::new(&rec.gridsquare)
                                .monospace().color(egui::Color32::from_gray(170)));
                        }
                    } else {
                        ui.label(egui::RichText::new("None logged yet")
                            .color(egui::Color32::from_gray(170)));
                    }
                });

                ui.add_space(4.0);

                // ── Accumulated decodes panel ─────────────────────────────
                // Shows ★ rows only when no direct decode exists for the same
                // DF bin in the last 120 seconds. If direct decodes are coming
                // through cleanly, the accumulator adds nothing useful.
                let their_hint = self.settings.their_call_hint.clone();
                let now_ts = chrono::Utc::now();
                let cutoff_120s = now_ts - chrono::Duration::seconds(120);

                let mut acc_decodes: Vec<_> = self.decodes.iter()
                    .filter(|e| e.is_accumulated && e.confidence >= 0.45)
                    .filter(|acc| {
                        // Suppress if any direct decode shares the same DF bin
                        // and was received in the last 120 seconds
                        let acc_bin = acc.df_bin_hz;
                        !self.decodes.iter().any(|d| {
                            !d.is_accumulated
                                && d.timestamp > cutoff_120s
                                && d.df_bin_hz == acc_bin
                        })
                    })
                    .cloned()
                    .collect();
                // Sort best confidence first
                acc_decodes.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap());
                // In QSO mode, cap at 3 entries
                if their_hint.is_some() { acc_decodes.truncate(3); }

                if !acc_decodes.is_empty() {
                    ui.label(egui::RichText::new("★ Accumulated")
                        .strong()
                        .color(egui::Color32::from_rgb(220, 180, 50)));
                    ui.separator();
                    for e in &acc_decodes {
                        ui.horizontal(|ui| {
                            // Confidence bar
                            let bar_w = (e.confidence * 60.0).clamp(2.0, 60.0);
                            let bar_col = if e.confidence >= 0.75 {
                                egui::Color32::from_rgb(80, 220, 80)
                            } else if e.confidence >= 0.50 {
                                egui::Color32::from_rgb(220, 180, 50)
                            } else {
                                egui::Color32::from_rgb(160, 160, 80)
                            };
                            let (bar_rect, _) = ui.allocate_exact_size(
                                egui::vec2(bar_w, 8.0), egui::Sense::hover()
                            );
                            ui.painter().rect_filled(bar_rect, 2.0, bar_col);
                            ui.add_space(4.0);
                            // Decoded text — strip ★ prefix
                            let text = e.raw.trim_start_matches('★').trim();
                            // Highlight text green if it contains their callsign
                            let text_col = if their_hint.as_deref()
                                .map(|h| text.to_uppercase().contains(h))
                                .unwrap_or(false)
                            {
                                egui::Color32::from_rgb(80, 255, 120)
                            } else {
                                egui::Color32::from_rgb(255, 210, 80)
                            };
                            ui.label(egui::RichText::new(text)
                                .monospace().color(text_col));
                        });
                        // conf + time only — DF is meaningless for a multi-fragment accumulation
                        ui.label(egui::RichText::new(
                            format!("conf {:.2}  {}",
                                e.confidence,
                                e.timestamp.format("%H:%M:%S")))
                            .color(egui::Color32::from_gray(170)));
                        ui.add_space(2.0);
                    }
                } else {
                    ui.label(egui::RichText::new("No accumulated decodes yet")
                        .color(egui::Color32::from_gray(170)));
                }
            }); // end right vertical
            }); // end outer horizontal

            ui.add_space(4.0);

            if !self.qso_log.is_empty() {
                ui.separator();
                let start = self.qso_log.len().saturating_sub(4);
                for e in &self.qso_log[start..] {
                    ui.label(egui::RichText::new(e)
                        .color(egui::Color32::from_rgb(160, 160, 160)));
                }
            }
        });

        // ── Decode list ───────────────────────────────────────────────────

        egui::CentralPanel::default().show(ctx, |ui| {
            // ── Spectrogram: X=time (0..30s), Y=frequency (0..3kHz) ──────────
            {
                let wf_height = 150.0f32;
                let avail_w   = ui.available_width();
                let (rect, _) = ui.allocate_exact_size(
                    egui::vec2(avail_w, wf_height), egui::Sense::hover()
                );
                let painter = ui.painter_at(rect);
                painter.rect_filled(rect, 0.0, egui::Color32::from_rgb(0, 0, 20));

                let n_cols = self.wf_columns.len();
                if n_cols > 0 {
                    // Columns per period: period_secs * 11025 / 1024
                    let period_secs = match self.settings.period {
                        Period::TxFirst15 | Period::TxSecond15 => 15u32,
                        _ => 30u32,
                    };
                    let max_cols = (period_secs * 11025 / 1024).max(1) as usize;
                    // col_w fills exactly avail_w over the full period
                    let col_w = avail_w / max_cols as f32;
                    let bin_h = wf_height / DISPLAY_BINS as f32;

                    for (ci, col) in self.wf_columns.iter().enumerate() {
                        let x = rect.left() + ci as f32 * col_w;
                        for (bi, &v) in col.iter().enumerate() {
                            // bi=0 is DC (bottom), bi=DISPLAY_BINS-1 is 3kHz (top)
                            // Invert: low freq at bottom, high freq at top
                            let y = rect.bottom() - (bi as f32 + 1.0) * bin_h;
                            if v > 0.05 {
                                painter.rect_filled(
                                    egui::Rect::from_min_size(
                                        egui::pos2(x, y),
                                        egui::vec2(col_w.max(1.5), bin_h.max(1.0)),
                                    ),
                                    0.0,
                                    heat_color(v),
                                );
                            }
                        }
                    }

                    // FSK441 tone markers — subtle dashed lines, low alpha
                    // Drawn as short segments with gaps to avoid visual clutter
                    let dash_len = 8.0f32;
                    let gap_len  = 12.0f32;
                    let step     = dash_len + gap_len;
                    let tone_col = egui::Color32::from_rgba_unmultiplied(180, 160, 60, 55);
                    for &hz in &FSK441_TONES_HZ {
                        let bin = hz_to_bin(hz);
                        let y = rect.bottom() - (bin as f32 + 0.5) * bin_h;
                        let mut x = rect.left();
                        while x < rect.right() {
                            let x_end = (x + dash_len).min(rect.right());
                            painter.line_segment(
                                [egui::pos2(x, y), egui::pos2(x_end, y)],
                                egui::Stroke::new(0.8, tone_col),
                            );
                            x += step;
                        }
                    }

                    // Frequency axis — just 0 (DC, bottom) and 3k (top)
                    for (hz, label) in [(0u32, "0"), (3000u32, "3k")] {
                        let bin = hz_to_bin(hz as f32);
                        if bin >= DISPLAY_BINS { continue; }
                        let y_raw = rect.bottom() - bin as f32 * bin_h;
                        // Raise the 0 label by half its font height so it clears the border
                        let y = if hz == 0 { y_raw - 6.0 } else { y_raw };
                        painter.text(
                            egui::pos2(rect.right() - 4.0, y),
                            egui::Align2::RIGHT_CENTER,
                            label,
                            egui::FontId::monospace(10.0),
                            egui::Color32::from_rgba_unmultiplied(200, 200, 200, 160),
                        );
                    }

                    // Amplitude envelope trace — green line, dB scale
                    // 40 dB dynamic range: noise floor at bottom, signals above
                    if self.wf_amplitude.len() > 1 {
                        let trace_h  = wf_height * 0.28;
                        let baseline = rect.bottom();
                        let db_range = 40.0f32; // dB shown (noise floor to top)

                        // Estimate noise floor as 20th percentile of period so far
                        let mut sorted = self.wf_amplitude.clone();
                        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
                        let noise = sorted[sorted.len() / 5].max(1e-7);

                        let mut pts: Vec<egui::Pos2> = Vec::with_capacity(n_cols);
                        for (ci, &amp) in self.wf_amplitude.iter().enumerate() {
                            let x  = rect.left() + ci as f32 * col_w + col_w * 0.5;
                            let db = 20.0 * (amp / noise).log10(); // dB above noise floor
                            let norm = (db / db_range).clamp(0.0, 1.0);
                            let y  = baseline - norm * trace_h;
                            pts.push(egui::pos2(x, y));
                        }

                        for i in 1..pts.len() {
                            painter.line_segment(
                                [pts[i-1], pts[i]],
                                egui::Stroke::new(1.5,
                                    egui::Color32::from_rgba_unmultiplied(0, 220, 80, 210)),
                            );
                        }

                        // dB axis labels removed — not useful visually
                    }

                    // Time cursor — white vertical line at current position
                    let x_now = rect.left() + n_cols as f32 * col_w;
                    painter.line_segment(
                        [egui::pos2(x_now, rect.top()), egui::pos2(x_now, rect.bottom())],
                        egui::Stroke::new(1.0, egui::Color32::from_rgba_unmultiplied(255,255,255,60)),
                    );
                }

                painter.rect_stroke(rect, 0.0,
                    egui::Stroke::new(1.0, egui::Color32::from_gray(170)));
            }

            // ── DF Scatter Strip: X=time (synced to waterfall), Y=DF ±100Hz ──
            {
                let strip_h  = 40.0f32;
                let df_range = 100.0f32; // ±100 Hz maps to top/bottom
                let avail_w  = ui.available_width();
                let (rect, _) = ui.allocate_exact_size(
                    egui::vec2(avail_w, strip_h), egui::Sense::hover()
                );
                let painter = ui.painter_at(rect);
                // Background
                painter.rect_filled(rect, 0.0, egui::Color32::from_rgb(0, 0, 15));

                let period_secs = match self.settings.period {
                    Period::TxFirst15 | Period::TxSecond15 => 15u32,
                    _ => 30u32,
                };
                let max_cols = (period_secs * 11025 / 1024).max(1) as usize;
                let col_w    = avail_w / max_cols as f32;
                let _n_cols  = self.wf_columns.len();

                // Zero line (df=0) — dim white
                let y_zero = rect.top() + strip_h * 0.5;
                painter.line_segment(
                    [egui::pos2(rect.left(), y_zero), egui::pos2(rect.right(), y_zero)],
                    egui::Stroke::new(1.0, egui::Color32::from_rgba_unmultiplied(255,255,255,25)),
                );
                // ±200 Hz guide lines — very dim
                for df_guide in [-200.0f32, 200.0] {
                    let y = rect.top() + strip_h * (0.5 - df_guide / (df_range * 2.0));
                    painter.line_segment(
                        [egui::pos2(rect.left(), y), egui::pos2(rect.right(), y)],
                        egui::Stroke::new(1.0, egui::Color32::from_rgba_unmultiplied(100,100,100,30)),
                    );
                }

                // Draw dots — size proportional to log(CCF), bright = high conf
                for dot in &self.df_dots {
                    let x = rect.left() + dot.col_idx as f32 * col_w;
                    if x < rect.left() || x > rect.right() { continue; }
                    let df_clamped = dot.df_hz.clamp(-df_range, df_range);
                    let y = rect.top() + strip_h * (0.5 - df_clamped / (df_range * 2.0));
                    // Radius: 2px base + log scale up to 6px at CCF=5000
                    let r = (2.0 + (dot.ccf.max(1.0).ln() / 5000_f32.ln()) * 4.0).min(6.0);
                    let alpha = if dot.ccf > 500.0 { 230u8 } else { 180u8 };
                    painter.circle_filled(
                        egui::pos2(x, y), r,
                        egui::Color32::from_rgba_unmultiplied(dot.r, dot.g, dot.b, alpha),
                    );
                    // Vertical tick for strong signals (CCF>300) so clusters are visible
                    if dot.ccf > 300.0 {
                        painter.line_segment(
                            [egui::pos2(x, y - r - 2.0), egui::pos2(x, y + r + 2.0)],
                            egui::Stroke::new(1.0, egui::Color32::from_rgba_unmultiplied(dot.r, dot.g, dot.b, 80)),
                        );
                    }
                }

                // Y-axis labels
                let label_col = egui::Color32::from_gray(170);
                painter.text(egui::pos2(rect.right() - 38.0, rect.top() + 2.0),
                    egui::Align2::LEFT_TOP, "+100Hz", egui::FontId::proportional(9.0), label_col);
                painter.text(egui::pos2(rect.right() - 38.0, rect.bottom() - 11.0),
                    egui::Align2::LEFT_TOP, "-100Hz", egui::FontId::proportional(9.0), label_col);
                painter.text(egui::pos2(rect.right() - 22.0, y_zero - 5.0),
                    egui::Align2::LEFT_TOP, "0", egui::FontId::proportional(9.0), label_col);

                painter.rect_stroke(rect, 0.0,
                    egui::Stroke::new(1.0, egui::Color32::from_gray(170)));
            }

            ui.add_space(2.0);
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Decoded Pings").strong()
                    .color(egui::Color32::from_rgb(180, 200, 255)));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {

                    // Max Data toggle — auto-saves ring buffer at every TX start
                    let max_col = if self.settings.save_max_data {
                        egui::Color32::from_rgb(80, 200, 220)
                    } else {
                        egui::Color32::from_gray(160)
                    };
                    if ui.add(egui::Button::new(
                        egui::RichText::new("📊 Max Data").color(max_col)))
                        .on_hover_text(if self.settings.save_max_data {
                            "Max Data ON — soft data saved at every TX and retained in DB. Click to disable."
                        } else {
                            "Max Data OFF — soft data scored then discarded. Click to retain for analysis."
                        })
                        .clicked()
                    {
                        self.settings.save_max_data = !self.settings.save_max_data;
                        save_config(&self.settings);
                    }
                    ui.separator();
                    if ui.button("Clear").clicked() {
                        self.clear_decodes();
                    }
                });
            });
            ui.separator();
            ui.horizontal(|ui| {
                ui.monospace(format!("{:<10} {:>7} {:>7}  Decode",
                    "Time", "DF(Hz)", "Dur/S"));
            });
            ui.separator();

            egui::ScrollArea::vertical()
                .auto_shrink([false; 2])
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    for i in 0..self.decodes.len() {
                        let e   = &self.decodes[i];
                        if e.is_accumulated { continue; } // shown in QSO panel
                        let sel = self.selected == Some(i);
                        let _e_passed = e.passed_threshold;
                        let _e_accum  = e.is_accumulated;

                        // Dur/S: duration ms / MSHV-style strength 1-9
                        // MSHV maps CCF to 1-9 on a doubling scale from threshold
                        let dur_str = if e.duration_ms > 0.0 {
                            let strength = match e.ccf_ratio as u32 {
                                0..=49   => 1u32,
                                50..=99  => 2,
                                100..=199 => 3,
                                200..=399 => 4,
                                400..=799 => 5,
                                800..=1599 => 6,
                                1600..=3199 => 7,
                                3200..=6399 => 8,
                                _ => 9,
                            };
                            format!("{:.0}/{}", e.duration_ms, strength)
                        } else { String::new() };
                        let header = format!("{:<10} {:>+7.0} {:>7}  ",
                            e.timestamp.format("%H:%M:%S"), e.df_hz, dur_str);

                        // Render row with click detection via allocate_rect
                        let row_start = ui.cursor().min;
                        ui.horizontal(|ui| {
                            ui.spacing_mut().item_spacing.x = 0.0;

                            // Selection highlight
                            if sel {
                                let r = ui.max_rect();
                                ui.painter().rect_filled(r, 0.0,
                                    egui::Color32::from_rgba_unmultiplied(255,255,255,18));
                            }

                            // Header in dim grey monospace (italic cyan for second pass)
                            let header_rt = if e.is_second_pass {
                                egui::RichText::new(&header)
                                    .monospace()
                                    .italics()
                                    .color(egui::Color32::from_rgb(80, 200, 220))
                            } else {
                                egui::RichText::new(&header)
                                    .monospace()
                                    .color(egui::Color32::from_gray(170))
                            };
                            ui.label(header_rt);

                            // Render only the meaningful portion of the decode:
                            // trim leading/trailing characters below conf_thresh_show.
                            // This strips the surrounding noise and shows only where
                            // the decoder found real signal — much cleaner on weak pings.
                            let chars: Vec<char> = e.raw.trim().chars().collect();
                            let confs = &e.char_confs;
                            let conf_thresh_bold = 0.5f32;
                            let conf_thresh_show = 0.15f32;

                            // Find the meaningful span using a higher trim threshold (0.35)
                            // Characters below this are indistinguishable from noise
                            // Second pass: render hypothesis with confirmed chars bright, unconfirmed dim
                            if e.is_second_pass {
                                for (ch, confirmed) in &e.second_pass_chars {
                                    let col = if *confirmed {
                                        egui::Color32::from_rgb(80, 220, 240) // bright cyan = confirmed
                                    } else {
                                        egui::Color32::from_rgb(40, 120, 140) // dim cyan = unconfirmed
                                    };
                                    let rt = egui::RichText::new(ch.to_string())
                                        .monospace().italics().color(col);
                                    let rt = if *confirmed { rt.strong() } else { rt };
                                    ui.label(rt);
                                }
                            } else {

                            let conf_trim = 0.35f32;
                            let first = chars.iter().enumerate()
                                .find(|(ci, _)| confs.get(*ci).copied().unwrap_or(0.0) >= conf_trim)
                                .map(|(ci, _)| ci)
                                .unwrap_or(0);
                            let last = chars.iter().enumerate().rev()
                                .find(|(ci, _)| confs.get(*ci).copied().unwrap_or(0.0) >= conf_trim)
                                .map(|(ci, _)| ci)
                                .unwrap_or(chars.len().saturating_sub(1));
                            // If trimmed significantly, show ellipsis indicator
                            let trimmed_front = first > 2;
                            let trimmed_back  = last + 3 < chars.len();
                            if trimmed_front {
                                ui.label(egui::RichText::new("…").monospace()
                                    .color(egui::Color32::from_gray(170)));
                            }

                            for (ci, ch) in chars.iter().enumerate() {
                                // Skip chars outside the meaningful span
                                if ci < first || ci > last { continue; }
                                let conf = confs.get(ci).copied().unwrap_or(0.0);
                                let (r, g, b, bold) = if e.is_accumulated {
                                    if conf >= conf_thresh_bold { (255u8, 200u8,  50u8, true) }
                                    else if conf >= conf_thresh_show { (180u8, 140u8, 40u8, false) }
                                    else { (100u8, 80u8, 30u8, false) }
                                } else if conf >= conf_thresh_bold {
                                    if e.score >= 80 { (80u8,  255u8,  80u8, true) }
                                    else             { (200u8, 200u8,  60u8, true) }
                                } else if conf >= conf_thresh_show {
                                    (180u8, 180u8, 180u8, false)
                                } else {
                                    (110u8, 110u8, 110u8, false)
                                };
                                let rt = egui::RichText::new(ch.to_string())
                                    .monospace()
                                    .color(egui::Color32::from_rgb(r, g, b));
                                let rt = if bold { rt.strong() } else { rt };
                                ui.label(rt);
                            }

                            if trimmed_back {
                                ui.label(egui::RichText::new("…").monospace()
                                    .color(egui::Color32::from_gray(170)));
                            }

                            } // end non-second-pass
                        });

                        let in_qso = self.settings.their_call_hint.is_some();
                        // Invisible clickable overlay across the full row
                        let row_rect = egui::Rect::from_min_max(
                            row_start,
                            egui::pos2(ui.max_rect().right(), ui.cursor().min.y),
                        );
                        let resp = ui.allocate_rect(row_rect, egui::Sense::click_and_drag());

                        // Right-click copies the full decode text
                        resp.context_menu(|ui| {
                            if ui.button("📋 Copy decode").clicked() {
                                ui.output_mut(|o| o.copied_text = e.raw.trim().to_string());
                                ui.close_menu();
                            }
                            if ui.button("📋 Copy with header").clicked() {
                                let strength = match e.ccf_ratio as u32 {
                                    0..=49    => 1u32, 50..=99   => 2, 100..=199  => 3,
                                    200..=399 => 4,   400..=799  => 5, 800..=1599  => 6,
                                    1600..=3199 => 7, 3200..=6399 => 8, _ => 9,
                                };
                                let dur_str = if e.duration_ms > 0.0 {
                                    format!("{:.0}/{}", e.duration_ms, strength)
                                } else { String::new() };
                                ui.output_mut(|o| o.copied_text = format!(
                                    "{} DF={:+.0} {} {}",
                                    e.timestamp.format("%H:%M:%S"), e.df_hz,
                                    dur_str, e.raw.trim()
                                ));
                                ui.close_menu();
                            }
                        });

                        // Handle row click — second click deselects, click elsewhere deselects.
                        // Deselection works regardless of QSO state.
                        // Auto-population of their call only happens when not locked.
                        if resp.clicked() || resp.secondary_clicked() {
                            if self.selected == Some(i) {
                                self.selected = None;
                                continue;
                            }
                            self.selected = Some(i);
                            // Only auto-populate call/loc/report when not locked
                            if !in_qso {
                                let my = self.settings.my_call.to_uppercase();
                                let foreign: Vec<&String> = e.callsigns.iter()
                                    .filter(|c| !c.eq_ignore_ascii_case(&my))
                                    .collect();
                                if foreign.len() == 1 {
                                    self.their_call_edit = foreign[0].clone();
                                    self.tx_msgs_key.clear();
                                } else if foreign.is_empty() {
                                    if let Some(c) = e.callsigns.first() {
                                        self.their_call_edit = c.clone();
                                        self.tx_msgs_key.clear();
                                    }
                                }
                                if let Some(loc) = &e.locator { self.their_loc_edit = loc.clone(); }
                                if let Some(rpt) = &e.report  {
                                    let clean = rpt.trim_start_matches('R').trim_start_matches('r').to_string();
                                    self.report_rcvd = clean;
                                }
                            }
                        }

                        // If multiple callsigns on this row, show small clickable call buttons
                        // These appear when the row is selected, overlaid in a popup-style strip
                        if sel && !in_qso {
                            let my = self.settings.my_call.to_uppercase();
                            let foreign: Vec<String> = e.callsigns.iter()
                                .filter(|c| !c.eq_ignore_ascii_case(&my))
                                .cloned()
                                .collect();
                            if foreign.len() > 1 {
                                ui.horizontal(|ui| {
                                    ui.label(egui::RichText::new("  → ")
                                        .color(egui::Color32::YELLOW));
                                    for call in &foreign {
                                        if ui.add(egui::Button::new(
                                            egui::RichText::new(call).monospace()
                                                .color(egui::Color32::from_rgb(100, 200, 255))
                                        ).frame(true)).clicked() {
                                            self.their_call_edit = call.clone();
                                            self.tx_msgs_key.clear();
                                            if let Some(loc) = &e.locator {
                                                self.their_loc_edit = loc.clone();
                                            }
                                        }
                                    }
                                });
                            }
                        }

                        if resp.double_clicked() && e.is_cq && !self.their_call_edit.is_empty() {
                            let mc = self.settings.my_call.clone();
                            let ml = self.settings.my_loc.clone();
                            let tc = self.their_call_edit.trim().to_uppercase();
                            let tl = self.their_loc_edit.trim().to_uppercase();
                            let rs = self.report_sent.clone();
                            self.qso = QsoState::answer_cq(mc, ml, tc, tl, rs);
                            let msg = self.qso.tx_message().unwrap_or_default();
                            self.qso_log.push(format!("Answer CQ: {}", msg));
                            self.send_tx(&msg);
                        }

                        // Highlight selected row
                        if sel {
                            ui.painter().rect_filled(
                                ui.min_rect(),
                                0.0,
                                egui::Color32::from_rgba_unmultiplied(255, 255, 255, 15),
                            );
                        }
                    }
                });
        });

        // ── Settings window (matches MSK2K layout) ────────────────────────
        if self.settings_open {
            egui::Window::new("⚙ Settings")
                .collapsible(false).resizable(true).default_width(450.0)
                .show(ctx, |ui| {

                    // ── Station Setup ─────────────────────────────────────
                    ui.heading("Station Setup");
                    ui.horizontal(|ui| {
                        ui.label("My Callsign:");
                        if ui.text_edit_singleline(&mut self.settings.my_call).changed() {
                            self.settings.my_call = self.settings.my_call.to_uppercase();
                        }
                    });
                    ui.horizontal(|ui| {
                        ui.label("My Locator: ");
                        if ui.text_edit_singleline(&mut self.settings.my_loc).changed() {
                            self.settings.my_loc = self.settings.my_loc.to_uppercase();
                        }
                    });

                    // ── Rig Control ───────────────────────────────────────
                    ui.separator();
                    ui.heading("Rig Control (Hamlib)");
                    ui.horizontal(|ui| {
                        ui.checkbox(&mut self.settings.hamlib_enabled, "Enable CAT Control");
                    });

                    if self.settings.hamlib_enabled {
                        ui.indent("cat", |ui| {
                            egui::Grid::new("rig_grid")
                                .num_columns(2).spacing([10.0, 8.0])
                                .show(ui, |ui| {

                                ui.label("Rig Selection:");
                                ui.vertical(|ui| {
                                    ui.text_edit_singleline(&mut self.rig_search)
                                        .on_hover_text("Type model number to filter (e.g. 9700)");
                                    let cur = if self.settings.rig_model.is_empty() {
                                        "Select Rig...".to_string()
                                    } else {
                                        self.rig_list.iter()
                                            .find(|(id, _)| id == &self.settings.rig_model)
                                            .map(|(_, n)| n.clone())
                                            .unwrap_or_else(|| format!("ID: {}", self.settings.rig_model))
                                    };
                                    egui::ComboBox::from_id_salt("rig_sel")
                                        .selected_text(cur).width(250.0)
                                        .show_ui(ui, |ui| {
                                            let srch = self.rig_search.to_uppercase();
                                            for (id, name) in &self.rig_list.clone() {
                                                if srch.is_empty()
                                                    || name.to_uppercase().contains(&srch)
                                                    || id.contains(&srch)
                                                {
                                                    ui.selectable_value(
                                                        &mut self.settings.rig_model,
                                                        id.clone(), name);
                                                }
                                            }
                                        });
                                });
                                ui.end_row();

                                ui.label("Serial Port:");
                                ui.horizontal(|ui| {
                                    let port_disp = if self.settings.rig_port.is_empty() {
                                        "Select Port...".to_string()
                                    } else {
                                        self.settings.rig_port.clone()
                                    };
                                    egui::ComboBox::from_id_salt("ser_port")
                                        .selected_text(&port_disp).width(280.0)
                                        .show_ui(ui, |ui| {
                                            for p in &self.serial_ports.clone() {
                                                ui.selectable_value(
                                                    &mut self.settings.rig_port, p.clone(), p);
                                            }
                                        });
                                    if ui.button("🔄").clicked() {
                                        self.serial_ports = enumerate_serial_ports();
                                    }
                                });
                                ui.end_row();

                                ui.label("Baud Rate:");
                                ui.text_edit_singleline(&mut self.settings.rig_baud);
                                ui.end_row();
                            });
                        });
                    }

                    // ── PTT Control ───────────────────────────────────────
                    ui.separator();
                    ui.heading("PTT Control");
                    egui::Grid::new("ptt_grid").num_columns(2).spacing([10.0, 8.0]).show(ui, |ui| {
                        ui.label("PTT Method:");
                        ui.horizontal(|ui| {
                            let prev = self.settings.ptt_method.clone();
                            ui.radio_value(&mut self.settings.ptt_method, PttMethod::HamlibCat, "Hamlib CAT")
                                .on_hover_text("Key TX via CAT command through hamlib (default)");
                            ui.radio_value(&mut self.settings.ptt_method, PttMethod::Rts, "RTS")
                                .on_hover_text("Assert RTS pin on selected serial port to key TX");
                            ui.radio_value(&mut self.settings.ptt_method, PttMethod::Dtr, "DTR")
                                .on_hover_text("Assert DTR pin on selected serial port to key TX");
                            ui.radio_value(&mut self.settings.ptt_method, PttMethod::VoxOnly, "VOX / None")
                                .on_hover_text("No software PTT — use VOX or key manually");
                            if self.settings.ptt_method != prev {
                                self.open_ptt_port();
                                save_config(&self.settings);
                            }
                        });
                        ui.end_row();

                        if matches!(self.settings.ptt_method, PttMethod::Rts | PttMethod::Dtr) {
                            ui.label("PTT Port:");
                            ui.horizontal(|ui| {
                                let prev_port = self.settings.ptt_serial_port.clone();
                                let cur_text = if self.settings.ptt_serial_port.is_empty() {
                                    "Select port...".to_string()
                                } else {
                                    self.settings.ptt_serial_port.clone()
                                };
                                egui::ComboBox::from_id_salt("ptt_port_sel")
                                    .selected_text(cur_text)
                                    .width(220.0)
                                    .show_ui(ui, |ui| {
                                        for p in &self.serial_ports.clone() {
                                            ui.selectable_value(
                                                &mut self.settings.ptt_serial_port,
                                                p.clone(), p);
                                        }
                                    });
                                if ui.small_button("🔄").on_hover_text("Refresh port list").clicked() {
                                    self.serial_ports = enumerate_serial_ports();
                                }
                                if self.settings.ptt_serial_port != prev_port {
                                    self.open_ptt_port();
                                    save_config(&self.settings);
                                }
                                // Status indicator
                                let (col, label) = if self.ptt_port.is_some() {
                                    (egui::Color32::from_rgb(0, 200, 80), "● Open")
                                } else {
                                    (egui::Color32::from_rgb(180, 40, 40), "● Not open")
                                };
                                ui.label(egui::RichText::new(label).color(col).monospace());
                            });
                            ui.end_row();
                        }
                    });

                    // ── Audio Hardware ────────────────────────────────────
                    ui.separator();
                    ui.heading("Audio Hardware");
                    ui.horizontal(|ui| {
                        if ui.button("🔄 Refresh Devices")
                            .on_hover_text("Re-scan audio hardware. Use after swapping USB radios.")
                            .clicked()
                        {
                            self.refresh_audio_devices();
                        }
                        if self.settings.sel_in.is_none() || self.settings.sel_out.is_none() {
                            ui.label(egui::RichText::new("⚠ No device selected")
                                .color(egui::Color32::from_rgb(255, 180, 0)));
                        }
                    });

                    ui.add_space(4.0);
                    ui.label("Input Device:");
                    egui::ComboBox::from_id_salt("in_dev")
                        .selected_text(self.settings.sel_in.clone()
                            .unwrap_or_else(|| "Default".into()))
                        .width(300.0)
                        .show_ui(ui, |ui| {
                            ui.selectable_value(&mut self.settings.sel_in, None, "Default");
                            for d in &self.in_devs.clone() {
                                ui.selectable_value(
                                    &mut self.settings.sel_in, Some(d.clone()), d);
                            }
                        });

                    ui.add_space(8.0);
                    ui.label("Output Device:");
                    egui::ComboBox::from_id_salt("out_dev")
                        .selected_text(self.settings.sel_out.clone()
                            .unwrap_or_else(|| "Default".into()))
                        .width(300.0)
                        .show_ui(ui, |ui| {
                            ui.selectable_value(&mut self.settings.sel_out, None, "Default");
                            for d in &self.out_devs.clone() {
                                ui.selectable_value(
                                    &mut self.settings.sel_out, Some(d.clone()), d);
                            }
                        });

                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        ui.label("TX Level:");
                        ui.add(egui::Slider::new(&mut self.settings.tx_level, 0.0..=1.0)
                            .custom_formatter(|v, _| format!("{:.0}%", v * 100.0)));
                    });

                    // ── Filtering ─────────────────────────────────────────
                    ui.separator();
                    ui.heading("Filtering");
                    egui::Grid::new("filt").num_columns(2).spacing([10.0, 6.0]).show(ui, |ui| {
                        ui.label("cty.dat path:"); ui.text_edit_singleline(&mut self.settings.cty_path); ui.end_row();
                        ui.label("Max range (km):"); ui.add(egui::Slider::new(&mut self.settings.max_km, 500.0..=5000.0).integer()); ui.end_row();
                        ui.label("");
                        ui.label(egui::RichText::new("cty.dat defines valid callsign prefixes to decode")
                            .color(egui::Color32::from_gray(140))
                            .italics()
                            .size(11.0));
                        ui.end_row();
                    });

                    // ── Antenna ───────────────────────────────────────────
                    ui.separator();
                    ui.heading("Antenna");
                    egui::Grid::new("ant").num_columns(2).spacing([10.0, 6.0]).show(ui, |ui| {
                        ui.label("H-beamwidth:");
                        ui.add(egui::Slider::new(&mut self.settings.ant_bw_horiz, 5.0..=120.0)
                            .suffix("°").fixed_decimals(0));
                        ui.end_row();
                        ui.label("V-beamwidth:");
                        ui.add(egui::Slider::new(&mut self.settings.ant_bw_vert, 5.0..=120.0)
                            .suffix("°").fixed_decimals(0));
                        ui.end_row();
                    });

                    ui.separator();
                    if ui.button("Save & Close").clicked() {
                        self.settings_open = false;
                        save_config(&self.settings);
                        self.open_ptt_port();
                        // Apply changes to running TxEngine immediately
                        let _ = self.tx_cmd_tx.send(TxCommand::SetPeriod(self.settings.period));
                        self.period_timer = PeriodTimer::new(self.settings.period);
                        let _ = self.tx_cmd_tx.send(TxCommand::SetOutputDevice(self.settings.audio_out()));
                        let _ = self.tx_cmd_tx.send(TxCommand::SetHamlib(self.settings.hamlib_addr()));
                        let _ = self.tx_cmd_tx.send(TxCommand::SetVolume(self.settings.tx_level));
                        // Kill existing rigctld if hamlib disabled
                        if !self.settings.hamlib_enabled {
                            std::process::Command::new("pkill").args(["-f", "rigctld"]).spawn().ok();
                            log::info!("[LAUNCHER] Hamlib disabled — killed rigctld");
                        }
                    }
                });
        }
    }
}

// ─── Engine ───────────────────────────────────────────────────────────────────

async fn run_engine(settings: Settings, event_tx: mpsc::UnboundedSender<EngineEvent>, tx_active: std::sync::Arc<std::sync::atomic::AtomicBool>, mut settings_rx: tokio::sync::watch::Receiver<Settings>) {
    let mut settings = settings; // mutable local copy

    let qth = Qth::from_maidenhead(&settings.my_loc).unwrap_or_default();
    let db  = PrefixDb::load(&PathBuf::from(&settings.cty_path));
    let geo = GeoValidator::new(db, qth, settings.max_km);
    // Use ~/.fsk441/fsk441.db — don't exit on failure, just log and continue
    let db_path = {
        let mut p = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        p.push(".fsk441");
        std::fs::create_dir_all(&p).ok();
        p.push("fsk441.db");
        p
    };
    let store_opt = match Store::open(&db_path) {
        Ok(s) => { log::info!("[DB] Opened {:?}", db_path); Some(s) }
        Err(e) => { log::warn!("[DB] Cannot open {:?}: {} — continuing without DB", db_path, e); None }
    };
    let store = store_opt;
    let session_id = store.as_ref().and_then(|s|
        s.new_session(Some(settings.sel_in.as_deref().unwrap_or("default")), None).ok()
    ).unwrap_or(0);

    // Two channels from audio: one for detector, one for spectrum
    let (audio_tx, audio_rx) = mpsc::unbounded_channel::<Vec<f32>>();
    let (audio_spec_tx, mut audio_spec_rx) = mpsc::unbounded_channel::<Vec<f32>>();
    let (ping_tx, mut ping_rx) = mpsc::unbounded_channel::<detector::DetectedPing>();

    // Keep a clone of audio_tx for Linux stream restart after TX
    let _audio_tx_for_restart = audio_tx.clone();
    let _audio_input_device   = settings.audio_in();

    #[allow(unused_mut, unused_variables)]
    let mut audio_stop_handle = match audio::start_live(settings.audio_in(), audio_tx).await {
        Ok(h) => h,
        Err(e) => { log::error!("[ENGINE] Audio: {}", e); return; }
    };

    // Fan out raw audio to detector AND spectrum computer
    let spec_tx2 = event_tx.clone();
    #[allow(unused_variables)]
    let tx_active_spec = tx_active.clone();
    tokio::spawn(async move {
        let mut planner = rustfft::FftPlanner::<f32>::new();
        let mut _chunk_count = 0u64;
        let mut rms_acc = 0.0f32;
        let mut rms_count = 0u32;
        const LOG_EVERY: u32 = 54; // ~5 seconds at 93ms/chunk
        while let Some(chunk) = audio_spec_rx.recv().await {
            // Every audio chunk = one column — 1024/11025 = ~93ms per column
            let rms = {
                let sum: f32 = chunk.iter().map(|&s| s * s).sum();
                (sum / chunk.len() as f32).sqrt()
            };
            // Accumulate for periodic level logging
            rms_acc   += rms * rms;
            rms_count += 1;
            if rms_count >= LOG_EVERY {
                let avg_rms  = (rms_acc / rms_count as f32).sqrt();
                let dbfs     = if avg_rms > 1e-9 { 20.0 * avg_rms.log10() } else { -999.0 };
                let is_tx    = tx_active_spec.load(std::sync::atomic::Ordering::Relaxed);
                log::info!(
                    "[AUDIO_LEVEL] {} input RMS={:.4} ({:.1}dBFS)  [{}]",
                    chrono::Utc::now().format("%H:%M:%S"),
                    avg_rms, dbfs,
                    if is_tx { "TX — audio gated" } else { "RX" }
                );
                rms_acc   = 0.0;
                rms_count = 0;
            }
            let bins = compute_column(&chunk, &mut planner);
            let _ = spec_tx2.send(EngineEvent::Spectrum(bins, rms));
        }
    });

    // Fan audio to detector and spectrum — gate both on tx_active
    let (fanout_ping_tx, fanout_ping_rx) = mpsc::unbounded_channel::<Vec<f32>>();
    let tx_active_fanout = tx_active.clone();
    tokio::spawn(async move {
        let mut rx = audio_rx;
        let mut was_tx = false;
        // Post-TX blanking: discard audio for a fixed window after tx_active goes false.
        // Covers the ~93ms cpal drain tail + rig TX→RX switch time. Without this,
        // our own transmitted audio leaks back through the mic and gets decoded.
        // 250ms covers cpal drain (93ms) + typical rig switch time (100ms) + margin.
        const POST_TX_BLANK_MS: u64 = 250;
        let mut blank_until: Option<std::time::Instant> = None;
        while let Some(chunk) = rx.recv().await {
            let is_tx_now = tx_active_fanout.load(std::sync::atomic::Ordering::Relaxed);
            if was_tx && !is_tx_now {
                // TX→RX transition: drain queued chunks and start blanking window
                let mut drained = 0u32;
                while rx.try_recv().is_ok() { drained += 1; }
                blank_until = Some(std::time::Instant::now()
                    + std::time::Duration::from_millis(POST_TX_BLANK_MS));
                log::info!("[FANOUT] TX→RX: drained {} chunks, blanking {}ms", drained, POST_TX_BLANK_MS);
                was_tx = false;
                continue;
            }
            was_tx = is_tx_now;
            if is_tx_now {
                continue; // TX — discard
            }
            // RX — check blanking window
            if let Some(blank_end) = blank_until {
                if std::time::Instant::now() < blank_end {
                    continue; // Still in post-TX blank — discard
                }
                blank_until = None;
            }
            let _ = audio_spec_tx.send(chunk.clone());
            let _ = fanout_ping_tx.send(chunk);
        }
    });

    tokio::spawn(run_detector(fanout_ping_rx, ping_tx, settings.threshold, params::DEFAULT_DFTOL));

    let mut frag_acc = FragmentAccumulator::new();
    let mut current_slot_idx: i64 = -1;
    let mut accum_best_score: u8 = 0;  // highest validity seen from accumulator this slot
    // Analysis ring buffer: rolling 30s of soft-bit data, written on demand
    const RING_MAX: usize = 300; // ~300 pings max in 30s
    let mut analysis_ring: std::collections::VecDeque<AnalysisPing> = std::collections::VecDeque::with_capacity(RING_MAX);

    // ── Adaptive noise floor tracker ─────────────────────────────────────────
    // EMA of mean_confidence for pings below current threshold (noise only).
    // α=0.001 → time constant ~1000 pings ≈ 3 minutes at typical ping rates.
    // Only pings BELOW current adaptive_conf_threshold feed the EMA, so real
    // signal bursts and interference spikes don't contaminate the noise estimate.
    //
    // threshold = noise_mean + k_sigma × noise_sigma
    //
    // This is mathematically derived: mean_confidence = (best-second)/best for
    // 4-tone FSK, where noise energies are i.i.d. exponential. The EMA measures
    // the actual distribution on the operator's band and k_sigma is the sole
    // operator control — no hardcoded floor needed or justified.
    const EMA_ALPHA:    f32 = 0.001;
    let mut noise_mean:     f32 = 0.44;   // prior: E[normalised margin] for 4-tone noise
    let mut noise_var:      f32 = 0.0004; // prior: σ² ≈ 0.02²
    // Cold-start threshold from priors: 0.44 + 5×0.02 = 0.54
    // Once EMA warms up it takes over completely.
    let mut adaptive_conf_threshold: f32 =
        noise_mean + settings.k_sigma * noise_var.sqrt();
    let mut ema_ping_count: u32 = 0;
    const EMA_WARMUP_PINGS: u32 = 500;
    // Brief startup delay — allow audio stream to stabilise before accepting pings
    let engine_start = std::time::Instant::now();
    const STARTUP_MS: u128 = 3000;
    let mut was_transmitting = false;

    loop {
        // Use a timeout so we wake up periodically during TX even when
        // the fanout is blanking audio and no pings arrive.
        let ping = match tokio::time::timeout(
            std::time::Duration::from_millis(500),
            ping_rx.recv()
        ).await {
            Ok(Some(p)) => p,
            Ok(None)    => break, // channel closed
            Err(_)      => {
                // Timeout — detect RX→TX slot boundary during audio silence.
                let is_tx = tx_active.load(std::sync::atomic::Ordering::Relaxed);
                let in_qso = settings.their_call_hint.is_some();
                // Second-pass: save analysis ring at RX→TX boundary when in QSO
                if is_tx && !was_transmitting && in_qso {
                    if let Some(ref s) = store {
                        let entries: Vec<AnalysisPing> = analysis_ring.iter().cloned().collect();
                        let n = entries.len();
                        if n > 0 {
                            match s.save_analysis(&entries) {
                                Ok(cid) => {
                                    let _ = event_tx.send(EngineEvent::AnalysisSaved(cid, n));
                                    log::info!("[2PASS] RX period saved: {} pings capture_id={}", n, cid);
                                    analysis_ring.clear();
                                }
                                Err(e) => log::error!("[DB] 2PASS save failed: {}", e),
                            }
                        } else {
                            log::info!("[2PASS] RX→TX in QSO but ring empty");
                        }
                    }
                }
                if is_tx { was_transmitting = true; } else { was_transmitting = false; }
                // Linux half-duplex: release input stream at TX start so TxEngine
                // can open the same ALSA device for output. Restart after TX ends.
                #[cfg(target_os = "linux")]
                if is_tx && !was_transmitting {
                    log::info!("[AUDIO] Linux: stopping input stream for TX");
                    let _ = audio_stop_handle.send(());
                }
                #[cfg(target_os = "linux")]
                if !is_tx && was_transmitting {
                    // 500ms settling delay — ALSA needs time after TX output closes
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    match audio::start_live(_audio_input_device.clone(), _audio_tx_for_restart.clone()).await {
                        Ok(h) => {
                            audio_stop_handle = h;
                            log::info!("[AUDIO] Linux: input stream restarted after TX");
                        }
                        Err(e) => log::error!("[AUDIO] Linux: restart failed: {}", e),
                    }
                }
                continue;
            }
        };
        let is_tx = tx_active.load(std::sync::atomic::Ordering::Relaxed);

        // Pull latest settings if changed (their_call_hint, period, etc.)
        if settings_rx.has_changed().unwrap_or(false) {
            let old_hint = settings.their_call_hint.clone();
            settings = settings_rx.borrow_and_update().clone();

            // QSY detected — reset EMA immediately to new noise environment
            if settings.qsy_reset {
                noise_mean = 0.44;
                noise_var  = 0.0004;
                ema_ping_count = 0;
                adaptive_conf_threshold = noise_mean + settings.k_sigma * noise_var.sqrt();
                log::info!("[NOISE] EMA reset after QSY — restarting noise floor measurement");
            }

            // Clear accumulator pulse — fired from LOG QSO button
            if settings.clear_accumulator {
                frag_acc.clear();
                frag_acc.clear_constraint();
                accum_best_score = 0;
                log::info!("[ACCUM] Cleared between QSOs");
            }

            // Sync QSO context constraint with their_call_hint
            match (&old_hint, &settings.their_call_hint) {
                (None, Some(their)) => {
                    // QSO just started — apply constraint
                    frag_acc.set_constraint(&settings.my_call, their);
                }
                (Some(_), None) => {
                    // QSO ended — clear constraint
                    frag_acc.clear_constraint();
                }
                (Some(old), Some(new)) if old != new => {
                    // Their call changed mid-QSO (unlikely but handle it)
                    frag_acc.set_constraint(&settings.my_call, new);
                }
                _ => {}
            }
        }

        // Drain any pings that arrived during TX — they are our own audio.
        if is_tx {
            was_transmitting = true;
            frag_acc.prune_older_than(300);
            frag_acc.clear();
            accum_best_score = 0;
            while ping_rx.try_recv().is_ok() {}
            continue;
        }
        was_transmitting = false;

        // Detect slot boundary → process accumulated fragments and flush analysis ring
        let now_ms    = chrono::Utc::now().timestamp_millis();
        let slot_idx  = now_ms / 30_000;
        if slot_idx != current_slot_idx && current_slot_idx >= 0 {
            // ── Flush analysis ring to DB when Max Data is on ─────────────────
            // Saves soft-bit char_probs for every ping captured this slot,
            // regardless of QSO state — for post-session offline analysis.
            if settings.save_max_data {
                if let Some(ref s) = store {
                    let entries: Vec<AnalysisPing> = analysis_ring.iter().cloned().collect();
                    if !entries.is_empty() {
                        match s.save_analysis(&entries) {
                            Ok(cid) => {
                                log::info!("[MAX_DATA] Slot flush: {} analysis pings saved (capture_id={})", entries.len(), cid);
                                analysis_ring.clear();
                            }
                            Err(e) => log::error!("[DB] Max Data slot flush failed: {}", e),
                        }
                    }
                }
            }
            // ── Process accumulated fragments ──────────────────────────────────
            let n = frag_acc.fragment_count();
            if n > 0 {
                log::info!("[ACCUM] Slot boundary — processing {} cross-slot fragments", n);
                for acc in frag_acc.process() {
                    // Use mean_conf as quality proxy — freeze accumulator once
                    // we have a high-confidence decode (conf >= 0.70)
                    let score: u8 = if acc.mean_conf >= 0.70 { 60 } else { 0 };
                    if score > accum_best_score {
                        accum_best_score = score;
                    }
                    let _ = event_tx.send(EngineEvent::Accumulated(acc));
                }
                // Don't clear — keep accumulating cross-slot
                // Prune fragments older than 5 minutes
                frag_acc.prune_older_than(300);
                // Reset freeze for next slot
                accum_best_score = 0;
            }
        }
        current_slot_idx = slot_idx;

        // Offload CPU-bound demodulation to blocking thread pool
        let audio   = ping.audio.clone();
        let df_hz   = ping.df_hz;
        let result  = tokio::task::spawn_blocking(move || {
            longx(&audio, df_hz, params::DEFAULT_DFTOL)
        }).await.unwrap();
        let mut parsed = ParsedMessage::parse_geo(&result.raw_decode, Some(&geo));
        // Remove own callsign — it appears in every TX message so adds no information
        // and would incorrectly colour-code noise pings that happen to contain it
        let my_call_upper = settings.my_call.trim().to_uppercase();
        parsed.valid_callsigns.retain(|c| *c != my_call_upper);
        parsed.callsigns.retain(|c| *c != my_call_upper);

        if result.raw_decode.trim().len() < 2 { continue; }

        // Startup guard — ignore pings for first 3s while audio settles
        if engine_start.elapsed().as_millis() < STARTUP_MS { continue; }

        // TX loopback guard — own audio leakage has astronomically high CCF.
        // Legitimate overdense "rock crusher" pings can exceed 50,000 so we use
        // 500,000 as the cap. Real TX leakage is typically 10x-100x higher than
        // any meteor scatter ping. The tx_active blanking window is the primary
        // defence; this is just a backstop for extreme loopback cases.
        if ping.ccf_ratio > 500_000.0 {
            log::debug!("[ENGINE] Rejected extreme-CCF ping (likely TX leakage) ccf={:.0}", ping.ccf_ratio);
            continue;
        }

        // ── Adaptive noise floor update ──────────────────────────────────────
        // Only pings below current threshold feed the EMA — self-protecting
        // against signal contamination and interference spikes.
        let conf = result.mean_confidence;
        if conf < adaptive_conf_threshold {
            let delta = conf - noise_mean;
            noise_mean += EMA_ALPHA * delta;
            noise_var   = (1.0 - EMA_ALPHA) * (noise_var + EMA_ALPHA * delta * delta);
            ema_ping_count += 1;

            // Once warmed up, let the EMA drive the threshold
            if ema_ping_count >= EMA_WARMUP_PINGS {
                let noise_sigma = noise_var.sqrt();
                adaptive_conf_threshold = (noise_mean + settings.k_sigma * noise_sigma)
                    .min(0.90);
            }
        }

        // Settled = warmed up AND σ/μ < 8% — EMA has converged on this band's noise floor.
        // Naturally goes false again if band conditions shift significantly.
        let nf_settled = ema_ping_count >= EMA_WARMUP_PINGS
            && noise_mean > 1e-6
            && (noise_var.sqrt() / noise_mean) < 0.08;

        // Send conf + settled state to UI on every ping for live NF label colour
        let _ = event_tx.send(EngineEvent::ConfUpdate(conf, nf_settled));

        // Log and report adaptive threshold every ~5 minutes
        if ema_ping_count > 0 && ema_ping_count % 1500 == 0 {
            let noise_sigma = noise_var.sqrt();
            log::info!("[NOISE] μ={:.4} σ={:.4} threshold={:.4} k={:.1}",
                noise_mean, noise_sigma, adaptive_conf_threshold, settings.k_sigma);
            let _ = event_tx.send(EngineEvent::NoiseStats {
                mean: noise_mean, sigma: noise_sigma,
                threshold: adaptive_conf_threshold,
            });
        }

        // ── DB insert ────────────────────────────────────────────────────────
        // Max Data ON: store everything the detector fires on (full research corpus)
        // Max Data OFF: store only pings above the adaptive gate (lean operational DB)
        // EMA always runs regardless of save_max_data — it's in-memory only
        let should_store = settings.save_max_data || conf >= adaptive_conf_threshold;
        if should_store {
            if let Some(ref s) = store { let _ = s.insert_ping(session_id, &ping, &result, &parsed); }
        }

        // ── Single adaptive conf gate ────────────────────────────────────────
        // Anything above adaptive_conf_threshold is above the noise floor and
        // potentially real FSK441 signal. CCF is no longer used as a gate —
        // conf alone determines whether energy is FSK441-modulated.
        let in_qso = settings.their_call_hint.is_some();
        let arm1_conf = adaptive_conf_threshold;

        let threshold_pass = if in_qso {
            conf >= arm1_conf || conf >= 0.70
        } else {
            conf >= arm1_conf
        };

        // ── Accumulator feed — intentionally BEFORE display gate ────────────
        // Feed any ping with conf above noise_mean + 2σ regardless of CCF.
        // Stop feeding once a good decode has been achieved (validity >= 60)
        // to prevent noise fragments from corrupting a settled accumulation.
        let accum_gate_conf = noise_mean + 2.0 * noise_var.sqrt();
        let accum_gate = conf >= accum_gate_conf
            && result.raw_decode.trim().len() >= 2
            && accum_best_score < 60;  // freeze once good decode seen
        if accum_gate {
            if let Some(frag) = Fragment::from_result(&result, ping.ccf_ratio) {
                frag_acc.add(frag);
            }
        }

        // Learn their DF from threshold-passing pings during QSO
        if in_qso && threshold_pass && settings.their_df_hz.is_none() {
            // First good ping in QSO — record their DF
            // Send back via a side-channel: update settings and broadcast
            // (We can't mutate settings directly here, but we can note it for
            //  the next settings update. Use a workaround: store in a local.)
        }

        // QSO content override — callsign matching is specific enough without DF gate
        let content_override = in_qso
            && conf >= noise_mean + 3.0 * noise_var.sqrt()
            && {
                if let Some(ref their) = settings.their_call_hint {
                    qso_content_match(&result.raw_decode, their)
                } else { false }
            };

        let show = threshold_pass || content_override;

        // ── Display gate ──────────────────────────────────────────────────────
        // RAW mode = MSHV-equivalent: show what WSJT would show
        // Normal mode = our pipeline
        // In both cases threshold_pass drives colour coding
        // Add to analysis ring buffer — captures pings near threshold for second-pass.
        // Capture gate is deliberately LOWER than the display gate: we want to catch
        // pings that just miss the display cut but still carry char_probs energy.
        // Use K-1.5σ (min 0.5σ) so the ring gate tracks the user's K slider
        // automatically — no separate control needed.
        let noise_sigma_now = noise_var.sqrt();
        let capture_k = (settings.k_sigma - 1.5_f32).max(0.5);
        let capture_gate_conf = noise_mean + capture_k * noise_sigma_now;
        if conf >= capture_gate_conf && !result.char_probs.is_empty() {
            let ap = AnalysisPing {
                detected_at:     ping.timestamp.to_rfc3339(),
                df_hz:           ping.df_hz,
                ccf_ratio:       ping.ccf_ratio,
                mean_confidence: result.mean_confidence,
                validity_score:  parsed.validity_score,
                raw_decode:      result.raw_decode.clone(),
                char_probs:      result.char_probs.clone(),
            };
            analysis_ring.push_back(ap);
            while analysis_ring.len() > RING_MAX {
                analysis_ring.pop_front();
            }
        }

        if !show { continue; }

        // Per-character confidence: max of each char's 48-element prob array
        let char_confs: Vec<f32> = result.char_probs.iter()
            .map(|probs| probs.iter().cloned().fold(0.0f32, f32::max))
            .collect();

        let _ = event_tx.send(EngineEvent::Decode(DecodeEntry {
            timestamp:  ping.timestamp, df_hz: ping.df_hz, ccf_ratio: ping.ccf_ratio,
            duration_ms: ping.duration_ms,
            confidence: result.mean_confidence, score: parsed.validity_score,
            raw: result.raw_decode, callsigns: parsed.valid_callsigns,
            locator: parsed.locator, report: parsed.report, is_cq: parsed.is_cq,
            char_confs, is_accumulated: false,
            my_call: settings.my_call.clone(),
            df_bin_hz: None,
            passed_threshold: threshold_pass,
            is_second_pass: false,
            second_pass_chars: vec![],
        }));
    }
}


// ─── Config persistence (same pattern as MSK2K) ───────────────────────────────

fn config_path() -> PathBuf {
    let mut p = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    p.push(".fsk441");
    std::fs::create_dir_all(&p).ok();
    p.push("fsk441.cfg");
    p
}

fn save_config(s: &Settings) {
    let period = match s.period {
        Period::TxFirst    => "first",
        Period::TxSecond   => "second",
        Period::TxFirst15  => "first15",
        Period::TxSecond15 => "second15",
    };
    let ptt_method_str = match s.ptt_method {
        PttMethod::HamlibCat => "hamlib_cat",
        PttMethod::Rts       => "rts",
        PttMethod::Dtr       => "dtr",
        PttMethod::VoxOnly   => "vox",
    };
    let data = format!(
        "my_call={}\nmy_loc={}\ninput={}\noutput={}\nrig_model={}\nrig_port={}\nrig_baud={}\nhamlib_enabled={}\nperiod={}\ncty_path={}\nmax_km={}\nmin_ccf={}\ntx_level={}\nant_bw_horiz={}\nant_bw_vert={}\nk_sigma={}\nsave_max_data={}\nptt_method={}\nptt_serial_port={}\n",
        s.my_call,
        s.my_loc,
        s.sel_in.as_deref().unwrap_or(""),
        s.sel_out.as_deref().unwrap_or(""),
        s.rig_model,
        s.rig_port,
        s.rig_baud,
        s.hamlib_enabled,
        period,
        s.cty_path,
        s.max_km,
        s.min_ccf,
        s.tx_level,
        s.ant_bw_horiz,
        s.ant_bw_vert,
        s.k_sigma,
        s.save_max_data,
        ptt_method_str,
        s.ptt_serial_port,
    );
    if let Err(e) = std::fs::write(config_path(), data) {
        log::warn!("Failed to save config: {}", e);
    } else {
        log::info!("Config saved to {:?}", config_path());
    }
}

fn load_config() -> Settings {
    let mut s = Settings::default();
    let path  = config_path();
    let Ok(text) = std::fs::read_to_string(&path) else { return s; };
    for line in text.lines() {
        let Some((k, v)) = line.split_once('=') else { continue; };
        let v = v.trim();
        match k.trim() {
            "my_call"        => s.my_call   = v.to_string(),
            "my_loc"         => s.my_loc    = v.to_string(),
            "input"          => s.sel_in    = if v.is_empty() { None } else { Some(v.to_string()) },
            "output"         => s.sel_out   = if v.is_empty() { None } else { Some(v.to_string()) },
            "rig_model"      => s.rig_model = v.to_string(),
            "rig_port"       => s.rig_port  = v.to_string(),
            "rig_baud"       => s.rig_baud  = v.to_string(),
            "hamlib_enabled" => s.hamlib_enabled = v == "true",
            "period"         => s.period    = match v {
                "first"    => Period::TxFirst,
                "first15"  => Period::TxFirst15,
                "second15" => Period::TxSecond15,
                _          => Period::TxSecond,
            },
            "cty_path"       => s.cty_path  = v.to_string(),
            "max_km"         => s.max_km    = v.parse().unwrap_or(3000.0),
            "min_ccf"        => s.min_ccf   = v.parse().unwrap_or(200.0),  // legacy
            "min_conf"       => {}  // legacy — removed; EMA derives threshold automatically
            "tx_level"       => s.tx_level  = v.parse().unwrap_or(1.0),
            "ant_bw_horiz"   => s.ant_bw_horiz = v.parse().unwrap_or(40.0),
            "ant_bw_vert"    => s.ant_bw_vert   = v.parse().unwrap_or(40.0),
            "k_sigma"        => s.k_sigma       = v.parse().unwrap_or(5.0),
            "arm2_ccf"       => {}  // legacy — silently ignored
            "arm2_conf"      => {}  // legacy — silently ignored
            "save_max_data"  => s.save_max_data  = v == "true",
            "ptt_method"     => s.ptt_method = match v {
                "rts"        => PttMethod::Rts,
                "dtr"        => PttMethod::Dtr,
                "vox"        => PttMethod::VoxOnly,
                _            => PttMethod::HamlibCat,
            },
            "ptt_serial_port" => s.ptt_serial_port = v.to_string(),
            _ => {}
        }
    }
    // Re-resolve cty.dat if saved path no longer valid
    if !std::path::Path::new(&s.cty_path).exists() {
        let resolved = default_cty_path();
        log::warn!("[CFG] cty.dat not found at {:?} — resolved to {:?}", s.cty_path, resolved);
        s.cty_path = resolved;
    }
    log::info!("Config loaded: call={} loc={} in={:?} out={:?}", s.my_call, s.my_loc, s.sel_in, s.sel_out);
    s
}

// ─── main ─────────────────────────────────────────────────────────────────────

fn main() -> eframe::Result<()> {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info")
    ).init();
    eframe::run_native(&format!("FSK441 PLUS v{}", env!("CARGO_PKG_VERSION")),
        eframe::NativeOptions {
            viewport: egui::ViewportBuilder::default()
                .with_inner_size([1100.0, 700.0])
                .with_min_inner_size([800.0, 520.0]),
            ..Default::default()
        },
        Box::new(|cc| {
            // Force dark theme regardless of OS setting
            cc.egui_ctx.set_visuals(egui::Visuals::dark());
            Ok(Box::new(Fsk441App::new(cc)))
        }))
}


impl Drop for Fsk441App {
    fn drop(&mut self) {
        if let Some(ref mut child) = self._rigctld {
            log::info!("[LAUNCHER] Killing rigctld (pid={})", child.0.id());
            let _ = child.0.kill();
        }
    }
}
