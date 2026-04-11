// src/fsk441rx/main.rs

use clap::Parser;
use std::path::PathBuf;
use std::io::Write;
use tokio::sync::mpsc;
use anyhow::Result;

mod params;
mod audio;
mod detector;
mod demod;
mod filter;
mod store;
mod geo;

use detector::{DetectedPing, run_detector};
use demod::longx;
use filter::ParsedMessage;
use store::Store;
use geo::{GeoValidator, PrefixDb, Qth};

#[derive(Parser, Debug)]
#[command(name = "fsk441rx", version = "0.1.0",
    about = "FSK441 meteor scatter monitor")]
struct Args {
    #[arg(long)]
    device: Option<String>,

    #[arg(long)]
    wav: Option<PathBuf>,

    #[arg(long)]
    realtime: bool,

    #[arg(long, default_value = "fsk441rx.db")]
    db: PathBuf,

    /// Path to cty.dat for geographic callsign validation
    #[arg(long)]
    cty: Option<PathBuf>,

    /// Operator QTH as Maidenhead locator (default: IO82KM)
    #[arg(long, default_value = "IO82KM")]
    qth: String,

    /// Maximum great-circle distance km for callsign validation (default: 3000)
    #[arg(long, default_value_t = 3000.0f64)]
    max_km: f64,

    #[arg(long, default_value_t = params::DEFAULT_THRESHOLD)]
    threshold: f32,

    #[arg(long, default_value_t = params::DEFAULT_DFTOL)]
    dftol: i32,

    #[arg(long)]
    verbose: bool,

    #[arg(long, default_value_t = 60u8)]
    min_score: u8,

    #[arg(long, default_value_t = 0.65f32)]
    min_conf: f32,

    #[arg(long, default_value_t = 80.0f32)]
    min_ccf: f32,

    /// Disable ANSI bold highlighting
    #[arg(long)]
    no_colour: bool,

    #[arg(long)]
    note: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("warn")
    ).init();

    let args = Args::parse();

    // ── Geographic validator ─────────────────────────────────────────────────
    let qth = Qth::from_maidenhead(&args.qth)
        .unwrap_or_else(|| {
            eprintln!("[GEO] Invalid QTH '{}', using IO82KM", args.qth);
            Qth::default()
        });

    let prefix_db = match &args.cty {
        Some(path) => PrefixDb::load(path),
        None => {
            // Try standard MSHV location relative to binary
            let default = PathBuf::from("bin/settings/database/cty.dat");
            if default.exists() {
                PrefixDb::load(&default)
            } else {
                eprintln!("[GEO] No cty.dat found — use --cty path/to/cty.dat for geo filtering");
                PrefixDb::load(&PathBuf::from("nonexistent")) // returns empty
            }
        }
    };

    let geo = GeoValidator::new(prefix_db, qth, args.max_km);

    eprintln!("FSK441 Meteor Scatter Monitor v0.1");
    eprintln!("QTH       : {} ({:.2}N, {:.2}E)", args.qth, qth.lat, qth.lon);
    eprintln!("Max range : {:.0} km", args.max_km);
    eprintln!("Threshold : {:.1}  MinConf: {:.2}  MinCCF: {:.0}",
        args.threshold, args.min_conf, args.min_ccf);

    let store = Store::open(&args.db)?;
    let source = args.device.as_deref()
        .or_else(|| args.wav.as_ref().and_then(|p| p.to_str()))
        .unwrap_or("default");
    let session_id = store.new_session(Some(source), args.note.as_deref())?;
    eprintln!("Session   : {}", session_id);

    println!("{:<12} {:>8} {:>7} {:>5} {:>3}  {}",
             "Time(UTC)", "DF(Hz)", "CCF", "Conf", "Scr", "Decode");
    println!("{}", "-".repeat(72));

    let (audio_tx, audio_rx) = mpsc::unbounded_channel::<Vec<f32>>();
    let (ping_tx, mut ping_rx) = mpsc::unbounded_channel::<DetectedPing>();

    if let Some(ref wav_path) = args.wav {
        let path = wav_path.clone();
        let realtime = args.realtime;
        let tx = audio_tx;
        tokio::spawn(async move {
            if let Err(e) = audio::process_wav(&path, tx, realtime).await {
                eprintln!("[AUDIO] WAV error: {}", e);
            }
        });
    } else {
        audio::start_live(args.device.clone(), audio_tx).await?;
    }

    tokio::spawn(run_detector(audio_rx, ping_tx, args.threshold, args.dftol));

    let (ctrlc_tx, mut ctrlc_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        let _ = ctrlc_tx.send(());
    });

    let colour = !args.no_colour;

    loop {
        tokio::select! {
            result = ping_rx.recv() => {
                match result {
                    Some(ping) => process_ping(
                        &store, session_id, &ping,
                        args.verbose, args.min_score,
                        args.min_conf, args.min_ccf,
                        colour, &geo,
                    ),
                    None => { eprintln!("Audio finished."); break; }
                }
            }
            _ = &mut ctrlc_rx => { eprintln!("\n[Ctrl-C]"); break; }
        }
    }

    let summary = store.session_summary(session_id)?;
    store.close_session(session_id)?;

    eprintln!();
    eprintln!("Total pings    : {}", summary.total_pings);
    eprintln!("Displayed      : {}", summary.valid_pings);
    eprintln!("High conf      : {}", summary.high_confidence);
    eprintln!("Callsigns seen : {}", summary.unique_callsigns);
    eprintln!("Locators seen  : {}", summary.unique_locators);

    Ok(())
}

fn highlight_decode(raw: &str, parsed: &ParsedMessage, colour: bool) -> String {
    if !colour { return raw.to_string(); }

    let bold  = "\x1b[1m";
    let reset = "\x1b[0m";
    let mut result = raw.to_string();

    // Bold geo-valid callsigns in green, invalid ones in dim
    for call in &parsed.callsigns {
        if call.is_empty() { continue; }
        let is_valid = parsed.valid_callsigns.contains(call);
        let fmt = if is_valid {
            format!("{}{}{}", bold, call, reset)
        } else {
            // Geographically implausible — show dimmed
            format!("\x1b[2m{}\x1b[0m", call)
        };
        result = result.replacen(call.as_str(), &fmt, 1);
    }

    if let Some(ref loc) = parsed.locator {
        result = result.replace(loc.as_str(), &format!("{}{}{}", bold, loc, reset));
    }
    if let Some(ref rpt) = parsed.report {
        result = result.replace(rpt.as_str(), &format!("{}{}{}", bold, rpt, reset));
    }

    result
}

fn process_ping(
    store:      &Store,
    session_id: i64,
    ping:       &DetectedPing,
    verbose:    bool,
    min_score:  u8,
    min_conf:   f32,
    min_ccf:    f32,
    colour:     bool,
    geo:        &GeoValidator,
) {
    let result = longx(&ping.audio, ping.df_hz, params::DEFAULT_DFTOL);

    // Parse with geographic validation
    let parsed = ParsedMessage::parse_geo(&result.raw_decode, Some(geo));

    // Always store in DB
    if let Err(e) = store.insert_ping(session_id, ping, &result, &parsed) {
        eprintln!("[DB] insert error: {}", e);
    }

    if !verbose {
        if result.raw_decode.trim().len() < 2 { return; }
        if result.mean_confidence < min_conf && ping.ccf_ratio < min_ccf { return; }
        if parsed.validity_score < min_score { return; }
    }

    let stars = match parsed.validity_score {
        80..=100 => "***",
        60..=79  => "** ",
        40..=59  => "*  ",
        _        => "   ",
    };

    let df   = if ping.df_hz.is_finite()            { ping.df_hz }   else { 0.0 };
    let ccf  = if ping.ccf_ratio.is_finite()         { ping.ccf_ratio } else { 0.0 };
    let conf = if result.mean_confidence.is_finite() { result.mean_confidence } else { 0.0 };
    let ts   = ping.timestamp.format("%H:%M:%S").to_string();
    let dec  = highlight_decode(&result.raw_decode, &parsed, colour);

    let line = format!("{:<12} {:>+8.0} {:>7.2} {:>5.2} {:>3}{}  {}",
        ts, df, ccf, conf, parsed.validity_score, stars, dec);

    let _ = writeln!(std::io::stdout().lock(), "{}", line);
}
