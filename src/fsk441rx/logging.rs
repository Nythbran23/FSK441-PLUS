// src/fsk441rx/logging.rs
//
// WSJTX UDP broadcast — sends QSOLogged and LoggedADIF messages on LOG QSO.
// Compatible with Logger32, N1MM+, JTAlert, DX Lab Suite, HRD etc.
// Default port 2237 — same as MSHV/WSJT-X.

use std::net::UdpSocket;
use chrono::Timelike;

const MAGIC:            u32 = 0xadbccbda;
const SCHEMA:           u32 = 3;
const TYPE_QSO_LOGGED:  u32 = 5;
const TYPE_LOGGED_ADIF: u32 = 12;
const APP_ID: &str          = "FSK441 Plus";

pub struct LogRecord {
    pub my_call:   String,
    pub my_grid:   String,
    pub dx_call:   String,
    pub dx_grid:   String,
    pub freq_hz:   u64,
    pub mode:      String,
    pub rst_sent:  String,
    pub rst_rcvd:  String,
    pub time_on:   chrono::DateTime<chrono::Utc>,
    pub time_off:  chrono::DateTime<chrono::Utc>,
    pub comment:   String,
    pub prop_mode: String,
}

pub fn broadcast(host: &str, port: u16, rec: &LogRecord) {
    let addr = format!("{}:{}", host, port);
    let sock = match UdpSocket::bind("0.0.0.0:0") {
        Ok(s)  => s,
        Err(e) => { log::warn!("[UDP] bind failed: {}", e); return; }
    };
    if let Err(e) = sock.connect(&addr) {
        log::warn!("[UDP] connect to {} failed: {}", addr, e); return;
    }
    let msg = build_qso_logged(rec);
    match sock.send(&msg) {
        Ok(_)  => log::info!("[UDP] QSOLogged -> {} ({} bytes)", addr, msg.len()),
        Err(e) => log::warn!("[UDP] QSOLogged send failed: {}", e),
    }
    let msg2 = build_logged_adif(rec);
    match sock.send(&msg2) {
        Ok(_)  => log::info!("[UDP] LoggedADIF -> {} ({} bytes)", addr, msg2.len()),
        Err(e) => log::warn!("[UDP] LoggedADIF send failed: {}", e),
    }
}

fn build_qso_logged(rec: &LogRecord) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    write_header(&mut buf, TYPE_QSO_LOGGED);
    write_qdatetime(&mut buf, rec.time_off);
    write_utf8(&mut buf, &rec.dx_call);
    write_utf8(&mut buf, &rec.dx_grid);
    write_u64(&mut buf, rec.freq_hz);
    write_utf8(&mut buf, &rec.mode);
    write_utf8(&mut buf, &rec.rst_sent);
    write_utf8(&mut buf, &rec.rst_rcvd);
    write_utf8(&mut buf, "");
    write_utf8(&mut buf, &rec.comment);
    write_utf8(&mut buf, "");
    write_qdatetime(&mut buf, rec.time_on);
    write_utf8(&mut buf, "");
    write_utf8(&mut buf, &rec.my_call);
    write_utf8(&mut buf, &rec.my_grid);
    write_utf8(&mut buf, "");
    write_utf8(&mut buf, "");
    write_utf8(&mut buf, &rec.prop_mode);
    buf
}

fn build_logged_adif(rec: &LogRecord) -> Vec<u8> {
    let mut buf = Vec::with_capacity(512);
    write_header(&mut buf, TYPE_LOGGED_ADIF);
    let adif = format!(
        "\n<ADIF_VER:5>3.1.0\n<PROGRAMID:9>FSK441Pls\n<EOH>\n{}<EOR>",
        adif_record(rec)
    );
    write_bytes(&mut buf, adif.as_bytes());
    buf
}

fn adif_record(rec: &LogRecord) -> String {
    let freq_mhz = format!("{:.6}", rec.freq_hz as f64 / 1_000_000.0);
    let band = freq_to_band(rec.freq_hz);
    let mut s = String::new();
    s += &af("STATION_CALLSIGN", &rec.my_call);
    s += &af("MY_GRIDSQUARE",    &rec.my_grid);
    s += &af("CALL",             &rec.dx_call);
    s += &af("GRIDSQUARE",       &rec.dx_grid);
    s += &af("MODE",             &rec.mode);
    s += &af("RST_SENT",         &rec.rst_sent);
    s += &af("RST_RCVD",         &rec.rst_rcvd);
    s += &af("QSO_DATE",         &rec.time_on.format("%Y%m%d").to_string());
    s += &af("TIME_ON",          &rec.time_on.format("%H%M%S").to_string());
    s += &af("QSO_DATE_OFF",     &rec.time_off.format("%Y%m%d").to_string());
    s += &af("TIME_OFF",         &rec.time_off.format("%H%M%S").to_string());
    s += &af("FREQ",             &freq_mhz);
    s += &af("BAND",             &band);
    if !rec.prop_mode.is_empty() { s += &af("PROP_MODE", &rec.prop_mode); }
    if !rec.comment.is_empty()   { s += &af("COMMENT",   &rec.comment);   }
    s
}

fn af(tag: &str, val: &str) -> String {
    if val.is_empty() { return String::new(); }
    format!("<{}:{}>{}", tag, val.len(), val)
}

fn freq_to_band(hz: u64) -> String {
    match hz / 1_000_000 {
        1..=2     => "160m", 3..=4   => "80m",  5       => "60m",
        7..=8     => "40m",  10      => "30m",   14..=15 => "20m",
        18..=19   => "17m",  21..=22 => "15m",  24..=25 => "12m",
        28..=30   => "10m",  50..=54 => "6m",   70..=71 => "4m",
        144..=148 => "2m",   430..=440 => "70cm",
        _ => "other",
    }.to_string()
}

fn write_header(buf: &mut Vec<u8>, msg_type: u32) {
    write_u32(buf, MAGIC);
    write_u32(buf, SCHEMA);
    write_u32(buf, msg_type);
    write_utf8(buf, APP_ID);
}

fn write_u32(buf: &mut Vec<u8>, v: u32)  { buf.extend_from_slice(&v.to_be_bytes()); }
fn write_u64(buf: &mut Vec<u8>, v: u64)  { buf.extend_from_slice(&v.to_be_bytes()); }
fn write_utf8(buf: &mut Vec<u8>, s: &str) { write_bytes(buf, s.as_bytes()); }

fn write_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    if bytes.is_empty() { write_u32(buf, 0xffff_ffff); }
    else { write_u32(buf, bytes.len() as u32); buf.extend_from_slice(bytes); }
}

fn write_qdatetime(buf: &mut Vec<u8>, dt: chrono::DateTime<chrono::Utc>) {
    use chrono::Datelike;
    let (y, m, d) = (dt.year() as i64, dt.month() as i64, dt.day() as i64);
    let jdn = (1461 * (y + 4800 + (m - 14) / 12)) / 4
            + (367  * (m - 2 - 12 * ((m - 14) / 12))) / 12
            - (3    * ((y + 4900 + (m - 14) / 12) / 100)) / 4
            + d - 32075;
    let ms = dt.hour() as u32 * 3_600_000
           + dt.minute() as u32 * 60_000
           + dt.second() as u32 * 1_000
           + dt.nanosecond() / 1_000_000;
    buf.extend_from_slice(&jdn.to_be_bytes());
    write_u32(buf, ms);
    buf.push(1u8); // UTC
}
