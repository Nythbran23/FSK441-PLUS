#![allow(dead_code)]
// src/fsk441rx/store.rs
//
// SQLite persistence. WAL mode for safe concurrent reads (e.g. from DB Browser
// or DBeaver while the monitor is running).
//
// Storage philosophy: store metadata only — soft_dits NOT stored.
// At 3.5KB per ping they balloon to 1GB+ quickly. The raw_decode,
// confidence and CCF columns contain everything needed for analysis.
// soft_dits can be re-enabled per-session for targeted experiments.
// preserve all soft information for future off-line analysis.

use std::path::Path;
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{Connection, params};

use crate::detector::DetectedPing;
use crate::demod::DemodResult;
use crate::filter::ParsedMessage;

/// One entry in the analysis ring buffer — metadata + soft data for Doppler and second-pass
#[derive(Clone)]
pub struct AnalysisPing {
    pub detected_at:     String,
    pub df_hz:           f32,
    pub ccf_ratio:       f32,
    pub mean_confidence: f32,
    pub validity_score:  u8,
    pub raw_decode:      String,
    pub char_probs:      Vec<[f32; 48]>,
    /// Per-dit tone energy vectors Vec<[f32; 4]>, one entry per FSK441 dit.
    /// Used for intra-ping Doppler rate measurement: argmax(energies[i]) gives
    /// the dominant tone at each dit position; drift across the burst encodes
    /// the radial velocity change of the meteor trail.
    pub tone_energies:   Vec<[f32; 4]>,
}

pub struct Store {
    conn: Connection,
}

impl Drop for Store {
    /// Checkpoint the WAL on shutdown so the DB file is complete.
    /// Without this, the DB tail can be malformed if copied while WAL is live.
    fn drop(&mut self) {
        let _ = self.conn.execute_batch(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        );
        log::info!("[DB] WAL checkpoint completed on shutdown");
    }
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("Cannot open database: {}", path.display()))?;

        conn.execute_batch("
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous  = NORMAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS sessions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at  TEXT    NOT NULL,
                ended_at    TEXT,
                device      TEXT,
                notes       TEXT
            );

            CREATE TABLE IF NOT EXISTS pings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id      INTEGER NOT NULL REFERENCES sessions(id),
                -- Timing
                detected_at     TEXT    NOT NULL,
                df_hz           REAL,
                ccf_ratio       REAL,
                duration_ms     REAL,
                -- Hard decode
                raw_decode      TEXT,
                validity_score  INTEGER NOT NULL DEFAULT 0,
                message_type    TEXT,
                callsign_a      TEXT,
                callsign_b      TEXT,
                locator         TEXT,
                report          TEXT,
                is_cq           INTEGER NOT NULL DEFAULT 0,
                -- Soft metric summary
                mean_confidence REAL,
                min_confidence  REAL,
                n_ambiguous     INTEGER,
                -- Soft dit matrix: Vec<[f32;4]> stored as little-endian bytes.
                -- 100 dits × 4 tones × 4 bytes = 1600 bytes typical.
                soft_dits       BLOB
            );

            CREATE INDEX IF NOT EXISTS idx_pings_session
                ON pings(session_id, detected_at);
            CREATE INDEX IF NOT EXISTS idx_pings_score
                ON pings(validity_score DESC);
            CREATE INDEX IF NOT EXISTS idx_pings_callsigns
                ON pings(callsign_a, callsign_b);
        ").context("Database schema creation")?;

        // ── Migrations: run on every open, idempotent ─────────────────────
        // Each block uses IF NOT EXISTS / IF NOT column so safe to re-run.
        conn.execute_batch("
            -- v2: analysis ring buffer table (added 2026-04-10)
            CREATE TABLE IF NOT EXISTS analysis_pings (
                id              INTEGER PRIMARY KEY,
                capture_id      INTEGER NOT NULL,
                captured_at     TEXT NOT NULL,
                detected_at     TEXT NOT NULL,
                df_hz           REAL,
                ccf_ratio       REAL,
                mean_confidence REAL,
                validity_score  INTEGER,
                raw_decode      TEXT,
                char_probs      BLOB
            );
            CREATE INDEX IF NOT EXISTS idx_analysis_capture
                ON analysis_pings(capture_id, detected_at);
        ").context("Database migration v2")?;

        // v3: per-dit tone energy vectors for Doppler analysis (2026-04-16)
        // ALTER TABLE ignores if column already exists via the error-swallow pattern
        let _ = conn.execute_batch(
            "ALTER TABLE analysis_pings ADD COLUMN tone_energies BLOB;"
        );
        // Also start storing soft_dits in pings table (was always NULL previously)
        // soft_dits column already exists in schema — just ensure it's there
        let _ = conn.execute_batch(
            "ALTER TABLE pings ADD COLUMN soft_dits BLOB;"
        );

        Ok(Self { conn })
    }

    pub fn new_session(&self, device: Option<&str>, notes: Option<&str>) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO sessions (started_at, device, notes) VALUES (?1,?2,?3)",
            params![Utc::now().to_rfc3339(), device, notes],
        ).context("Insert session")?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Save a batch of analysis pings (ring buffer contents) to analysis_pings.
    /// Returns the capture_id assigned.
    pub fn save_analysis(&self, entries: &[AnalysisPing]) -> Result<i64> {
        if entries.is_empty() { return Ok(0); }
        let captured_at = chrono::Utc::now().to_rfc3339();
        // Get next capture_id
        let capture_id: i64 = self.conn.query_row(
            "SELECT COALESCE(MAX(capture_id), 0) + 1 FROM analysis_pings",
            [], |r| r.get(0)
        ).unwrap_or(1);
        for e in entries {
            // Serialise char_probs: Vec<[f32;48]> → flat f32 bytes LE
            let blob: Vec<u8> = e.char_probs.iter()
                .flat_map(|row| row.iter().flat_map(|&f| f.to_le_bytes()))
                .collect();
            // Serialise tone_energies: Vec<[f32;4]> → flat f32 bytes LE
            // n_dits × 4 tones × 4 bytes — encodes per-dit spectral fingerprint
            let tone_blob: Vec<u8> = e.tone_energies.iter()
                .flat_map(|row| row.iter().flat_map(|&f| f.to_le_bytes()))
                .collect();
            self.conn.execute(
                "INSERT INTO analysis_pings
                 (capture_id, captured_at, detected_at, df_hz, ccf_ratio,
                  mean_confidence, validity_score, raw_decode, char_probs, tone_energies)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                rusqlite::params![
                    capture_id, captured_at, e.detected_at, e.df_hz,
                    e.ccf_ratio, e.mean_confidence, e.validity_score,
                    e.raw_decode, blob,
                    if tone_blob.is_empty() { None } else { Some(tone_blob) },
                ],
            )?;
        }
        log::info!("[DB] Saved {} analysis pings as capture_id={}", entries.len(), capture_id);
        Ok(capture_id)
    }

    /// Delete all analysis_pings — call after processing to keep DB lean.
    /// The ring buffer in RAM is the live store; the DB is only written on demand.
    pub fn delete_analysis(&self, capture_id: i64) -> Result<()> {
        self.conn.execute(
            "DELETE FROM analysis_pings WHERE capture_id = ?1",
            rusqlite::params![capture_id],
        )?;
        log::info!("[DB] Deleted analysis capture_id={}", capture_id);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn close_session(&self, session_id: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE sessions SET ended_at = ?1 WHERE id = ?2",
            params![Utc::now().to_rfc3339(), session_id],
        ).context("Close session")?;
        Ok(())
    }

    pub fn insert_ping(
        &self,
        session_id: i64,
        ping:       &DetectedPing,
        result:     &DemodResult,
        parsed:     &ParsedMessage,
    ) -> Result<i64> {
        // Store soft_dits tone energies for every ping that reaches insert_ping —
        // the should_store gate in run_engine already made the decision to store.
        // No secondary gate here: if the ping is worth storing, its energies are too.
        let soft_blob: Option<Vec<u8>> = if result.soft_dits.is_empty() {
            None
        } else {
            Some(result.soft_dits.iter()
                .flat_map(|d| d.energies.iter().flat_map(|&f| f.to_le_bytes()))
                .collect())
        };

        self.conn.execute(
            "INSERT INTO pings (
                session_id, detected_at, df_hz, ccf_ratio, duration_ms,
                raw_decode, validity_score, message_type,
                callsign_a, callsign_b, locator, report, is_cq,
                mean_confidence, min_confidence, n_ambiguous,
                soft_dits
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
            params![
                session_id,
                ping.timestamp.to_rfc3339(),
                result.df_hz,
                ping.ccf_ratio,
                ping.duration_ms,
                if result.raw_decode.is_empty() { None } else { Some(&result.raw_decode) },
                parsed.validity_score,
                format!("{:?}", parsed.message_type),
                parsed.callsign_a(),
                parsed.callsign_b(),
                parsed.locator.as_deref(),
                parsed.report.as_deref(),
                parsed.is_cq as i32,
                result.mean_confidence,
                result.min_confidence,
                result.n_ambiguous as i64,
                soft_blob,
            ],
        ).context("Insert ping")?;

        Ok(self.conn.last_insert_rowid())
    }

    #[allow(dead_code)]
    pub fn session_summary(&self, session_id: i64) -> Result<SessionSummary> {
        let q = |sql: &str| -> i64 {
            self.conn.query_row(sql, params![session_id], |r| r.get(0)).unwrap_or(0)
        };

        Ok(SessionSummary {
            total_pings:       q("SELECT COUNT(*) FROM pings WHERE session_id=?1"),
            valid_pings:       q("SELECT COUNT(*) FROM pings WHERE session_id=?1 AND validity_score>=60"),
            high_confidence:   q("SELECT COUNT(*) FROM pings WHERE session_id=?1 AND validity_score>=80"),
            unique_callsigns:  q("SELECT COUNT(DISTINCT callsign_a) FROM pings WHERE session_id=?1 AND callsign_a IS NOT NULL"),
            unique_locators:   q("SELECT COUNT(DISTINCT locator) FROM pings WHERE session_id=?1 AND locator IS NOT NULL"),
        })
    }

    /// Clear threshold_history and analysis_pings so the optimiser recalibrates
    /// from scratch on next run. Keeps pings table intact.
    pub fn reset_for_recalibration(&self) -> Result<()> {
        self.conn.execute_batch(
            "DELETE FROM threshold_history; DELETE FROM analysis_pings;"
        )?;
        log::info!("[DB] Reset threshold_history and analysis_pings for recalibration");
        Ok(())
    }

    /// Count total pings in DB — used to trigger early auto-calibration
    pub fn count_pings(&self) -> i64 {
        self.conn.query_row(
            "SELECT COUNT(*) FROM pings", [],
            |r| r.get(0)
        ).unwrap_or(0)
    }

    /// True if threshold_history is empty — indicates a fresh/reset DB
    pub fn needs_calibration(&self) -> bool {
        self.conn.query_row(
            "SELECT COUNT(*) FROM threshold_history", [],
            |r| r.get::<_, i64>(0)
        ).unwrap_or(0) == 0
    }
}#[derive(Debug)]
#[allow(dead_code)]
pub struct SessionSummary {
    pub total_pings:      i64,
    pub valid_pings:      i64,
    pub high_confidence:  i64,
    pub unique_callsigns: i64,
    pub unique_locators:  i64,
}
