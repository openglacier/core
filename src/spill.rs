//! Reusable temporary spill-file primitives.
#![cfg_attr(rustfmt, rustfmt_skip)]
use crate::helpers::{elapsed_micros, usize_to_u64_saturating};

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

static NEXT_SPILL_ID: AtomicU64 = AtomicU64::new(1);
static SPILL_RUNS_CREATED: AtomicU64 = AtomicU64::new(0);
static SPILL_RUNS_ACTIVE: AtomicU64 = AtomicU64::new(0);
static SPILL_RUNS_PEAK_ACTIVE: AtomicU64 = AtomicU64::new(0);
static SPILL_RUNS_DELETED: AtomicU64 = AtomicU64::new(0);
static SPILL_RECORDS_WRITTEN: AtomicU64 = AtomicU64::new(0);
static SPILL_PAYLOAD_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);
static SPILL_FRAMING_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);
static SPILL_RECORDS_READ: AtomicU64 = AtomicU64::new(0);
static SPILL_PAYLOAD_BYTES_READ: AtomicU64 = AtomicU64::new(0);
static SPILL_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static SPILL_PEAK_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static SPILL_FLUSH_US: AtomicU64 = AtomicU64::new(0);
static SPILL_SYNC_US: AtomicU64 = AtomicU64::new(0);
static SPILL_CREATE_US: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default)]
pub struct SpillStatsSnapshot {
    pub runs_created: u64,
    pub runs_active: u64,
    pub runs_peak_active: u64,
    pub runs_deleted: u64,
    pub records_written: u64,
    pub payload_bytes_written: u64,
    pub framing_bytes_written: u64,
    pub records_read: u64,
    pub payload_bytes_read: u64,
    pub live_bytes: u64,
    pub peak_live_bytes: u64,
    pub create_us: u64,
    pub flush_us: u64,
    pub sync_us: u64,
}

#[must_use]
pub fn stats_snapshot() -> SpillStatsSnapshot {
    SpillStatsSnapshot {
        runs_created: SPILL_RUNS_CREATED.load(Ordering::Relaxed),
        runs_active: SPILL_RUNS_ACTIVE.load(Ordering::Relaxed),
        runs_peak_active: SPILL_RUNS_PEAK_ACTIVE.load(Ordering::Relaxed),
        runs_deleted: SPILL_RUNS_DELETED.load(Ordering::Relaxed),
        records_written: SPILL_RECORDS_WRITTEN.load(Ordering::Relaxed),
        payload_bytes_written: SPILL_PAYLOAD_BYTES_WRITTEN.load(Ordering::Relaxed),
        framing_bytes_written: SPILL_FRAMING_BYTES_WRITTEN.load(Ordering::Relaxed),
        records_read: SPILL_RECORDS_READ.load(Ordering::Relaxed),
        payload_bytes_read: SPILL_PAYLOAD_BYTES_READ.load(Ordering::Relaxed),
        live_bytes: SPILL_LIVE_BYTES.load(Ordering::Relaxed),
        peak_live_bytes: SPILL_PEAK_LIVE_BYTES.load(Ordering::Relaxed),
        create_us: SPILL_CREATE_US.load(Ordering::Relaxed),
        flush_us: SPILL_FLUSH_US.load(Ordering::Relaxed),
        sync_us: SPILL_SYNC_US.load(Ordering::Relaxed),
    }
}

fn update_peak(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SpillEngine { directory: PathBuf, }

impl Default for SpillEngine {
    fn default() -> Self {
        let directory = std::env::var_os("OGD_TEMP_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join("ogd-spill");
        Self { directory }
    }
}

impl SpillEngine {
    #[must_use]
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }
    pub fn create_run(&self) -> io::Result<SpillRunWriter> {
        let started = Instant::now();
        fs::create_dir_all(&self.directory)?;
        let sequence = NEXT_SPILL_ID.fetch_add(1, Ordering::Relaxed);
        let path = self
            .directory
            .join(format!("ogd-{}-{sequence}.run", std::process::id()));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        SPILL_CREATE_US.fetch_add(elapsed_micros(started), Ordering::Relaxed);
        Ok(SpillRunWriter {
            path,
            writer: Some(BufWriter::new(file)),
            records: 0,
            payload_bytes: 0,
        })
    }
}

#[derive(Debug)]
pub struct SpillRunWriter {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    records: u64,
    payload_bytes: u64,
}
impl SpillRunWriter {
    pub fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let len = u64::try_from(bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "spill record too large"))?;
        let writer = self.writer.as_mut().expect("spill writer already finished");
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(bytes)?;
        self.records = self.records.saturating_add(1);
        self.payload_bytes = self
            .payload_bytes
            .saturating_add(usize_to_u64_saturating(bytes.len()));
        Ok(())
    }
    pub fn finish(mut self) -> io::Result<SpillRun> {
        let mut writer = self.writer.take().expect("spill writer already finished");
        let flush_started = Instant::now();
        writer.flush()?;
        SPILL_FLUSH_US.fetch_add(elapsed_micros(flush_started), Ordering::Relaxed);
        // Spill runs are process-local scratch data, not durable storage. A buffered
        // flush is sufficient before reopening the run for merge; forcing fsync here
        // only turns temporary query pressure into storage latency.
        let framing_bytes = self.records.saturating_mul(8);
        let bytes = self.payload_bytes.saturating_add(framing_bytes);
        SPILL_RUNS_CREATED.fetch_add(1, Ordering::Relaxed);
        let active = SPILL_RUNS_ACTIVE
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        update_peak(&SPILL_RUNS_PEAK_ACTIVE, active);
        SPILL_RECORDS_WRITTEN.fetch_add(self.records, Ordering::Relaxed);
        SPILL_PAYLOAD_BYTES_WRITTEN.fetch_add(self.payload_bytes, Ordering::Relaxed);
        SPILL_FRAMING_BYTES_WRITTEN.fetch_add(framing_bytes, Ordering::Relaxed);
        let live = SPILL_LIVE_BYTES
            .fetch_add(bytes, Ordering::Relaxed)
            .saturating_add(bytes);
        update_peak(&SPILL_PEAK_LIVE_BYTES, live);
        Ok(SpillRun {
            path: self.path.clone(),
            records: self.records,
            bytes,
        })
    }
}
impl Drop for SpillRunWriter {
    fn drop(&mut self) {
        if self.writer.is_some() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[derive(Debug)]
pub struct SpillRun {
    path: PathBuf,
    records: u64,
    bytes: u64,
}
impl SpillRun {
    #[must_use]
    pub const fn records(&self) -> u64 { self.records }
    #[must_use]
    pub const fn bytes(&self) -> u64 { self.bytes }
    #[must_use]
    pub fn path(&self) -> &Path { &self.path }
    pub fn reader(&self) -> io::Result<SpillRunReader> { Ok(SpillRunReader { reader: BufReader::new(File::open(&self.path)?), }) }
}
impl Drop for SpillRun {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        SPILL_RUNS_ACTIVE.fetch_sub(1, Ordering::Relaxed);
        SPILL_RUNS_DELETED.fetch_add(1, Ordering::Relaxed);
        SPILL_LIVE_BYTES.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct SpillRunReader { reader: BufReader<File>, }
impl SpillRunReader {
    pub fn next_record(&mut self) -> io::Result<Option<Vec<u8>>> {
        let mut len = [0u8; 8];
        match self.reader.read_exact(&mut len) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error),
        }
        let len = usize::try_from(u64::from_le_bytes(len)).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "spill record length overflow")
        })?;
        let mut bytes = vec![0u8; len];
        self.reader.read_exact(&mut bytes)?;
        SPILL_RECORDS_READ.fetch_add(1, Ordering::Relaxed);
        SPILL_PAYLOAD_BYTES_READ.fetch_add(usize_to_u64_saturating(len), Ordering::Relaxed);
        Ok(Some(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn run_round_trips_and_is_deleted() { let root = std::env::temp_dir().join(format!( "ogd-spill-test-{}", NEXT_SPILL_ID.fetch_add(1, Ordering::Relaxed) )); let engine = SpillEngine::new(&root); let mut writer = engine.create_run().unwrap(); writer.append(b"one").unwrap(); writer.append(b"two").unwrap(); let run = writer.finish().unwrap(); let path = run.path().to_path_buf(); let mut reader = run.reader().unwrap(); assert_eq!(reader.next_record().unwrap(), Some(b"one".to_vec())); assert_eq!(reader.next_record().unwrap(), Some(b"two".to_vec())); assert_eq!(reader.next_record().unwrap(), None); drop(run); assert!(!path.exists()); let _ = fs::remove_dir(root); }
}
