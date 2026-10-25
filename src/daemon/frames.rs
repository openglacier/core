//! Response frames of one streaming query, written in batches.
//!
//! Clients receive exactly the same bytes in the same order. The first
//! frames are written one by one, as before. Later frames share writes of
//! about [`FLUSH_BYTES`], and a flusher thread makes sure no frame waits more
//! than [`FLUSH_DELAY`] in the buffer, so a client sees each row at most that
//! much later.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::{
    io::{self, Write},
    net::TcpStream,
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde::Serialize;

use super::ConnectionError;
use crate::protocol::{ensure_payload_size, MessageKind, ProtocolError, StreamResponse, LENGTH_PREFIX_BYTES, MAX_RESPONSE_BYTES};

/// Frames written directly: a small answer never starts the flusher.
const DIRECT_FRAMES: u64 = 32;
/// Buffered bytes that trigger a write.
pub(super) const FLUSH_BYTES: usize = 64 * 1024;
/// Longest time a frame waits in the buffer.
const FLUSH_DELAY: Duration = Duration::from_millis(1);

pub(super) struct FrameWriter<'a> {
    stream: &'a mut TcpStream,
    frames: u64,
    scratch: Vec<u8>,
    flusher: Option<(Arc<Shared>, JoinHandle<()>)>,
}

struct Shared {
    pending: Mutex<Pending>,
    wake: Condvar,
}

/// Frames not written yet, on a clone of the connection stream.
struct Pending {
    stream: TcpStream,
    bytes: Vec<u8>,
    /// When the oldest buffered frame was queued.
    since: Option<Instant>,
    failed: Option<io::Error>,
    closed: bool,
}

impl Pending {
    fn write(&mut self) -> Result<(), ConnectionError> {
        if let Some(error) = self.failed.take() {
            return Err(ConnectionError::Write(error));
        }
        let written = self.stream.write_all(&self.bytes);
        self.bytes.clear();
        self.since = None;
        written.map_err(ConnectionError::Write)
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Pending> {
        crate::helpers::lock_unpoisoned(&self.pending)
    }

    fn flush_until_closed(&self) {
        let mut pending = self.lock();
        while !pending.closed {
            let Some(since) = pending.since else {
                pending = self.wake.wait(pending).unwrap_or_else(PoisonError::into_inner);
                continue;
            };
            let wait = FLUSH_DELAY.saturating_sub(since.elapsed());
            if wait.is_zero() {
                if let Err(ConnectionError::Write(error)) = pending.write() {
                    pending.failed = Some(error);
                }
            } else {
                pending = self.wake.wait_timeout(pending, wait).unwrap_or_else(PoisonError::into_inner).0;
            }
        }
    }
}

impl<'a> FrameWriter<'a> {
    pub(super) fn new(stream: &'a mut TcpStream) -> Self {
        Self { stream, frames: 0, scratch: Vec::new(), flusher: None }
    }

    /// Queues one response frame holding `value`.
    pub(super) fn push<T: Serialize>(&mut self, value: &T) -> Result<(), ConnectionError> {
        self.frames += 1;
        if self.flusher.is_none() && self.frames <= DIRECT_FRAMES {
            self.scratch.clear();
            encode_frame(&mut self.scratch, value)?;
            return self.stream.write_all(&self.scratch).map_err(ConnectionError::Write);
        }
        let shared = self.shared()?;
        let mut pending = shared.lock();
        encode_frame(&mut pending.bytes, value)?;
        if pending.bytes.len() >= FLUSH_BYTES {
            return pending.write();
        }
        if pending.since.is_none() {
            pending.since = Some(Instant::now());
            shared.wake.notify_one();
        }
        Ok(())
    }

    /// Writes the buffered frames, then the final `response`.
    pub(super) fn finish(mut self, response: &StreamResponse) -> Result<(), ConnectionError> {
        let encoded = crate::protocol::encode_stream_response(response).map_err(ConnectionError::Encode)?;
        let Some((shared, _)) = &self.flusher else {
            return self.stream.write_all(&encoded).map_err(ConnectionError::Write);
        };
        let mut pending = shared.lock();
        pending.bytes.extend_from_slice(&encoded);
        let written = pending.write();
        drop(pending);
        self.stop();
        written
    }

    fn shared(&mut self) -> Result<Arc<Shared>, ConnectionError> {
        if let Some((shared, _)) = &self.flusher {
            return Ok(Arc::clone(shared));
        }
        let pending = Pending {
            stream: self.stream.try_clone().map_err(ConnectionError::Write)?,
            bytes: Vec::with_capacity(2 * FLUSH_BYTES),
            since: None,
            failed: None,
            closed: false,
        };
        let shared = Arc::new(Shared { pending: Mutex::new(pending), wake: Condvar::new() });
        let flusher = {
            let shared = Arc::clone(&shared);
            thread::Builder::new()
                .name("og-frames".into())
                .spawn(move || shared.flush_until_closed())
                .map_err(ConnectionError::Write)?
        };
        self.flusher = Some((Arc::clone(&shared), flusher));
        Ok(shared)
    }

    fn stop(&mut self) {
        if let Some((shared, flusher)) = self.flusher.take() {
            shared.lock().closed = true;
            shared.wake.notify_one();
            let _ = flusher.join();
        }
    }
}

impl Drop for FrameWriter<'_> {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Appends one length-prefixed `MessagePack` frame to `buffer`; on error the
/// buffer is left as it was.
fn encode_frame<T: Serialize>(buffer: &mut Vec<u8>, value: &T) -> Result<(), ConnectionError> {
    let start = buffer.len();
    buffer.extend_from_slice(&[0; LENGTH_PREFIX_BYTES]);
    let encoded = rmp_serde::encode::write_named(buffer, value)
        .map_err(|error| ConnectionError::Encode(ProtocolError::InvalidMessagePackEncode(error)))
        .and_then(|()| {
            let length = buffer.len() - start - LENGTH_PREFIX_BYTES;
            ensure_payload_size(MessageKind::Response, length, MAX_RESPONSE_BYTES).map_err(ConnectionError::Encode)?;
            u32::try_from(length).map_err(|_| {
                ConnectionError::Encode(ProtocolError::MessageTooLarge { kind: MessageKind::Response, actual: length, maximum: MAX_RESPONSE_BYTES })
            })
        });
    match encoded {
        Ok(length) => {
            buffer[start..start + LENGTH_PREFIX_BYTES].copy_from_slice(&length.to_be_bytes());
            Ok(())
        }
        Err(error) => {
            buffer.truncate(start);
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Read, net::TcpListener};

    fn frames_of(count: u64, pause_after: Option<u64>) -> (Vec<u8>, Vec<u8>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let reader = thread::spawn(move || {
            let mut received = Vec::new();
            TcpStream::connect(address).unwrap().read_to_end(&mut received).unwrap();
            received
        });
        let (mut stream, _) = listener.accept().unwrap();
        let mut expected = Vec::new();
        let mut writer = FrameWriter::new(&mut stream);
        for index in 0..count {
            let value = serde_json::json!({ "index": index, "text": "x".repeat(40) });
            encode_frame(&mut expected, &value).unwrap();
            writer.push(&value).unwrap();
            if pause_after == Some(index) {
                thread::sleep(FLUSH_DELAY * 20);
            }
        }
        let last = StreamResponse::error(None, crate::protocol::WireError::new("query.execution_failed", "late"));
        expected.extend_from_slice(&crate::protocol::encode_stream_response(&last).unwrap());
        writer.finish(&last).unwrap();
        drop(stream);
        (reader.join().unwrap(), expected)
    }

    #[test] fn batched_frames_are_the_frames_written_one_by_one() { for count in [0, 1, DIRECT_FRAMES, DIRECT_FRAMES + 1, 5_000] { let (received, expected) = frames_of(count, Some(DIRECT_FRAMES + 3)); assert_eq!(received, expected, "{count} frames"); } }

    #[test] fn a_buffered_frame_is_written_within_the_flush_delay() { let listener = TcpListener::bind("127.0.0.1:0").unwrap(); let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap(); let (mut stream, _) = listener.accept().unwrap(); let mut writer = FrameWriter::new(&mut stream); let mut frame = Vec::new(); for index in 0..=DIRECT_FRAMES { frame.clear(); encode_frame(&mut frame, &index).unwrap(); writer.push(&index).unwrap(); } /* The frame after the direct ones is buffered, then written by the */ /* flusher while the query is still running. */ client.set_read_timeout(Some(Duration::from_secs(5))).unwrap(); let mut received = vec![0; frame.len() * (DIRECT_FRAMES as usize + 1)]; client.read_exact(&mut received).unwrap(); assert!(received.ends_with(&frame)); drop(writer); }
}
