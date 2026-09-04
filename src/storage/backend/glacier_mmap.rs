//! Safe read-only mmap support for immutable committed Glacier snapshots.
//!
//! `og-core` deliberately forbids unsafe code. On 64-bit targets file-backed
//! mappings are therefore created through `mmap-guard`, whose safe API owns the
//! platform-specific `memmap2` boundary. Glacier only exposes slices inside the
//! file length captured at the beginning of a sequential scan.
//!
//! Non-64-bit and non-Unix targets keep the existing buffered read path. This avoids making
//! multi-gigabyte virtual mappings part of the compatibility contract for small
//! address spaces.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{io, path::Path};

#[cfg(all(target_pointer_width = "64", target_family = "unix"))]
use mmap_guard::{map_file, FileData};

/// Owns one read-only mapping for an immutable-length Glacier file snapshot.
#[cfg(all(target_pointer_width = "64", target_family = "unix"))]
pub(super) struct GlacierReadOnlyMap {
    data: FileData,
    snapshot_len: usize,
}

/// Placeholder on small address-space targets; mapping is intentionally disabled.
#[cfg(not(all(target_pointer_width = "64", target_family = "unix")))]
pub(super) struct GlacierReadOnlyMap;

impl GlacierReadOnlyMap {
    #[inline]
    pub(super) const fn supported() -> bool {
        cfg!(all(target_pointer_width = "64", target_family = "unix"))
    }

    /// Creates a read-only mapping whose visible range is capped to
    /// `snapshot_len`, the file size observed by the scan before mapping.
    #[cfg(all(target_pointer_width = "64", target_family = "unix"))]
    pub(super) fn map(path: &Path, snapshot_len: u64) -> io::Result<Self> {
        let snapshot_len = usize::try_from(snapshot_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Glacier scan snapshot does not fit the target address space",
            )
        })?;
        if snapshot_len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot mmap an empty Glacier snapshot",
            ));
        }

        let data = map_file(path)?;
        if data.len() < snapshot_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Glacier file shrank while creating the read snapshot",
            ));
        }

        Ok(Self { data, snapshot_len })
    }

    /// Mapping is intentionally unavailable outside 64-bit Unix targets.
    #[cfg(not(all(target_pointer_width = "64", target_family = "unix")))]
    pub(super) fn map(_path: &Path, _snapshot_len: u64) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Glacier mmap scan snapshots are disabled outside 64-bit Unix targets",
        ))
    }

    /// Returns a range only when it is fully contained in the scan snapshot.
    #[cfg(all(target_pointer_width = "64", target_family = "unix"))]
    #[inline]
    pub(super) fn slice(&self, offset: u64, len: usize) -> io::Result<&[u8]> {
        let start = usize::try_from(offset).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Glacier mmap offset does not fit the target address space",
            )
        })?;
        let end = start.checked_add(len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Glacier mmap range overflow")
        })?;
        if end > self.snapshot_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Glacier mmap range exceeds the scan snapshot",
            ));
        }
        Ok(&self.data[start..end])
    }

    /// Mapping is intentionally unavailable outside 64-bit Unix targets.
    #[cfg(not(all(target_pointer_width = "64", target_family = "unix")))]
    #[inline]
    pub(super) fn slice(&self, _offset: u64, _len: usize) -> io::Result<&[u8]> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Glacier mmap scan snapshots are disabled outside 64-bit Unix targets",
        ))
    }
}
