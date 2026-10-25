# Storage backends

A backend owns persistence and implements [`StorageEngine`](../mod.rs) directly: snapshots,
transactions and atomic batches. `MemoryStorage` and `GlacierStorage` name the two backends.

| Backend | `OGD_STORAGE` | Source | Persistence |
|---|---|---|---|
| Memory | `memory` (default) | `memory.rs` | none: data goes with the process (history with `OGD_HISTORY=on`) |
| Glacier | `glacier` | `glacier/` | a directory of immutable column segments, a write-ahead log and a manifest |

Glacier's on-disk format, write path, reads, recovery and history are described in [GLACIER.md](GLACIER.md).
