# Glacier file structure

Glacier is a column-segment store, on-disk format version 6 (`GLACIER_FORMAT_VERSION`; the file magics start with
`OG6`). A store is a directory (`OGD_STORAGE=glacier`, `OGD_STORAGE_PATH=<directory>`):

```text
<store>/
  MANIFEST            live segments, deleted rows, field facts, at one generation
  0000000000000005.g6   immutable column segments (one collection each), number in 16 hex digits;
                        older stores hold `.seg` files, still read under their name
  ...
  wal-00000007.log    commits after the manifest generation
  LOCK                held (flock, exclusive) by the one handle that has the store open
```

Source: `glacier/`, one file per layer: `codec.rs` (bytes, checksum, value codec), `encoding.rs` (value streams),
`column.rs` (column chunks), `segment.rs`, `wal.rs`, `manifest.rs`, `mod.rs` (state, commit, flush), `read.rs`, `tx.rs`.

## 1. Segments

```text
"OG6SEG01" | chunk ... chunk | footer | footer_len u32 | footer checksum u64 | "OG6SEGFT"
```

- Rows are sorted by identifier and split into **row groups** of up to 65 536 rows; a flush or an import writes
  segments of up to 1 048 576 rows.
- Each group has one identifier chunk, one version chunk and **one chunk per top-level field** (none when no row
  of the group holds the field).
- The footer holds the field names once, each group's identifier range and chunk locations, and one checksum per
  chunk, verified on the chunk's first read.
- Footer version 2 adds **per-chunk statistics**: the kinds present and, per kind, the smallest and largest
  signed, unsigned, float and string value (strings over 64 bytes leave the string bounds unknown). They feed
  `StorageRead::field_bounds`, which the planner uses to prove expressions total. Version 1 segments stay readable,
  their fields' bounds unknown.

| Chunk | Encoding |
|---|---|
| identifiers | one 16-byte anchor per 1 024-row block, then the deltas as an integer stream when each fits 64 bits; otherwise two integer streams: the high halves as deltas within each block, the low halves as values (raw 16 bytes in older segments) |
| versions | integer stream |
| field | kind tags + one dense stream per kind holding a payload |

### Column chunk

```text
rows u32 | tags | kinds present u16 | count u32 per stream kind | ranks | streams
```

- **Tags**: one kind per cell (missing, null, false, true, signed, unsigned, float, string, nested), stored as one
  constant, as a dominant kind with sorted exceptions (under 10 % of rows), or bit-packed on 4 bits.
- **Ranks**: for mixed chunks, each stream's position at the start of every 1 024-row block, so any block decodes
  on its own.
- **Streams** (each picks its smallest encoding):

| Kind | Encodings |
|---|---|
| signed, unsigned | constant · frame of reference + bit packing · sorted dictionary + bit-packed codes |
| float | scaled decimal `m / 10^e` (bit-exact, exceptions kept raw) as an integer stream · raw bit patterns as an integer stream |
| string | dictionary + bit-packed codes · plain offsets + bytes |
| nested (array, object) | plain offsets + value-codec bytes |

On `ana` (16 753 758 rows, 7 fields): **264 MB, 15.8 bytes per row**, against 7 185 MB (429 bytes) for the former
record store (Glacier 5). Its UUIDv7
identifiers take about 1 byte per row split in halves; written raw, as before the split mode, they took 16 (515 MB, 30.7 bytes per row).

## 2. Writes

| Commit | Path |
|---|---|
| fewer than 16 384 puts | appended to the log, `sync_data`, kept in the in-memory delta |
| 16 384 puts or more | written straight to segments, then a new manifest |

- A replace or a delete marks the old row dead (a per-segment bitmap) or drops it from the delta.
- **Flush**: when the delta reaches 65 536 rows (or the log 64 MiB), the delta is written as segments and a new
  manifest is installed (write, sync, rename, sync directory); the log restarts empty.
- **Compaction**, during a flush: segments with half their rows dead, and runs of 8 small segments (under 262 144
  rows), are rewritten with the delta.
- Transactions are optimistic: a commit fails with a conflict if another commit happened since
  the transaction began.

## 3. Reads

Readers take an immutable snapshot of the whole state (`Arc<State>`). Segment files are mapped read-only.

- Unordered reads walk segments block by block (1 024 rows), then the delta.
- Ordered reads (`scan_each`, `scan_projected_each`, …) merge segments and delta by identifier, in both
  directions, only when their identifier ranges overlap.
- `scan_projected_blocks_each` hands whole blocks, column by column, to the engine's batch runner
  (`engine::scan_batched`). A column whose cells share one kind over the block (signed, unsigned, float, string) is
  decoded straight into a slice of that kind (`BlockColumn`), without a per-cell value; other columns come as values.
  Under `ScanOptions::without_locators` (the caller reads neither identifiers nor versions), neither is decoded and
  `ProjectedBlock::ids` stays empty. A nested path (`montant.ca`) is read in the block too: its top-level chunk is
  decoded once per block, and each row's encoded value is walked to the path without decoding the fields it does not
  cross (`codec::value_at_path`); strings borrow the segment bytes.
- Bit-packed streams unpack 8 values per bounds check, at offsets fixed per width; integer dictionaries of up to 256
  entries are decoded into a table once per block.
- `scan_projected_blocks_each` hands the engine each row group's facts per projected field (missing rows, kinds, bounds, from the
  footer statistics; unknown for a nested path) before reading it, and leaves the group out when the engine proves its leading filters drop every row
  (`query::GroupPruning`); delta rows are always read.
- `sample_row_ids` draws distinct positions among stored rows, redraws dead ones, and decodes only the drawn identifiers and versions.
- `count`, `field_facts` and `top_level_fields` are exact at every snapshot.

## 4. Recovery

Open: lock `LOCK` (a second handle, in this process or another, is refused), read `MANIFEST`, map its segments, delete
unreferenced segment and log files, replay the log records after the manifest generation.

- A store writes its `MANIFEST` when it is created: a directory holding segments, a log past the first one or `hist/`
  without a `MANIFEST` is refused, never opened as a new store (which would delete those files).
- A torn tail record (the last one, cut by a crash) ends the replay and is cut. A bad record with a valid later record
  behind it is corruption: the store refuses to open and the log is left untouched (moved aside, the store opens on
  its manifest without the commits after the bad record).
- A flush creates the next log, syncs the directory, then installs the manifest that names it: a failure before the
  manifest leaves the store writing to the log the current manifest replays.
- A segment chunk is checked when first read; `ogd verify-glacier <store>` checks every chunk of every live segment
  (the store must not be open).

## 5. History

Off by default. `OGD_HISTORY=on` (or `GlacierBackend::record_history`) turns it on for good: it then stays on
across restarts, until `hist/` is removed. It serves `on c as of …` and `on c history` (`storage::history`).

```text
<store>/hist/
  BASE                       generation at which recording started (its presence turns recording on)
  JOURNAL                    instead of BASE, a backup journal: the generation after which commits are kept
  TIMELINE                   time and author of each commit, appended without sync
  b-<gen>-<n>.g6 [.dead]     baseline: the live segments at BASE (hard links) and their deleted rows
  w-<base>.log               a retired log, holding the commits after generation <base>
  s-<gen>-<n>.g6             the rows put by commit <gen> written straight to segments, or imported (hard links)
  d-<gen>.log                the deletes of such a commit (log format)
```

- Nothing is copied: every version is already written once, to the log (small commits) or to a segment (large
  commits, imports). Recording keeps those files: a flush renames the log it retires into `hist/` instead of
  deleting it; a large commit or an import hard links its segments there. With history on, a large commit
  writes its puts to their own segments rather than merging them with the delta in the same flush.
- `OGD_HISTORY=journal` (`GlacierBackend::record_journal`) keeps a backup journal instead: the same files without
  baseline nor timeline, enough for the commits after its generation, which is what a backup diff reads. A full
  backup releases the files holding only commits up to its generation and moves `JOURNAL` there; `as of` and
  `history` stay unavailable. A store keeping a journal refuses `OGD_HISTORY=on` until `hist/` is removed.
- Turning recording on writes the delta to segments, links the live segments as the baseline (with their
  deleted rows), then writes `BASE`. History starts there: earlier states are an error.
- Write path: one unsynced `TIMELINE` append per commit (time, author from the committing thread). A crash may
  lose the last ones: those commits then have no time or author. Read path of current queries: unchanged.
- Recovery: a log left behind by a crash between manifest and rename is archived at open; `s-`/`d-` files of a
  commit whose manifest was never installed (generation above the recovered one) are removed.
- Disk: everything written since `BASE` stays, in log form for small commits.
- Reads: the history source lists `hist/` and the live log under the writer lock, then reads them in generation
  order; segments answer document lookups by identifier, logs are replayed.

## 6. Migrating from Glacier 5

Glacier 5, the record store of ogd 1.0.3 and earlier (one `.glacier` file with `.checkpoint` and `.primary.*` sidecars),
is no longer read. Move its data with a standard backup, which keeps identifiers and versions:

1. With the old `ogd` on the Glacier 5 store: `.backup.create {"name": "migration"}` (written under `OGD_BACKUP_PATH`).
2. With the new `ogd` on an empty directory (`OGD_STORAGE=glacier`, `OGD_STORAGE_PATH=<new directory>`, same
   `OGD_BACKUP_PATH`): `.backup.restore {"name": "migration"}`.

`.backup.inspect {"name": "migration"}` shows the collections and document counts the backup holds.
