<p align="center">
  <img src="github-static/logo.png" alt="openglacier" width="280">
</p>

<h1 align="center">openglacier Core</h1>

<p align="center">
  <strong>An event-first, schema-less document database engine and capability runtime written in Rust.</strong>
</p>

<p align="center">

![Version](https://img.shields.io/badge/version-2.0.0-blue)
[![Build](https://github.com/openglacier/core/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/openglacier/core/actions/workflows/build.yml)
[![Tests](https://github.com/openglacier/core/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/openglacier/core/actions/workflows/tests.yml)
[![Clippy](https://github.com/openglacier/core/actions/workflows/clippy.yml/badge.svg?branch=main)](https://github.com/openglacier/core/actions/workflows/clippy.yml)
[![Audit](https://github.com/openglacier/core/actions/workflows/audit.yml/badge.svg?branch=main)](https://github.com/openglacier/core/actions/workflows/audit.yml)
![Architectures](https://img.shields.io/badge/release_targets-19-informational)

</p>

> **True — openglacier Core is a database engine.**
>
> It includes its own query language, planner, execution engine, transactional storage abstraction and native persistent storage backend.
>
> It does NOT sit on top of another database.

openglacier Core provides three closely related pieces:

- **`og-core`** — the database kernel and execution engine;
- **`ogd`** — a long-lived daemon exposing database and service capabilities;
- **`ogcli`** — a thin interactive and command-line client.

The native persistent storage engine is called **Glacier**.

## 🔥 antaa palaa — First stable release
>
> **openglacier Core v1.0 is here, code name `antaa palaa`.**
>
> **`antaa palaa` means the core architecture is now stable enough to build on.**
>
> This milestone comes after an extended period of real-world use and hostile testing: continuous fuzzing, sustained read/write workloads, failure injection, storage corruption and I/O failure testing, crash recovery, backup and restore validation, and a large integration test suite.
>
> It has also been exercised extensively across architectures and generations of hardware — from **Raspberry Pi 3 and 4 to the Odroid XU4, a Fairphone 2, and even my beloved Intel Core m7 (I know).**
>
> **Resources are valuable.** openglacier is deliberately built to remain useful on modest and older hardware — keeping the core portable, predictable and economical in its use of CPU, memory, storage and power.
>
> v1.0 is not a claim that the project is finished. It is a commitment that its foundations are stable enough to be used, relied upon, and built upon.
>
> Start exploring with the [Quick Start](#quick-start) below.


> *Small core, long life.*

---

# Quick start

## Method 1 : Get the docker slim image :
```bash
docker pull openglacier/slim
```

## Method 2 : using install.sh
```bash
chmod a+x install.sh && ./install.sh
```

## Method 3 : From source
Clone the repository and start `ogd`:

```bash
cargo run --release --bin ogd
```

By default:

```text
address:      127.0.0.1:7878
storage:      memory
authorization: permissive
capabilities: auth,database,files,events
```

In another terminal:

```bash
cargo run --release --bin ogcli
```

You should get:

```text
Connected to 127.0.0.1:7878. Type help for commands.
openglacier>
```

Insert some documents:

```text
from products
| insert {
    _id: "keyboard",
    name: "Keyboard",
    category: "hardware",
    price: 129
}
```

```text
from products
| insert {
    _id: "mouse",
    name: "Mouse",
    category: "hardware",
    price: 49
}
```

```text
from products
| insert {
    _id: "desk",
    name: "Desk",
    category: "furniture",
    price: 399
}
```

Query them:

```text
from products
| where category == "hardware"
| sort price desc
| select name, price
```

Or execute a query directly:

```bash
cargo run --release --bin ogcli -- \
  'from products | where price > 100 | sort price desc'
```

No schema declaration.

No SQL server.

No external database.

---

# Database engine

At its foundation, openglacier Core is a pipeline-oriented document database.

A textual query is transformed through a conventional compiler-style pipeline:

```text
Query
  │
  ▼
Lexer
  │
  ▼
Parser
  │
  ▼
AST
  │
  ▼
Normalizer
  │
  ▼
Plan
  │
  ▼
Executor
  │
  ▼
Storage
```

The query engine remains independent from transport protocols, client applications and concrete storage implementations.

Its responsibilities include:

- parsing;
- expression evaluation;
- planning;
- query execution;
- filtering;
- projection;
- transformations;
- joins;
- grouping;
- sorting;
- bounded Top-N execution;
- vector distance computation;
- transactional writes;
- memory governance;
- spill-to-disk execution;
- storage access.

The AST is the internal language contract.

The syntax is only one way to target it.

---

# Query language

openglacier uses its own declarative, pipeline-oriented query language.

It is intentionally:

- not SQL;
- not MongoDB syntax;
- not a scripting language.

A query describes a sequence of transformations.

```text
from orders
| where status == "paid"
| derive net_total = total - discount
| sort net_total desc
| limit 20
```

Each stage has one responsibility.

Execution order is explicit.

Execution strategy is not.

The planner remains responsible for deciding how a pipeline should actually run.

---

## Sources

A query starts from a collection:

```text
from customers
```

or:

```text
on customers
```

`from` and `on` are aliases.

A source can also be read at a past point, or as the list of its stored versions (with `OGD_HISTORY=on`):

```text
on customers as of "2026-09-01T00:00:00Z"
on customers as of 1842
on customers history | where _op == "delete"
```

See [`src/query/README.md` §5.1.1](src/query/README.md) for the fields of history rows (`_doc_id`, `_version`,
`_op`, `_generation`, `_at`, `_by`) and the rules.

A source can also be a view, a named query saved in the Place with `define view` (see
[`src/query/README.md` §5.7](src/query/README.md)):

```text
define view sales.paid | exec | on orders | where status == "paid" and amount >= $min | end | end
let $min = 100 | on view sales.paid | sort amount desc | limit 10
```

Or literal rows, read like a collection:

```text
on orders | lookup values [{code: "FR", rate: 0.2}, {code: "DE", rate: 0.19}] as v on code == country into vat
```

---

## Filtering

```text
from customers
| where country == "FR" and active == true
```

Expressions can combine:

- comparisons;
- arithmetic;
- boolean operators;
- field access;
- literals;
- functions;
- parameters.

---

## Projection

```text
from customers
| select name, email, country
```

Fields can also be removed:

```text
from customers
| drop internal_notes
```

or renamed:

```text
from customers
| rename customer_name as name
```

---

## Derived values

```text
from orders
| derive net_total = total - discount
| where net_total > 100
```

---

## Sorting and limits

```text
from products
| sort price desc
| limit 20
```

The engine can use bounded execution strategies for eligible pipelines instead of materializing every complete document before selecting the final results.

---

## Grouping

Compact grouping:

```text
from sales
| group Article_Code
| sort count desc
```

Grouping with one dimension and one summed value:

```text
from sales
| group Article_Code, CAFacture as CA
| sort CA desc
```

Multiple dimensions:

```text
from sales
| group Receptionnaire_Code as Client, Article_Code as Produit, CAFacture as CA
| sort CA desc
```

Compound grouping is available when more explicit aggregation is required:

```text
from sales
| group
    | by Article_Code as Produit
    | sum CAFacture as Total_CA
| end
| sort Total_CA desc
```

---

## Joins

Related documents can be brought into a pipeline:

```text
on users as u
| join workspace as w
    | where u._id in w.share
    | into public
| end
```

`lookup` is an alias of `join`.

---

## `unwind`

Arrays can be expanded into individual pipeline rows:

```text
from orders
| unwind items
```

---

## `root`

`root` replaces the current document with an object contained in one of its fields.

```text
from events
| root payload
```

Given:

```json
{
  "kind": "user",
  "payload": {
    "name": "Alice",
    "age": 42
  }
}
```

the next stage receives:

```json
{
  "name": "Alice",
  "age": 42
}
```

The original outer document is no longer the pipeline root.

This is particularly useful when processing envelopes, imported records or event payloads.

---

## `sample`

`sample` returns a random set of distinct documents.

```text
from products
| sample 10
```

The result contains at most the requested number of documents.

```text
from products
| sample 0
```

returns an empty result.

`sample` is a set-level operation.

---

## `near`

`near` computes vector proximity using cosine distance.

```text
from memories
| near embedding, [1.0, 0.0]
| sort _distance asc
| limit 5
```

For every candidate document, `near` adds:

```text
_distance
```

Smaller values are nearer to the requested vector.

For example:

```json
{
  "label": "same",
  "embedding": [1.0, 0.0],
  "_distance": 0.0
}
```

`near` is deliberately part of the normal query pipeline.

That means it composes naturally with other stages:

```text
from knowledge
| where type == "documentation"
| near embedding, [0.12, -0.42, 0.91]
| sort _distance asc
| limit 8
| select title, content, _distance
```

The current implementation performs exact vector distance computation over candidate documents.

It is not an approximate nearest-neighbour index such as HNSW.

---

## `distinct`

```text
from customers
| distinct country
```

---

## Pagination

```text
from products
| sort name asc
| skip 100
| limit 50
```

`offset` is an alias of `skip`.

---

## Single-result stages

```text
from products
| first
```

```text
from products
| single
```

---

## Counting

```text
from products
| where active == true
| count
```

---

## Writes

Documents can be created:

```text
from users
| insert {
    _id: "u1",
    name: "John",
    tags: ["rust", "database"]
}
```

Existing data can be changed using `set`.

Documents can also be deleted through the query pipeline.

---

## Current stage vocabulary

The current language includes:

```text
on / from
where
near
derive
join / lookup
unwind
root
group
set
insert
load
pivot
select
rename
drop
distinct
sort
skip / offset
limit
first
sample
single
count
```

The vocabulary is intentionally compact.

New functionality should preferably emerge through composition rather than by multiplying special-purpose APIs.

The detailed language direction lives in:

```text
src/query/README.md
```

---

# Query execution

The execution engine distinguishes between operators that can process documents incrementally and operators that require set-level state.

Conceptually:

```text
streaming operators
    │
    ├── where
    ├── select
    ├── derive
    └── ...
```

and:

```text
set-level operators
    │
    ├── sort
    ├── sample
    ├── grouping
    └── ...
```

The physical planner carries execution properties describing how stages affect:

- cardinality;
- order;
- document shape;
- scope;
- memory behavior;
- materialization;
- projected access.

These properties allow storage-specific execution paths without changing the query language.

---

# Projected execution

Glacier can execute eligible pipelines using only the fields required by the current operation.

Conceptually:

```text
Glacier column segments
      │
      ▼
read required columns only
      │
      ▼
where / sort / distinct / near
      │
      ▼
determine winning documents
      │
      ▼
hydrate complete documents only when required
```

This avoids decoding and allocating data that the query does not need.

For example:

```text
from memories
| near embedding, [...]
| sort _distance asc
| limit 5
```

can read the embedding column for candidate rows and load complete documents only for the final winners.

Projected access is an optimization.

It does not change query semantics.

---

# Memory governance

openglacier Core is designed to run under explicit memory constraints.

The engine contains a shared memory governor used by query execution and internal caches.

A process-wide limit can be configured with:

```bash
OGD_MEMORY_LIMIT=1GiB
```

Memory-sensitive execution paths can:

- reserve memory explicitly;
- operate within bounded buffers;
- spill intermediate execution state when required.

The goal is predictable failure and bounded resource usage rather than uncontrolled allocation.

---

# Storage abstraction

The query engine does not depend directly on Glacier.

Storage is accessed through the Core storage contracts.

The crate currently ships with two storage backends:

```text
MemoryStorage
GlacierStorage
```

Both are consumed through the same engine abstraction.

---

# Memory storage

Memory storage is the default backend.

Start it with:

```bash
OGD_STORAGE=memory \
cargo run --release --bin ogd
```

or simply:

```bash
cargo run --release --bin ogd
```

It is useful for:

- development;
- tests;
- temporary datasets;
- experiments;
- benchmarks.

Data disappears with the process.

---

# Glacier storage

For persistent workloads, use the native **Glacier** backend:

```bash
OGD_STORAGE=glacier \
OGD_STORAGE_PATH=./data/ogd.glacier \
cargo run --release --bin ogd
```

A Glacier store is a directory:

```text
<store>/
  MANIFEST              live segments, deleted rows and field facts at one generation
  <number>.g6           immutable column segments, one collection each (`.seg` in older stores)
  wal-<number>.log      commits made after the manifest generation
  LOCK                  held by the one process that has the store open
```

- **Segments** hold rows sorted by identifier, in row groups of up to 65 536 rows, one chunk per top-level field.
  Each column picks its own encoding (constant, frame of reference with bit packing, dictionary, scaled decimal…)
  and carries per-chunk statistics.
- **Writes**: small commits are appended to the log and kept in an in-memory delta; large commits go straight to
  segments. The delta is flushed to segments, and small or mostly deleted segments are compacted, as it grows.
- **Reads** work on an immutable snapshot. Analytical scans hand whole blocks of columns to the batch engine,
  and skip row groups that the statistics prove the leading filters drop.
- **Recovery**: open replays the log after the manifest; a torn last record is cut, corruption is refused.
  `ogd verify-glacier <store>` checks every segment chunk.

The complete format is described in [`src/storage/backend/GLACIER.md`](src/storage/backend/GLACIER.md).

## Migrating from Glacier 5

ogd 1.0.3 and earlier stored Glacier data in a single `.glacier` file (format 5). That format is no longer read:
create a backup with the old `ogd` (`.backup.create {"name": "migration"}`), then restore it with the new one on an
empty store directory (`.backup.restore {"name": "migration"}`). Identifiers and versions are kept.

---

# Transactions and visibility

Storage writes are transactional.

A transaction accumulates mutations and commits them as one storage generation.

Glacier tracks generation and version information for document visibility.

Segments are immutable: a reader keeps the snapshot it started with while later commits install new manifests.

---

# Backup

Database backup operations are exposed through the daemon:

```text
backup.create
backup.inspect
backup.restore
```

For diagnostics or automation they can be called directly through `ogcli`.

Example:

```text
.backup.create {"name": "monday"}
.backup.create {"name": "tuesday", "parent": "monday"}
.backup.restore {"name": "tuesday", "replace": true}
```

A backup (`OGB2`, `src/backup.rs`) is its own format, read by any storage backend: each collection in blocks of
65,536 documents laid out by field (identifiers and integers as deltas, strings through a dictionary of the block),
compressed with zstd; values keep their exact types. On the TAURUS 360 demo (6.2 M documents, a 121 MB glacier
store) it holds 73.6 MB, written in 14 s and restored in 22 s. `OGB1` files are still read.

With `parent`, `backup.create` writes a diff: the documents changed since the parent backup and the ones removed.
It needs a store keeping history (`OGD_HISTORY=on`) or a backup journal (`OGD_HISTORY=journal`, glacier: the
commits since the last full backup, released by the next one). Restoring a diff restores its chain, the full backup
then each diff, found by id in the backup directory and checked before the store is touched. A restored store gets
a new lineage: its next backup is a full one.

With `"history": true` (a store with `OGD_HISTORY=on`), the full backup also holds the recorded history: the state
when history started, every write and delete with its generation and version, and the time and author of each
commit. `backup.restore` on a running store restores the state; `ogd restore-backup <backup.ogb> <new-store>`
restores the history into a new store directory, with the original generations, so `as of` and `history` answer as
before over the whole period (demo with history: 6.45 M events, 150 MB written in 33 s, restored in 36 s).

Use:

```text
.core.operations
```

to inspect the exact operation contracts exposed by the running daemon.

---

# `ogd`

`ogd` is the long-lived daemon built around `og-core`.

It provides:

- the database engine;
- transport;
- authentication;
- operation routing;
- service capabilities;
- events;
- Files;
- node identity;
- Hub connectivity;
- worker integration.

By default it listens on:

```text
127.0.0.1:7878
```

The default storage backend is:

```text
memory
```

The default service capabilities are:

```text
auth,database,files,events
```

---

# Capabilities

Operations are grouped into service capabilities.

The current built-in capabilities are:

| Capability | Purpose |
|---|---|
| `auth` | Authentication, identities, enrollment and devices |
| `database` | Queries, collections, database metadata and database control plane |
| `files` | File storage, metadata, versions and synchronization |
| `events` | Event subscriptions, heartbeat and delivery |
| `data.import` | Isolated data-processing worker execution |

Capabilities are runtime service boundaries.

They are distinct from document/value capabilities used internally by the query and storage layers.

---

## Configure node capabilities

The enabled service capabilities are configured with:

```bash
OGD_NODE_CAPABILITIES=auth,database,files,events
```

For example:

```bash
OGD_NODE_CAPABILITIES=database \
cargo run --release --bin ogd
```

or:

```bash
OGD_NODE_CAPABILITIES=database,files,data.import \
cargo run --release --bin ogd
```

The operation router only exposes operations supported by the selected capability set.

The same capability list is also included in node status and node announcements.

---

# Database capability

The `database` capability includes operations around:

- query execution;
- query context resolution;
- collections;
- storage statistics;
- backups;
- Places;
- Apps;
- App instances;
- permissions;
- sharing;
- data analysis;
- data import control plane;
- data mappings.

Examples include:

```text
query.execute
query.context.resolve

collections.list
storage.stats

backup.create
backup.inspect
backup.restore

place.*
app.*
app.instance.*

permission.*
sharing.*

data.analyze
data.import
data.mapping.*
```

The database capability is larger than the storage backend itself.

Glacier is storage.

`database` is a service capability.

They are not the same concept.

---

# Files capability

The `files` capability provides operations including:

```text
file.capabilities

file.list
file.stat

file.mkdir
file.move
file.copy

file.read
file.write

file.delete
file.delete.permanent

file.trash.list
file.trash.empty
file.restore

file.versions
file.version.read
file.version.restore
file.version.delete

file.sync.config.get
file.sync.config.set
file.sync.folders
file.sync.selection.set
file.sync.selection.remove
file.sync.run
file.sync.status
```

Files participate in the openglacier execution and authorization model while remaining a separate capability from database queries.

---

# Events capability

The `events` capability provides the event delivery layer used by long-lived nodes.

It includes:

```text
events.subscribe
```

and the runtime mechanisms surrounding:

- heartbeat;
- event outbox;
- retry;
- event deduplication;
- upstream relay;
- local subscribers;
- file-sync wakeups.

The outbox is event-driven rather than relying on permanent short-interval polling.

Delivered events are identified by `event_id` so retries can be deduplicated.

---

# Data import capability

openglacier separates the data import control plane from worker execution.

Database-side operations include:

```text
data.analyze
data.import
data.mapping.*
```

Actual worker execution is exposed separately as:

```text
data.worker.run
```

under the:

```text
data.import
```

service capability.

Conceptually:

```text
database
   │
   │ data.worker.run
   ▼
data.import provider
   │
   ▼
Python worker
   │
   ▼
isolated venv
```

This keeps Python-specific transformation logic outside the Rust database kernel.

## App table semantics

When an App is saved (and for every built-in App at start), og-core checks the semantic keys of `model.tables.<table>.fields`
and of the `fields` of each `queryViews` entry: unique names; `role` (`measure`, `axis`, `attribute`); `aggregate` (`sum`,
`last`, `avg`, `min`, `max`, `count`); `good` (`up`, `down`); a ratio's `num` and `den` name declared fields; `vocabulary`
maps languages to lists of words; `dimension`, `provides` and `values.source` / `values.code` are identifiers; `measures`
is `{"from": "<view>"}`. The format itself is described in `og-hub/REPORT_RUNTIME.md` (Table semantics).

## Place schema

`place.schema {"placeId"}` answers the declared schema of a Place, assembled from the App instances the caller sees (same
access as `app.instance.list`; an Authority operation, like it):

- `instances`: each visible instance (`instanceId`, `appId`, name, version) and `shows`, the fields its blocks show, tile
  then full view: what the App deems important;
- `tables`: each table of each instance (its rows are scoped to it) with its collection, `provides`, whether it imports,
  `analytics` and its fields with their semantics;
- `views`: the query views of the installed Apps, once per App, without their query: name, description, `fields`,
  `measures`;
- `referentials`: the tables that provide a referential of the Place (`provides`);
- `collections`: each collection with the tables over it, its `owner` (the table that imports into it, else the first) and
  their fields merged, the owner's first.

The observed layer (the collections that hold rows of the Place) stays `collections.list`, on the node that holds the data.

## Place search

`place.search {"placeId", "text", "scope"?, "limit"?}` (feature `search`, in the default build; `src/search/`) finds what
a Place holds by its words: the distinct text values of its collections (with their row count), its files and folders
(with their path), the text of its files in passages of about 1,000 characters, its App instances and tables. Plain text
formats (txt, md, csv, json, html, xml …) are read with `search`; PDF, spreadsheets (xlsx, xls, ods) and office documents
(docx, pptx, odt, odp) with `search-documents` (default and `full`; `pdf-extract`, `calamine`, `zip`). A file over 32 MB, or
one its reader fails on, is found by its name only. A word of the text finds the words equal to it, starting with it, or, when
no entry holds it as it is, one typo away; accents, case, plurals and acronyms (« T.V.A. » → `tva`) are folded. Each hit
gives `kind` (`value`, `file`, `folder`, `passage`, `app`, `table`), `text` (for a passage, the words around what was
found), `count`, `score` and what it refers to (`collection`, `field`, `label`; `fileId`, `path`, `passage`; `instanceId`). `scope` keeps the hits under every key it names: `app:<instanceId>`,
`collection:<name>`, `field:<collection>.<field>`, `folder:<fileId>` (what the folder holds, at any depth).

The index is read within the caller's access: the hits are those of the App instances it sees, and values are read under
its row filters (`place.access.set` `filters`). It is made of parts, one per source (each collection, the file list, the
text of each file, the catalog), kept until the change token of their collection moves (`Engine::change_token`) or, for a
file's text, its size, etag or time change: a write reads its part again, not the whole Place. A search reads file texts
for 3 s at most; the others wait for the next search (`pending`). TAURUS 360 demo (180,679 entries): first search 1.4 s,
then 3 ms in ogd; a write to `creances` reads that part again in 76 ms. 647 files (6.9 MB of Markdown, a PDF, two Excel
workbooks; 6,132 passages): their texts read in 443 ms, then 1 to 5 ms per search, a new file read in 6 ms. A new kind of content is one more `Source` in `src/search/sources.rs`.

Ask (`agent.app`) searches the request (without its linking, common and catalog words, nor years) when it routes it to
`absent` or `rechercher`: `search` holds the hits, and `text` lists them (« Le catalogue d'analyse ne couvre pas cette
demande, mais la Place contient : … »). A `rechercher` that finds nothing asked for an analysis (« les clients aux créances
échues de plus de 90 jours »): it is built as an App (`searched: true`).

## Checked imports

`data.import` writes the rows of the file into the App table only once the checks the App declares for that table let it.
The rows first wait in `<collection>.staged`, in the scope of the target App instance, each with `import_id`; each check
is a query that reads them (`$import` names the import, usually through a view) and returns what stops the import:

```json
"model": { "tables": { "ledger": { "collection": "l", "import": {
  "summary": "on view finance.import.summary",
  "checks": [{ "id": "imbalance", "message": "Unbalanced months", "query": "on view finance.import.imbalance" },
             { "id": "rows", "message": "Invalid rows", "query": "on view x.invalid", "blocking": true }],
  "replace": ["scenario", "version", "entity", "date"],
  "after": ["on view finance.import.record | into finance_imports"] } } } }
```

- A check that returns rows stops the import: the answer is `status: "confirm"` with `importId`, the `summary` rows and,
  for each check, its first 100 rows (`more` when there are others). The caller resumes with `importId` and `accept`
  (the ids of the checks it accepts), or drops the import with `importId` and `cancel: true`. The checks run again at
  each resume: a rule added in between counts. A `blocking` check cannot be accepted (`status: "blocked"`).
- `checkOnly: true` stages and checks without writing (`status: "checked"`): the « import? » question.
- Once nothing stops it, the stored rows sharing the `replace` fields with a staged row give way (an entity's months
  imported again), the staged rows are written, then the `after` queries run (in the scope of the target instance) and
  the staged rows go, all in one transaction (each step reads the rows as they were before the import). An import nobody
  resumes goes after a week.
- Checks and summary read as the App reads the Place (scope of the Place, its views and their caches).
- In Gateway fabric mode, the authority Core authorizes the import and answers its destination and checks (`planOnly`:
  `collection`, `mode`, `import`); the Gateway gives them (`plan`) with the rows to the database Core of the Place, which
  stages, checks and writes them under the Gateway's `database` delegation for the target App instance (write role).

---

# Places and execution context

openglacier data operations can execute inside a **Place**.

A Place is an authorization and execution scope.

Some operations can be scoped further to an application instance.

Conceptually:

```text
Place
  │
  ├── Place-wide data
  │
  ├── AppInstance A
  │
  └── AppInstance B
```

The effective scope is applied by the execution layer.

It is not left to arbitrary query text to decide which Place or application instance may be accessed.

This allows the same query engine to execute in different authorized contexts without embedding authorization logic into the query language.

An App can be open to some Place roles only: `access.visibleTo` in its definition, or `config.visibleTo` on one instance
(which wins), lists the roles (`owner`, `resident`, `member`); none listed means everyone who reads the Place. `ogd` leaves
the other instances out of `app.instance.list` and refuses their context (`app.instance_forbidden`) to queries, files and App
operations. The Owner and the App's maintainers always reach it; a public reader without a role does not reach a restricted
App.

A member's access can also restrict the rows it reads: `place.access.set` takes `filters`, `{"entity": ["BU1", "BU2"]}`
(`{}` lifts them). `ogd` then keeps, in every source a query of that member reads in the Place (`lookup` and `union`
included, the `let` queries and the views it reads), only the rows whose field, when present, holds one of the values:
`entity? == null or entity? in ["BU1", "BU2"]`. On a collection whose table, in an App of the Place, declares the field
(`model.tables.*.fields`), the filter is strict: `entity? in ["BU1", "BU2"]`, a row without the field is not read there. A
key `<collection>:<field>` gives a strict filter on that collection only. A materialized view has rows of its own for each
set of filters, computed under them. A write must stay within the filters: a row inserted, copied (`into`) or changed by
`set` holding the field takes one of its values, and on a strict collection must hold it (`set` and `delete` only reach
the rows the member reads). The Owner is never restricted. In Gateway mode, `query.context.resolve` returns the filters
and the Gateway passes them to the database Core with the role.

Profiles of a Place gather these settings under a name, set by its Owner: `place.update` takes `profiles`, the whole
list, each `{"profileId", "name", "apps"?, "filters"?}`; `apps` lists the App instances its members see (left out: all of
them), `filters` is as above. `place.access.set` takes `profileId` (`""` takes it away; left out, the member keeps its
profile); a member has one profile at most, its filters restricted further by those of its own access. A profile given
to a member cannot be removed (`place.profile_in_use`). The profiles are returned to the Owner only, in the Place record.
The Owner previews a profile with `asProfile` in a query context (`query.execute`, `query.context.resolve`): its reads
and App instances are those of the profile.

For the broader openglacier model and project vision, see:

https://github.com/openglacier/openglacier

---

# Authentication and identity

`ogd` supports authenticated identities and devices.

Authorization enforcement is disabled by default for a simple local development setup.

Enable it with:

```bash
OGD_AUTH_REQUIRED=true
```

Node identities can be loaded through:

```text
OGD_NODE_IDENTITY
OGD_NODE_IDENTITY_FILE
OGD_NODE_IDENTITY_PASSWORD
```

Enrollment and classic authentication also have dedicated configuration switches.

### Classic authentication

With `OGD_CLASSIC_AUTH_ENABLED`, an Identity signs in with an identifier and a password (`auth.classic.register`, then
`auth.classic.login`), and a Place is shared with it by that identifier as well as by its Identity ID: `place.access.set`
resolves an `identityId` that names no Identity through the active classic identifiers (case ignored), and
`place.access.list` gives each member its `login`.

`OGD_CLASSIC_AUTH_EMAIL` (default `false`) requires the identifier to be a valid email address when it is registered. With
the `mail` capability on the same node (`mailsend` build, `OGD_MAIL_*`), it also offers a forgotten password reset:

- `auth.classic.reset.request {"identifier", "language"?}` mails a code of 8 characters (`XXXX-XXXX`, about 40 bits) to
  the address, valid 30 minutes; the answer is the same whether the address is registered or not, the mail leaves on its
  own thread, and one mail per address goes out a minute at most;
- `auth.classic.reset.complete {"identifier", "code", "password"}` sets the new password (12 bytes at least); a code
  allows 5 tries, then the attempts of the address slow down like failed sign-ins.

`core.discover` answers `classicAuthEmail` and `passwordReset` (both on) so the Hub asks for an email and shows the
forgotten password link.

For local query-engine development, authentication does not need to be enabled.

---

# Node mode and Hub connectivity

An `ogd` process can run as:

```text
master
```

or:

```text
node
```

configured with:

```bash
OGD_NODE_ROLE=master
```

or:

```bash
OGD_NODE_ROLE=node
```

A node identity allows `ogd` to participate as an openglacier node and announce its available service capabilities.

The distributed fabric is built around operation contracts and explicit capability advertisement.

A node does not need to expose every service that exists in the binary.

---

# Operation model

Wire operations are defined centrally.

Each operation has a canonical definition containing information such as:

- wire name;
- stable operation kind;
- authorization policy;
- execution mode;
- handler domain;
- payload type.

Examples:

```text
ping
core.health
core.discover
core.operations
node.status

query.execute
query.stages
collections.list
storage.stats

file.read
file.write

events.subscribe

data.worker.run
```

`core.discover` is the public client hello: it returns the Core options a client needs before choosing its entry flow (`authRequired`, `enrollmentMode`, `classicAuthEnabled`, `classicAuthEmail`, `passwordReset`, `allowPublic`, capabilities). `core.health` stays the liveness probe.

The operation catalogue is itself discoverable.

Call:

```text
.core.operations
```

from `ogcli` to inspect what the current daemon exposes.

This is preferable to hard-coding assumptions about a running node.

---

# `ogcli`

`ogcli` is intentionally thin.

Its job is to speak the `ogd` protocol and get out of the way.

Start the interactive shell:

```bash
cargo run --release --bin ogcli
```

or, once installed:

```bash
ogcli
```

The prompt is:

```text
openglacier>
```

---

## Execute a query

```bash
ogcli 'from products | where price > 100 | sort price desc'
```

---

## Raw operations

Prefix an operation name with `.`:

```text
.ping
```

```text
.core.health
```

```text
.core.discover
```

```text
.core.operations
```

Payloads are JSON:

```text
.collections.list {"stats":true}
```

---

## Load a CSV or TSV file

```text
import ventes.csv
import "exports/clients 2025.tsv" clients
import codes.csv --sep ; --text
```

The first line names the fields; the collection defaults to the file name (`ventes`), made an identifier.
The separator is a tab for `.tsv`, otherwise the most frequent of `, ; tab |` in the header (`--sep` forces it).
Quoted fields, doubled quotes, line breaks inside quotes, CRLF and a UTF-8 BOM are handled.

Cells are typed: integers (a leading zero such as `07500` stays text, and so does an integer beyond ±2⁵³), floats
(`12,5` as well with a `;` or tab separator), `true`/`false`, empty = `null`, text otherwise. `--text` keeps every cell as text.

Rows are sent as `load | with merge` requests of about 7 MiB, each committed on its own (a failure keeps the requests
already committed); the next request is read while ogd writes the current one. Importing a file twice adds its rows twice.
Measured: 2 M rows (97 MB, 7 columns) in 24.7 s on a local Glacier store, 1.5 s of it in the client.

From the shell: `ogcli import ventes.csv` (progress on stderr, summary on stdout).

---

## Tables

`--table`, or `format table` in the shell (`format pretty|compact|quiet` to go back), draws query results as a table:
one column per top-level field, numbers right-aligned, null blank, objects and arrays as compact JSON, cells cut at 48
characters, the first 1 000 rows, then the row count and the time. Operation results that are a list of objects
(`.collections.list`) are drawn the same way. In the TUI, a table is not wrapped: ←/→ scroll it when the input is empty.

---

## Completion

Tab completes, in the shell and in the TUI: commands and `on`/`from` first, `.operations`, stage names after `|`,
collections after `on`, `from`, `into`, `lookup`, `join`, `union`, fields of the `on` collection (read from its first
200 rows) elsewhere, file paths after `import`. Collections and fields are cached for a few seconds.

---

## Edit and rerun in the TUI

`ogcli --tui` runs each line as a job in its own tab or tile. To work on one query without opening new tabs:

| Key | Effect |
|---|---|
| Ctrl-O, `:edit` | Puts the focused job's line in the input |
| Alt-Enter | Runs the input in the focused job, replacing its output (Enter opens a new job) |
| Ctrl-R, `:rerun` | Runs the focused job's line again, in place |
| ↑ / ↓ | Earlier and later lines, from an empty input; editing the shown line stops browsing |
| Shift-↑ / Shift-↓, PgUp / PgDn | Scroll the focused job's output |

A job replaced while it runs takes a new number; the output of its previous run is dropped, and that request continues
on ogd unless cancelled (Ctrl-X for an LLM run).

---

## Connect to another daemon

`ogcli` can connect to another `ogd` address using its command-line configuration.

Use:

```bash
ogcli --help
```

for the options supported by the current version.

---

## Reconnect

Inside the interactive shell:

```text
reconnect
```

or:

```text
.reconnect
```

---

## Exit

```text
exit
quit
.exit
.quit
```

---

# Configuration

`ogd` is primarily configured through environment variables.

Important variables include:

| Variable | Purpose |
|---|---|
| `OGD_BIND` | Main daemon bind address |
| `OGD_LOCAL_BIND` | Optional local bind address |
| `OGD_STORAGE` | `memory` or `glacier` |
| `OGD_STORAGE_PATH` | Glacier store path |
| `OGD_MEMORY_LIMIT` | Process memory limit |
| `OGD_READ_TIMEOUT_MS` | Read timeout |
| `OGD_WRITE_TIMEOUT_MS` | Write timeout |
| `OGD_NODE_CAPABILITIES` | Enabled service capabilities |
| `OGD_NODE_ROLE` | `master` or `node` |
| `OGD_NODE_IDENTITY` | Node identity |
| `OGD_NODE_IDENTITY_FILE` | Node identity file |
| `OGD_NODE_IDENTITY_PASSWORD` | Password for the node identity file |
| `OGD_FILES_PATH` | Files storage path |
| `OGD_FILES_SYNC_ROOT` | Files synchronization root |
| `OGD_BACKUP_PATH` | Backup directory |
| `OGD_AUTH_REQUIRED` | Enable authorization enforcement |
| `OGD_CLASSIC_AUTH_ENABLED` | Enable classic authentication |
| `OGD_CLASSIC_AUTH_EMAIL` | Classic identifiers must be email addresses (default `false`); with the `mail` capability on the node, enables the forgotten password reset by mail |
| `OGD_ALLOW_PUBLIC` | Let Owners open a Place to the public, read-only or read/write (default `true`); off, `place.create` and `place.public.set` refuse a public access, a public Place can still go back to private, and `core.discover` answers `allowPublic: false` so the Hub hides the option |
| `OGD_HEARTBEAT_ENABLED` | Enable heartbeat |
| `OGD_HEARTBEAT_INTERVAL_MS` | Heartbeat interval |
| `OGD_DEBUG_QUERY` | Query debugging |
| `OGD_QUERY_TRACE` | File receiving one JSON line per `query.execute` (scope, prepared-text hash, durations, result cache) |
| `OGD_RESULT_CACHE` | Keep the rows of repeated Place reads (on by default; `off` for A/B) |
| `OGD_IMPORT_METRICS` | Import worker metrics |

Default values include:

```text
OGD_BIND               127.0.0.1:7878
OGD_STORAGE            memory
OGD_STORAGE_PATH       data/ogd.glacier
OGD_NODE_CAPABILITIES  auth,database,files,events
OGD_NODE_ROLE          master
```

Authorization enforcement defaults to disabled.

Heartbeat defaults to enabled.

---

# Persistent local node example

A simple persistent daemon can be started with:

```bash
OGD_STORAGE=glacier \
OGD_STORAGE_PATH=./data/core.glacier \
OGD_FILES_PATH=./data/files \
OGD_MEMORY_LIMIT=2GiB \
cargo run --release --bin ogd
```

Then:

```bash
cargo run --release --bin ogcli
```

---

# Database-only runtime example

At runtime, service capabilities can already be restricted:

```bash
OGD_STORAGE=glacier \
OGD_NODE_CAPABILITIES=database \
cargo run --release --bin ogd
```

This restricts the operations advertised and routed by the daemon.

It does **not** currently remove unused code or dependencies from the compiled binary.

Build-time capability modularization is a separate concern.

---

# Repository structure

The main source areas are:

```text
src/
├── access/
├── files/
├── model/
├── operation/
├── query/
├── storage/
│   └── backend/
├── backup.rs
├── engine.rs
├── event_engine.rs
├── indexing.rs
├── memory.rs
├── protocol.rs
├── spill.rs
└── bin/
    ├── ogd.rs
    └── ogcli.rs
```

---

## Query engine

```text
src/query/
```

contains:

- lexer;
- parser;
- AST;
- normalization;
- expression semantics;
- plans;
- planner;
- executor;
- runtime operators;
- projected-value execution;
- execution properties.

---

## Storage

```text
src/storage/
```

defines the storage contracts consumed by the engine.

Native implementations include:

```text
MemoryStorage
GlacierStorage
```

The physical Glacier format is documented in:

```text
src/storage/backend/README.md
```

---

## Operations

```text
src/operation/
```

contains the canonical operation catalogue, payload definitions, routing and execution contracts.

Documentation for adding operations is available in:

```text
src/operation/README.md
```

---

# Design principles

openglacier Core deliberately favors a small number of strong concepts.

---

## Database first

The query engine and storage contracts are first-class components.

External services do not define the data model.

---

## Pipeline-oriented

Queries compose independent stages.

Complex behavior should preferably emerge from composition rather than special-case commands.

---

## Declarative

Queries describe what should happen.

Planning and optimization remain engine responsibilities.

---

## AST-first

The AST is the durable contract between syntax and execution.

A future syntax should be able to target the same execution engine without redesigning it.

---

## Strong typing

Native values include:

- Boolean;
- Integer;
- Floating point;
- String;
- Date / Time;
- Array;
- Object;
- Null.

Implicit conversion should remain minimal and predictable.

---

## Storage-independent

The query engine depends on storage contracts.

It should not know whether a document lives in memory, in Glacier or in a future backend.

---

## Bounded

Memory usage should be explicit and governable.

Large operations should not assume unlimited RAM.

---

## Observable

Important execution decisions should be inspectable.

Diagnostics are part of the architecture, not an afterthought.

---

## Headless

Core does not depend on a graphical interface.

It should behave consistently:

- on a developer workstation;
- in a server;
- in a container;
- on a small machine;
- under automated tests.

---

## Modular

Database, files, events, authentication and processing are separate service domains.

New capabilities should integrate without changing the semantics of existing ones.

---

# Non-goals

openglacier Core is not:

- a UI framework;
- a dashboard framework;
- an ORM;
- a REST framework;
- an application server;
- a business application;
- a wrapper around another database.

The query language also intentionally does not currently target:

- recursive queries;
- scripting;
- user-defined functions;
- window functions;
- distributed transactions;
- full-text search;
- geospatial operators.

Those may evolve independently without changing the fundamental architecture.

---

# Build

Debug build:

```bash
cargo build
```

Release build:

```bash
cargo build --release
```

The release profile currently uses:

```text
opt-level = 3
LTO = thin
codegen-units = 1
```

---

# Tests

Run the complete test suite:

```bash
cargo test
```

Individual integration tests can also be run directly.

For example:

```bash
cargo test --test near
```

```bash
cargo test --test root_sample
```

---

# Run

Daemon:

```bash
cargo run --release --bin ogd
```

CLI:

```bash
cargo run --release --bin ogcli
```

---

# Supported release targets

openglacier Core currently publishes or targets 19 architectures/platform combinations.

<details>
<summary><strong>Linux GNU</strong></summary>

- `x86_64-unknown-linux-gnu`
- `x86_64v3-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `armv7-unknown-linux-gnueabihf`
- `arm-unknown-linux-gnueabihf`
- `i686-unknown-linux-gnu`
- `riscv64gc-unknown-linux-gnu`
- `powerpc64le-unknown-linux-gnu`
- `s390x-unknown-linux-gnu`
- `loongarch64-unknown-linux-gnu`

</details>

<details>
<summary><strong>Linux musl</strong></summary>

- `x86_64-unknown-linux-musl`
- `aarch64-unknown-linux-musl`
- `armv7-unknown-linux-musleabihf`
- `i686-unknown-linux-musl`

</details>

<details>
<summary><strong>macOS</strong></summary>

- `x86_64-apple-darwin`
- `aarch64-apple-darwin`

</details>

<details>
<summary><strong>Windows</strong></summary>

- `x86_64-pc-windows-msvc`
- `aarch64-pc-windows-msvc`
- `i686-pc-windows-msvc`

</details>

---

# openglacier

This repository contains the Core database engine and daemon runtime.

The broader openglacier project defines the distributed architecture and higher-level concepts built around Core.

Project repository:

https://github.com/openglacier/openglacier

The distinction is intentional:

```text
openglacier/core
    │
    └── how the engine works

openglacier/openglacier
    │
    └── why the ecosystem exists
```

Core should remain understandable without requiring the reader to learn the entire openglacier ecosystem first.

---

# Final thought

Databases, protocols, applications and AI systems evolve at different speeds.

Core should provide a durable foundation underneath them.

Keep the data model understandable.

Keep execution predictable.

Keep interfaces small.

> *Small core, long life.*

---

# License

MIT