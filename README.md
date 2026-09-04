<p align="center">
  <img src="github-static/logo.png" alt="openglacier" width="280">
</p>

<h1 align="center">openglacier Core</h1>

<p align="center">
  <strong>An event-first, schema-less document database engine and capability runtime written in Rust.</strong>
</p>

<p align="center">

![Version](https://img.shields.io/badge/version-0.70.4-blue)
[![Build](https://github.com/openglacier/core/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/openglacier/core/actions/workflows/build.yml)
[![Tests](https://github.com/openglacier/core/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/openglacier/core/actions/workflows/tests.yml)
[![Clippy](https://github.com/openglacier/core/actions/workflows/clippy.yml/badge.svg?branch=main)](https://github.com/openglacier/core/actions/workflows/clippy.yml)
![Architectures](https://img.shields.io/badge/release_targets-19-informational)

</p>

> **Yes — openglacier Core is a database engine.**
>
> It includes its own query language, planner, execution engine, transactional storage abstraction and native persistent storage backend.
>
> It does NOT sit on top of another database.

openglacier Core provides three closely related pieces:

- **`og-core`** — the database kernel and execution engine;
- **`ogd`** — a long-lived daemon exposing database and service capabilities;
- **`ogcli`** — a thin interactive and command-line client.

The native persistent storage engine is called **Glacier**.

> *Small core, long life.*

---

# Quick start

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
Logical Plan
  │
  ▼
Physical Plan
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
- logical planning;
- physical planning;
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
Glacier record
      │
      ▼
read required fields only
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

can read the embedding projection for candidate records and load complete documents only for the final winners.

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
- release reclaimable cache memory;
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

Glacier is an append-only, record-backed storage layout.

Its main file starts with a fixed superblock followed by self-contained segment frames:

```text
+--------------------+--------------------+--------------------+-----+
| Superblock         | Segment frame      | Segment frame      | ... |
+--------------------+--------------------+--------------------+-----+
```

Each segment contains:

```text
segment header
directory
metadata delta
records
```

Writes append new segment data rather than mutating existing records in place.

---

## Physical document layout

Stored documents use a field directory.

Conceptually:

```text
Physical document
      │
      ├── header
      │
      ├── field directory
      │      ├── name
      │      ├── kind
      │      ├── capabilities
      │      ├── offset
      │      └── length
      │
      └── encoded field payloads
```

A reader can therefore locate and decode selected fields without necessarily decoding the whole document.

This is the basis for projected execution.

---

## Glacier sidecars

Glacier uses sidecar files to keep startup and primary lookups efficient.

These include:

```text
.checkpoint
.primary.<hash>
```

The checkpoint stores a snapshot of recoverable state.

The compact primary index stores ordered record pointers used for document lookup.

On startup, Glacier can restore from a checkpoint and replay only subsequent segment frames.

---

## mmap-backed reads

On supported 64-bit Unix targets, Glacier can use memory-mapped access for eligible read paths.

Other targets retain a buffered fallback.

The storage implementation also integrates mmap behavior with the process memory governor so mapped file pages and reclaimable application memory can be treated differently.

---

# Transactions and visibility

Storage writes are transactional.

A transaction accumulates mutations and commits them as one storage generation.

Glacier tracks generation and version information for document visibility.

Its append-only representation allows readers to resolve the visible version of a document without modifying previous committed records.

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
.backup.create {}
```

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
core.operations
node.status

query.execute
collections.list
storage.stats

file.read
file.write

events.subscribe

data.worker.run
```

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
.core.operations
```

Payloads are JSON:

```text
.collections.list {"stats":true}
```

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
| `OGD_HEARTBEAT_ENABLED` | Enable heartbeat |
| `OGD_HEARTBEAT_INTERVAL_MS` | Heartbeat interval |
| `OGD_DEBUG_QUERY` | Query debugging |
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
- normalization/lowering;
- expression semantics;
- logical plans;
- physical plans;
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