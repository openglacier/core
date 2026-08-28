# og-core Query Language Evolution Guide

## Purpose

This document defines the long-term direction of the og-core query language.

It is not a parser specification nor a user tutorial.
Its purpose is to establish the functional contract of the language before the grammar is finalized.

---

# Vision

The og-core query language is a **pipeline-oriented**, **declarative** language dedicated to document processing.

It is intentionally **not SQL** and **not MongoDB syntax**.

Instead, it borrows proven concepts while keeping a coherent identity centered on document transformation.

---

# Core Principles

## Pipeline First

A query is a sequence of stages.

Each stage receives the output of the previous stage.

```text
Source
 │
 ▼
Stage 1
 │
 ▼
Stage 2
 │
 ▼
Stage 3
 │
 ▼
Result
```

Execution order is explicit.

---

## Declarative

Queries describe *what* should happen, never *how* to execute it.

Planning and optimization remain internal engine responsibilities.

---

## AST First

The Abstract Syntax Tree is the internal contract.

Multiple syntaxes may target the same AST:

- Native pipeline syntax
- Compact API syntax
- SQL-compatible dialect (optional)
- Future visual builders

Changing syntax must not require changes to the execution engine.

---

## Strong Typing

The language manipulates native document values:

- Boolean
- Integer
- Floating point
- String
- Date / Time
- Array
- Object
- Null

Implicit conversions should remain minimal and predictable.

---

## Extensibility

New stages or functions should integrate without redesigning the grammar.

The language should grow by composition rather than special cases.

---

# Query Pipeline

A typical query is composed of independent stages.

Example:

```text
from orders
| where status == "paid"
| derive net_total = total - discount
| sort net_total desc
| limit 20
```
```text
on users as u
| join workspace as w
    | where u._id in w.share
    | into public
| end
```
```text
on data | group Article_Code | sort count desc
on data | group Article_Code, CAFacture as CA | sort CA desc
on data | group Receptionnaire_Code as Client, Article_Code as Produit, CAFacture as CA | sort CA desc
on data | group Receptionnaire_Code as Client, CAFacture as CA | sort CA desc
on data | group Article_Code | select Article_Code, count | limit 10
```

```text
on data
| group
    | by Article_Code as Produit
    | sum CAFacture as Total_CA
| end
| sort Total_CA desc
```

For compact `group`, one item is a grouping key. With two or more items, every item except the last is a grouping dimension and the last item is summed. Therefore `group x, y, z` groups by `(x, y)` and sums `z`. `as` can rename every dimension and the summed value. Use the compound `group | by ... | sum ...` form when several aggregate measures are required.

Every stage has a single responsibility.

---

# Stages

| Stage | Purpose | Code | Tests | Comment |
|--------|---------|--------|-------|---------|
| on | Select source collection | :white_check_mark: | :white_check_mark: | Alias : from |
| where | Filter documents | :white_check_mark: | :white_check_mark: | |
| derive | Compute new fields | :white_check_mark: | :white_check_mark: | |
| join | Load related documents | :white_check_mark: | :white_check_mark: | Alias : lookup |
| unwind | Expand arrays | :white_check_mark: | | |
| group | Build groups | :white_check_mark: | | |
| set | Update documents or chunks | :white_check_mark: | | |
| insert | Create one or more documents | :white_check_mark: | | |
| load | Load documents or chunks | :white_check_mark: | | working but slow |
| pivot | Pivot data | :white_check_mark: | | Expect rows, columns, values and aggregate function |
| aggregate | Compute aggregates | not working | | |
| select | Project fields | :white_check_mark: | | |
| rename | Rename fields | :white_check_mark: | | |
| drop | Remove fields | :white_check_mark: | | |
| distinct | Remove duplicates | :white_check_mark: | | |
| sort | Order documents | :white_check_mark: | | |
| skip | Skip documents | :white_check_mark: | | Alias : offset |
| limit | Limit result size | :white_check_mark: | | |
| first | Return first document | :white_check_mark: | | |
| single | Return exactly one document | :white_check_mark: | | |
| count | Count results | :white_check_mark: | | |
| .collections | Show a list of collections | :white_check_mark: | | Option : stats for more infos |
| .backup | Create backup | :white_check_mark: | | Use inspect to read backup metadata |
| .restore | Restore backup | :white_check_mark: | | Use --replace to drop and restore|
| .storage | Show a list of storage statistics | :white_check_mark: | | Option : stats for more details |

The final vocabulary should remain intentionally compact.

---

# Expressions

Expressions should be reusable across all stages.

Supported concepts include:

- arithmetic
- comparisons
- logical operators
- function calls
- field access
- literals
- parameters

Expressions should never depend on the stage where they appear.

---

# Functions

Functions are grouped by domain.

Examples:

- String
- Numeric
- Date
- Array
- Object
- Aggregate

Adding a new function should not require grammar changes.

---

# Parameters

Queries should support external parameters.

Example:

```text
where status == $status
```

Parameter binding belongs to the execution context rather than the parser.

---

# Future Compatibility

The language should evolve while preserving AST compatibility whenever possible.

Parser changes are acceptable.

AST breaking changes should remain exceptional.

---

# Non Goals

Version 1 intentionally excludes:

- recursive queries
- scripting
- user-defined functions
- window functions
- distributed transactions
- full-text search
- geospatial operators

These features may be added later without changing the core architecture.

---

# Design Philosophy

The language should remain:

- small
- readable
- deterministic
- predictable
- strongly typed
- storage independent

The objective is not to compete with SQL.

The objective is to provide a clean and consistent document query language that naturally fits the architecture of og-core.


Execution rule: a terminal `group ... | limit N` is bounded by the observable
result set. Since group rows are emitted in group-key order, the engine keeps
only the `N` smallest live group keys and their exact aggregate states. Keys
outside that frontier are not spilled. This rule applies equally to one or
several grouping dimensions and to aliased keys; adding a dimension must not
turn a bounded query into unbounded temporary-disk growth.

The bounded policy is applied inside the standard incremental projected-value
group consumer, not as a second execution engine. Compatible collection scans
therefore decode only fields required by `group`, aggregate borrowed scalar
values in one pass, and never perform a failed full-group probe followed by a
second document scan. Unsupported transformed prefixes fall directly to the
exact bounded document path, also without a preliminary rescan.
