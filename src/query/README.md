# og-core Query Language Evolution Guide

## Purpose

This document defines the long-term direction of the og-core query language.

It is not a parser specification nor a user tutorial.
Its purpose is to establish the functional contract of the language before the grammar is finalized.

> **Update.** The language is now implemented. This file therefore has two parts:
>
> - **Part I: Design and direction.** The original evolution guide: vision, principles and roadmap. Where the implementation differs from the text, the difference is noted inline.
> - **[Part II: Language reference](#part-ii--language-reference).** An exhaustive reference for the language as it runs today in `ogd` 1.0. It covers every stage, operator and literal form, plus execution rules and error behaviour. Every example in Part II was run against a live `ogd`, and the outputs shown are real.

---

# Part I — Design and direction

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

> **Current state:** integers keep their physical kind (signed or unsigned 64-bit), floats are finite IEEE-754 doubles, and a value is never silently coerced between strings and numbers. There is no dedicated Date/Time type yet. Store timestamps as numbers (for example Unix milliseconds) or as ISO-8601 strings, which compare correctly as text. See [Values and types](#3-values-and-types).

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

> **Current state of the `join` example above:** the compound `join … | into … | end` syntax, the `in` operator and correlated predicates (`u.…` for the enriched document, `w.…` for the joined one) are all implemented. The example works as written when `share` is an array. See [`lookup` / `join`](#lookup--join).

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

> **Note:** this compound form is supported: exactly one `by`, then any number of `sum` directives. `group` opens a sub-pipeline when its header is empty, which is the same syntactic property `load` has (see [`SubPipelinePolicy`](#53-compound-stages-and-end)).

For compact `group`, one item is a grouping key. With two or more items, every item except the last is a grouping dimension and the last item is summed. Therefore `group x, y, z` groups by `(x, y)` and sums `z`. `as` can rename every dimension and the summed value. Use the compound `group | by ... | sum ... | avg ... | end` form for several measures or for other aggregates (`avg`, `min`, `max`, `first`, `last`, `count`).

Every stage has a single responsibility.

---

# Stages

| Stage | Purpose | Code | Tests | Comment |
|--------|---------|--------|-------|---------|
| on | Select source collection | :white_check_mark: | :white_check_mark: | Alias : from |
| where | Filter documents | :white_check_mark: | :white_check_mark: | |
| near | Vector nearby lookup | :white_check_mark: | :white_check_mark: | Adds `_distance` (cosine). Combine with `sort _distance \| limit k` |
| derive | Compute new fields | :white_check_mark: | :white_check_mark: | Any expression; assignments chain left to right |
| join | Load related documents | :white_check_mark: | :white_check_mark: | Alias : lookup. Correlated predicates: `o.user == u.name`. One-line equality form: `lookup orders on user == name into orders` |
| unwind | Expand arrays | :white_check_mark: | :white_check_mark: | Streams mid-chain: one row per array item, in order. Options `with index <field>`, `keep empty` |
| root | Change root to specified field | :white_check_mark: | :white_check_mark: | |
| group | Build groups | :white_check_mark: | :white_check_mark: | Compact form, or compound `\| by … \| sum\|avg\|min\|max\|first\|last\|count … \| end` for several measures |
| bucket | Count rows per value range | :white_check_mark: | :white_check_mark: | `bucket age by [0, 18, 65] [default "other"] [as band]` |
| set | Update documents or chunks | :white_check_mark: | :white_check_mark: | Terminal, transactional, full expressions (`age = age + 1`) |
| insert | Create one or more documents | :white_check_mark: | :white_check_mark: | One object, or an array of objects inserted atomically |
| load | Load documents or chunks | :white_check_mark: | | Streaming form `with replace\|update\|merge` + `chunk` |
| into | Insert the result into another collection | :white_check_mark: | :white_check_mark: | Alias : out. Terminal: `on users \| where active \| into archive`. One atomic insert with fresh `_id`s; target must differ from the source and cannot be a system collection. Needs write access on the target and read access on the source |
| pivot | Pivot data | :white_check_mark: | :white_check_mark: | Expect rows, columns, values and aggregate function. Keeps one aggregate cell per row key and column, never the rows |
| fill | Add the rows a grid misses | :white_check_mark: | :white_check_mark: | `fill month from "2026-01" to "2026-12" step "month" by entity with balance = 0` |
| window | Values over ordered partitions | :white_check_mark: | :white_check_mark: | `\| partition by … \| order by … \| lag\|lead\|cumsum\|first\|last\|row_number … \| end`; keeps every row |
| aggregate | Compute aggregates | not working | | Not a stage: use `group`, `count` or `pivot` |
| select | Project fields | :white_check_mark: | | Alias : project. Accepts `expression as alias` |
| rename | Rename fields | :white_check_mark: | | |
| drop | Remove fields | :white_check_mark: | | |
| distinct | Remove duplicates | :white_check_mark: | | |
| sort | Order documents | :white_check_mark: | :white_check_mark: | Keys may be expressions: `sort price * qty desc` |
| skip | Skip documents | :white_check_mark: | | Alias : offset |
| limit | Limit result size | :white_check_mark: | | |
| first | Return first document | :white_check_mark: | :white_check_mark: | Declared row bound of 1: stops the scan, works in `lookup`/`union`, `sort \| first` is a Top-1 |
| sample | Provide a random set of n document(s) | :white_check_mark: | :white_check_mark: | Reservoir sampling: memory bounded by n |
| single | Return exactly one document | :white_check_mark: | :white_check_mark: | Fails with 0 or more than 1 row |
| count | Count results | :white_check_mark: | | `count as <alias>` |
| delete | Delete documents | :white_check_mark: | | Terminal, transactional |
| union | Append another pipeline | :white_check_mark: | | `\| union \| on <collection> … \| end` |
| explain | Describe the plan instead of running the query | :white_check_mark: | :white_check_mark: | Query prefix, not a stage: `explain on users \| where …` |
| let | Declare reusable variables | :white_check_mark: | :white_check_mark: | Header before `on`: `let $min = 18 \| on users \| where age >= $min`. Evaluated once per query |
| define view | Save a named read query in the Place, read by `on view <name>` | :white_check_mark: | | Statement, not a stage: `define view <name> \| description "…" \| [materialized \|] exec \| <query> \| end \| end`; `undefine view <name>`. See [5.7](#57-views-define-view-on-view) |
| .collections | Show a list of collections | :white_check_mark: | | ogcli operation `.collections.list`, option `{"stats": true}` |
| .backup | Create backup | :white_check_mark: | | ogcli operation `.backup.create {"name": …}`; `.backup.inspect` reads metadata |
| .restore | Restore backup | :white_check_mark: | | ogcli operation `.backup.restore {"name": …, "replace": true}` |
| .storage | Show a list of storage statistics | :white_check_mark: | | ogcli operation `.storage.stats` |

The final vocabulary should remain intentionally compact.

> The rows starting with `.` are **not** query stages. They are `ogcli` shortcuts for protocol operations. See [ogcli and dot-operations](#10-ogcli-and-dot-operations).

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

> **Current state:** done. One grammar and **one evaluator** (`query::eval`) serve every stage: `where`, `set`, `derive`, `select … as`, `lookup` correlation and projected scans. The language covers literals, field paths, arrays, comparisons, `in`/`not in`, boolean logic, arithmetic and function calls, and any construct can nest inside any other. Parameters (`$name`) are bound by the request. See [Expressions](#4-expressions) and [Parameters](#54-parameters-name).

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

> **Current state:** 65 built-in functions are available in every stage (numeric, conversions, text, arrays, objects, dates, conditionals, null handling). Names and argument counts are checked when the query is parsed. See [4.4 Functions](#44-functions). Aggregation over documents stays in stages: `count`, `group` (sum, avg, min, max, first, last, count), `bucket` and `pivot`.

---

# Parameters

Queries should support external parameters.

Example:

```text
where status == $status
```

Parameter binding belongs to the execution context rather than the parser.

> **Current state:** implemented. The `query.execute` request carries a `params` object; each `$name` outside string literals is replaced by the escaped literal of its value before authorization and planning, so a value can never inject syntax. `ogcli --param name=value` sets them. See [5.4 Parameters](#54-parameters-name).

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

---
---

# Part II — Language reference

Contents:

1. [A five-minute tour](#1-a-five-minute-tour)
2. [Lexical structure](#2-lexical-structure)
3. [Values and types](#3-values-and-types)
4. [Expressions](#4-expressions)
5. [Query structure](#5-query-structure)
6. [Stage reference](#6-stage-reference)
7. [Pipeline composition rules](#7-pipeline-composition-rules)
8. [System collections](#8-system-collections)
9. [Results and statistics](#9-results-and-statistics)
10. [ogcli and dot-operations](#10-ogcli-and-dot-operations)
11. [Errors](#11-errors)
12. [Cookbook](#12-cookbook)
13. [Known limitations (ogd 1.0)](#13-known-limitations-ogd-10)
14. [Grammar summary](#14-grammar-summary)

All examples use the three collections below. To keep outputs short, the `_id` field of each result document is omitted unless it matters.

```text
users     { name, age, city, active, tags: [...], profile: { country, score } }
orders    { order_id, user, product, qty, price, status, month, region }
products  { code, label, category, embedding: [x, y, z] }
```

| users | age | city | active | tags | profile.country | profile.score |
|---|---|---|---|---|---|---|
| Alice | 31 | Paris | true | admin, dev | FR | 9.5 |
| Bob | 17 | Lyon | false | dev | FR | 7 |
| Carol | 45 | Berlin | true | *(empty)* | DE | 8.25 |
| Dan | 28 | Paris | true | ops | FR | 6 |

| orders | user | product | qty | price | status | month | region |
|---|---|---|---|---|---|---|---|
| 1 | Alice | P1 | 2 | 10.0 | paid | 2026-01 | EU |
| 2 | Alice | P2 | 1 | 99.5 | paid | 2026-02 | EU |
| 3 | Bob | P1 | 5 | 10.0 | pending | 2026-01 | EU |
| 4 | Carol | P3 | 1 | 250.0 | paid | 2026-02 | US |
| 5 | Dan | P2 | 3 | 99.5 | cancelled | 2026-01 | US |
| 6 | Carol | P1 | 4 | 10.0 | paid | 2026-01 | US |

| products | label | category | embedding |
|---|---|---|---|
| P1 | Pen | office | [1.0, 0.0, 0.0] |
| P2 | Printer | office | [0.9, 0.1, 0.0] |
| P3 | Phone | mobile | [0.0, 1.0, 0.0] |

---

## 1. A five-minute tour

Filter, compute, rank. This is a Top-N over a computed value, streamed in one pass:

```text
on orders
| where status != "cancelled"
| select user, product, price * qty as total
| sort total desc
| limit 3
```
```json
{"product":"P3","total":250.0,"user":"Carol"}
{"product":"P2","total":99.5,"user":"Alice"}
{"product":"P1","total":50.0,"user":"Bob"}
```

Revenue per region. Here `derive` feeds a grouped sum, which is then sorted:

```text
on orders
| where status == "paid"
| derive revenue = price * qty
| group region, revenue as revenue
| sort revenue desc
```
```json
{"region":"US","revenue":290.0}
{"region":"EU","revenue":119.5}
```

Several measures at once, with the compound `group`:

```text
on orders | group | by region | sum qty as units | sum price as list_value | end
```
```json
{"list_value":119.5,"region":"EU","units":8}
{"list_value":359.5,"region":"US","units":8}
```

Expressions are one language everywhere: arithmetic, membership and functions work in filters, projections and updates alike:

```text
on orders
| where price * qty > 50 and status in ["paid", "pending"]
| select order_id, round(price * qty, 1) as total, if(qty > 2, "bulk", "unit") as kind
```

```text
on users | where "dev" in tags | select name, upper(concat(name, "@", lower(city))) as handle
on users | where name == "Bob" | set age = age + 1, nickname = lower(name)
```

Multi-dimensional grouping with renamed keys:

```text
on orders | group region as zone, month, qty as units | sort units desc
```
```json
{"month":"2026-01","units":7,"zone":"EU"}
{"month":"2026-01","units":7,"zone":"US"}
{"month":"2026-02","units":1,"zone":"EU"}
{"month":"2026-02","units":1,"zone":"US"}
```

Vector similarity search (k-NN by cosine distance):

```text
on products
| near embedding, [0.1, 0.9, 0.0]
| select code, label, _distance
| sort _distance
| limit 2
```
```json
{"_distance":0.006116265326381098,"code":"P3","label":"Phone"}
{"_distance":0.7804878048780488,"code":"P2","label":"Printer"}
```

Correlated joins: each user receives their own large orders.

```text
on users as u
| where age > 30
| lookup orders as o
    | where o.user == u.name and o.price * o.qty > 20
    | select order_id, price
    | into big_orders
| end
```
```json
{"age":31,"big_orders":[{"order_id":2,"price":99.5}],"name":"Alice",…}
{"age":45,"big_orders":[{"order_id":4,"price":250.0},{"order_id":6,"price":10.0}],"name":"Carol",…}
```

Embedded sub-pipelines: attach related documents, or concatenate another collection:

```text
on users
| lookup orders
    | where status == "cancelled"
    | select order_id, user
    | into cancelled_orders
| end
```
```text
on users | select name | union | on products | select label | end
```
```json
{"name":"Alice"}
{"name":"Bob"}
{"name":"Carol"}
{"name":"Dan"}
{"label":"Pen"}
{"label":"Printer"}
{"label":"Phone"}
```

Transactional writes. `set` acts on exactly the documents that reach it, even after a sort and a limit:

```text
on users | sort name | limit 1 | set top = true
on orders | where status == "cancelled" | delete
on staging | load | with merge | chunk [{"sku":"A","qty":3}] | chunk [{"sku":"B","qty":1}] | end
```

Engine introspection is queryable like any other collection:

```text
on _memory | where scope == "global" | select profile, current_bytes, pressure_state
on _index_observations | sort executions desc | limit 10
```

---

## 2. Lexical structure

### 2.1 Whitespace and layout

- Spaces, tabs and newlines are interchangeable. A query can sit on one line or span many, and indenting sub-pipelines is purely cosmetic.
- There are **no comments** in the language.

```text
on users
| where age >= 18
| select name
```

is identical to `on users | where age >= 18 | select name`.

### 2.2 The pipe `|`

`|` separates stages and is the only statement separator. A single `|` outside brackets is always a stage boundary. `||` is always the boolean "or" operator, never two stage boundaries, so `where a || b` is one stage.

### 2.3 Identifiers

- Identifiers start with a letter (any Unicode alphabetic character) or `_`. They continue with letters, digits or `_`.
- Identifiers are case-sensitive.
- Unicode identifiers work everywhere:

```text
on données | insert {prénom: "Élodie"}
on données | where prénom == "Élodie" | select prénom
```

- **Field paths** use dots: `profile.country`, `a.b.c`. Each segment must be a valid identifier. A key containing a space or hyphen can be *stored* (`insert {"display name": "Zed"}`) but cannot be referenced as a path: read it with `get(document_field, "display name")`.
- A `?` after a path segment makes the path optional: `nick?`, `profile?.country` (see [3.4](#34-missing-fields-are-strict)).
- **Collection names** can also be dotted (`on shop.orders`).

### 2.4 Keywords

These words are reserved by the pipeline lexer:

```text
on  from  as  where  set  lookup  join  union  load  pivot
into  with  chunk  replace  update  merge  end
and  or  not  true  false  null
```

Other stage names (`select`, `sort`, `group`, `limit`, …) are ordinary identifiers that are recognised in stage position.

Inside expressions, `in` is also reserved (membership operator). `between` is recognised only after an operand (`age between 18 and 30`), so it remains usable as a field name. Function names are not reserved: `round(x)` is a call and `round` alone is a field.

### 2.5 Literals

| Literal | Examples | Notes |
|---|---|---|
| Null | `null` | |
| Boolean | `true`, `false` | |
| Integer | `0`, `42`, `18446744073709551615` | Stored as signed 64-bit when it fits, otherwise unsigned 64-bit |
| Float | `9.5`, `.75`, `1e3`, `1.5e-10` | Finite IEEE-754 doubles. `1` and `1.0` are distinct spellings but compare equal |
| String | `"Paris"`, `"a\"b"`, `"line\nbreak"`, `"\u00e9t\u00e9"` | Double quotes only. JSON escapes: `\" \\ \/ \b \f \n \r \t` and `\uXXXX` (a surrogate pair such as `\ud83d\ude00` is one character). No raw newline inside a string |
| Array | `[1, 2]`, `["a", qty * 2, lower(name)]` | Elements are any expressions. Pure JSON arrays also work in `insert`, `near` and `load` payloads |
| Object (JSON) | `{"a": 1}`, `{name: "Alice", profile: {country: "FR"}}` | Anywhere in an expression with quoted keys (`merge(profile, {"level": 2})`); `insert` also accepts unquoted keys |

### 2.6 Numbers and signs

A leading `+` or `-` directly applied to a numeric literal is part of the literal: `-5`, `- 5` and `+2.5` are constants in every stage, including `where` and `set`. Between two operands, `-` and `+` are binary: `a - 5` is a subtraction and `a > -5` is a comparison with the constant -5. Applied to a field (`-qty`), `-` is a runtime negation.

### 2.7 Operators and punctuation

| Token | Meaning |
|---|---|
| `==` `!=` `<` `<=` `>` `>=` | Comparisons |
| `and`, `or`, `not` | Boolean operators |
| `&&`, `\|\|`, `!` | Symbolic synonyms of `and`, `or`, `not`. A lone `&` is rejected |
| `+ - * / %` | Arithmetic |
| `in`, `not in` | Membership in an array (literal or field) |
| `between … and …`, `not between … and …` | Inclusive range test: `age between 18 and 30` |
| `?` | Optional field path: `nick?` reads `null` when the field is missing |
| `name(…)` | Function call |
| `( )` | Grouping |
| `[ ]` | Array constructor |
| `{ } : ,` | JSON objects, argument lists |
| `.` | Field-path / collection-path separator |
| `=` | Assignment in `set` and `derive` (never comparison) |

---

## 3. Values and types

### 3.1 The value model

Documents are schema-less. Every field holds one of:

| Kind | Description |
|---|---|
| `null` | Explicit null |
| boolean | `true` / `false` |
| number | Signed integer (i64), unsigned integer (u64) or finite float (f64). The physical kind is preserved on storage |
| string | UTF-8 text |
| array | Ordered values of any kind, including mixed kinds |
| object | Nested document |

### 3.2 `_id`

Every stored document has an `_id`, a **UUID v7** (for example `01900000-0000-7000-8000-000000000003`).

- `insert` and `load` generate it when absent.
- `load` accepts an explicit `_id` (a 32-hex-digit UUID, with or without dashes). This is how you get idempotent imports and upserts.
- `where _id == "<uuid>"` is detected by the planner and executed as a **primary-key lookup** (strategy `primary_key_lookup`) instead of a scan.
- `select` always keeps `_id`, and `ogcli` shows it.
- Synthetic rows produced by `group` carry a deterministic synthetic `_id`.

### 3.3 Comparison and coercion rules

- Integers and floats compare numerically: `age == 31.0` matches `age: 31`.
- Strings compare lexicographically: `name >= "B" and name < "D"`.
- **No cross-kind coercion.** `age == "31"` is an error (`operation "==" is incompatible with values …`), not `false`. Membership is the exception: `x in [...]` never fails on kinds, it just does not match.
- Field-to-field comparisons are allowed: `where age >= profile.score`, `where o.user == name`.

### 3.4 Missing fields are strict

A field referenced in a predicate or in an expression **must exist** on every document that reaches it. Otherwise the query fails with:

```text
expression evaluation failed: field path "nickname" is missing
```

Streaming results emitted before the failing document have already been sent, so filter heterogeneous collections first on a field that exists everywhere. This rule is deliberate: a typo in a field name surfaces as an error instead of silently returning nothing.

Missing is not `null`, and the language gives you explicit tools for optional fields:
- `exists(nickname)` is `true` or `false` and never fails.
- `coalesce(nickname, name)` returns its first present, non-null argument.
- `and`, `or`, `if` and `case` evaluate lazily, so `exists(x) and x > 3` and `if(exists(x), x, 0)` are safe.
- A `?` after a path segment reads a missing field as `null`: `nick?`, `profile?.country` (the whole path becomes optional). `==` and `!=` accept `null` on either side, so `where nick? == "Zed"` simply does not match documents without `nick`.

```text
on users | select name, coalesce(nick, name) as display, exists(nickname) as has_nick
on users | where profile?.level == "gold" | select name, nick? as nick
```

Other exceptions:
- `near` silently **discards** documents that lack the vector field.
- Projection stages (`select field`, `drop`, `rename`) tolerate missing fields.

---

## 4. Expressions

### 4.1 Forms

| Form | Example |
|---|---|
| Literal | `42`, `"paid"`, `true`, `null` |
| Field path | `age`, `profile.country` |
| Unary | `not active`, `!active`, `-qty`, `+qty` |
| Binary | `age >= 18`, `price * qty` |
| Group | `(city == "Paris" or city == "Lyon")` |
| Membership | `status in ["paid", "pending"]`, `"dev" in tags`, `region not in ["US"]` |
| Range | `age between 18 and 30`, `price not between 10 and 20` |
| Optional path | `nick?`, `profile?.country` |
| Array | `[qty, qty * 2, "x"]` |
| Function call | `round(price * qty, 2)`, `coalesce(nick, name)` |
| JSON object literal | `{"a": 1}`, `merge(meta, {"v": 2})` (quoted keys) |

Every form nests inside every other: `round(div(price * qty, qty - 1, 0), 2)`, `upper(concat(name, "@", lower(city)))`, `max([a, b, c]) in allowed`.

### 4.2 Precedence

From lowest to highest. Binary operators are left-associative.

| Level | Operators |
|---|---|
| 1 | `or`, `\|\|` |
| 2 | `and`, `&&` |
| 3 (prefix) | `not`, `!` |
| 4 | `==` `!=` |
| 5 | `<` `<=` `>` `>=` `in` `not in` `between … and …` |
| 6 | `+` `-` |
| 7 | `*` `/` `%` |
| 8 (prefix) | unary `-`, unary `+` |

Like in SQL and Python, `not` binds looser than comparisons: `not city == "Berlin"` means `not (city == "Berlin")`, and `not active and age > 18` means `(not active) and age > 18`.

```text
on users | where age > 30 || profile.country == "DE" | select name
-- {"name":"Alice"}
-- {"name":"Carol"}

on users | where not age < 18 && active | select name
-- {"name":"Alice"}
-- {"name":"Carol"}
-- {"name":"Dan"}
```

```text
on users | where age >= 18 and not (city == "Berlin") | select name, profile.country as country
```
```json
{"country":"FR","name":"Alice"}
{"country":"FR","name":"Dan"}
```

### 4.3 One evaluator for every stage

There is a single expression evaluator (`query::eval`). Every stage uses it, and stages differ only by where field values come from: a stored document, a projected row, or the joined and enriched documents of a `lookup`. Anything that works in `where` therefore works in `set`, `derive`, `select … as` and inside `lookup`/`union` sub-pipelines, with the same results and the same errors.

| Construct | Semantics |
|---|---|
| Comparisons | Numbers compare across integer and float; strings lexicographically; no other cross-kind comparison (error) |
| `and` / `or` / `not` | Operands must be booleans. `and` and `or` short-circuit |
| `in` / `not in` | The right side must be an array. Matching uses `==` semantics; incompatible kinds simply do not match |
| `+ - * %` | Integers stay integers while the result fits in i64 (`qty * 2 + 1` → `7`). Anything involving a float gives a float |
| `/` | Always a float (`7 / 2` → `3.5`) |
| Division by zero | Error `division by zero; use div(a, b, fallback) for a safe division` |
| Unary `-` | Integer negation when possible, otherwise float |
| Text | No `+` on strings: use `concat(…)` |

```text
on orders | where price * qty > 50 and status != "cancelled" | select order_id, round(price * qty, 1) as total
```
```json
{"order_id":2,"total":99.5}
{"order_id":4,"total":250.0}
```

```text
on orders | where status in ["paid", "pending"] and region not in ["US"] | select order_id, status
-- {"order_id":1,"status":"paid"}
-- {"order_id":2,"status":"paid"}
-- {"order_id":3,"status":"pending"}

on users | where abs(age - 30) <= 2 and not (name in ["Bob"]) | select name, age
-- {"age":31,"name":"Alice"}
-- {"age":28,"name":"Dan"}
```

A bare boolean field or literal is a valid predicate: `where active`, `where true`.

### 4.4 Functions

Functions are called by name with parenthesised arguments, and any expression can be an argument. Unknown names and wrong argument counts are rejected when the query is parsed:

```text
invalid expression "nope(age) > 1" for stage "where": unknown function `nope`; available functions: abs, sqrt, …
invalid expression "abs(age, 2) > 1" for stage "where": function `abs` expects 1 argument(s), got 2
```

**Numeric**

| Function | Result | Example → result |
|---|---|---|
| `abs(x)` | Absolute value, integer kept | `abs(-qty)` → `3` |
| `sqrt(x)` | Square root (float); negative input is an error | `sqrt(16)` → `4.0` |
| `pow(x, y)` | Power; integer when exact | `pow(qty, 2)` → `9` |
| `round(x [, digits])` | Rounds half away from zero. Integer without `digits`, float with `digits` (0 to 15) | `round(13.333, 2)` → `13.33` |
| `trunc(x [, digits])` | Rounds toward zero, same rules as `round` | `trunc(-2.7)` → `-2` |
| `floor(x)`, `ceil(x)` | Rounded down / up, as an integer | `floor(12.5)` → `12` |
| `sign(x)` | `-1`, `0` or `1` | `sign(-qty)` → `-1` |
| `ln(x)`, `log10(x)`, `log(x, base)`, `exp(x)` | Logarithms (positive input) and exponential, as floats | `log(8, 2)` → `3.0` |
| `min(a, b, …)`, `max(a, b, …)` | Smallest / largest argument; with one array argument, over its elements | `max(age, 30)`, `max(scores)` |
| `sum(a, b, …)`, `avg(a, b, …)` | Sum (integer while every term is) / mean; with one array argument, over its elements; `null` items are skipped | `sum(scores)`, `avg([])` → `null` |
| `div(a, b [, fallback])` | Safe division: `fallback` when `b` is 0 (`null` without fallback), else `a / b` | `div(price, qty - 1, 0)` |

**Logic, missing values and conversions**

| Function | Result | Example → result |
|---|---|---|
| `if(cond, then, else)` | Only the chosen branch is evaluated | `if(age >= 18, "adult", "minor")` |
| `case(c1, v1, c2, v2, …, [default])` | Value of the first true condition, else `default` (or `null`); lazy | `case(qty > 5, "bulk", qty > 1, "few", "one")` |
| `coalesce(a, b, …)` | First argument that is present and not `null` | `coalesce(nick, name)` |
| `exists(x)` | Whether `x` is present (never fails) | `exists(nickname)` |
| `optional(x)` | `x`, or `null` when missing; what `x?` means | `optional(nick)` |
| `nullif(a, b)` | `null` when `a == b`, else `a` | `nullif(status, "")` |
| `between(x, low, high)` | `low <= x <= high`; what `x between low and high` means | |
| `bucket(x, [b0, b1, …] [, default])` | Lower bound `b(i)` of the range `[b(i), b(i+1))` holding `x`, else `default` (or `null`) | `bucket(age, [0, 18, 65])` → `18` |
| `to_int(x)` | Integer from a number (truncated), a numeric string, a boolean; `null` stays `null` | `to_int("42")` → `42` |
| `to_float(x)` | Float from a number, a numeric string or a boolean | `to_float("2.5")` → `2.5` |
| `to_string(x)` | Text; numbers, booleans, arrays and objects are written as JSON | `to_string(qty)` → `"3"` |
| `to_bool(x)` | From a boolean, a number (non-zero), or `true/false/1/0/yes/no` | `to_bool("yes")` → `true` |
| `type(x)` | `"null"`, `"bool"`, `"number"`, `"string"`, `"array"` or `"object"` | `type(profile)` → `"object"` |

**Text** (positions count characters, not bytes; a negative start counts from the end)

| Function | Result | Example → result |
|---|---|---|
| `len(x)` | Characters of a string, items of an array, keys of an object | `len(tags)` → `2` |
| `lower(s)`, `upper(s)`, `trim(s)`, `ltrim(s)`, `rtrim(s)` | Text transforms | `upper(trim(name))` |
| `concat(a, b, …)` | Joins text (numbers and booleans are written as text, `null` is skipped); if every argument is an array, concatenates the arrays | `concat(name, "@", city)`, `concat(tags, ["new"])` |
| `substr(s, start [, length])` | Substring | `substr("Paris", 0, 3)` → `"Par"` |
| `split(s, separator)` | Array of parts; an empty separator splits characters | `split("a,b", ",")` → `["a", "b"]` |
| `replace(s, from, to)` | Replaces every occurrence | `replace(city, " ", "-")` |
| `index_of(x, y)` | Position of `y` in a string or an array, `-1` when absent | `index_of(tags, "dev")` → `1` |
| `pad_left(s, width [, fill])`, `pad_right(…)` | Pads to `width` characters with `fill` (default space) | `pad_left(to_string(qty), 3, "0")` → `"003"` |
| `contains(x, y)` | Substring test on strings, element test on arrays | `contains(name, "li")`, `contains(tags, "dev")` |
| `starts_with(s, p)`, `ends_with(s, p)` | Prefix / suffix test | `starts_with(city, "Pa")` |

**Arrays**

| Function | Result | Example → result |
|---|---|---|
| `at(array, i)` | Item `i` (negative from the end), `null` when out of range | `at(tags, -1)` → `"dev"` |
| `slice(array, start [, length])` | Sub-array | `slice(tags, 1)` |
| `reverse(x)` | Reversed array or string | `reverse(tags)` |
| `sort_array(array [, "asc"\|"desc"])` | Sorted copy; items must be comparable | `sort_array(scores, "desc")` |
| `unique(array)` | Distinct items, first occurrence kept | `unique([1, 1, 2])` → `[1, 2]` |
| `flatten(array)` | Flattens one level | `flatten([[1], 2])` → `[1, 2]` |
| `range(start, end [, step])` | Integers from `start` up to (excluding) `end`; at most 1,000,000 items | `range(0, 6, 2)` → `[0, 2, 4]` |

**Objects**

| Function | Result | Example → result |
|---|---|---|
| `keys(o)`, `values(o)` | Keys (sorted) / values of an object | `keys(profile)` → `["country", "score"]` |
| `get(o, key [, default])` | Value under any key, including keys that are not identifiers | `get(meta, "display name", "")` |
| `has(o, key)` | Whether the key is present | `has(profile, "score")` |
| `merge(o1, o2, …)` | Shallow merge, later objects win; `null` arguments are skipped | `merge(profile, {"level": 2})` |

**Dates.** There is no date type: a timestamp is a number of milliseconds since the Unix epoch, or an ISO-8601 string (`2026-03-09`, `2026-03`, `2026-03-09T10:00:00Z`, `2026-03-09 10:00:00+02:00`). Every computation is in UTC. Units are `year`, `quarter`, `month`, `week` (weeks start on Monday), `day`, `hour`, `minute`, `second`, `millisecond`, singular or plural.

| Function | Result | Example → result |
|---|---|---|
| `now()` | Current time, epoch milliseconds | |
| `parse_date(t)` | Epoch milliseconds | `parse_date("1970-01-02")` → `86400000` |
| `date_trunc(t, unit)` | Start of the unit, epoch milliseconds | `date_trunc(at, "month")` |
| `date_add(t, n, unit)` | Epoch milliseconds; adding months clamps the day | `date_add("2026-01-31", 1, "month")` → 2026-02-28 |
| `date_diff(start, end, unit)` | Unit boundaries crossed from `start` to `end` | `date_diff("2025-12-31", "2026-01-01", "year")` → `1` |
| `date_part(t, part)` | `year`, `quarter`, `month`, `week` (ISO), `day`, `day_of_week` (1 = Monday), `day_of_year`, `hour`, `minute`, `second`, `millisecond` | `date_part(at, "day_of_week")` |
| `date_format(t [, pattern])` | Text; default ISO-8601. Directives `%Y %m %d %H %M %S %L` (ms) `%j %u %V %F %T %%` | `date_format(at, "%Y-%m")` → `"2026-03"` |
| `fiscal_year(t, closing)` | Fiscal year holding `t`, named after the year it closes in, for a year closing at the end of month `closing` (1 to 12; 12 is the calendar year), or opening on the date `closing` (a timestamp on the first day of a month, repeated every year) | `fiscal_year("2026-07-01", 6)` → `2027`, as `fiscal_year("2026-07-01", "2019-07-01")` |
| `fiscal_month(t, closing)` | Month within the fiscal year, 1 to 12 | `fiscal_month("2026-07-01", 6)` → `1` |
| `fiscal_start(t, closing)`, `fiscal_end(t, closing)` | First and last day of the fiscal year, epoch milliseconds | `fiscal_end("2026-07-01", 6)` → 2027-06-30 |

```text
on users | select name, upper(concat(name, "@", lower(city))) as handle, coalesce(nick, name) as display, if(age >= 18, "adult", "minor") as bracket
```
```json
{"bracket":"adult","display":"Alice","handle":"ALICE@PARIS","name":"Alice"}
{"bracket":"minor","display":"Bob","handle":"BOB@LYON","name":"Bob"}
{"bracket":"adult","display":"Carol","handle":"CAROL@BERLIN","name":"Carol"}
{"bracket":"adult","display":"Dan","handle":"DAN@PARIS","name":"Dan"}
```

```text
on orders | select order_id, div(price, qty - 1, 0) as per_extra, round(sqrt(pow(qty, 2) + 9), 2) as hyp
-- {"hyp":3.61,"order_id":1,"per_extra":10.0}
-- {"hyp":3.16,"order_id":2,"per_extra":0}
-- {"hyp":5.83,"order_id":3,"per_extra":2.5}
```

---

## 5. Query structure

```text
<source> ( "|" <stage> )*
```

### 5.1 Source

```text
on <collection> [as of <date|generation> | history] [as <alias>]
from <collection> [as of <date|generation> | history] [as <alias>]
```

- `on` and `from` are synonyms.
- The collection name may be dotted: `on shop.orders`.
- Collections are created implicitly by the first write. Reading a collection that does not exist returns nothing.
- A query may consist of the source alone: `on users` returns every document.
- The alias names the source. At the top level `u.age` and `age` are the same field (`on users as u | where u.age > 30`); the qualifier is dropped before planning, outside string literals, so an alias shadows a document field of the same name. In `lookup`/`union` sub-pipelines the alias names the outer document.
- `on values [{…}, …]` reads literal rows, objects with bare or quoted keys: `on values [{code: "FR", rate: 0.2}, {code: "DE", rate: 0.19}] | where rate > 0.19`. It works wherever a collection is read (`lookup values […] as r | … | end`, `union | on values […] | end`). The rows are written once in a constant collection named after their content (`_values.v<hash>`), so they are read like any collection (indexes of `lookup`, streaming), by every Place, and can never be written.
- `on view <name> [version <n>] [as <alias>]` reads a view ([5.7](#57-views-define-view-on-view)).

#### 5.1.1 Past states and history (`as of`, `history`)

```text
on orders as of "2026-09-01T12:00:00Z" | where status == "paid" | count
on orders as of 1842 | sort total desc | limit 10
on orders history | where _doc_id == "0190f3c2-…" | sort _generation
on orders as of "2026-09-01" | into orders_restored
```

- `as of <string>` reads the store as it was at that time (ISO 8601, the date alone meaning midnight UTC):
  the state after the last commit made at or before it. `as of <number>` reads it at that commit generation.
  Every collection of the query is read at that point, `lookup` and `union` collections included.
- `history` returns one row per stored write of the collection, in commit order: each row holds the document
  of the version plus the system fields below. Other collections of the query (`lookup`, `union`) are read
  in their current state.

  | Field | Meaning |
  |---|---|
  | `_id` | Identifier of the history row (commit generation, position in the commit): distinct and ordered. |
  | `_doc_id` | Identifier of the document (string). |
  | `_version` | Version of the document written, or deleted for a `delete` row. |
  | `_op` | `insert` (version 1), `replace`, `delete` (the row holds the document as it was deleted), `baseline` (present when history recording started, written earlier). |
  | `_generation` | Commit generation. |
  | `_at` | Commit time (ISO 8601), `null` when not recorded. |
  | `_by`, `_by_device` | Identity and device that made the commit, `null` when unknown (anonymous, or a write made outside a query). |

  These names replace document fields of the same name in history rows.
- Both are read-only: a stage that writes is rejected, except a terminal `into`, which copies the rows into
  another collection (a restore).
- A Place or App Instance scope applies to every version, through its `_place`/`_app_instance` fields.
- `as of` a generation after the current one, or before the start of the kept history, is an error. A date
  after the last commit reads the current state; a date before the first recorded commit time is an error.
- History needs the store to keep it: `OGD_HISTORY=on` (memory and glacier). Without it, `as of` and `history`
  fail with `this store does not keep history`.
- Cost: `as of` reads the current state minus the documents changed since the requested point, plus the versions
  those documents had then; a collection not changed since is read at full speed. `history` reads every stored
  version of the collection.
- `as of` without a value keeps its old meaning: an alias named `of` (`on users as of | where of.age > 3`).

### 5.2 Simple stages

```text
| <stage-name> <arguments>
```

Everything up to the next top-level `|` is the stage's argument text. Parentheses, brackets and braces must balance. A `|` inside `[...]`, `{...}` or `(...)` does not end the stage.

### 5.3 Compound stages and `end`

Whether a stage opens a **sub-pipeline** is a syntactic property of its name (`SubPipelinePolicy` in the parser):

| Policy | Stages | Meaning |
|---|---|---|
| `Always` | `union`, `pivot`, `window` | Always compound |
| `UnlessHeaderHasInto` | `lookup`/`join` | Compound, except the one-line form whose header contains `into` |
| `WhenHeaderEmpty` | `load`, `group` | Compound when written without arguments; compact single-line form otherwise |
| `Never` | every other stage | Always simple |

A sub-pipeline is a sequence of stages closed by `| end`:

```text
on users
| lookup orders
    | where status == "paid"
    | into paid
| end
| ...
```

- A missing `| end` is an error (`sub-pipeline opened by stage at … is missing '| end'`).
- An `end` without an open sub-pipeline is also an error.
- Sub-pipelines can nest.

### 5.4 Parameters (`$name`)

A query can reference named parameters. Their values travel next to the query in the `query.execute` request:

```json
{"query": "on users | where age >= $min and city in $cities | limit $n",
 "params": {"min": 18, "cities": ["Paris", "Lyon"], "n": 10}}
```

```text
ogcli --param min=18 --param 'cities=["Paris","Lyon"]' --param n=10 'on users | where age >= $min and city in $cities | limit $n'
```

- Each `$name` outside a string literal is replaced by the literal of its value before the query is authorized, planned or run. Strings are quoted and escaped, so a value never changes the structure of the query: `$name` bound to `x" | delete` stays one string.
- Any JSON value works: numbers, strings, booleans, `null`, arrays (for `in`), objects (for `insert`). A parameter can appear wherever its literal could (`limit $n`, `near embedding, $vector`).
- An unbound `$name` fails the request (`variable $name is not bound; …`); unused parameters are ignored. With `ogcli`, `VALUE` is parsed as JSON when it can be, otherwise taken as a string.

### 5.5 Variables (`let`)

A query can start with one or more `let` headers that name values reused by its stages:

```text
let <$name> = <expression>, <$name> = <expression>, ...
| [let ... |]
on <collection> | ...
```

```text
let $threshold = 50, $since = parse_date("2026-02-01")
| on orders
| where price * qty > $threshold and parse_date(at) >= $since
| derive margin = price * qty - $threshold
| select order_id, margin
```
```json
{"margin":49.5,"order_id":2}
{"margin":200.0,"order_id":4}
```

- Each expression is evaluated **once per query**, before planning, then used wherever `$name` appears: `where`, `derive`, `set`, `select`, `sort`, `limit`, sub-pipelines. `let $now = now()` therefore gives every row the same instant.
- An expression may use any function, request parameters and variables declared before it (`let $max = $min * 2`, or a later `let` header). It cannot read document fields: `let $x = qty` is rejected.
- A name is bound once: redeclaring a variable, or declaring a name also passed in `params`, is an error.
- `let $name ?= <expression>` gives a default: it binds the name only when it is not bound already (by `params`, an earlier variable, or the query reading the view): `let $gran ?= "ytd" | …`.
- Variables are bound like parameters (as escaped literals), so the plan, `explain` and authorization see the resulting values: `explain let $n = 2 | on orders | limit $n` shows `limit 2`.
- A value written as a query in parentheses is the array of its rows, read once in the scope of the request (its Place and App instance), before the query is planned: `let $settings = (on settings | limit 1) | let $opening = get(at($settings, 0), "opening") | on ledger | where month >= $opening`. The query may use the variables declared before it and read views; it must not write and returns at most 1,000 rows. Like `on view`, it is read through `query.execute`.

### 5.6 `explain`

Prefixing a query with `explain` plans it without running it and returns one document describing the plan. Authorization is the same as for the query itself.

```text
explain on orders | where qty > 1 | sort price * qty desc | limit 3
```
```json
{"access":"collection_scan","collection":"orders","executor":"governed_blocking","mode":"governed_blocking","operators":[{"detail":"qty > 1","stage":"filter"},{"detail":"__og_sort_0 = price * qty","stage":"derive"},{"detail":"__og_sort_0 desc","stage":"sort"},{"detail":"__og_sort_0","stage":"drop"},{"detail":"3","stage":"limit"}],"read_only":true}
```

| Field | Meaning |
|---|---|
| `access` | `collection_scan` or `primary_key_lookup` |
| `executor` | `streaming`, `governed_blocking`, `transactional` (writes), or `rejected` when the shape has no bounded executor ([7.2](#72-execution-shapes-accepted-by-ogd-memory-contract)) |
| `mode` | Memory contract of the plan |
| `operators` | Physical operators in order, with shorthand forms expanded |

### 5.7 Views (`define view`, `on view`)

A view is a named read query kept in the Place, in the `_views` collection, one document per version. Like `explain`, these are statements, not stages:

```text
define view finance.cube
| description "Monthly cube by account"
| exec
  | on ledger | where scenario == $scenario and month <= $to
  | group | by account, month | sum amount as amount | end
  | end
| end
```
```json
{"name":"finance.cube","version":1,"state":"active","description":"Monthly cube by account","query":"on ledger | where scenario == $scenario and month <= $to\n  | group | by account, month | sum amount as amount | end","parameters":["scenario","to"],"reads":["ledger"],"createdBy":"…","createdAt":…}
```

- `exec` is the last clause; the body runs from `exec` to the two final `end` (its own `end` stay in it). Each `define` writes a new version and marks the previous one `superseded`; without `description`, the description is kept.
- The body's parameters are the `$name` it reads without declaring them with `let`; they are not bound when the view is defined, and are listed in `parameters`.
- The body is checked when defined: planned with its parameters read as `1`, and refused when it writes (`view.invalid`).
- `define view <name> | description "…" | end` writes a new version with another description; `define view <name> | version <n> | end` restores version `n` as a new version; `undefine view <name>` marks the active version `deleted`. History is never rewritten.
- `explain define view …` checks without writing. In a Place, views are defined by its Owner; a view defined outside a Place is a standard view and needs the right to write `_views`.
- `on _views` lists and reads them back; in a Place it reads only that Place's views, and only `define` / `undefine` write them: `on _views | where state == "active" | select name, version, parameters`.
- `| materialized |` (before `exec`) keeps the view's rows: the first read in a scope (Place, App instance) computes them into a cache collection (`_cache.v<key>s<0|1>`, rows scoped like any write), and later reads in that scope read it, until a collection the view reads changes for that Place: a write of the Place, or a write og-core cannot attribute to one Place (unscoped, `load`), which counts for every Place (the storage tells the generation of each collection's last change, og-core the Place of each scoped write; a restart computes the caches again). The rows are then computed again into the scope's other cache collection. A materialized view takes no parameter (its `let` bind them); filters after it reach the cache like any source. `on _cache` lists the caches (scope, view version, collection, `computedAt`); only og-core writes them. A view defined over a materialized view lists the collections it reads among its own `reads`.

A view is read as a source, with `on view <name> [version <n>] [as <alias>]`, in the main pipeline or a `union` branch, and as the joined collection of `lookup view <name> [version <n>] as <alias> | … | end` (or the one-line `lookup view <name> as <alias> on <field> == <field> into <field>`):

```text
let $scenario = "actual", $to = "2026-06" | on view finance.cube | where account == "601"
on view finance.cube version 1 | union | on view finance.cube | end
```

- The view's query replaces the source before the query is planned: the plan, `explain` and authorization see the collections it reads, and the stages that follow are optimized with it.
- Its parameters are bound from the reading query: request `params` and its `let` variables. The view's own `let` stay local: they may shadow a variable of the reading query, which cannot read them.
- In a Place, the Place's view is read, else the standard view of that name; its active version, or `version <n>`.
- A view can read other views, 8 deep at most; a cycle is refused when the view is defined. `lookup view` needs a view whose source is a bare `on <collection>` or `on values […]`; `as <alias>` names the view's source, which must not have one already.
- A built-in App delivers standard views in its definition, `"queryViews": [{"name": …, "description": …, "query": …, "materialized": true}]`, in order (a view may read the ones before it). They are defined when `ogd` starts: a changed query or description writes a new version, a view the App no longer lists is undefined. `system.finance` delivers the standard finance views (`finance.classified` and `finance.cube` from `$from` to `$to`, the latter reading the materialized `finance.cube.all` (the union of `finance.cube.standard` and `finance.cube.custom`, the Place's own formats of `finance_formats`), which classify through the materialized `finance.accounts` (each account's category for a debit and a credit balance, in each format), `finance.period` for `$end`, `$gran`, `$shifts` (with `$offsets`, `$grans`), `$landing`, `$entities` and `$consolidate`, `finance.unclassified`, `finance.indicators` (the 188 indicators of `system.finance.json`, translated into `derive`) and `finance.indicators.definitions`, `finance.statement` with `$format` = `sig`, `pcg` or a format of the Place, and the reference tables they read as `values`), generated by `og-360/tools/finance_views.py`; dates are epoch milliseconds.

---

## 6. Stage reference

Stages are grouped by role. Each stage lists its syntax, its semantics, examples and any `ogd` specifics.

### Filtering and projection

#### `where`

```text
where <predicate>
```

Keeps documents for which the predicate is `true`. Several `where` stages may be chained, and each one narrows the result further.

```text
on users | where age >= 18 and city == "Paris" | select name, age
```
```json
{"age":31,"name":"Alice"}
{"age":28,"name":"Dan"}
```

```text
on users | where (city == "Paris" or city == "Lyon") and age < 30 | select name
on users | where active == false | select name
on users | where profile.score >= 8 | select name, profile.score
on users | where age >= 18 | where active | select name
on orders | where order_id <= 2 or status == "cancelled" | select order_id
```

`where _id == "<uuid>"` becomes a primary-key lookup.

#### `select` (alias `project`)

```text
select <item>, <item>, ...
item := <field.path> | <expression> as <field.path>
```

Keeps only the listed fields, plus `_id`. Nested sources are rebuilt nested (`select profile.score` returns `{"profile":{"score":…}}`). An alias can be a nested path. A computed item needs `as`.

```text
on orders | select order_id, price * qty as total, product as sku | limit 2
```
```json
{"order_id":1,"sku":"P1","total":20.0}
{"order_id":2,"sku":"P2","total":99.5}
```

```text
on users  | select name, profile.country as geo.country | limit 1
-- {"geo":{"country":"FR"},"name":"Alice"}

on orders | select order_id, "fixed" as label, 42 as answer, price / 4 as quarter | limit 1
-- {"answer":42,"label":"fixed","order_id":1,"quarter":2.5}

on orders | project order_id, (price * qty) - 1 as net | limit 1
-- {"net":19.0,"order_id":1}
```

Items are evaluated left to right, and a later item can use an alias introduced earlier (`select price * qty as total, total > 50 as big`). A pipeline may contain at most one `select`.

#### `derive`

```text
derive <field.path> = <expression>, <field.path> = <expression>, ...
```

Adds or overwrites fields while keeping the rest of the document. Target paths may be nested (`meta.total = …`). Assignments are applied **left to right**, and each one sees the fields computed before it, so a single `derive` can build a chain of computations:

```text
on orders | derive total = price * qty, big = total > 100, label = if(big, "big", "small") | select order_id, total, label | limit 2
-- {"label":"small","order_id":1,"total":20.0}
-- {"label":"small","order_id":2,"total":99.5}
```

```text
on orders | derive total = price * qty, half = price / 2 | select order_id, total, half | limit 2
```
```json
{"half":5.0,"order_id":1,"total":20.0}
{"half":49.75,"order_id":2,"total":99.5}
```

```text
on orders | derive r = qty % 2, n = -qty | select qty, r, n | limit 2
-- {"n":-2,"qty":2,"r":0}
-- {"n":-1,"qty":1,"r":1}

on users | derive inactive = not active | select name, inactive | limit 2
-- {"inactive":false,"name":"Alice"}
-- {"inactive":true,"name":"Bob"}

on orders | derive flag = true, label = "x", pair = [qty, qty * 2], obj = {"a": 1}, meta.total = price * qty | limit 1
```

The assignment list is split only on top-level commas, so arrays and function calls with several arguments work anywhere.

#### `rename`

```text
rename <source.path> as <target.path>
```

Moves one field. Source and target must differ.

```text
on users | rename name as full_name | select full_name, age | limit 2
-- {"age":31,"full_name":"Alice"}
-- {"age":17,"full_name":"Bob"}

on users | rename profile.score as rating | limit 1
-- … "profile":{"country":"FR"},"rating":9.5 …
```

#### `drop`

```text
drop <field.path>, <field.path>, ...
```

Removes fields from the output. It does not modify stored data.

```text
on users | drop tags, profile | limit 2
```
```json
{"active":true,"age":31,"city":"Paris","name":"Alice"}
{"active":false,"age":17,"city":"Lyon","name":"Bob"}
```

#### `root`

```text
root <field.path>
```

Replaces each document with the object stored at the path.

```text
on users | where name == "Alice" | root profile
```
```json
{"country":"FR","score":9.5}
```

### Vector search

#### `near`

```text
near <vector.field>, [<f64>, <f64>, ...]
```

Computes the **cosine distance** between the query vector and the document's vector field, and writes it to `_distance` (0 means same direction). Documents without the field are discarded, and a dimension mismatch is an error. `near` does not reorder documents: pair it with `sort _distance` and `limit k` for k-nearest-neighbours, or with `where _distance < t` for a radius search.

```text
on products | near embedding, [0.0, 1.0, 0.0] | sort _distance | limit 2
on products | near embedding, [0.0, 1.0, 0.0] | where _distance < 0.5 | select code, _distance
-- {"_distance":0.0,"code":"P3"}
on products | where category == "office" | near embedding, [0.0, 1.0, 0.0] | select code
```

### Ordering and paging

#### `sort`

```text
sort <key> [asc|desc], ...
key := <field.path> | <expression>
```

Stable ordering on one or more keys. A key can be any expression: `sort price * qty desc`, `sort len(name), name`. An expression key is computed into a hidden field by a `derive`, sorted on, then dropped, so it follows the same execution rules as `derive … | sort …`. The default direction is `asc`. Missing fields and `null` sort before every other value in ascending order, and after them in descending order. `sort` is **blocking**: it must see every input document before emitting any. Followed by `limit N`, it runs as a bounded **Top-N** that keeps only N rows in memory.

```text
on orders | sort price desc | limit 2
on orders | derive total = price * qty | sort total desc | limit 3
on orders | sort price * qty desc | limit 3          -- same, without keeping `total`
on users  | select name, city | sort city
```

`sort` may appear once per pipeline. For which stages may follow it, see [section 7](#7-pipeline-composition-rules).

#### `limit`, `skip` (alias `offset`)

```text
limit <non-negative integer>
skip  <non-negative integer>
```

```text
on orders | skip 2 | limit 2 | select order_id
-- {"order_id":3}
-- {"order_id":4}
on orders | offset 4 | select order_id
-- {"order_id":5}
-- {"order_id":6}
```

Each stage may appear once per pipeline. A negative count is a planning error.

#### `first`, `single`

```text
first [<field.path>]
single [<field.path>]
```

`first` returns the first document that reaches it. `single` returns the only document that reaches it and fails when there are zero or several. With a path, both also project that field.

```text
on users | first | select name
-- {"name":"Alice"}
on users | first profile
-- {"profile":{"country":"FR","score":9.5}}
on orders | select order_id, price | sort price desc | first          -- Top-1
-- {"order_id":4,"price":250.0}
```

`first` declares a **row bound** of 1, the same execution property `limit N` declares. Every executor consumes that property instead of recognising the stage, so `first` gets the same behaviour as `limit`:
- It stops the storage scan as soon as one row has passed.
- It works inside `lookup` and `union` sub-pipelines.
- Without a projection path, it leaves the row's fields untouched. That makes it a pure bound, so `sort … | first` runs as a bounded Top-1. `first <path>` also projects, so after a blocking stage write `select <path> | sort … | first` instead.
- Unlike `limit`, `first` can appear more than once.

`single` declares an **exact** row count of 1 through the same property (`Bound::Exact`). Executors let the first row through, fail as soon as a second row reaches the stage, and fail at the end of the scan if none did:

```text
on users | where name == "Alice" | single | select name, age
-- {"age":31,"name":"Alice"}
on users | where city == "Paris" | single
-- error: single expected exactly 1 row(s), found more
on users | where age > 100 | single
-- error: single expected exactly 1 row(s), found 0
```

In streaming mode the first row may already have been sent when a second one triggers the error. The response still ends in error, so treat the query as failed.

#### `sample`

```text
sample <n>
```

Returns `n` documents chosen uniformly at random, or all of them when there are fewer. `sample` declares that it **retains at most `n` rows**. Executors run it as a reservoir sample (algorithm R), whose memory is bounded by `n` whatever the input size. Both the streaming and the materialized paths use the same reservoir.

```text
on orders | where status == "paid" | select order_id | sample 2
on orders | select order_id | sample 3 | limit 2
```

#### `unwind`

```text
unwind <field.path> [with index <field.path>] [keep empty]
```

Emits one row per item of the array at the path, with the item in place of the array, in order. A row whose field is missing or not an array passes through unchanged; a row whose array is empty is dropped. `unwind` streams mid-pipeline and works inside `lookup` sub-pipelines.

- `with index <field>` stores the 0-based position of each item (`null` on rows that were not expanded).
- `keep empty` keeps rows whose array is empty or `null`, with the field set to `null`.

```text
on users | unwind tags with index position | select name, tags, position
-- {"name":"Alice","position":0,"tags":"admin"}
-- {"name":"Alice","position":1,"tags":"dev"}
-- {"name":"Bob","position":0,"tags":"dev"}
-- {"name":"Dan","position":0,"tags":"ops"}
on users | unwind tags keep empty | where tags == null | select name
-- {"name":"Carol"}
```

### Deduplication and counting

#### `distinct`

```text
distinct                      -- whole-document equality
distinct <field.path>, ...    -- keeps the first document per distinct key
```

```text
on orders | select status | distinct
```
```json
{"status":"paid"}
{"status":"pending"}
{"status":"cancelled"}
```

```text
on orders | distinct region, status        -- first order of each (region, status) pair
```

#### `count`

```text
count
count as <alias>
```

Terminal. It returns a single document holding the number of input documents.

```text
on orders | where price > 20 | count as expensive_orders
-- {"expensive_orders":3}
```

### Aggregation

#### `group` (compact form)

```text
group <key> [as <alias>]                                   -- group + count
group <key> [as <alias>], ..., <measure> [as <alias>]       -- group + sum(measure)
```

- **One item:** group by that key and emit `count`.
- **Two or more items:** every item except the last is a grouping key, and the **last item is summed**.
- Every key and the measure can be renamed with `as`, and keys may be nested paths.

```text
on orders | group status
```
```json
{"count":4,"status":"paid"}
{"count":1,"status":"pending"}
{"count":1,"status":"cancelled"}
```

```text
on orders | group region, qty
-- {"qty":8,"region":"EU"}
-- {"qty":8,"region":"US"}

on orders | group region as zone, status as state, qty as units
-- {"state":"paid","units":3,"zone":"EU"} …

on orders | where status == "paid" | group user as customer, qty | sort qty desc
-- {"customer":"Carol","qty":5}
-- {"customer":"Alice","qty":3}

on orders | group product | sort count desc | limit 2
-- {"count":3,"product":"P1"}
-- {"count":2,"product":"P2"}
```

Groups are emitted in group-key order. `group | limit N` is bounded: only the N smallest live keys are kept (see the execution rule in Part I). A sum stays an integer while every term is an integer and the total fits in 64 bits; otherwise it is a float. Non-numeric values are ignored by sums. Grouping keys are field paths: to group by a computed value, `derive` it first, as in the tour; the compound form below measures expressions directly.

**Compound form**, for several measures:

```text
group
| by <key> [as <alias>], ...
| sum|avg|min|max|first|last|list <value> [where <predicate>] [as <alias>]
| first|last <value> by <order> [where <predicate>] [as <alias>]
| count [<value>] [where <predicate>] [as <alias>]
| ...
| end
```

`by` is required and appears exactly once. Measures appear zero or more times, in any order:

| Measure | Result | Default alias |
|---|---|---|
| `sum <field>` | Sum of the numeric values (integer while exact); the field is omitted when the group has no number | `<field>` |
| `avg <field>` | Mean of the numeric values, `null` when there are none | `avg_<field>` |
| `min <field>`, `max <field>` | Smallest / largest non-null value; values must be comparable (numbers, strings or booleans) | `min_<field>`, `max_<field>` |
| `first <field>`, `last <field>` | Value on the first / last row of the group, in input order (`null` when missing) | `first_<field>`, `last_<field>` |
| `first <value> by <order>`, `last <value> by <order>` | Value on the row of smallest / largest `<order>`, whatever the input order; rows whose order is `null` or missing are skipped, equal orders keep the first / last row read; `null` when no row has an order | `first_<field>`, `last_<field>` |
| `count` | Number of rows | `count` |
| `list <field>` | Array of the values neither missing nor null, in input order (`unwind` gives the rows back) | `list_<field>` |
| `count <field>` | Number of rows where the field is present and not `null` | `count_<field>` |

With no measure, the group emits `count`. Every measure runs on the same bounded, spillable executor as the compact form.

A measure's `<value>` (and a `by` order) may be any expression; an expression needs `as <alias>`. `where <predicate>` measures only the rows the predicate keeps: the value counts as `null` on the others, which every measure ignores (`count where <predicate>` counts the kept rows). `first`/`last` with `where` need `by <order>`. An expression, or a value under `where`, is a `derive` of a hidden field before the `group`, as for `pivot` values; `first`/`last … by` read their value and order fields directly.

```text
on ledger
| group
    | by entity, account
    | last balance by month as closing
    | sum amount where category in ["Sales", "Services"] as revenue
    | count where amount < 0 as credits
    | sum amount * rate as converted
| end
```

```text
on orders
| where status != "cancelled"
| derive revenue = price * qty
| group
    | by region, month
    | sum qty as units
    | sum revenue
| end
| sort revenue desc
```
```json
{"month":"2026-02","region":"US","revenue":250.0,"units":1}
{"month":"2026-02","region":"EU","revenue":99.5,"units":1}
{"month":"2026-01","region":"EU","revenue":70.0,"units":7}
{"month":"2026-01","region":"US","revenue":40.0,"units":4}
```

```text
on orders | group | by status | end          -- same as `group status`

on orders | group | by region | avg price as mean | max qty | count as rows | end
-- {"max_qty":5,"mean":39.833333333333336,"region":"EU","rows":3}
-- {"max_qty":4,"mean":119.83333333333333,"region":"US","rows":3}
```

#### `bucket`

```text
bucket <expression> by [<b0>, <b1>, ...] [default <value>] [as <alias>]
```

Counts rows per range `[b(i), b(i+1))`: each row falls in the bucket named by its lower bound. Rows outside every range go to `default`, or to `null` without it. The alias defaults to `bucket`. Boundaries must be strictly ascending. `bucket` is shorthand for `derive <alias> = bucket(<expression>, [...], <default>) | group <alias>`, so it follows the rules of `group`.

```text
on users | bucket age by [0, 18, 40] default "40+" as band
```
```json
{"band":0,"count":1}
{"band":18,"count":2}
{"band":"40+","count":1}
```

#### `pivot`

```text
pivot
| rows    <field>, ...
| columns <field>, ...
| values  <field> [as <alias>] | <expression> as <alias>, ...
| aggregate sum|count|avg|average|min|minimum|max|maximum|first|last
| end
```

`values` takes one or more measures, written like `select` items: a field, or any expression with an alias (`CAFacture - COGS as marge`). With one measure, each cell is named after its column; with several, `<column>.<name>`, where the name is the alias or the field's last segment, and names must be unique. An expression value is computed by a hidden `derive` before the pivot, so it runs on the same streamed prefix.

Each directive appears exactly once. Stages may follow `pivot`, which emits one row per row key once its input is read: `| pivot | … | end | derive marge = coalesce(sales?, 0) - coalesce(costs?, 0)`; a cell with no row is absent (read it with `?`). Cells use the same aggregates as the compound `group`: sums stay integers when exact (a cell with no number is `0`), non-numeric values are ignored by `sum`/`avg`, missing values are skipped by every aggregate except `count`, which counts rows. It runs in `ogd` on a bounded executor of the `group` family: the prefix streams and only the aggregate cells are retained (one per row key and column). The state is charged to the query working set; a pivot with too many distinct rows × columns fails with `pivot cells exceed the governed query working set` (it does not spill, unlike `group`).

#### `window`

```text
window
| partition by <field>, ...
| order by <field> [asc|desc], ...
| lag|lead <field> [<rows>] [as <alias>]
| cumsum|first|last <field> [as <alias>]
| row_number [as <alias>]
| ...
| end
```

Adds to every row values computed over its **partition** (the rows whose `partition by` fields compare equal as `sort` compares them, so `1` and `1.0` share one; the whole input without it), taken in `order by` order. `order by` is required and appears once; `partition by` is optional; at least one function is required. Rows come out sorted by partition, then order, and keep all their fields.

| Function | Value on each row | Default alias |
|---|---|---|
| `lag <field> [<rows>]` | The field `<rows>` rows earlier in the partition (default 1); `null` before the first | `lag_<field>` |
| `lead <field> [<rows>]` | The field `<rows>` rows later; `null` after the last | `lead_<field>` |
| `cumsum <field>` | Running sum up to this row, as `group`'s `sum` (integer while exact, non-numbers ignored); `null` before the first number | `cumsum_<field>` |
| `first <field>`, `last <field>` | The field on the partition's first / last row | `first_<field>`, `last_<field>` |
| `row_number` | 1, 2, 3… within the partition | `row_number` |

Functions run in order, so one may read a field an earlier one added. `window` is a `sort` by the partition and order fields (stable: equal orders keep their input order; it spills like any sort), then a pass that holds one partition at a time in the query working set: a partition larger than it fails with `a window partition exceeds the governed query working set`.

```text
on ledger
| window
    | partition by entity, account, fiscal_year
    | order by month
    | lag balance as previous
    | cumsum movement as ytd
| end
| derive variation = balance - coalesce(previous, 0)
```

#### `fill`

```text
fill <field> from <start> to <end> step <step> [by <field>, ...] [with <field> = <value>, ...]
```

Adds, in each partition (the rows whose `by` fields compare equal, as `sort` compares them; the whole input without `by`), a row for every value of the grid `start`, `start + step`, … up to `end` that no row of the partition holds in `<field>`. A new row holds the partition's `by` fields, the grid value and the `with` values; every other field is missing. Rows already there pass unchanged, values off the grid included; rows come out sorted by the `by` fields, then `<field>`.

The grid steps by a positive number (`from 0 to 100 step 10`), or by a date unit for date bounds (`step "month"`, units as in `date_add`), written as `start` is: `"2026-01"` gives `"2026-02"`, `"2026-03"`…, a full date gives full dates, epoch milliseconds give milliseconds. A grid holds at most 100,000 values. Bounds may be parameters (`from $start to $end`). An empty input stays empty: a partition exists only through its rows.

```text
on ledger
| fill month from $opening to $closing step "month" by entity, account with balance = 0, movement = 0
| window | partition by entity, account | order by month | lag balance 12 as last_year | end
```

`fill` is a `sort` by the `by` fields and `<field>` (it spills like any sort), then a pass that holds nothing but the current partition's last row: the added rows stream out as the grid reaches them.

### Combining collections

#### `lookup` / `join`

```text
lookup <collection> [as <alias>]
| <read-only stages>
| into <field>
| end
```

For each input document, `lookup` runs the sub-pipeline over `<collection>` and stores the resulting documents as an **array** in `<field>`.

- `into <field>` is mandatory and appears exactly once.
- The sub-pipeline accepts read-only, non-terminal stages (`where`, `select`, `skip`, `limit`, …).
- `on`, `from`, `with` and `chunk` are rejected inside it.
- **Field scope.** Inside the sub-pipeline, `<lookup alias>.field` reads the joined document, `<outer alias>.field` reads the document being enriched, and unqualified fields read the joined document. The outer alias is the source alias (`on users as u`), or the alias of the enclosing `lookup`/`union` for nested lookups. Any expression can mix both documents: comparisons, arithmetic, `in`, functions.

```text
on users
| where name == "Alice"
| lookup orders
    | where status == "cancelled"
    | select order_id
    | into cancelled
| end
```
```json
{"active":true,"age":31,"cancelled":[{"order_id":5}],"city":"Paris","name":"Alice",…}
```

Row bounds work inside the sub-pipeline. For example, attach only the first paid order:

```text
on users | where name == "Alice" | lookup orders | where status == "paid" | first | select order_id | into first_paid_order | end
-- {…,"first_paid_order":[{"order_id":1}],"name":"Alice",…}
```

Correlated join:

```text
on users as u | lookup orders as o | where o.user == u.name | select order_id, status | into orders | end
```
```json
{"name":"Alice","orders":[{"order_id":1,"status":"paid"},{"order_id":2,"status":"paid"}],…}
{"name":"Bob","orders":[{"order_id":3,"status":"pending"}],…}
{"name":"Carol","orders":[{"order_id":4,"status":"paid"},{"order_id":6,"status":"paid"}],…}
{"name":"Dan","orders":[{"order_id":5,"status":"cancelled"}],…}
```

```text
on orders as o | lookup products as p | where p.code == o.product | select label, category | into product | end
-- {…,"order_id":1,"product":[{"category":"office","label":"Pen"}],…}
```

**One-line equality form.** When the join is a single equality, the sub-pipeline can be omitted:

```text
lookup <collection> [as <alias>] on <joined field> == <outer field> into <field>
```

```text
on orders | lookup products on code == product into product
on users as u | lookup orders as o on o.user == u.name into orders
```

The left side reads the joined collection and the right side the document being enriched; unqualified names are qualified accordingly. Rows where either field is missing do not match. It expands to `lookup <collection> | where exists(…) and exists(…) and <joined> == <outer> | into <field> | end`.

The correlation is not a special case: every stage of the sub-pipeline (`where`, `derive`, `select`…) reads fields through a *lookup field scope*, a field resolver that routes each alias to its document; `select o.region` writes `region`, as the source alias does at the top level. The sub-pipeline may hold blocking stages (`sort | first`, `group`, `distinct`…), which run on the rows joined to each outer row; the row-local stages after them still read the outer row (`lookup windows as w | group | by from, to | list w as ws | end | where from <= o.date | into hit | end`), and stages may follow the `end`.

**Execution.** When the sub-pipeline's first `where` begins (after `exists(…)` guards) with `joined == outer` or `starts_with(outer, joined)`, the joined collection is indexed once on the joined field: an equality probes one key, a prefix probes the outer text's prefixes of every key length present, so the longest-prefix rule reads only the matching rules. Several leading equalities make one composite key (`where s.entity == t.entity and s.date == t.date and s.version? == t.version? and …`), and a side read with `?` keys a missing field as `null`:

```text
on ledger as a
| lookup rules as r
    | where starts_with(a.account, r.prefix)
    | derive length = len(prefix) | sort length desc | first
    | select category
    | into rule
| end
```

When the sub-pipeline reads nothing of the outer row but known fields and is deterministic (no `sample`, no `now()`), its result is kept per value of those fields: outer rows sharing them (the same account on many months) share one run. Both the index and these results are held within the query working set; past it, rows are joined by scan, and results stop being kept. The joined documents are shared with the outer row, not copied.

#### `union`

```text
union
| on <collection> [as <alias>]
| <read-only stages>
| end
```

Emits the main stream, then the documents of the sub-pipeline. The first sub-stage must be `on <collection>`; `from` is rejected there because `from` is a keyword that cannot start a stage.

```text
on users | select name | union | on products | select label | end
```

A branch takes any read pipeline (`sort`, `group`, `lookup`, another `union`…): it then runs as a query of its own, governed like the main one, and any stage may follow the `end`: `on actual | group | by month | sum amount as a | end | union | on forecast | group | by month | sum amount as a | end | end | group | by month | sum a as a | end`.

### Writes

Every write runs in **one transaction**: all mutations commit, or none do. Write stages are **terminal**: nothing can follow them. The response contains the written documents and the mutation counters (`inserted`, `replaced`, `deleted`).

A write (`set`, `delete`, `into`) streams its rows into the transaction and charges what it holds to the query memory budget. Past it, the write fails. Only the documents a `set` returns stay in memory. og-core also runs several writes in one transaction (`Engine::execute_together`, used by checked imports): each reads the rows as they were before it.

#### `insert`

```text
insert { <key>: <value>, ... }
```

Inserts **one** document and returns it with its generated `_id`. Keys may be unquoted identifiers or JSON strings. Values are JSON (nested objects and arrays included). `insert` must be the only stage of its pipeline.

```text
insert [ { <key>: <value>, ... }, ... ]
```

With an array, `insert` writes every object in one transaction. It runs as a streaming `load` in `merge` mode: rows without `_id` get a fresh one, and a row carrying the `_id` of an existing document is merged into it. In a Place or App instance scope, each row gets the scope fields, and a row may not merge into a stored document.

```text
on users | insert {name: "Frank", age: 52, tags: ["new"], profile: {country: "IT"}, active: true}
-- {"_id":"01a0d795-…","active":true,"age":52,"name":"Frank","profile":{"country":"IT"},"tags":["new"]}
on users | insert [{name: "Gus", age: 20}, {name: "Hal", age: 33}]
```

#### `set`

```text
set <field.path> = <expression>, ...
```

Updates every document that reaches it. Target paths can be nested and are created when missing. Values are full expressions (arithmetic, functions, comparisons…). Like an SQL `UPDATE`, every value is computed from the document **as it was before the `set`**, so `set a = b, b = a` swaps two fields.

```text
on users | where name == "Bob" | set active = true, profile.level = "gold", age = 18
on users | where name == "Bob" | set nick = name               -- copy a field
on users | where name == "Bob" | set nick = null
on users | where name == "Bob" | set age = age + 1, nickname = lower(name), grade = if(profile.score >= 7, "B", "C")
on users | sort name | limit 1 | set top = true                -- only the first user by name
```

Writes are executed on the transactional (non-streamed) path, so `sort`, `skip` and `limit` can precede `set`.

#### `delete`

```text
delete
```

Deletes every document that reaches it. It takes no arguments.

```text
on orders | where status == "cancelled" | delete
on staging | delete                           -- empties the collection
```

#### `into` (alias `out`)

```text
into <collection>
```

Terminal. Inserts every row that reaches it into another collection, in one transaction, and returns no rows (the counters report `inserted`). Each stored row gets a fresh `_id`, even when the pipeline kept the source `_id`; the source collection is never modified.

- The target must differ from the source and cannot be a system collection (`_…`).
- Authorization requires write access on the target and read access on the source.
- The rows stream into the transaction; past the query memory budget, Glacier moves them to disk (see [Writes](#writes)).

```text
on users | where active | into archive
on orders | group | by region | sum qty as units | end | into region_totals
```

#### `load`: bulk import and upsert

```text
load
| with replace | update | merge
| chunk [ {…}, {…}, ... ]
| chunk [ ... ]
| end
```

- `with` sets the mode and is required exactly once. At least one `chunk` is required.
- Each chunk is a JSON array of objects.
- Rows without an `_id` get a fresh one. Rows with an `_id` are matched against existing documents:

| Mode | `_id` exists | `_id` absent from collection | Row without `_id` |
|---|---|---|---|
| `replace` | whole document replaced | inserted | inserted |
| `merge` | incoming fields merged over stored ones | inserted | inserted |
| `update` | incoming fields merged over stored ones | **error**, nothing committed | **error** |

- The whole load is atomic. An invalid `_id`, a duplicate `_id` in one request or an `update` miss rolls back every chunk.
- `load` must be the first stage.

```text
on tmp | load | with merge | chunk [{"_id":"01900000-0000-7000-8000-0000000000a1","a":1,"b":1}] | end
on tmp | load | with merge | chunk [{"_id":"01900000-0000-7000-8000-0000000000a1","b":2,"c":3}] | end
on tmp
-- {"_id":"01900000-0000-7000-8000-0000000000a1","a":1,"b":2,"c":3}

on tmp | load | with replace | chunk [{"_id":"01900000-0000-7000-8000-0000000000a1","z":9}] | end
on tmp
-- {"_id":"01900000-0000-7000-8000-0000000000a1","z":9}

on tmp | load | with merge | chunk [{"n":1},{"n":2}] | chunk [{"n":3}] | end   -- 3 inserts
```

The compact form `load <target>` (with arguments and no sub-pipeline) is a runtime hook. In `ogd` it re-writes the documents unchanged. For data import, use the streaming form above.

---

## 7. Pipeline composition rules

### 7.1 Planner rules (all hosts)

Each stage is declared once in the stage catalogue (`src/query/stage.rs`): name and aliases, accepted forms and
arguments, placement, effect and flow. The planner, plan validation and execution read it, and
`.query.stages` lists it. The rules below follow from it.

| Rule | Error |
|---|---|
| Nothing may follow a terminal stage: `set`, `delete`, `insert`, `load`, `count` | `follows a terminal operator` |
| `insert` must be the only stage (a batch `insert [...]` too, planned as `load`) | `insert … must be the only operator` |
| `load` must be the first stage | `load operator at index N must be first` |
| Unknown stage names are rejected | `unknown stage "frobnicate"` |
| Stage arguments are validated before any storage access | `invalid limit syntax … expected a non-negative integer` |

**Filters move towards the source.** The planner splits a `where a and b` into `where a | where b` (same result, same
errors, same order) and moves each filter before the stages that leave the fields it reads and the set of rows alone:
`derive`, `select`, `rename`, `drop` and `unwind` that do not write those fields, `lookup` that does not fill one of them,
`sort`; `group` and `pivot` when it reads only their keys (it then reads them as optional: a group key of a row without the
field is `null`), `window` when it reads only partition fields, and `union`, where it is also copied at the end of the
branch (and moves on there; not when it reads the branch's alias). It stops at any other stage (`limit`, `skip`, `first`,
`distinct`, `fill`…) and after another filter, whose order it keeps; a **guard** then goes on alone, past filters
too: for each comparison of a field with a constant, `type(f?) != "string" or f? == "x"`, which cannot fail and drops only
rows the filter drops without failing; it is kept when it passes a filter written apart. The filter itself stays where it
stopped, so no row reaches it that did not before, and no error is lost. Conjuncts of one filter that end side by side are
joined back, so a plan where nothing moves is the plan as written. A conditional whose condition is constant once the
parameters are bound is replaced by the branch it takes (`if(false, a, b)` is `b`, `true or x` is `true`, `coalesce(null, x)`
is `x`, `case` drops its false conditions), and `derive f = f` writes nothing: `derive entity = if($consolidate, null, entity)`
lets a filter on `entity` pass when `$consolidate` is `false`. `explain` shows the result. A filter written after a view therefore reaches its source:

```text
let $end = "2026-03", $format = "sig" | on view finance.statement | where entity == "E1"
```

reads `finance` with `type(entity?) != "string" or entity? == "E1"` right after its date filter (10.2 s → 0.25 s on 1.8 M
rows).

### 7.2 Execution shapes accepted by `ogd` (memory contract)

`ogd` never falls back to "load everything into RAM". Read queries must fit one of the shapes it can execute with bounded memory:

1. **Pure streaming.** Any mix of `where`, `select`/`project`, `derive`, `rename`, `drop`, `root`, `near`, `skip`, `limit`, `first`, `single`, followed optionally by `count`. Documents flow one by one, and any stage that declares a row bound (`limit`, `first`) stops the scan early.
2. **Streaming prefix, then blocking stages.** The blocking stages are `sort`, `distinct`, `group`, `pivot`, `sample N`,
   `lookup`, and `union` (a branch holding blocking stages runs as a query of its own). Any number of
   them may follow one another, each followed by row-local stages (`where`, `select`, `derive`, `skip`, `limit`, `first`, `single`,
   `unwind`, …), and the query may end with `count`.
   - The first blocking stage reads the streaming prefix. `group | sort` and `sort | distinct` (when the rows after it are bounded)
     run there as one stage.
   - Each later blocking stage takes the rows of the stages before it, once they have all been produced, and hands its own rows to
     the stages after it.
   - A leading `skip k` / `limit n` (or `first`) after a blocking stage bounds that stage, even when stages that turn each row into
     exactly one row (`select`, `derive`, `rename`, `drop`, `root`) sit in between; stages that can drop rows (`where`, `near`) stop
     the push: `sort k | skip 10 | limit 5` runs as a bounded Top-15, `sort k | first name` as a Top-1 followed by the projection.
   - `sort` and `group` spill sorted runs to disk when their state outgrows the memory they can get; `distinct` after another
     blocking stage keeps only the keys it has seen and emits the first row of each key as it comes.
   - Examples: `… | sort k | skip 10 | limit 5`, `… | group … | where count > 2 | sort count desc`, `… | sort CA desc | limit 100 |
     group client`, `… | distinct code | sort code`, `… | lookup … | end | group …`, `… | group … | sample 3 | count`.

   Every blocking stage of a query reserves memory as its state grows, and all of them together stay within the query budget of the
   memory profile. A query is admitted with a small reservation, so concurrent queries share the memory by what they use.

Any other shape is rejected before execution:

```text
query plan contains a blocking stage sequence without a bounded executor;
materialized fallback is forbidden by the memory contract
```

Writes (`set`, `delete`, `load`, `insert`) and system collections (`_…`) use the transactional path and are not restricted by these shapes.

---

## 8. System collections

Collections whose name starts with `_` are reserved. Four are **virtual**: they are computed on read from live engine state and can be queried with the full language.

| Collection | Content |
|---|---|
| `_memory` | One `global` row (profile, limits, current/peak bytes, pressure state, RSS, spill and external-group counters) and one row per memory class |
| `_query_memory` | Query admission budget (`global` row) and one row per running operation |
| `_memory_events` | Recent memory reservations and releases (bounded ring) |
| `_index_observations` | Per-query-shape execution statistics (fingerprint, access path, executions, scanned, returned, elapsed and average µs), the raw material for index decisions |

```text
on _memory | where scope == "global" | select profile, current_bytes, pressure_state
-- {"current_bytes":39009,"pressure_state":"unlimited","profile":"unlimited"}

on _index_observations | where collection == "users" | select fingerprint, access, executions, average_elapsed_us
```

Other system collections (`_apps`, `_devices`, `_identities`, `_permissions`, `_places`, …) are managed by `ogd` and protected by authorization.

---

## 9. Results and statistics

A query returns a stream of documents followed by execution statistics. With `ogcli` (without `-q`):

```json
{"documents":[…],
 "statistics":{"committed":false,"compact":false,"deleted":0,"filtered":1,"inserted":0,
               "replaced":0,"returned":3,"scanned":4,
               "strategies":["collection_scan"],"streamed":true,
               "timings_us":{"parse":29,"plan":116,"execute":436,"materialize_response":55}}}
```

| Field | Meaning |
|---|---|
| `scanned` / `filtered` / `returned` | Documents read, rejected by predicates, emitted |
| `inserted` / `replaced` / `deleted` / `committed` | Write counters, and whether a transaction was committed |
| `strategies` | Physical strategies used: `collection_scan`, `primary_key_lookup`, `in_memory_sort`, `streaming_count`, … |
| `streamed` | `true` when rows were streamed to the client as they were produced |
| `timings_us` | Per-phase timings in microseconds. Parsed plans are cached, so a repeated query skips planning |

Integers above 2³² are sent as floats on the wire so that JavaScript clients never lose precision silently. Integers beyond 2⁵³ are rejected on the wire.

---

## 10. ogcli and dot-operations

```text
ogcli [--address HOST:PORT] [--compact] [-q] ['QUERY']
```

- With a query argument, `ogcli` runs it and exits.
- With piped stdin, it runs one query per line.
- On a terminal, it opens a REPL.
- `-q` prints only the documents, and `--compact` prints one JSON document per line.
- `--param NAME=VALUE` binds `$NAME` (repeatable); see [5.4](#54-parameters-name).

A line starting with `.` sends a raw protocol operation (`.OPERATION [JSON payload]`). These are not query stages:

| Command | Effect |
|---|---|
| `.collections.list` | Lists collections, with `system`/`virtual` flags |
| `.collections.list {"stats": true}` | Adds document counts |
| `.storage.stats` | Storage backend, collection count, document count |
| `.backup.create {"name": "nightly"}` | Writes a consistent backup |
| `.backup.inspect {"name": "nightly"}` | Reads backup metadata (format, version, counts, source host) |
| `.backup.restore {"name": "nightly", "replace": true}` | Restores; `replace` drops existing data first |
| `.core.operations` | Lists every operation the node exposes |
| `.query.stages` | Lists every query stage with its forms, arguments, placement and properties |

---

## 11. Errors

Errors come from four layers, and each message carries a byte span or a stage location.

| Layer | When | Example |
|---|---|---|
| Lexer | Unknown character, bad string/number | `lex error: unexpected character '$' at 27..28` |
| Parser | Pipeline structure | `parse error: expected stage name, found operator at 11..12` (from `\| + x`), `unexpected 'end' outside a sub-pipeline` |
| Planner | Stage arguments, expressions, ordering | `unknown stage "aggregate"`, `unknown function \`nope\``, `function \`abs\` expects 1 argument(s), got 2`, `limit operator is duplicated` |
| Execution | Data-dependent | `field path "nickname" is missing`, `operation "==" is incompatible with values …`, `division by zero; use div(a, b, fallback) …`, `single expected exactly 1 row(s), found 0`, `streaming-load update target … does not exist` |

Planning errors are raised before any storage access. Write errors roll back the whole transaction.

---

## 12. Cookbook

```text
-- Top 3 customers by paid revenue
on orders | where status == "paid" | derive revenue = price * qty | group user as customer, revenue as revenue | sort revenue desc | limit 3

-- Distinct values of a field
on orders | select region | distinct

-- How many adults per city
on users | where age >= 18 | group city | sort count desc

-- Monthly units per region (two dimensions + measure)
on orders | group region, month, qty as units

-- Fetch one document by id (primary-key lookup)
on users | where _id == "01900000-0000-7000-8000-000000000003"

-- Semantic search: 5 nearest products within a category
on products | where category == "office" | near embedding, [0.1, 0.9, 0.0] | sort _distance | limit 5

-- Radius search
on products | near embedding, [0.1, 0.9, 0.0] | where _distance < 0.25

-- Flatten a sub-object into the result
on users | where name == "Alice" | root profile

-- Idempotent upsert of a batch (re-running it changes nothing)
on customers | load | with merge | chunk [{"_id":"01900000-0000-7000-8000-00000000c001","name":"ACME","tier":"gold"}] | end

-- Partial update that must hit existing rows only
on customers | load | with update | chunk [{"_id":"01900000-0000-7000-8000-00000000c001","tier":"platinum"}] | end

-- Mark the oldest pending order
on orders | where status == "pending" | sort order_id | limit 1 | set priority = true

-- Increment a counter
on counters | where name == "visits" | set value = value + 1

-- Orders with the product label, in one pass (correlated join)
on orders as o | lookup products as p | where p.code == o.product | select label | into product | end

-- Tag-based filtering and normalised output
on users | where "admin" in tags or len(tags) == 0 | select name, lower(trim(city)) as city

-- Safe ratio and bucketing
on orders | derive ratio = div(price, qty, 0), bucket = if(ratio > 50, "high", "low") | select order_id, ratio, bucket

-- Fetch exactly one document, or fail
on users | where name == "Alice" | single

-- Random sample of 100 paid orders
on orders | where status == "paid" | sample 100

-- Purge a collection
on staging | delete

-- Average basket and last order per customer
on orders | group | by user | avg price as basket | last order_id | end

-- Orders per month from an ISO timestamp field
on orders | derive month = date_format(created_at, "%Y-%m") | group month

-- Age bands
on users | bucket age by [0, 18, 30, 50, 120] as band

-- Parameterized lookup (with "params": {"name": "Alice"})
on users | where name == $name | lookup orders on user == name into orders

-- One threshold reused by several stages
let $big = 100 | on orders | where price * qty > $big | derive excess = price * qty - $big

-- See how a query will run
explain on orders | sort price * qty desc | limit 10

-- Engine health
on _memory | where scope == "global" | select pressure_state, current_bytes, peak_bytes, spill_runs_active
```

---

## 13. Known limitations (ogd 1.0)

This list is kept honest on purpose. Every item was observed on a live `ogd` build.

| Area | Limitation | Workaround |
|---|---|---|
| Parameters and `let` (`$name`) | Bound as literals before planning: each distinct set of values is planned (and cached) separately | None needed |
| Missing fields | A missing field fails the query, except in `exists`, `coalesce` and optional paths | `x?`, `exists(x) and …`, `coalesce(x, default)` |
| `pivot` | Aggregate cells must fit the query working set (no spill) | Filter or `select` before it to cut row/column cardinality |
| `fill` | Top level only; `<field>` and `by` are field paths; at most 100,000 grid values | `derive` computed fields first |
| `window` | Each partition must fit the query working set (the sort before it spills); top level only; partition and order keys are field paths | Partition more finely; `derive` computed keys first |
| `group` keys | Field paths only | `derive` the computed key first |
| `sort` | A key's values other than `null` must all be numbers, all texts or all booleans (values of different kinds do not order) | `to_string(…)` or `to_float(…)` in a `derive` first |
| `aggregate` | Not a stage | `group`, `count`, `pivot` |
| Date/Time | No native type; date functions read epoch milliseconds or ISO-8601 strings, in UTC | `date_*` functions |

---

## 14. Grammar summary

The pipeline layer (lexer and parser) is below. The planner then interprets each stage's argument text with the stage-specific grammar from [section 6](#6-stage-reference).

```ebnf
request      = [ "explain" ] , { let-header } , query ;   (* `$name` values are bound before parsing *)
let-header   = "let" , variable , "=" , expression , { "," , variable , "=" , expression } , "|" ;
variable     = "$" , identifier ;
query        = source , { "|" , stage } ;
source       = ( "on" | "from" ) , collection , [ "as" , identifier ] ;
collection   = identifier , { "." , identifier } ;

stage        = simple-stage | compound-stage ;
simple-stage = stage-name , arguments ;
compound-stage
             = ( "lookup" | "join" ) , arguments , sub-pipeline   (* unless the header contains `into` *)
             | "union" , sub-pipeline
             | "pivot" , sub-pipeline
             | ( "load" | "group" ) , sub-pipeline ;   (* header-less form only *)
sub-pipeline = { "|" , stage } , "|" , "end" ;
arguments    = { token } ;                        (* up to the next top-level "|", brackets balanced *)

expression   = or-expr | json-literal ;
or-expr      = and-expr , { ( "or" | "||" ) , and-expr } ;
and-expr     = not-expr , { ( "and" | "&&" ) , not-expr } ;
not-expr     = ( "not" | "!" ) , eq-expr | eq-expr ;
eq-expr      = rel-expr , { ( "==" | "!=" ) , rel-expr } ;
rel-expr     = add-expr , { ( "<" | "<=" | ">" | ">=" | "in" | "not" , "in" ) , add-expr
                          | [ "not" ] , "between" , add-expr , ( "and" | "&&" ) , add-expr } ;
add-expr     = mul-expr , { ( "+" | "-" ) , mul-expr } ;
mul-expr     = unary , { ( "*" | "/" | "%" ) , unary } ;
unary        = ( "-" | "+" ) , number                  (* folded into a signed literal *)
             | ( "-" | "+" ) , unary | not-expr | primary ;
primary      = literal | call | field-path | array | object | "(" , expression , ")" ;
object       = "{" , JSON members with quoted keys , "}" ;
call         = identifier , "(" , [ expression , { "," , expression } ] , ")" ;
array        = "[" , [ expression , { "," , expression } ] , "]" ;
field-path   = identifier , [ "?" ] , { "." , identifier , [ "?" ] } ;   (* any "?" makes the path optional *)
literal      = "null" | "true" | "false" | number | string ;

identifier   = ( letter | "_" ) , { letter | digit | "_" } ;   (* Unicode letters *)
number       = [ "+" | "-" ] , digit , { digit } , [ "." , digit , { digit } ] , [ ( "e" | "E" ) , [ "+" | "-" ] , digit , { digit } ]
             | "." , digit , { digit } ;
string       = '"' , { char | escape } , '"' ;
escape       = "\" , ( '"' | "\" | "/" | "b" | "f" | "n" | "r" | "t" ) ;
```
