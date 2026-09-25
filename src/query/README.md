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

> **Note:** the last line (`group … | select … | limit 10`) is valid, but `ogd` currently rejects it under its memory contract: after a `group`, only `sort` and/or `limit` may follow. See [Execution shapes accepted by `ogd`](#7-pipeline-composition-rules).

```text
on data
| group
    | by Article_Code as Produit
    | sum CAFacture as Total_CA
| end
| sort Total_CA desc
```

> **Note:** this compound form is supported: exactly one `by`, then any number of `sum` directives. `group` opens a sub-pipeline when its header is empty, which is the same syntactic property `load` has (see [`SubPipelinePolicy`](#53-compound-stages-and-end)).

For compact `group`, one item is a grouping key. With two or more items, every item except the last is a grouping dimension and the last item is summed. Therefore `group x, y, z` groups by `(x, y)` and sums `z`. `as` can rename every dimension and the summed value. Use the compound `group | by ... | sum ...` form when several aggregate measures are required.

Every stage has a single responsibility.

---

# Stages

| Stage | Purpose | Code | Tests | Comment |
|--------|---------|--------|-------|---------|
| on | Select source collection | :white_check_mark: | :white_check_mark: | Alias : from |
| where | Filter documents | :white_check_mark: | :white_check_mark: | |
| near | Vector nearby lookup | :white_check_mark: | :white_check_mark: | Adds `_distance` (cosine). Combine with `sort _distance \| limit k` |
| derive | Compute new fields | :white_check_mark: | :white_check_mark: | Any expression; assignments chain left to right |
| join | Load related documents | :white_check_mark: | :white_check_mark: | Alias : lookup. Correlated predicates: `o.user == u.name` |
| unwind | Expand arrays | :white_check_mark: | :white_check_mark: | Streams mid-chain: one row per array item, in order |
| root | Change root to specified field | :white_check_mark: | :white_check_mark: | |
| group | Build groups | :white_check_mark: | :white_check_mark: | Compact form, or compound `\| by … \| sum … \| end` for several measures |
| set | Update documents or chunks | :white_check_mark: | :white_check_mark: | Terminal, transactional, full expressions (`age = age + 1`) |
| insert | Create one or more documents | :white_check_mark: | | One object per query; use `load` for batches |
| load | Load documents or chunks | :white_check_mark: | | Streaming form `with replace\|update\|merge` + `chunk` |
| into | Insert the result into another collection | :white_check_mark: | | Terminal: `on users \| where active \| into archive`. One atomic insert with fresh `_id`s; target must differ from the source and cannot be a system collection. Needs write access on the target and read access on the source |
| pivot | Pivot data | :white_check_mark: | :white_check_mark: | Expect rows, columns, values and aggregate function. Keeps one aggregate cell per row key and column, never the rows |
| aggregate | Compute aggregates | not working | | Not a stage: use `group`, `count` or `pivot` |
| select | Project fields | :white_check_mark: | | Alias : project. Accepts `expression as alias` |
| rename | Rename fields | :white_check_mark: | | |
| drop | Remove fields | :white_check_mark: | | |
| distinct | Remove duplicates | :white_check_mark: | | |
| sort | Order documents | :white_check_mark: | | |
| skip | Skip documents | :white_check_mark: | | Alias : offset |
| limit | Limit result size | :white_check_mark: | | |
| first | Return first document | :white_check_mark: | :white_check_mark: | Declared row bound of 1: stops the scan, works in `lookup`/`union`, `sort \| first` is a Top-1 |
| sample | Provide a random set of n document(s) | :white_check_mark: | :white_check_mark: | Reservoir sampling: memory bounded by n |
| single | Return exactly one document | :white_check_mark: | :white_check_mark: | Fails with 0 or more than 1 row |
| count | Count results | :white_check_mark: | | `count as <alias>` |
| delete | Delete documents | :white_check_mark: | | Terminal, transactional |
| union | Append another pipeline | :white_check_mark: | | `\| union \| on <collection> … \| end` |
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

> **Current state:** done. One grammar and **one evaluator** (`query::eval`) serve every stage: `where`, `set`, `derive`, `select … as`, `lookup` correlation and projected scans. The language covers literals, field paths, arrays, comparisons, `in`/`not in`, boolean logic, arithmetic and function calls, and any construct can nest inside any other. Only parameters remain to do. See [Expressions](#4-expressions).

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

> **Current state:** 21 built-in functions are available in every stage (numeric, text, collections, conditionals, null handling). Names and argument counts are checked when the query is parsed. See [4.4 Functions](#44-functions). Aggregation over documents stays in stages: `count`, `group` (count and sums) and `pivot`.

---

# Parameters

Queries should support external parameters.

Example:

```text
where status == $status
```

Parameter binding belongs to the execution context rather than the parser.

> **Current state:** not implemented. `$` is rejected by the lexer (`unexpected character '$'`). Build the query text on the client and quote strings as JSON strings.

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
{"list_value":119.5,"region":"EU","units":8.0}
{"list_value":359.5,"region":"US","units":8.0}
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
{"month":"2026-01","units":7.0,"zone":"EU"}
{"month":"2026-01","units":7.0,"zone":"US"}
{"month":"2026-02","units":1.0,"zone":"EU"}
{"month":"2026-02","units":1.0,"zone":"US"}
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

- **Field paths** use dots: `profile.country`, `a.b.c`. Each segment must be a valid identifier. A key containing a space or hyphen can be *stored* (`insert {"display name": "Zed"}`) but cannot be *referenced* in an expression.
- **Collection names** can also be dotted (`on shop.orders`).

### 2.4 Keywords

These words are reserved by the pipeline lexer:

```text
on  from  as  where  set  lookup  join  union  load  pivot
into  with  chunk  replace  update  merge  end
and  or  not  true  false  null
```

Other stage names (`select`, `sort`, `group`, `limit`, …) are ordinary identifiers that are recognised in stage position.

Inside expressions, `in` is also reserved (membership operator). Function names are not reserved: `round(x)` is a call and `round` alone is a field.

### 2.5 Literals

| Literal | Examples | Notes |
|---|---|---|
| Null | `null` | |
| Boolean | `true`, `false` | |
| Integer | `0`, `42`, `18446744073709551615` | Stored as signed 64-bit when it fits, otherwise unsigned 64-bit |
| Float | `9.5`, `.75`, `1e3`, `1.5e-10` | Finite IEEE-754 doubles. `1` and `1.0` are distinct spellings but compare equal |
| String | `"Paris"`, `"a\"b"`, `"line\nbreak"` | Double quotes only. Escapes: `\" \\ \/ \b \f \n \r \t`. No raw newline inside a string |
| Array | `[1, 2]`, `["a", qty * 2, lower(name)]` | Elements are any expressions. Pure JSON arrays also work in `insert`, `near` and `load` payloads |
| Object (JSON) | `{"a": 1}`, `{name: "Alice", profile: {country: "FR"}}` | `insert` also accepts unquoted keys |

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
- `and`, `or` and `if` evaluate lazily, so `exists(x) and x > 3` and `if(exists(x), x, 0)` are safe.

```text
on users | select name, coalesce(nick, name) as display, exists(nickname) as has_nick
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
| Array | `[qty, qty * 2, "x"]` |
| Function call | `round(price * qty, 2)`, `coalesce(nick, name)` |
| JSON object literal | `{"a": 1}` (whole expression only) |

Every form nests inside every other: `round(div(price * qty, qty - 1, 0), 2)`, `upper(concat(name, "@", lower(city)))`, `max([a, b, c]) in allowed`.

### 4.2 Precedence

From lowest to highest. Binary operators are left-associative.

| Level | Operators |
|---|---|
| 1 | `or`, `\|\|` |
| 2 | `and`, `&&` |
| 3 | `==` `!=` |
| 4 | `<` `<=` `>` `>=` `in` `not in` |
| 5 | `+` `-` |
| 6 | `*` `/` `%` |
| 7 (prefix) | `not`/`!`, unary `-`, unary `+` |

Because prefix operators bind tightest, `not city == "Berlin"` parses as `(not city) == "Berlin"` and fails. Write **`not (city == "Berlin")`** or **`city != "Berlin"`** instead.

```text
on users | where age > 30 || profile.country == "DE" | select name
-- {"name":"Alice"}
-- {"name":"Carol"}

on users | where !(age < 18) && active | select name
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

| Function | Result | Example → result |
|---|---|---|
| `abs(x)` | Absolute value, integer kept | `abs(-qty)` → `3` |
| `sqrt(x)` | Square root (float); negative input is an error | `sqrt(16)` → `4.0` |
| `pow(x, y)` | Power; integer when exact | `pow(qty, 2)` → `9` |
| `round(x [, digits])` | Rounds half away from zero. Integer without `digits`, float with `digits` (0 to 15) | `round(13.333, 2)` → `13.33` |
| `floor(x)`, `ceil(x)` | Rounded down / up, as an integer | `floor(12.5)` → `12` |
| `min(a, b, …)`, `max(a, b, …)` | Smallest / largest argument; with one array argument, over its elements | `max(age, 30)`, `max(scores)` |
| `div(a, b [, fallback])` | Safe division: `fallback` when `b` is 0 (`null` without fallback), else `a / b` | `div(price, qty - 1, 0)` |
| `if(cond, then, else)` | Only the chosen branch is evaluated | `if(age >= 18, "adult", "minor")` |
| `coalesce(a, b, …)` | First argument that is present and not `null` | `coalesce(nick, name)` |
| `exists(x)` | Whether `x` is present (never fails) | `exists(nickname)` |
| `len(x)` | Characters of a string, items of an array, keys of an object | `len(tags)` → `2` |
| `lower(s)`, `upper(s)`, `trim(s)` | Text transforms | `upper(trim(name))` |
| `concat(a, b, …)` | Joins text (numbers and booleans are written as text, `null` is skipped); if every argument is an array, concatenates the arrays | `concat(name, "@", city)`, `concat(tags, ["new"])` |
| `contains(x, y)` | Substring test on strings, element test on arrays | `contains(name, "li")`, `contains(tags, "dev")` |
| `starts_with(s, p)`, `ends_with(s, p)` | Prefix / suffix test | `starts_with(city, "Pa")` |
| `type(x)` | `"null"`, `"bool"`, `"number"`, `"string"`, `"array"` or `"object"` | `type(profile)` → `"object"` |

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
on <collection> [as <alias>]
from <collection> [as <alias>]
```

- `on` and `from` are synonyms.
- The collection name may be dotted: `on shop.orders`.
- Collections are created implicitly by the first write. Reading a collection that does not exist returns nothing.
- A query may consist of the source alone: `on users` returns every document.
- The alias names the source. At the top level `u.age` and `age` are the same field (`on users as u | where u.age > 30`); the qualifier is dropped before planning, outside string literals, so an alias shadows a document field of the same name. In `lookup`/`union` sub-pipelines the alias names the outer document.

### 5.2 Simple stages

```text
| <stage-name> <arguments>
```

Everything up to the next top-level `|` is the stage's argument text. Parentheses, brackets and braces must balance. A `|` inside `[...]`, `{...}` or `(...)` does not end the stage.

### 5.3 Compound stages and `end`

Whether a stage opens a **sub-pipeline** is a syntactic property of its name (`SubPipelinePolicy` in the parser):

| Policy | Stages | Meaning |
|---|---|---|
| `Always` | `lookup`/`join`, `union`, `pivot` | Always compound |
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
sort <field.path> [asc|desc], ...
```

Stable ordering on one or more keys. The default direction is `asc`. `sort` is **blocking**: it must see every input document before emitting any. Followed by `limit N`, it runs as a bounded **Top-N** that keeps only N rows in memory.

```text
on orders | sort price desc | limit 2
on orders | derive total = price * qty | sort total desc | limit 3
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
-- {"qty":8.0,"region":"EU"}
-- {"qty":8.0,"region":"US"}

on orders | group region as zone, status as state, qty as units
-- {"state":"paid","units":3.0,"zone":"EU"} …

on orders | where status == "paid" | group user as customer, qty | sort qty desc
-- {"customer":"Carol","qty":5.0}
-- {"customer":"Alice","qty":3.0}

on orders | group product | sort count desc | limit 2
-- {"count":3,"product":"P1"}
-- {"count":2,"product":"P2"}
```

Groups are emitted in group-key order. `group | limit N` is bounded: only the N smallest live keys are kept (see the execution rule in Part I). Sums are floats. Summing a computed value takes two steps, `derive` then `group`, as in the tour.

**Compound form**, for several measures:

```text
group
| by <key> [as <alias>], ...
| sum <field> [as <alias>]
| sum <field> [as <alias>]
| end
```

`by` is required and appears exactly once. `sum` appears zero or more times; with no `sum`, the group emits `count`. The compound form is the compact form's big brother: it produces the same kind of rows, with any number of summed measures.

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
{"month":"2026-02","region":"US","revenue":250.0,"units":1.0}
{"month":"2026-02","region":"EU","revenue":99.5,"units":1.0}
{"month":"2026-01","region":"EU","revenue":70.0,"units":7.0}
{"month":"2026-01","region":"US","revenue":40.0,"units":4.0}
```

```text
on orders | group | by status | end          -- same as `group status`
```

#### `pivot`

```text
pivot
| rows    <field>, ...
| columns <field>, ...
| values  <field>, ...
| aggregate sum|count|avg|average|min|minimum|max|maximum|first|last
| end
```

Each directive appears exactly once. `pivot` is a terminal stage. It runs in `ogd` on a bounded executor of the `group` family: the prefix streams and only the aggregate cells are retained (one per row key and column). The state is charged to the query working set; a pivot with too many distinct rows × columns fails with `pivot cells exceed the governed query working set` (it does not spill, unlike `group`).

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

The correlation is not a special case: the sub-pipeline's filters are evaluated by the common evaluator over a *lookup field scope*, a field resolver that routes each alias to its document.

`ogd` limitations:
- `lookup` must be the **last** stage.
- Its sub-pipeline may only contain filter, projection, skip, limit and row-bound stages (`first`).

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

In `ogd`, only streaming stages (`where`, `select`, `skip`, `limit`, `count`, …) may follow a `union`.

### Writes

Every write runs in **one transaction**: all mutations commit, or none do. Write stages are **terminal**: nothing can follow them. The response contains the written documents and the mutation counters (`inserted`, `replaced`, `deleted`).

#### `insert`

```text
insert { <key>: <value>, ... }
```

Inserts **one** document and returns it with its generated `_id`. Keys may be unquoted identifiers or JSON strings. Values are JSON (nested objects and arrays included). `insert` must be the only stage of its pipeline. For several documents, use `load`.

```text
on users | insert {name: "Frank", age: 52, tags: ["new"], profile: {country: "IT"}, active: true}
-- {"_id":"01a0d795-…","active":true,"age":52,"name":"Frank","profile":{"country":"IT"},"tags":["new"]}
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

| Rule | Error |
|---|---|
| `select`, `sort`, `limit`, `skip`, `distinct`, `count`, `group`, `pivot`, `load`, `delete`, `insert` appear at most once | `logical <op> operator is duplicated` |
| Nothing may follow a terminal stage: `set`, `delete`, `insert`, `load`, `count`, `pivot` | `appears after a terminal operator` |
| `insert` must be the only stage | `insert … must be the only operator` |
| `load` must be the first stage | `logical load operator at index N must be first` |
| Unknown stage names are rejected | `unknown stage "frobnicate"` |
| Stage arguments are validated before any storage access | `invalid limit syntax … expected a non-negative integer` |

### 7.2 Execution shapes accepted by `ogd` (memory contract)

`ogd` never falls back to "load everything into RAM". Read queries must fit one of the shapes it can execute with bounded memory:

1. **Pure streaming.** Any mix of `where`, `select`/`project`, `derive`, `rename`, `drop`, `root`, `near`, `skip`, `limit`, `first`, `single`, followed optionally by `count`. Documents flow one by one, and any stage that declares a row bound (`limit`, `first`) stops the scan early.
2. **Streaming prefix + blocking core + streaming tail.** The core is one of:
   - `sort`, `distinct`, `group` or `pivot` (terminal, bounded by its cell count);
   - `group | sort`;
   - `sort | distinct` (only when the tail bounds the rows);
   - `sample N` (retains at most N rows);
   - `lookup` or `union` whose sub-pipeline is made only of filter, projection, skip, limit and row-bound stages.

   The tail is any run of row-local stages applied one row at a time to the core's output: `where`, `select`, `derive`, `skip`, `limit`, `first`, `single`, `unwind`, …, optionally ending with `count`. A leading `skip k` / `limit n` (or `first`) of the tail, possibly separated by projections, is pushed into the core: `sort k | skip 10 | limit 5` runs as a bounded Top-15, `sort k | first name` as a Top-1 followed by the projection.
   - `… | sort k | skip 10 | limit 5`, `… | sort k | first name`
   - `… | group … | select …`, `… | group … | count`
   - `… | sample N | select …`, `… | sample N | count`
   - `… | lookup … | end | where …`, `… | union | on … | end | limit N`

Any other shape is rejected before execution:

```text
query plan contains a blocking stage sequence without a bounded executor;
materialized fallback is forbidden by the memory contract
```

Examples that are rejected, with the equivalent query that is accepted:

| Rejected | Accepted equivalent |
|---|---|
| `sort price desc \| limit 1 \| select order_id` | `select order_id, price \| sort price desc \| first` |
| `sort order_id \| skip 2` | `skip 2` (the natural order is the `_id` order) |
| `sort price \| count` | `count` |

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

---

## 11. Errors

Errors come from four layers, and each message carries a byte span or a stage location.

| Layer | When | Example |
|---|---|---|
| Lexer | Unknown character, bad string/number | `lex error: unexpected character '$' at 27..28` |
| Parser | Pipeline structure | `parse error: expected stage name, found operator at 11..12` (from `\| + x`), `unexpected 'end' outside a sub-pipeline` |
| Planner | Stage arguments, expressions, ordering | `unknown stage "aggregate"`, `unknown function \`nope\``, `function \`abs\` expects 1 argument(s), got 2`, `logical limit operator is duplicated` |
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

-- Engine health
on _memory | where scope == "global" | select pressure_state, current_bytes, peak_bytes, spill_runs_active
```

---

## 13. Known limitations (ogd 1.0)

This list is kept honest on purpose. Every item was observed on a live `ogd` build.

| Area | Limitation | Workaround |
|---|---|---|
| Parameters (`$name`) | Not implemented | Client-side query construction |
| Missing fields | A missing field fails the query, except in `exists` / `coalesce` | `exists(x) and …`, `coalesce(x, default)` |
| `lookup`, `union` | Only a streaming tail (`where`, `select`, `limit`, `count`, …) may follow; a blocking stage after them is rejected | Put `sort`, `group`, `distinct` before them |
| `unwind` | Fails in streaming execution (`streaming custom operator unexpectedly expanded rows`) | None yet |
| `pivot` | Aggregate cells must fit the query working set (no spill) | Filter or `select` before it to cut row/column cardinality |
| Blocking stages | Only the shapes listed in [7.2](#72-execution-shapes-accepted-by-ogd-memory-contract) are accepted | Reorder as shown there |
| `insert` | One document per query | `load` for batches |
| `aggregate` | Not a stage | `group`, `count`, `pivot` |
| Date/Time | No native type | Unix milliseconds or ISO-8601 strings |

---

## 14. Grammar summary

The pipeline layer (lexer and parser) is below. The planner then interprets each stage's argument text with the stage-specific grammar from [section 6](#6-stage-reference).

```ebnf
query        = source , { "|" , stage } ;
source       = ( "on" | "from" ) , collection , [ "as" , identifier ] ;
collection   = identifier , { "." , identifier } ;

stage        = simple-stage | compound-stage ;
simple-stage = stage-name , arguments ;
compound-stage
             = ( "lookup" | "join" ) , arguments , sub-pipeline
             | "union" , sub-pipeline
             | "pivot" , sub-pipeline
             | ( "load" | "group" ) , sub-pipeline ;   (* header-less form only *)
sub-pipeline = { "|" , stage } , "|" , "end" ;
arguments    = { token } ;                        (* up to the next top-level "|", brackets balanced *)

expression   = or-expr | json-literal ;
or-expr      = and-expr , { ( "or" | "||" ) , and-expr } ;
and-expr     = eq-expr , { ( "and" | "&&" ) , eq-expr } ;
eq-expr      = rel-expr , { ( "==" | "!=" ) , rel-expr } ;
rel-expr     = add-expr , { ( "<" | "<=" | ">" | ">=" | "in" | "not" , "in" ) , add-expr } ;
add-expr     = mul-expr , { ( "+" | "-" ) , mul-expr } ;
mul-expr     = unary , { ( "*" | "/" | "%" ) , unary } ;
unary        = ( "-" | "+" ) , number                  (* folded into a signed literal *)
             | ( "not" | "!" | "-" | "+" ) , unary | primary ;
primary      = literal | call | field-path | array | "(" , expression , ")" ;
call         = identifier , "(" , [ expression , { "," , expression } ] , ")" ;
array        = "[" , [ expression , { "," , expression } ] , "]" ;
field-path   = identifier , { "." , identifier } ;
literal      = "null" | "true" | "false" | number | string ;

identifier   = ( letter | "_" ) , { letter | digit | "_" } ;   (* Unicode letters *)
number       = [ "+" | "-" ] , digit , { digit } , [ "." , digit , { digit } ] , [ ( "e" | "E" ) , [ "+" | "-" ] , digit , { digit } ]
             | "." , digit , { digit } ;
string       = '"' , { char | escape } , '"' ;
escape       = "\" , ( '"' | "\" | "/" | "b" | "f" | "n" | "r" | "t" ) ;
```
