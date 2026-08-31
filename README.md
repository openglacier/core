<p align="center">
  <img src="github-static/logo.png" alt="openglacier" width="280">
</p>

<p align="center">
  <h1>openglacier - core</h1>
</p>

<p align="center">

![Version](https://img.shields.io/badge/version-0.40.1-blue)
[![Build](https://github.com/openglacier/core/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/openglacier/core/actions/workflows/build.yml)
[![Tests](https://github.com/openglacier/core/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/openglacier/core/actions/workflows/tests.yml)
[![Clippy](https://github.com/openglacier/core/actions/workflows/clippy.yml/badge.svg?branch=main)](https://github.com/openglacier/core/actions/workflows/clippy.yml)

![Architectures](https://img.shields.io/badge/arch-armhf_v7%20%7C%20armhf_v8%20%7C%20arm64%20%7C%20i386%20%7C%20amd64-informational)

</p>

> *Small core, long life.*

> * steady state 1 on 30/07/2026 @12:12

> * steady state 2 with persistent storage on 30/07/2026 @14:56

> * steady state 3 with caching and default index on 31/07/2026 @13:33

> * steady state 4 with memory management on 01/08/2026 @14:59

> * steady state 5 with a lot of bugfix on 01/08/2026 @22:34

> * steady state 6 with a lot of performance improvement on 24/08/2026 @13:28

> * steady state 7, public release codename antaapalaa on 28/08/2026 @17:32

## Purpose

core is the heart of the openglacier ecosystem.

It is responsible for maintaining a coherent and observable Place.

A Place is the representation of where you can perform task and watch things being done.

It is not a user interface. It is not a web server. It is not an application framework.

It is a long-lived execution engine that orchestrates resources, agents, missions and activities through events.

Every other component of openglacier depends on it.

It should therefore remain as simple, predictable and durable as possible.

> *The engine behind every Place.*

---

# Design philosophy

The Core is built around a simple idea:

> Reality exists independently of applications.

Applications give meaning to data.

The Core owns reality.

The Hub and the Apps merely expose different perspectives of it.

This distinction influences every architectural decision.

---

# Responsibilities

The Core is responsible for:

- maintaining Place consistency;
- managing Resources;
- orchestrating Agents;
- executing Missions;
- recording Activities;
- maintaining shared Knowledge;
- processing Events.

The Core is **not** responsible for:

- graphical interfaces;
- dashboards;
- forms;
- widgets;
- REST APIs;
- mobile applications;
- business-specific applications.

Those belong to the Hub or external adapters.

---

# Event-driven by design

Everything meaningful happens through events.

Events represent facts.

Not intentions.

Examples:

- ResourceCreated
- ResourceUpdated
- DeviceConnected
- MissionStarted
- AgentAsked
- KnowledgeUpdated
- ActivityRecorded

The Core reacts to events.

It does not depend on the origin of those events.

Whether an event comes from:

- the Hub,
- an MQTT connector,
- a REST endpoint,
- a scheduler,
- an AI agent,

it is processed identically.

---

# The Place

A Place is the fundamental execution context.

It owns:

- Resources
- Activities
- Shared Knowledge
- Missions
- Team

Applications are **not** part of the Core model.

Applications are perspectives built on top of the Place.

---

# Resources

Resources represent reality.

A Resource may be:

- a file;
- a database;
- an API;
- a device;
- a sensor;
- a document;
- a business object;
- a communication channel.

Resources should remain durable.

They must never depend on an application.

---

# Agents

Agents are autonomous collaborators.

They observe.

They reason.

They communicate.

They act.

An AI model is only one possible implementation of an Agent.

The Core manages Agents independently from the underlying AI technology.

---

# Knowledge

Knowledge exists at multiple levels.

Place Knowledge

Shared by the Place.

Persistent.

Observable.

Agent Knowledge

Private to an Agent.

Used to support reasoning.

Not necessarily shared.

This distinction must remain explicit.

---

# Activities

Activities tell the story of a Place.

Every significant event may generate an Activity.

Activities exist to explain.

Not to monitor people.

The goal is understanding.

Not surveillance.

---

# Missions

A Mission represents a coordinated objective.

It may involve:

- people;
- agents;
- resources;
- external systems.

The Core orchestrates Missions through events.

Not through imperative workflows.

---

# Architecture principles

The Core should remain:

## Small

Every additional concept increases complexity.

Prefer fewer concepts with stronger semantics.

## Deterministic

The same sequence of events should always produce the same result.

## Observable

Nothing important should happen silently.

Every decision should be explainable.

## Headless

The Core must never depend on a graphical interface.

It should execute identically:

- inside a server,
- inside a container,
- on a Raspberry Pi,
- during automated tests.

## Modular

New connectors.

New protocols.

New AI providers.

New storage engines.

All should be replaceable without modifying the Core.

---

# What the Core should never become

The Core must never become:

- an application server;
- a UI framework;
- a REST framework;
- a dependency injection playground;
- a collection of business applications.

Its role is to provide a stable execution engine.

Nothing more.

---

# Success criteria

A successful Core is one that:

- survives technological trends;
- remains understandable after many years;
- keeps its public API small;
- is easy to test;
- is easy to observe;
- is easy to extend.

The best Core is almost invisible.

It simply keeps the Place alive.

---

# Final thought

Software evolves.

Frameworks evolve.

Protocols evolve.

User interfaces evolve.

The Core should evolve as little as possible.

Because its purpose is not to follow technology.

Its purpose is to preserve trust.

# og-core Architecture

## Overview

`og-core` is the engine at the heart of the OG database ecosystem.

Its responsibility is to transform a textual query into an executable pipeline while remaining independent from transport protocols, storage implementations, and client applications.

```text
Query
  │
Lexer
  │
Parser
  │
AST
  │
Normalizer
  │
Logical Plan
  │
Physical Plan
  │
Executor
  │
Storage
```

## Design Principles

- Pipeline-oriented
- Declarative
- Strongly typed
- Storage independent
- Extensible
- AST-first architecture

## Modules

### Lexer
Converts source text into tokens.

### Parser
Builds the Abstract Syntax Tree (AST).

### AST
Canonical language representation independent from syntax.

### Normalizer
Transforms equivalent syntax into a canonical AST.

### Logical Planner
Builds a logical representation of the query.

### Physical Planner
Produces executable operators.

### Executor
Executes operators, orchestrates transactions, and manages pipeline execution.

Row-local operators execute document-by-document.
Set-level operators execute on complete intermediate result sets.

### Runtime
Implements operator semantics such as predicate evaluation, projection, transformations, loading, aggregation, and custom operators.

### Storage
Abstract persistence interface providing scans, transactions, insert, replace, and delete operations.

## Execution Flow

1. Lexer
2. Parser
3. AST
4. Normalizer
5. Logical Planner
6. Physical Planner
7. Executor
8. Storage

## Architectural Goals

- Stable AST
- Clear separation of responsibilities
- Predictable execution
- Minimal implicit behavior
- Multiple future syntaxes targeting the same AST
