# Spool — Janitor

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/build-Maven-C71A36.svg)](https://maven.apache.org/)

**Janitor** is the **Spool** framework module responsible for **inbox housekeeping**: it periodically scans the inbox to update persisted envelopes, quarantine failed ones, remove expired ones, and republish those that are stuck.

---

## Table of Contents

- [What it does](#what-it-does)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Examples](#examples)
  - [Basic Polling Janitor](#basic-polling-janitor)
  - [Custom Polling Interval](#custom-polling-interval)
  - [Watchdog Integration](#watchdog-integration)
  - [Custom Error Routing](#custom-error-routing)
- [Key Concepts](#key-concepts)
- [License](#license)

---

## What it does

```
Every N seconds:

InboxStatusQuery.findByStatus(CAPTURED)
      │
      ▼
┌─────────────────────────────────────────────────┐
│  Pipeline                                        │
│                                                  │
│  1. UpdatePersistedEnvelopes   → mark PERSISTED  │
│  2. QuarantineFailedEnvelopes  → mark QUARANTINED│
│  3. RemoveExpiredEnvelopes     → delete (> TTL)  │
│  4. RepublishStuckEnvelopes    → re-publish      │
│                                 (> threshold)    │
└─────────────────────────────────────────────────┘
      │
      ▼
EventPublisher / InboxUpdater / InboxEnvelopeRemover
```

The Janitor runs on a fixed schedule and keeps the inbox clean and healthy. Without it, stuck or expired envelopes would accumulate indefinitely.

---

## Installation

```xml
<dependency>
    <groupId>io.github.spool-framework</groupId>
    <artifactId>janitor</artifactId>
    <version>1.0.5-SNAPSHOT</version>
</dependency>
```

```xml
<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/Spool-FRAMEWORK/*</url>
    </repository>
</repositories>
```

> Requires **Java 21+**.

---

## Quick Start

```java
Janitor janitor = JanitorBuilderFactory.polling()
        .from(inboxStatusQuery)             // query inbox by status
        .with(inboxUpdater)                 // update envelope statuses
        .removeWith(inboxEnvelopeRemover)   // remove expired envelopes
        .on(eventBus)                       // republish stuck envelopes
        .subscribeWith(eventBus)            // listen for persistence events
        .every(Duration.ofSeconds(30))      // run every 30 seconds
        .withMillisecondsThreshold(60_000)  // republish if stuck > 60s
        .withMillisecondsTtl(3_600_000)     // remove if expired > 1h
        .create();

SpoolNode node = SpoolNode.create()
        .register(janitor);

node.start();
```

---

## Examples

### Basic Polling Janitor

```java
Janitor janitor = JanitorBuilderFactory.polling()
        .from(inboxStatusQuery)
        .with(inboxUpdater)
        .removeWith(inboxEnvelopeRemover)
        .on(eventBus)
        .subscribeWith(eventBus)
        .every(Duration.ofSeconds(30))
        .withMillisecondsThreshold(60_000)
        .withMillisecondsTtl(3_600_000)
        .create();
```

---

### Custom Polling Interval

Tune the schedule to match your throughput and latency requirements.

```java
Janitor janitor = JanitorBuilderFactory.polling()
        .from(inboxStatusQuery)
        .with(inboxUpdater)
        .removeWith(inboxEnvelopeRemover)
        .on(eventBus)
        .subscribeWith(eventBus)
        .every(Duration.ofSeconds(5))       // check every 5 seconds for high-throughput
        .withMillisecondsThreshold(30_000)  // republish stuck after 30s
        .withMillisecondsTtl(600_000)       // remove expired after 10 min
        .create();
```

---

### Watchdog Integration

Register with the Watchdog so the Janitor sends periodic heartbeats.

```java
Janitor janitor = JanitorBuilderFactory
        .watchdog("http://spool-watchdog:8080", "my-janitor")
        .polling()
        .from(inboxStatusQuery)
        .with(inboxUpdater)
        .removeWith(inboxEnvelopeRemover)
        .on(eventBus)
        .subscribeWith(eventBus)
        .every(Duration.ofSeconds(30))
        .withMillisecondsThreshold(60_000)
        .withMillisecondsTtl(3_600_000)
        .create();
```

---

### Custom Error Routing

```java
ErrorRouter errorRouter = new ErrorRouter()
        .on(InboxReadException.class, (e, ctx) ->
                logger.error("Failed to read inbox: {}", e.getMessage()))
        .on(InboxUpdateException.class, (e, ctx) ->
                logger.error("Failed to update envelope: {}", e.getMessage()));

Janitor janitor = JanitorBuilderFactory.polling()
        .from(inboxStatusQuery)
        .with(inboxUpdater)
        .removeWith(inboxEnvelopeRemover)
        .on(eventBus)
        .subscribeWith(eventBus)
        .every(Duration.ofSeconds(30))
        .withMillisecondsThreshold(60_000)
        .withMillisecondsTtl(3_600_000)
        .withErrorRouter(errorRouter)
        .create();
```

---

## Key Concepts

| Concept | Description |
|---|---|
| **`Janitor`** | Main module implementing `SpoolModule`. Lifecycle via `SpoolNode`. |
| **`JanitorBuilderFactory`** | DSL entry point. Returns a `PollingJanitorBuilder`. |
| **`InboxStatusQuery`** | Interface to query envelopes by status from the inbox. |
| **`InboxUpdater`** | Interface to update an envelope's status. |
| **`InboxEnvelopeRemover`** | Interface to delete an envelope from the inbox. |
| **`withMillisecondsThreshold`** | Envelopes stuck for longer than this are republished. |
| **`withMillisecondsTtl`** | Envelopes older than this are removed permanently. |

### What the Janitor does each cycle

| Step | Action |
|---|---|
| **UpdatePersisted** | Marks envelopes as `PERSISTED` once confirmed downstream. |
| **QuarantineFailed** | Moves envelopes that exceeded retries to `QUARANTINED` status. |
| **RemoveExpired** | Deletes envelopes older than the configured TTL. |
| **RepublishStuck** | Re-publishes envelopes that have been `CAPTURED` for longer than the threshold. |

---

## License

Distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
