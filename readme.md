# go-schemadrift

[![Go Reference](https://pkg.go.dev/badge/github.com/VA-ibh-AV/go-schemadrift.svg)](https://pkg.go.dev/github.com/VA-ibh-AV/go-schemadrift)
[![Go Report Card](https://goreportcard.com/badge/github.com/VA-ibh-AV/go-schemadrift)](https://goreportcard.com/report/github.com/VA-ibh-AV/go-schemadrift)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**In-process schema drift detection for JSON message streams.**

Runs inside your consumer process. Calls back into your code on the first drifted message. No schema registry, no sidecar, no broker change.

`go-schemadrift` sits between your broker consumer and your business logic. It learns the structure of your messages during a configurable warm-up period, then detects and acts on structural changes — type changes, missing fields, unexpected new fields — before they silently corrupt your pipeline.

Core library has zero external dependencies. Adapters for Kafka, Redis Streams, NATS, and anything that produces `[]byte`.

---

## The Problem

In microservice architectures, message producers and consumers are often owned by different teams. A producer can silently change a payload — renaming a field, changing a `float64` to a `string`, adding an undocumented field — and your consumer receives no error at the broker level. The message is delivered successfully. Your service just behaves wrongly.

```
Before (what your consumer expects):
  {"alert_id": "abc", "value": 87.5, "fired_at": "2025-05-08T10:00:00Z"}
                               ↑ float64               ↑ RFC3339 string

After (what the producer silently changed):
  {"alert_id": "abc", "value": "87.5", "fired_at": 1715161200}
                               ↑ string                ↑ Unix timestamp (integer)
```

By the time you notice, thousands of malformed messages have flowed through. At 50k agents each emitting events, even a 0.1% drift rate is 50 bad messages per second.

`go-schemadrift` catches this on the **first drifted message**.

---

## Why not X?

| Alternative | When to use it | When `go-schemadrift` fits better |
|---|---|---|
| **Confluent / Apicurio Schema Registry** | You're already on Avro/Protobuf and your producers cooperate | You're on JSON, can't change producers, or want runtime enforcement at the consumer |
| **JSON Schema validators** (`gojsonschema`, etc.) | You have a hand-written schema and want producers to conform to it | You don't *have* a schema — you want one inferred from real traffic, with no manual upkeep |
| **Pact / contract testing** | You can run synthetic contracts in CI between known consumer/producer pairs | The producer is operated by another team or vendor and you only see runtime behavior |
| **Data observability platforms** (Monte Carlo, Soda) | You're in a warehouse / analytics pipeline and can tolerate batch detection | You're on the operational hot path and need an in-process callback in microseconds |

The differentiator: **no extra infrastructure.** Drop one library into your consumer, get a callback on the first message that doesn't match what the stream has been sending.

---

## Features

- **Learning phase** — observes N messages to build a structural baseline before activating. No configuration of field names or types needed.
- **Drift detection** — catches type changes, missing required fields, unexpected new fields, and nullability violations.
- **Three enforcement policies** — `Warn` (log and continue), `Block` (return error), `Quarantine` (route to DLQ).
- **Baseline persistence** — saves the learned baseline to disk (JSON) so restarts don't reset the learning phase.
- **Per-source baselines** — adapters maintain an independent baseline per topic / stream / subject.
- **Optional field tolerance** — fields absent in some learning-phase messages are marked optional and don't trigger drift.
- **Pluggable metrics** — implement the `metrics.Recorder` interface to emit to OTel, Prometheus, StatsD, or anything else.
- **Zero external dependencies** in the core. Adapters depend only on the broker SDK you were already using.

---

## Installation

```bash
go get github.com/VA-ibh-AV/go-schemadrift
```

---

## Quick Start

### Generic (any broker)

```go
import (
    "github.com/VA-ibh-AV/go-schemadrift/adapters/generic"
    "github.com/VA-ibh-AV/go-schemadrift/pkg/drift"
)

interceptor, err := generic.New(generic.Config{
    Source: "alerts.events.firing",
    Baseline: drift.BaselineConfig{
        LearningSamples:     100,
        AllowOptionalFields: true,
        AllowNullable:       true,
    },
    Policy: drift.PolicyConfig{
        Policy: drift.PolicyWarn,
    },
    StorePath: "/var/lib/myservice/alerts-baseline.json",
}, func(ctx context.Context, payload []byte) error {
    // your business logic here
    return processAlert(payload)
})

// In your consume loop:
for msg := range messages {
    if err := interceptor.Handle(ctx, msg.Value); err != nil {
        log.Error("drift enforcement rejected message", "err", err)
    }
}
```

### Kafka — strict block mode

```go
import (
    "github.com/VA-ibh-AV/go-schemadrift/adapters/kafka"
    "github.com/VA-ibh-AV/go-schemadrift/pkg/drift"
)

interceptor, err := kafka.NewInterceptor(kafka.ConsumerConfig{
    Baseline: drift.BaselineConfig{LearningSamples: 100},
    Policy:   drift.PolicyConfig{Policy: drift.PolicyBlock},
    StoreDir: "/var/lib/myservice/baselines",
})

for {
    msg := consumer.Poll(100)
    err := interceptor.Handle(ctx, msg, func(ctx context.Context, m kafka.KafkaMessage) error {
        return processMessage(m.Value())
    })
    if err != nil {
        // PolicyBlock returns an error; do not commit offset, retry, or page.
        log.Error("drift detected, message blocked", "err", err)
    }
}
```

### Kafka — quarantine to DLQ

```go
interceptor, err := kafka.NewInterceptor(kafka.ConsumerConfig{
    Baseline: drift.BaselineConfig{LearningSamples: 100},
    Policy: drift.PolicyConfig{
        Policy: drift.PolicyQuarantine,
        DLQ:    myDLQSink,
    },
    StoreDir: "/var/lib/myservice/baselines",
})
```

---

## Policies

| Policy | Behaviour | Use when |
|---|---|---|
| `PolicyWarn` | Logs the drift report, message proceeds to handler | You want visibility without blocking |
| `PolicyBlock` | Returns an error, message does not reach handler | You want strict enforcement; caller handles the error |
| `PolicyQuarantine` | Writes message + report to a DLQ sink, returns an error | You want a replay queue for drifted messages |

### Implementing a DLQ Sink

```go
type DLQSink interface {
    Write(ctx context.Context, msg drift.QuarantineMessage) error
}
```

A `QuarantineMessage` contains the original payload (byte-for-byte), a full `DriftReport`, and a timestamp. Implement this to write to a Kafka dead letter topic, a NATS subject, a Redis stream, a database, or a file.

```go
// File-backed DLQ for development
dlq := drift.DLQSinkFunc(func(ctx context.Context, msg drift.QuarantineMessage) error {
    line, _ := json.Marshal(msg)
    _, err := fmt.Fprintln(dlqFile, string(line))
    return err
})
```

### Replaying quarantined messages

After a baseline update — say, a producer legitimately added a new field — you'll have a backlog in the DLQ. There's no built-in replay loop because the right path depends on your DLQ:

- **Kafka topic DLQ**: re-consume the DLQ topic with a fresh interceptor pointed at the *new* baseline. Messages that pass go to your handler; ones that still fail stay quarantined.
- **File-backed DLQ**: loop over the JSONL file and re-run each `QuarantineMessage.Payload` through the interceptor.
- **Database DLQ**: same pattern, ordered by quarantine timestamp.

Because `QuarantineMessage` carries the original payload byte-for-byte, any sink can be the source of truth for replay.

---

## Baseline Configuration

```go
drift.BaselineConfig{
    // Number of messages to observe before activating drift detection.
    // Higher values build a more representative baseline.
    // Recommended: 100–500 for production.
    LearningSamples: 100,

    // If true, fields absent in some learning-phase messages are marked
    // Optional and won't trigger DriftMissingField after freeze.
    AllowOptionalFields: true,

    // If true, fields observed as null in some messages are marked Nullable
    // and won't trigger DriftNullabilityChange when null after freeze.
    AllowNullable: true,
}
```

### The learning-phase trap

The baseline is whatever the stream looks like during warm-up. **If drift starts during the warm-up window, you bake the drifted shape into the baseline and won't catch it later.** Mitigations:

1. **Seed from a file** — if you already know the schema, skip the learning window entirely.
2. **Inspect the stability report** — on freeze, the baseline exposes per-field statistics (presence ratio, type-stability ratio). Log this and review before relying on the detector.

### Seeding from a file

If you already know your schema, you can pre-seed the baseline from a JSON file instead of waiting for the learning phase:

```go
fs, _ := store.NewFileStore("/path/to/baseline.json")
baseline, _ := fs.Load(drift.DefaultBaselineConfig())
// baseline is immediately frozen if the file contains enough samples
```

---

## Drift Events

Every `DriftReport` contains one or more `DriftEvent` values:

| Kind | Description | Example |
|---|---|---|
| `type_change` | A field's JSON type changed | `value` was `number`, got `string` |
| `missing_field` | A required field is absent | `host` expected but not present |
| `new_field` | A field not in the baseline appeared | `severity` seen for the first time |
| `nullability_change` | A non-nullable field was received as null | `alert_id` is null |

```go
drift.PolicyConfig{
    OnDrift: func(report drift.DriftReport) {
        for _, ev := range report.Events {
            fmt.Printf("[%s] field=%s expected=%s got=%s\n",
                ev.Kind, ev.Field, ev.Expected, ev.Got)
        }
    },
}
```

### How types are inferred from JSON

The fingerprinter walks parsed JSON and assigns each field path one of: `string`, `number`, `boolean`, `null`, `object`, `array`, `mixed`.

- **Numbers**: JSON does not distinguish integers from floats on the wire, and `encoding/json` decodes both into `float64`. `42` and `42.0` are reported as the same `number` type. If you need integer-level enforcement, validate at the handler.
- **Heterogeneous arrays** (`[1, "two", {}]`): the element type collapses to `mixed`, and any object members across elements are merged into one optional-keyed schema. A first-element-only mode is opt-in.
- **Empty arrays and null fields**: contribute no type information during learning. They are merged with the first concrete observation that follows.

---

## Performance and concurrency

- After freeze, a baseline read is lock-free on the hot path (immutable snapshot pointer; updates copy-on-write).
- During learning, writes are serialised by an `RWMutex`. Concurrent message ingestion is safe.
- The fingerprinter does one `json.Decoder` pass per message and allocates one map per nested object. For high-throughput topics, consider `SampleRate` (roadmap) once your stream is stable.
- Benchmarks: see `bench/` (TBD).

---

## Metrics

Implement `metrics.Recorder` to plug in your metrics backend:

```go
type Recorder interface {
    RecordMessage(source string, drifted bool, events []drift.DriftEvent)
}
```

**Example: OTel counter**
```go
type otelRecorder struct {
    messagesTotal otelmetric.Int64Counter
    driftTotal    otelmetric.Int64Counter
}

func (r *otelRecorder) RecordMessage(source string, drifted bool, events []drift.DriftEvent) {
    attrs := metric.WithAttributes(attribute.String("source", source))
    r.messagesTotal.Add(ctx, 1, attrs)
    if drifted {
        r.driftTotal.Add(ctx, int64(len(events)), attrs)
    }
}

// Wire it in:
cfg.Policy.OnDrift = metrics.OnDriftFuncFromRecorder(myRecorder, "alerts.events.firing")
```

Suggested metric names:
- `schemadrift.messages.total` — all messages processed
- `schemadrift.drift.messages` — messages with at least one drift event
- `schemadrift.drift.events` — total individual drift events (by `kind` label)

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Your Consumer                      │
└───────────────────────┬─────────────────────────────┘
                        │ raw []byte
                        ▼
┌─────────────────────────────────────────────────────┐
│        Interceptor (generic / kafka / redis / nats)  │
│                                                      │
│  ┌──────────────┐    ┌───────────┐    ┌──────────┐  │
│  │ Fingerprinter│───▶│ Baseline  │───▶│ Detector │  │
│  │              │    │ (learn /  │    │ (diff    │  │
│  │ walks JSON   │    │  freeze)  │    │  engine) │  │
│  └──────────────┘    └───────────┘    └────┬─────┘  │
│                           │                │         │
│                      FileStore        DriftReport    │
│                      (persist)             │         │
│                                      ┌────▼─────┐   │
│                                      │ Enforcer │   │
│                                      │ Warn /   │   │
│                                      │ Block /  │   │
│                                      │ Quarant. │   │
│                                      └────┬─────┘   │
└───────────────────────────────────────────┼─────────┘
                        │                   │
                        ▼ (pass)      DLQ Sink (quarantine)
              ┌─────────────────┐
              │  Your Handler   │
              │ (business logic)│
              └─────────────────┘
```

---

## Project Structure

```
go-schemadrift/
├── pkg/
│   ├── drift/
│   │   ├── schema.go      # Fingerprinter — JSON → Schema (field paths + types)
│   │   ├── baseline.go    # Baseline — learning phase, thread-safe, serializable
│   │   ├── detector.go    # Detector — diffs incoming schema against baseline
│   │   └── policy.go      # Enforcer — Warn / Block / Quarantine + DLQSink interface
│   ├── store/
│   │   └── file.go        # Atomic JSON persistence for baselines
│   └── metrics/
│       └── recorder.go    # Zero-dep Recorder interface
├── adapters/
│   ├── generic/           # Broker-agnostic interceptor (raw []byte)
│   └── kafka/             # Per-topic Kafka interceptor (broker-lib agnostic)
├── examples/
│   └── generic/           # Runnable end-to-end example
└── go.mod                 # Zero external dependencies
```

---

## Roadmap

**Adapters**
- [ ] Redis Streams adapter (`adapters/redis`)
- [ ] NATS JetStream adapter (`adapters/nats`)
- [ ] HTTP middleware adapter (detect drift in REST request bodies)
- [ ] gRPC server interceptor

**Schema evolution**
- [ ] Auto-promote `new_field` to baseline after N consecutive observations
- [ ] Expire fields not seen in a configurable time window
- [ ] Drift budget — escalate from `Warn` to `Block` once drift rate exceeds X% over a window
- [ ] Shadow / candidate baseline — run two baselines in parallel and report divergence (safe rollout for schema changes)

**Encodings**
- [ ] Avro support (with Confluent wire format detection)
- [ ] Protobuf support (descriptor-driven)

**Tooling**
- [ ] CLI: inspect, diff, and merge baseline files
- [ ] CLI: bootstrap a baseline by replaying historical messages from Kafka / Redis Streams
- [ ] Schema Registry interop — export a learned baseline as JSON Schema or as an Avro candidate

**Sinks and integrations**
- [ ] Built-in Slack / PagerDuty / generic webhook `OnDrift` callbacks
- [ ] Prometheus `Recorder` implementation (`recorders/prometheus`)
- [ ] OTel `Recorder` and trace-span emitter (`recorders/otel`)
- [ ] Drift heatmap exporter — periodic summary of which fields drifted most, when

**Performance**
- [ ] `SampleRate` for high-volume topics where statistical drift visibility is enough
- [ ] Benchmarks against `testcontainers-go` Kafka in CI

---

## Contributing

Pull requests are welcome. For large changes, open an issue first to discuss the design.

```bash
git clone https://github.com/VA-ibh-AV/go-schemadrift
cd go-schemadrift
go test ./...
```

---

## License

MIT
