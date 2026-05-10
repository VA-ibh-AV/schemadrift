# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
go build ./...          # build all packages
go test ./...           # run all tests
go test ./pkg/drift/... # core only
go test -run TestName ./pkg/drift/  # single test
go vet ./...            # lint
```

After adding a dependency: `go mod tidy`

### Running examples

```bash
# start brokers
cd examples && docker compose up -d && cd ..

# kafka example (Ctrl-C to stop)
go run examples/kafka/main.go

# redis example (Ctrl-C to stop)
go run examples/redis/main.go
```

Baseline files land in `/tmp/schemadrift-{kafka,redis}-example/`.  
Redis quarantined messages land in `/tmp/schemadrift-redis-dlq.jsonl`.  
Delete those directories to reset the learning phase.

## Architecture

Data flow per message:

```
raw []byte → Fingerprinter → Baseline.Observe (learning) or Detect (frozen) → Enforcer → handler / DLQ
```

### Core (`pkg/drift/`)

| File | Responsibility |
|---|---|
| `schema.go` | `Fingerprint([]byte) Schema` — one JSON decode pass; produces `map[fieldPath]FieldType` using dot-notation (`user.address.city`, `items[].id`) |
| `baseline.go` | Thread-safe accumulator. `Observe(Schema) bool` returns true exactly once (freeze event). Post-freeze reads are lock-free via `atomic.Pointer[frozenSchema]`. `IsFrozen()` + `Fields()` are the hot-path methods. |
| `detector.go` | Stateless `Detect(source, Schema, map[string]FieldMeta) DriftReport`. Emits four event kinds: `type_change`, `missing_field`, `new_field`, `nullability_change`. |
| `policy.go` | `Enforcer.Enforce` applies `PolicyWarn` / `PolicyBlock` / `PolicyQuarantine`. `DLQSink` interface + `DLQSinkFunc` adapter. |

### Persistence (`pkg/store/`)

`FileStore` does atomic write (`.tmp` + `os.Rename`). `Load` returns a fresh unfrozen baseline when the file does not exist. Interceptors call `Save` once, on the freeze event.

### Adapters (`adapters/`)

Each adapter maintains a `map[topic/stream → perSourceState]` with lazy init under a `sync.Mutex`. The pattern is identical across all three:

1. Call `IsFrozen()` — if not, `Observe` and pass through.
2. If frozen, `Detect` then `Enforcer.Enforce`.
3. On the freeze boundary message, pass through (not drift-checked against itself).

**Redis note**: Redis stream values are strings on the wire. Set `ConsumerConfig.PayloadField` to the key that holds a JSON string; the adapter fingerprints that JSON instead of the raw string map. The examples use `"data"` as the payload field.

## Key design decisions

- Module is `github.com/VA-ibh-AV/go-schemadrift` — update when publishing.
- Core (`pkg/`) has zero external dependencies; adapters depend only on their broker SDK.
- Baseline freeze is one-way and irreversible at runtime. Delete the store file to reset.
- `AllowOptionalFields: false` + `AllowNullable: false` = strictest enforcement; use `true` for production with variable schemas.
- The learning-phase trap: if drift starts during warm-up it gets baked into the baseline. Mitigate with a pre-seeded baseline file or by inspecting `baseline.Fields()` after freeze.
