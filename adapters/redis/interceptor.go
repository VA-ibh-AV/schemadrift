// Package redis provides a drift-detection interceptor for Redis Streams consumers.
// It maintains an independent baseline per stream.
//
// Redis stream field values are stored as strings on the wire, so all values
// appear as TypeString when the full map is fingerprinted. For proper type-level
// drift detection, set ConsumerConfig.PayloadField to the key whose value contains
// a JSON-encoded payload; the interceptor will fingerprint that JSON instead of
// the raw string map.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	"github.com/VA-ibh-AV/go-schemadrift/pkg/drift"
	"github.com/VA-ibh-AV/go-schemadrift/pkg/metrics"
	"github.com/VA-ibh-AV/go-schemadrift/pkg/store"
)

// ConsumerConfig is shared across all streams handled by one Interceptor.
type ConsumerConfig struct {
	// Baseline controls the learning phase for each stream.
	Baseline drift.BaselineConfig
	// Policy controls enforcement after a stream baseline is frozen.
	Policy drift.PolicyConfig
	// StoreDir is an optional directory for per-stream baseline files.
	StoreDir string
	// Metrics is an optional recorder for per-message metrics.
	Metrics metrics.Recorder
	// PayloadField, when set, names the XMessage.Values key whose value is a
	// JSON string. That JSON is fingerprinted instead of the raw string map.
	// Use this to preserve numeric/boolean type information across the wire.
	PayloadField string
}

// Interceptor wraps a Redis Streams consumer loop with per-stream drift detection.
type Interceptor struct {
	cfg     ConsumerConfig
	mu      sync.Mutex
	streams map[string]*streamState
}

type streamState struct {
	baseline  *drift.Baseline
	enforcer  *drift.Enforcer
	fileStore *store.FileStore
}

// NewInterceptor creates an Interceptor. Per-stream state is allocated lazily.
func NewInterceptor(cfg ConsumerConfig) (*Interceptor, error) {
	return &Interceptor{
		cfg:     cfg,
		streams: make(map[string]*streamState),
	}, nil
}

// Handle processes one Redis XMessage. handler is called only when the message
// passes drift enforcement.
func (i *Interceptor) Handle(
	ctx context.Context,
	streamName string,
	msg goredis.XMessage,
	handler func(ctx context.Context, m goredis.XMessage) error,
) error {
	ss, err := i.getOrCreate(streamName)
	if err != nil {
		return fmt.Errorf("redis: setup stream %q: %w", streamName, err)
	}

	payload, err := i.extractPayload(msg)
	if err != nil {
		return fmt.Errorf("redis: extract payload stream %q msg %s: %w", streamName, msg.ID, err)
	}

	schema, err := drift.Fingerprint(payload)
	if err != nil {
		return fmt.Errorf("redis: fingerprint stream %q: %w", streamName, err)
	}

	rec := i.cfg.Metrics
	if rec == nil {
		rec = metrics.NoopRecorder{}
	}

	if !ss.baseline.IsFrozen() {
		justFrozen := ss.baseline.Observe(schema)
		if justFrozen && ss.fileStore != nil {
			_ = ss.fileStore.Save(ss.baseline)
		}
		rec.RecordMessage(streamName, false, nil)
		return handler(ctx, msg)
	}

	report := drift.Detect(streamName, schema, ss.baseline.Fields())
	rec.RecordMessage(streamName, report.HasDrift(), report.Events)

	if err := ss.enforcer.Enforce(ctx, report, payload); err != nil {
		return err
	}
	return handler(ctx, msg)
}

// extractPayload returns the bytes to fingerprint for the given message.
// When PayloadField is set, it extracts and returns that field's value as-is.
// Otherwise it JSON-encodes the full Values map (note: all values will be strings).
func (i *Interceptor) extractPayload(msg goredis.XMessage) ([]byte, error) {
	if i.cfg.PayloadField != "" {
		raw, ok := msg.Values[i.cfg.PayloadField]
		if !ok {
			return nil, fmt.Errorf("payload field %q not found in message %s", i.cfg.PayloadField, msg.ID)
		}
		switch v := raw.(type) {
		case string:
			return []byte(v), nil
		case []byte:
			return v, nil
		default:
			return json.Marshal(v)
		}
	}
	return json.Marshal(msg.Values)
}

func (i *Interceptor) getOrCreate(streamName string) (*streamState, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if ss, ok := i.streams[streamName]; ok {
		return ss, nil
	}

	var b *drift.Baseline
	var fs *store.FileStore

	if i.cfg.StoreDir != "" {
		// Sanitize stream name for use as a filename (replace : with _)
		safeName := streamName
		for idx := 0; idx < len(safeName); idx++ {
			if safeName[idx] == ':' || safeName[idx] == '/' {
				safeName = safeName[:idx] + "_" + safeName[idx+1:]
			}
		}
		path := filepath.Join(i.cfg.StoreDir, safeName+".json")
		var err error
		fs, err = store.NewFileStore(path)
		if err != nil {
			return nil, err
		}
		b, err = fs.Load(i.cfg.Baseline)
		if err != nil {
			return nil, err
		}
	} else {
		b = drift.NewBaseline(i.cfg.Baseline)
	}

	ss := &streamState{
		baseline:  b,
		enforcer:  drift.NewEnforcer(i.cfg.Policy),
		fileStore: fs,
	}
	i.streams[streamName] = ss
	return ss, nil
}
