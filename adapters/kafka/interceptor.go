// Package kafka provides a drift-detection interceptor for kafka-go consumers.
// It maintains an independent baseline per topic.
package kafka

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/VA-ibh-AV/go-schemadrift/pkg/drift"
	"github.com/VA-ibh-AV/go-schemadrift/pkg/metrics"
	"github.com/VA-ibh-AV/go-schemadrift/pkg/store"
)

// ConsumerConfig is shared across all topics handled by one Interceptor.
type ConsumerConfig struct {
	// Baseline controls the learning phase for each topic.
	Baseline drift.BaselineConfig
	// Policy controls enforcement after a topic baseline is frozen.
	Policy drift.PolicyConfig
	// StoreDir is an optional directory for per-topic baseline files.
	// Files are named <topic>.json inside this directory.
	StoreDir string
	// Metrics is an optional recorder for per-message metrics.
	Metrics metrics.Recorder
}

// Interceptor wraps a kafka-go consumer loop with per-topic drift detection.
type Interceptor struct {
	cfg ConsumerConfig
	mu  sync.Mutex
	// keyed by topic name
	topics map[string]*topicState
}

type topicState struct {
	baseline  *drift.Baseline
	enforcer  *drift.Enforcer
	fileStore *store.FileStore
}

// NewInterceptor creates an Interceptor. Per-topic state is allocated lazily on
// the first message for each topic.
func NewInterceptor(cfg ConsumerConfig) (*Interceptor, error) {
	return &Interceptor{
		cfg:    cfg,
		topics: make(map[string]*topicState),
	}, nil
}

// Handle processes one kafka-go Message. handler is called only when the
// message passes drift enforcement.
func (i *Interceptor) Handle(
	ctx context.Context,
	msg kafkago.Message,
	handler func(ctx context.Context, m kafkago.Message) error,
) error {
	ts, err := i.getOrCreate(msg.Topic)
	if err != nil {
		return fmt.Errorf("kafka: setup topic %q: %w", msg.Topic, err)
	}

	schema, err := drift.Fingerprint(msg.Value)
	if err != nil {
		return fmt.Errorf("kafka: fingerprint topic %q: %w", msg.Topic, err)
	}

	rec := i.cfg.Metrics
	if rec == nil {
		rec = metrics.NoopRecorder{}
	}

	if !ts.baseline.IsFrozen() {
		justFrozen := ts.baseline.Observe(schema)
		if justFrozen && ts.fileStore != nil {
			_ = ts.fileStore.Save(ts.baseline)
		}
		rec.RecordMessage(msg.Topic, false, nil)
		return handler(ctx, msg)
	}

	report := drift.Detect(msg.Topic, schema, ts.baseline.Fields())
	rec.RecordMessage(msg.Topic, report.HasDrift(), report.Events)

	if err := ts.enforcer.Enforce(ctx, report, msg.Value); err != nil {
		return err
	}
	return handler(ctx, msg)
}

func (i *Interceptor) getOrCreate(topic string) (*topicState, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if ts, ok := i.topics[topic]; ok {
		return ts, nil
	}

	var b *drift.Baseline
	var fs *store.FileStore

	if i.cfg.StoreDir != "" {
		path := filepath.Join(i.cfg.StoreDir, topic+".json")
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

	ts := &topicState{
		baseline:  b,
		enforcer:  drift.NewEnforcer(i.cfg.Policy),
		fileStore: fs,
	}
	i.topics[topic] = ts
	return ts, nil
}
