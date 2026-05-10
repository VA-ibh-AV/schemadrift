// Package generic provides a broker-agnostic drift-detection interceptor that
// works with any message source that delivers raw []byte payloads.
package generic

import (
	"context"
	"fmt"

	"github.com/VA-ibh-AV/go-schemadrift/pkg/drift"
	"github.com/VA-ibh-AV/go-schemadrift/pkg/metrics"
	"github.com/VA-ibh-AV/go-schemadrift/pkg/store"
)

// Config is the full configuration for a generic Interceptor.
type Config struct {
	// Source is a logical name for this message stream (used in drift reports).
	Source string
	// Baseline controls the learning phase.
	Baseline drift.BaselineConfig
	// Policy controls enforcement after the baseline is frozen.
	Policy drift.PolicyConfig
	// StorePath is an optional file path for baseline persistence. When set, the
	// baseline is loaded from disk on startup and saved when it first freezes.
	StorePath string
	// Metrics is an optional recorder for per-message metrics.
	Metrics metrics.Recorder
}

// Interceptor wraps a user-supplied handler with drift detection.
type Interceptor struct {
	source    string
	baseline  *drift.Baseline
	enforcer  *drift.Enforcer
	fileStore *store.FileStore
	rec       metrics.Recorder
	handler   func(ctx context.Context, payload []byte) error
}

// New creates an Interceptor. handler must not be nil.
func New(cfg Config, handler func(ctx context.Context, payload []byte) error) (*Interceptor, error) {
	if handler == nil {
		return nil, fmt.Errorf("generic: handler must not be nil")
	}

	var b *drift.Baseline
	var fs *store.FileStore

	if cfg.StorePath != "" {
		var err error
		fs, err = store.NewFileStore(cfg.StorePath)
		if err != nil {
			return nil, fmt.Errorf("generic: store: %w", err)
		}
		b, err = fs.Load(cfg.Baseline)
		if err != nil {
			return nil, fmt.Errorf("generic: load baseline: %w", err)
		}
	} else {
		b = drift.NewBaseline(cfg.Baseline)
	}

	rec := cfg.Metrics
	if rec == nil {
		rec = metrics.NoopRecorder{}
	}

	return &Interceptor{
		source:    cfg.Source,
		baseline:  b,
		enforcer:  drift.NewEnforcer(cfg.Policy),
		fileStore: fs,
		rec:       rec,
		handler:   handler,
	}, nil
}

// Handle fingerprints payload, runs drift detection once the baseline is frozen,
// enforces the configured policy, and (if not blocked) calls the handler.
func (i *Interceptor) Handle(ctx context.Context, payload []byte) error {
	schema, err := drift.Fingerprint(payload)
	if err != nil {
		return fmt.Errorf("generic: %w", err)
	}

	// While still in the learning phase (or on the freeze boundary message),
	// pass the payload straight through without drift checking.
	if !i.baseline.IsFrozen() {
		justFrozen := i.baseline.Observe(schema)
		if justFrozen && i.fileStore != nil {
			_ = i.fileStore.Save(i.baseline) // best-effort; non-fatal
		}
		i.rec.RecordMessage(i.source, false, nil)
		return i.handler(ctx, payload)
	}

	report := drift.Detect(i.source, schema, i.baseline.Fields())
	i.rec.RecordMessage(i.source, report.HasDrift(), report.Events)

	if err := i.enforcer.Enforce(ctx, report, payload); err != nil {
		return err
	}
	return i.handler(ctx, payload)
}

// Baseline exposes the underlying Baseline for inspection or manual persistence.
func (i *Interceptor) Baseline() *drift.Baseline { return i.baseline }
