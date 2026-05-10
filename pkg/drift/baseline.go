package drift

import (
	"encoding/json"
	"sync"
	"sync/atomic"
)

// FieldMeta describes a field in a frozen baseline.
type FieldMeta struct {
	Type     FieldType `json:"type"`
	Required bool      `json:"required"`
	Nullable bool      `json:"nullable"`
}

// BaselineConfig controls learning-phase behaviour.
type BaselineConfig struct {
	LearningSamples     int  `json:"learning_samples"`
	AllowOptionalFields bool `json:"allow_optional_fields"`
	AllowNullable       bool `json:"allow_nullable"`
}

// DefaultBaselineConfig returns production-ready defaults.
func DefaultBaselineConfig() BaselineConfig {
	return BaselineConfig{
		LearningSamples:     100,
		AllowOptionalFields: true,
		AllowNullable:       true,
	}
}

type fieldStat struct {
	presentCount int
	nullCount    int
	types        map[FieldType]int
}

type frozenSchema struct {
	fields map[string]FieldMeta
}

// Baseline accumulates message schemas during a learning phase and then freezes
// into an immutable snapshot. Post-freeze reads are lock-free.
type Baseline struct {
	mu      sync.Mutex
	cfg     BaselineConfig
	samples int
	stats   map[string]*fieldStat

	// snap is atomically swapped to non-nil exactly once, on freeze.
	snap atomic.Pointer[frozenSchema]
}

// NewBaseline creates an unfrozen baseline with the given config.
func NewBaseline(cfg BaselineConfig) *Baseline {
	return &Baseline{
		cfg:   cfg,
		stats: make(map[string]*fieldStat),
	}
}

// Observe adds a schema observation to the learning phase.
// Returns true exactly once: the call that caused the baseline to freeze.
// Returns false if already frozen (no-op) or still accumulating.
func (b *Baseline) Observe(s Schema) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.snap.Load() != nil {
		return false
	}

	b.samples++
	for path, typ := range s {
		st, ok := b.stats[path]
		if !ok {
			st = &fieldStat{types: make(map[FieldType]int)}
			b.stats[path] = st
		}
		st.presentCount++
		st.types[typ]++
		if typ == TypeNull {
			st.nullCount++
		}
	}

	if b.samples >= b.cfg.LearningSamples {
		b.freeze()
		return true
	}
	return false
}

func (b *Baseline) freeze() {
	fields := make(map[string]FieldMeta, len(b.stats))
	for path, stat := range b.stats {
		fields[path] = FieldMeta{
			Type:     dominantType(stat),
			Required: !b.cfg.AllowOptionalFields || stat.presentCount == b.samples,
			Nullable: b.cfg.AllowNullable && stat.nullCount > 0,
		}
	}
	b.snap.Store(&frozenSchema{fields: fields})
	b.stats = nil
}

func dominantType(stat *fieldStat) FieldType {
	best := TypeNull
	bestCount := 0
	for typ, count := range stat.types {
		if typ != TypeNull && count > bestCount {
			bestCount = count
			best = typ
		}
	}
	return best
}

// IsFrozen reports whether the learning phase is complete.
func (b *Baseline) IsFrozen() bool {
	return b.snap.Load() != nil
}

// Fields returns a copy of the frozen schema, or nil if not yet frozen.
func (b *Baseline) Fields() map[string]FieldMeta {
	snap := b.snap.Load()
	if snap == nil {
		return nil
	}
	cp := make(map[string]FieldMeta, len(snap.fields))
	for k, v := range snap.fields {
		cp[k] = v
	}
	return cp
}

// SampleCount returns the number of messages observed so far.
func (b *Baseline) SampleCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.samples
}

// MarshalJSON serialises the baseline for persistence.
func (b *Baseline) MarshalJSON() ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	snap := b.snap.Load()
	bj := baselineJSON{
		Config:  b.cfg,
		Samples: b.samples,
		Frozen:  snap != nil,
	}
	if snap != nil {
		bj.Schema = snap.fields
	}
	return json.Marshal(bj)
}

// UnmarshalJSON restores a baseline from persisted JSON.
func (b *Baseline) UnmarshalJSON(data []byte) error {
	var bj baselineJSON
	if err := json.Unmarshal(data, &bj); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cfg = bj.Config
	b.samples = bj.Samples
	if bj.Frozen && bj.Schema != nil {
		b.snap.Store(&frozenSchema{fields: bj.Schema})
	}
	return nil
}

type baselineJSON struct {
	Config  BaselineConfig       `json:"config"`
	Samples int                  `json:"samples"`
	Frozen  bool                 `json:"frozen"`
	Schema  map[string]FieldMeta `json:"schema,omitempty"`
}
