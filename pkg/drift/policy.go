package drift

import (
	"context"
	"fmt"
	"time"
)

// Policy controls what happens when drift is detected.
type Policy int

const (
	// PolicyWarn logs the drift report and lets the message through.
	PolicyWarn Policy = iota
	// PolicyBlock returns an error; the message does not reach the handler.
	PolicyBlock
	// PolicyQuarantine writes the message + report to the DLQ sink and returns an error.
	PolicyQuarantine
)

// QuarantineMessage is written to the DLQ for every quarantined payload.
type QuarantineMessage struct {
	Payload      []byte      `json:"payload"`
	Report       DriftReport `json:"report"`
	QuarantinedAt time.Time  `json:"quarantined_at"`
}

// DLQSink receives quarantined messages.
type DLQSink interface {
	Write(ctx context.Context, msg QuarantineMessage) error
}

// DLQSinkFunc adapts a plain function to the DLQSink interface.
type DLQSinkFunc func(ctx context.Context, msg QuarantineMessage) error

func (f DLQSinkFunc) Write(ctx context.Context, msg QuarantineMessage) error {
	return f(ctx, msg)
}

// PolicyConfig configures the Enforcer.
type PolicyConfig struct {
	Policy  Policy
	DLQ     DLQSink                // required when Policy == PolicyQuarantine
	OnDrift func(report DriftReport) // called before enforcement; optional
}

// Enforcer applies the configured policy to a DriftReport.
type Enforcer struct {
	cfg PolicyConfig
}

// NewEnforcer creates an Enforcer with the given config.
func NewEnforcer(cfg PolicyConfig) *Enforcer {
	return &Enforcer{cfg: cfg}
}

// Enforce applies the policy. Returns nil if the message should proceed
// (always for PolicyWarn, never for PolicyBlock/PolicyQuarantine with drift).
func (e *Enforcer) Enforce(ctx context.Context, report DriftReport, payload []byte) error {
	if !report.HasDrift() {
		return nil
	}
	if e.cfg.OnDrift != nil {
		e.cfg.OnDrift(report)
	}
	switch e.cfg.Policy {
	case PolicyWarn:
		return nil
	case PolicyBlock:
		return fmt.Errorf("schemadrift: drift on %q: %d event(s)", report.Source, len(report.Events))
	case PolicyQuarantine:
		if e.cfg.DLQ != nil {
			qm := QuarantineMessage{
				Payload:       payload,
				Report:        report,
				QuarantinedAt: time.Now().UTC(),
			}
			if err := e.cfg.DLQ.Write(ctx, qm); err != nil {
				return fmt.Errorf("schemadrift: quarantine write failed for %q: %w", report.Source, err)
			}
		}
		return fmt.Errorf("schemadrift: drift quarantined on %q: %d event(s)", report.Source, len(report.Events))
	}
	return nil
}
