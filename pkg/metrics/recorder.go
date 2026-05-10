package metrics

import "github.com/VA-ibh-AV/go-schemadrift/pkg/drift"

// Recorder is the interface for emitting per-message drift metrics.
// Implement this to plug in OTel, Prometheus, StatsD, or any other backend.
type Recorder interface {
	RecordMessage(source string, drifted bool, events []drift.DriftEvent)
}

// NoopRecorder discards all metrics. It is the default when no Recorder is supplied.
type NoopRecorder struct{}

func (NoopRecorder) RecordMessage(string, bool, []drift.DriftEvent) {}

// OnDriftFuncFromRecorder wraps a Recorder into the OnDrift callback format
// expected by drift.PolicyConfig.
func OnDriftFuncFromRecorder(r Recorder, source string) func(drift.DriftReport) {
	return func(report drift.DriftReport) {
		r.RecordMessage(source, true, report.Events)
	}
}
