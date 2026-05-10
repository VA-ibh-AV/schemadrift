// Kafka example — demonstrates per-topic schema drift detection.
//
// Start the broker first:
//
//	cd examples && docker compose up -d kafka
//
// Then run:
//
//	go run examples/kafka/main.go
//
// The producer sends 8 normal messages on two topics (learning phase = 5),
// then switches to a drifted schema. The consumer logs every drift event.
// Topic "alerts.firing" uses PolicyWarn (messages still reach the handler).
// Topic "metrics.server" uses PolicyBlock (drifted messages are rejected).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	kafkaadapter "github.com/VA-ibh-AV/go-schemadrift/adapters/kafka"
	"github.com/VA-ibh-AV/go-schemadrift/pkg/drift"
)

const broker = "localhost:9094"

// ── alert schemas ────────────────────────────────────────────────────────────

type AlertV1 struct {
	AlertID  string  `json:"alert_id"`
	Value    float64 `json:"value"`
	FiredAt  string  `json:"fired_at"` // RFC3339 string
	Host     string  `json:"host"`
	Resolved bool    `json:"resolved"`
}

// V2 drifts: Value becomes a string, FiredAt becomes Unix int, Severity added.
type AlertV2 struct {
	AlertID  string `json:"alert_id"`
	Value    string `json:"value"`    // DRIFT: number → string
	FiredAt  int64  `json:"fired_at"` // DRIFT: string → number
	Host     string `json:"host"`
	Severity string `json:"severity"` // DRIFT: new field
	// Resolved removed               // DRIFT: missing_field
}

// ── metric schemas ───────────────────────────────────────────────────────────

type MetricV1 struct {
	Name      string  `json:"name"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	Host      string  `json:"host"`
	Region    string  `json:"region"`
	Tags      []string `json:"tags"`
}

// V2 drifts: Value becomes a formatted string, Region removed, Datacenter added.
type MetricV2 struct {
	Name       string `json:"name"`
	Value      string `json:"value"`      // DRIFT: number → string
	Timestamp  int64  `json:"timestamp"`
	Host       string `json:"host"`
	Datacenter string `json:"datacenter"` // DRIFT: new field (Region removed)
	Tags       []string `json:"tags"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	interceptor, err := kafkaadapter.NewInterceptor(kafkaadapter.ConsumerConfig{
		Baseline: drift.BaselineConfig{
			LearningSamples:     5,
			AllowOptionalFields: false,
			AllowNullable:       false,
		},
		// OnDrift is called for every drifted message regardless of policy.
		// Override per-topic policy by wrapping the interceptor if needed.
		Policy: drift.PolicyConfig{
			Policy: drift.PolicyWarn, // default; metrics topic overrides below
			OnDrift: func(report drift.DriftReport) {
				log.Printf("⚠  DRIFT on [%s]:", report.Source)
				for _, ev := range report.Events {
					log.Printf("     %s", ev)
				}
			},
		},
		StoreDir: "/tmp/schemadrift-kafka-example",
	})
	if err != nil {
		log.Fatalf("interceptor: %v", err)
	}

	// Separate interceptor for the metrics topic with PolicyBlock.
	metricsInterceptor, err := kafkaadapter.NewInterceptor(kafkaadapter.ConsumerConfig{
		Baseline: drift.BaselineConfig{
			LearningSamples:     5,
			AllowOptionalFields: false,
			AllowNullable:       false,
		},
		Policy: drift.PolicyConfig{
			Policy: drift.PolicyBlock,
			OnDrift: func(report drift.DriftReport) {
				log.Printf("🚫 BLOCKED DRIFT on [%s]:", report.Source)
				for _, ev := range report.Events {
					log.Printf("     %s", ev)
				}
			},
		},
		StoreDir: "/tmp/schemadrift-kafka-example",
	})
	if err != nil {
		log.Fatalf("metricsInterceptor: %v", err)
	}

	go produce(ctx)
	consume(ctx, interceptor, metricsInterceptor)
}

// ── producer ─────────────────────────────────────────────────────────────────

func produce(ctx context.Context) {
	time.Sleep(2 * time.Second) // let readers connect first

	alertWriter := &kafkago.Writer{
		Addr:     kafkago.TCP(broker),
		Topic:    "alerts.firing",
		Balancer: &kafkago.LeastBytes{},
	}
	metricWriter := &kafkago.Writer{
		Addr:     kafkago.TCP(broker),
		Topic:    "metrics.server",
		Balancer: &kafkago.LeastBytes{},
	}
	defer alertWriter.Close()
	defer metricWriter.Close()

	log.Println("📤 producing 8 normal messages (learning phase = 5) …")

	for i := 0; i < 8; i++ {
		sendAlert(ctx, alertWriter, i, false)
		sendMetric(ctx, metricWriter, i, false)
		time.Sleep(300 * time.Millisecond)
	}

	log.Println("📤 producing 5 DRIFTED messages …")

	for i := 8; i < 13; i++ {
		sendAlert(ctx, alertWriter, i, true)
		sendMetric(ctx, metricWriter, i, true)
		time.Sleep(300 * time.Millisecond)
	}

	log.Println("📤 producer done")
}

func sendAlert(ctx context.Context, w *kafkago.Writer, i int, drifted bool) {
	var data []byte
	if !drifted {
		v := AlertV1{
			AlertID:  fmt.Sprintf("alert-%03d", i),
			Value:    float64(60 + i),
			FiredAt:  time.Now().UTC().Format(time.RFC3339),
			Host:     fmt.Sprintf("server-%d", i%4),
			Resolved: i%3 == 0,
		}
		data, _ = json.Marshal(v)
	} else {
		v := AlertV2{
			AlertID:  fmt.Sprintf("alert-%03d", i),
			Value:    fmt.Sprintf("%.1f%%", float64(60+i)),
			FiredAt:  time.Now().Unix(),
			Host:     fmt.Sprintf("server-%d", i%4),
			Severity: "critical",
		}
		data, _ = json.Marshal(v)
	}
	if err := w.WriteMessages(ctx, kafkago.Message{Value: data}); err != nil {
		log.Printf("alert write: %v", err)
	}
}

func sendMetric(ctx context.Context, w *kafkago.Writer, i int, drifted bool) {
	var data []byte
	if !drifted {
		v := MetricV1{
			Name:      fmt.Sprintf("cpu.usage.core%d", i%8),
			Value:     float64(30 + i*3),
			Timestamp: time.Now().Unix(),
			Host:      fmt.Sprintf("node-%d", i%5),
			Region:    "us-east-1",
			Tags:      []string{"env:prod", fmt.Sprintf("core:%d", i%8)},
		}
		data, _ = json.Marshal(v)
	} else {
		v := MetricV2{
			Name:       fmt.Sprintf("cpu.usage.core%d", i%8),
			Value:      fmt.Sprintf("%.1f%%", float64(30+i*3)),
			Timestamp:  time.Now().Unix(),
			Host:       fmt.Sprintf("node-%d", i%5),
			Datacenter: "us-east-1-az2",
			Tags:       []string{"env:prod"},
		}
		data, _ = json.Marshal(v)
	}
	if err := w.WriteMessages(ctx, kafkago.Message{Value: data}); err != nil {
		log.Printf("metric write: %v", err)
	}
}

// ── consumer ─────────────────────────────────────────────────────────────────

func consume(
	ctx context.Context,
	alertInterceptor *kafkaadapter.Interceptor,
	metricsInterceptor *kafkaadapter.Interceptor,
) {
	alertReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       "alerts.firing",
		StartOffset: kafkago.FirstOffset,
		MaxBytes:    10e6,
	})
	metricReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       "metrics.server",
		StartOffset: kafkago.FirstOffset,
		MaxBytes:    10e6,
	})
	defer alertReader.Close()
	defer metricReader.Close()

	log.Println("👂 consuming …")

	alertCh := make(chan kafkago.Message, 32)
	metricCh := make(chan kafkago.Message, 32)

	go readLoop(ctx, alertReader, alertCh)
	go readLoop(ctx, metricReader, metricCh)

	for {
		select {
		case <-ctx.Done():
			log.Println("👋 shutting down consumer")
			return
		case msg := <-alertCh:
			err := alertInterceptor.Handle(ctx, msg, func(ctx context.Context, m kafkago.Message) error {
				log.Printf("✅ alert handled: %s", m.Value)
				return nil
			})
			if err != nil {
				log.Printf("alert interceptor: %v", err)
			}
		case msg := <-metricCh:
			err := metricsInterceptor.Handle(ctx, msg, func(ctx context.Context, m kafkago.Message) error {
				log.Printf("✅ metric handled: %s", m.Value)
				return nil
			})
			if err != nil {
				// PolicyBlock returns an error — do not commit, surface to caller.
				log.Printf("❌ metric rejected (drift blocked): %v", err)
			}
		}
	}
}

func readLoop(ctx context.Context, r *kafkago.Reader, ch chan<- kafkago.Message) {
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("read error: %v", err)
			continue
		}
		select {
		case ch <- msg:
		case <-ctx.Done():
			return
		}
	}
}
