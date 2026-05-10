// Redis Streams example — demonstrates per-stream schema drift detection.
//
// Start Redis first:
//
//	cd examples && docker compose up -d redis
//
// Then run:
//
//	go run examples/redis/main.go
//
// Payloads are JSON-encoded in a "data" field so that numeric and boolean types
// are preserved (raw Redis stream values are always strings on the wire).
//
// Two streams are used:
//
//	"orders:updates" — order lifecycle events, PolicyQuarantine with file DLQ
//	"users:events"   — user action events, PolicyWarn
//
// The producer sends 8 normal messages per stream (learning phase = 5), then
// 5 drifted messages. Watch the logs for drift events and quarantine notices.
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

	goredis "github.com/redis/go-redis/v9"

	redisadapter "github.com/VA-ibh-AV/go-schemadrift/adapters/redis"
	"github.com/VA-ibh-AV/go-schemadrift/pkg/drift"
)

const redisAddr = "localhost:6379"

// ── order schemas ─────────────────────────────────────────────────────────────

type OrderV1 struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	Currency   string  `json:"currency"`
	Status     string  `json:"status"`
	CreatedAt  int64   `json:"created_at"`
	Items      int     `json:"items"`
}

// V2 drifts: Amount becomes a string, Currency removed, TaxAmount added, Items removed.
type OrderV2 struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     string  `json:"amount"`    // DRIFT: number → string
	Status     string  `json:"status"`
	CreatedAt  int64   `json:"created_at"`
	TaxAmount  float64 `json:"tax_amount"` // DRIFT: new field
	// Currency removed                    // DRIFT: missing_field
	// Items removed                       // DRIFT: missing_field
}

// ── user event schemas ────────────────────────────────────────────────────────

type UserEventV1 struct {
	UserID    string `json:"user_id"`
	Action    string `json:"action"`
	Resource  string `json:"resource"`
	Timestamp int64  `json:"timestamp"`
	Success   bool   `json:"success"`
	IP        string `json:"ip"`
}

// V2 drifts: Success becomes a string, IP removed, DeviceID + UserAgent added.
type UserEventV2 struct {
	UserID    string `json:"user_id"`
	Action    string `json:"action"`
	Resource  string `json:"resource"`
	Timestamp int64  `json:"timestamp"`
	Success   string `json:"success"`    // DRIFT: boolean → string
	DeviceID  string `json:"device_id"`  // DRIFT: new field
	UserAgent string `json:"user_agent"` // DRIFT: new field
	// IP removed                        // DRIFT: missing_field
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}
	defer rdb.Close()

	// File-backed DLQ for the orders stream.
	dlqFile, err := os.OpenFile("/tmp/schemadrift-redis-dlq.jsonl", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("dlq file: %v", err)
	}
	defer dlqFile.Close()
	dlq := drift.DLQSinkFunc(func(ctx context.Context, msg drift.QuarantineMessage) error {
		line, _ := json.Marshal(msg)
		_, err := fmt.Fprintln(dlqFile, string(line))
		return err
	})

	// Interceptor for orders — PolicyQuarantine.
	orderInterceptor, err := redisadapter.NewInterceptor(redisadapter.ConsumerConfig{
		Baseline: drift.BaselineConfig{
			LearningSamples:     5,
			AllowOptionalFields: false,
			AllowNullable:       false,
		},
		Policy: drift.PolicyConfig{
			Policy: drift.PolicyQuarantine,
			DLQ:    dlq,
			OnDrift: func(report drift.DriftReport) {
				log.Printf("🔴 QUARANTINE drift on [%s]:", report.Source)
				for _, ev := range report.Events {
					log.Printf("     %s", ev)
				}
			},
		},
		StoreDir:     "/tmp/schemadrift-redis-example",
		PayloadField: "data",
	})
	if err != nil {
		log.Fatalf("orderInterceptor: %v", err)
	}

	// Interceptor for user events — PolicyWarn.
	userInterceptor, err := redisadapter.NewInterceptor(redisadapter.ConsumerConfig{
		Baseline: drift.BaselineConfig{
			LearningSamples:     5,
			AllowOptionalFields: false,
			AllowNullable:       false,
		},
		Policy: drift.PolicyConfig{
			Policy: drift.PolicyWarn,
			OnDrift: func(report drift.DriftReport) {
				log.Printf("⚠  WARN drift on [%s]:", report.Source)
				for _, ev := range report.Events {
					log.Printf("     %s", ev)
				}
			},
		},
		StoreDir:     "/tmp/schemadrift-redis-example",
		PayloadField: "data",
	})
	if err != nil {
		log.Fatalf("userInterceptor: %v", err)
	}

	go produce(ctx, rdb)
	consume(ctx, rdb, orderInterceptor, userInterceptor)
}

// ── producer ─────────────────────────────────────────────────────────────────

func produce(ctx context.Context, rdb *goredis.Client) {
	time.Sleep(500 * time.Millisecond)
	log.Println("📤 producing 8 normal messages (learning phase = 5) …")

	for i := 0; i < 8; i++ {
		xadd(ctx, rdb, "orders:updates", marshalOrder(i, false))
		xadd(ctx, rdb, "users:events", marshalUserEvent(i, false))
		time.Sleep(250 * time.Millisecond)
	}

	log.Println("📤 producing 5 DRIFTED messages …")

	for i := 8; i < 13; i++ {
		xadd(ctx, rdb, "orders:updates", marshalOrder(i, true))
		xadd(ctx, rdb, "users:events", marshalUserEvent(i, true))
		time.Sleep(250 * time.Millisecond)
	}

	log.Println("📤 producer done — DLQ written to /tmp/schemadrift-redis-dlq.jsonl")
}

func marshalOrder(i int, drifted bool) []byte {
	if !drifted {
		v := OrderV1{
			OrderID:    fmt.Sprintf("ord-%05d", i),
			CustomerID: fmt.Sprintf("cust-%03d", i%50),
			Amount:     float64(100+i*7) + 0.99,
			Currency:   "USD",
			Status:     []string{"pending", "paid", "shipped"}[i%3],
			CreatedAt:  time.Now().Unix(),
			Items:       i%5 + 1,
		}
		b, _ := json.Marshal(v)
		return b
	}
	v := OrderV2{
		OrderID:    fmt.Sprintf("ord-%05d", i),
		CustomerID: fmt.Sprintf("cust-%03d", i%50),
		Amount:     fmt.Sprintf("$%.2f", float64(100+i*7)+0.99),
		Status:     []string{"pending", "paid", "shipped"}[i%3],
		CreatedAt:  time.Now().Unix(),
		TaxAmount:  float64(i) * 0.08,
	}
	b, _ := json.Marshal(v)
	return b
}

func marshalUserEvent(i int, drifted bool) []byte {
	if !drifted {
		v := UserEventV1{
			UserID:    fmt.Sprintf("user-%04d", i%200),
			Action:    []string{"login", "view", "purchase", "logout"}[i%4],
			Resource:  fmt.Sprintf("/api/v1/resource/%d", i%10),
			Timestamp: time.Now().Unix(),
			Success:   i%5 != 0,
			IP:        fmt.Sprintf("10.0.%d.%d", i%256, (i*7)%256),
		}
		b, _ := json.Marshal(v)
		return b
	}
	v := UserEventV2{
		UserID:    fmt.Sprintf("user-%04d", i%200),
		Action:    []string{"login", "view", "purchase", "logout"}[i%4],
		Resource:  fmt.Sprintf("/api/v1/resource/%d", i%10),
		Timestamp: time.Now().Unix(),
		Success:   fmt.Sprintf("%v", i%5 != 0),
		DeviceID:  fmt.Sprintf("dev-%08x", i*0xdeadbeef),
		UserAgent: "Mozilla/5.0 (compatible; schemadrift-example/1.0)",
	}
	b, _ := json.Marshal(v)
	return b
}

func xadd(ctx context.Context, rdb *goredis.Client, stream string, payload []byte) {
	if err := rdb.XAdd(ctx, &goredis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"data": string(payload)},
	}).Err(); err != nil {
		log.Printf("xadd %s: %v", stream, err)
	}
}

// ── consumer ─────────────────────────────────────────────────────────────────

func consume(
	ctx context.Context,
	rdb *goredis.Client,
	orderInterceptor *redisadapter.Interceptor,
	userInterceptor *redisadapter.Interceptor,
) {
	// Track last-seen IDs per stream.
	lastID := map[string]string{
		"orders:updates": "0",
		"users:events":   "0",
	}

	log.Println("👂 consuming …")

	for {
		select {
		case <-ctx.Done():
			log.Println("👋 shutting down consumer")
			return
		default:
		}

		streams, err := rdb.XRead(ctx, &goredis.XReadArgs{
			Streams: []string{
				"orders:updates", "users:events",
				lastID["orders:updates"], lastID["users:events"],
			},
			Count: 20,
			Block: time.Second,
		}).Result()

		if err == goredis.Nil {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("xread: %v", err)
			continue
		}

		for _, s := range streams {
			for _, msg := range s.Messages {
				lastID[s.Stream] = msg.ID
				switch s.Stream {
				case "orders:updates":
					if err := orderInterceptor.Handle(ctx, s.Stream, msg, func(ctx context.Context, m goredis.XMessage) error {
						log.Printf("✅ order handled [%s]: %v", m.ID, m.Values["data"])
						return nil
					}); err != nil {
						log.Printf("❌ order rejected: %v", err)
					}
				case "users:events":
					if err := userInterceptor.Handle(ctx, s.Stream, msg, func(ctx context.Context, m goredis.XMessage) error {
						log.Printf("✅ user event handled [%s]: %v", m.ID, m.Values["data"])
						return nil
					}); err != nil {
						log.Printf("user event error: %v", err)
					}
				}
			}
		}
	}
}
