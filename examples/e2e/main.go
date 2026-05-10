// E2E test: exercises Kafka and Redis adapters against live brokers.
//
// Run:
//
//	go run examples/e2e/main.go -kafka=100.97.194.79:9092 -redis=localhost:6379
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
	kafkago "github.com/segmentio/kafka-go"

	kafkaadapter "github.com/VA-ibh-AV/schemadrift/adapters/kafka"
	redisadapter "github.com/VA-ibh-AV/schemadrift/adapters/redis"
	"github.com/VA-ibh-AV/schemadrift/pkg/drift"
)

var (
	kafkaBroker = flag.String("kafka", "100.97.194.79:9092", "Kafka bootstrap broker")
	redisAddr   = flag.String("redis", "localhost:6379", "Redis address")
)

// run stamp makes all topic/stream names unique per run to avoid stale data.
var runStamp = fmt.Sprintf("%d", time.Now().Unix())

// ── result tracking ───────────────────────────────────────────────────────────

type result struct {
	name   string
	passed bool
	detail string
}

var results []result

func pass(name string) {
	results = append(results, result{name, true, ""})
	log.Printf("  ✅ PASS: %s", name)
}

func fail(name, detail string) {
	results = append(results, result{name, false, detail})
	log.Printf("  ❌ FAIL: %s — %s", name, detail)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func topic(suffix string) string { return "schemadrift-e2e-" + suffix + "-" + runStamp }

// ── schemas ───────────────────────────────────────────────────────────────────

type AlertV1 struct {
	AlertID  string  `json:"alert_id"`
	Value    float64 `json:"value"`
	FiredAt  string  `json:"fired_at"`
	Host     string  `json:"host"`
	Resolved bool    `json:"resolved"`
}
type AlertV2 struct { // type_change(value,fired_at), new(severity), missing(resolved)
	AlertID  string `json:"alert_id"`
	Value    string `json:"value"`
	FiredAt  int64  `json:"fired_at"`
	Host     string `json:"host"`
	Severity string `json:"severity"`
}

type MetricV1 struct {
	Name      string   `json:"name"`
	Value     float64  `json:"value"`
	Host      string   `json:"host"`
	Tags      []string `json:"tags"`
	Timestamp int64    `json:"timestamp"`
}
type MetricV2 struct { // type_change(value), new(datacenter), missing(nothing new here)
	Name       string   `json:"name"`
	Value      string   `json:"value"`
	Host       string   `json:"host"`
	Tags       []string `json:"tags"`
	Timestamp  int64    `json:"timestamp"`
	Datacenter string   `json:"datacenter"`
}

type OrderV1 struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	Currency   string  `json:"currency"`
	Status     string  `json:"status"`
	Items      int     `json:"items"`
}
type OrderV2 struct { // type_change(amount), new(tax_amount), missing(currency,items)
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     string  `json:"amount"`
	Status     string  `json:"status"`
	TaxAmount  float64 `json:"tax_amount"`
}

type UserEventV1 struct {
	UserID    string `json:"user_id"`
	Action    string `json:"action"`
	Timestamp int64  `json:"timestamp"`
	Success   bool   `json:"success"`
	IP        string `json:"ip"`
}
type UserEventV2 struct { // type_change(success), new(device_id), missing(ip)
	UserID    string `json:"user_id"`
	Action    string `json:"action"`
	Timestamp int64  `json:"timestamp"`
	Success   string `json:"success"`
	DeviceID  string `json:"device_id"`
}

// ── Kafka helpers ─────────────────────────────────────────────────────────────

func createTopic(ctx context.Context, t string) {
	conn, err := kafkago.DialContext(ctx, "tcp", *kafkaBroker)
	if err != nil {
		log.Fatalf("kafka dial: %v", err)
	}
	defer conn.Close()
	conn.CreateTopics(kafkago.TopicConfig{Topic: t, NumPartitions: 1, ReplicationFactor: 1})

	// Wait until the topic is visible in metadata (up to 10s).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(t)
		if err == nil && len(partitions) > 0 {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	log.Printf("warn: topic %s may not be ready", t)
}

func kproduce(ctx context.Context, t string, msgs [][]byte) {
	w := &kafkago.Writer{
		Addr:                   kafkago.TCP(*kafkaBroker),
		Topic:                  t,
		Balancer:               &kafkago.LeastBytes{},
		BatchTimeout:           10 * time.Millisecond,
		AllowAutoTopicCreation: true,
		MaxAttempts:            10,
	}
	defer w.Close()
	for _, m := range msgs {
		for attempt := 0; attempt < 5; attempt++ {
			if err := w.WriteMessages(ctx, kafkago.Message{Value: m}); err == nil {
				break
			} else if attempt == 4 {
				log.Printf("produce failed after retries: %v", err)
			} else {
				time.Sleep(200 * time.Millisecond)
			}
		}
	}
	// pause so messages are committed and available before consumer starts
	time.Sleep(500 * time.Millisecond)
}

// kconsume reads exactly n messages from a topic starting at offset 0.
// Returns after n messages or deadline.
func kconsume(ctx context.Context, t string, n int, fn func(kafkago.Message)) {
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     []string{*kafkaBroker},
		Topic:       t,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafkago.FirstOffset,
	})
	defer r.Close()

	deadline := time.Now().Add(20 * time.Second)
	read := 0
	for read < n && time.Now().Before(deadline) {
		rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		msg, err := r.ReadMessage(rctx)
		cancel()
		if err != nil {
			continue
		}
		fn(msg)
		read++
	}
}

// ── Kafka Test K1: AlertV1→V2, PolicyWarn ────────────────────────────────────

func testK1(ctx context.Context) {
	log.Println("--- K1: AlertV1→V2 type+missing+new, PolicyWarn ---")
	t := topic("alerts")
	createTopic(ctx, t)

	var msgs [][]byte
	for i := 0; i < 6; i++ { // 6 normal (freeze at 5, msg 6 passes through)
		msgs = append(msgs, mustJSON(AlertV1{
			AlertID: fmt.Sprintf("a%d", i), Value: float64(i + 10),
			FiredAt: time.Now().UTC().Format(time.RFC3339), Host: "h1", Resolved: true,
		}))
	}
	for i := 0; i < 4; i++ { // 4 drifted
		msgs = append(msgs, mustJSON(AlertV2{
			AlertID: fmt.Sprintf("a%d", i+6), Value: "high",
			FiredAt: time.Now().Unix(), Host: "h1", Severity: "crit",
		}))
	}
	kproduce(ctx, t, msgs)

	var driftCalls, handled atomic.Int64
	itx, _ := kafkaadapter.NewInterceptor(kafkaadapter.ConsumerConfig{
		Baseline: drift.BaselineConfig{LearningSamples: 5},
		Policy: drift.PolicyConfig{
			Policy:  drift.PolicyWarn,
			OnDrift: func(r drift.DriftReport) { driftCalls.Add(1) },
		},
		StoreDir: "/tmp/schemadrift-e2e/k1",
	})

	kconsume(ctx, t, 10, func(msg kafkago.Message) {
		itx.Handle(ctx, msg, func(ctx context.Context, m kafkago.Message) error {
			handled.Add(1)
			return nil
		})
	})

	if handled.Load() == 10 {
		pass("K1 all-10-messages-reach-handler (PolicyWarn never blocks)")
	} else {
		fail("K1 handler-count", fmt.Sprintf("want 10, got %d", handled.Load()))
	}
	if driftCalls.Load() == 4 {
		pass("K1 exactly-4-OnDrift-callbacks")
	} else {
		fail("K1 drift-count", fmt.Sprintf("want 4, got %d", driftCalls.Load()))
	}
}

// ── Kafka Test K2: MetricV1→V2, PolicyBlock ──────────────────────────────────

func testK2(ctx context.Context) {
	log.Println("--- K2: MetricV1→V2 type+new, PolicyBlock ---")
	t := topic("metrics")
	createTopic(ctx, t)

	var msgs [][]byte
	for i := 0; i < 6; i++ {
		msgs = append(msgs, mustJSON(MetricV1{
			Name: "cpu", Value: float64(i + 20), Host: "n1",
			Tags: []string{"env:prod"}, Timestamp: time.Now().Unix(),
		}))
	}
	for i := 0; i < 4; i++ {
		msgs = append(msgs, mustJSON(MetricV2{
			Name: "cpu", Value: "20%", Host: "n1",
			Tags: []string{"env:prod"}, Timestamp: time.Now().Unix(), Datacenter: "dc1",
		}))
	}
	kproduce(ctx, t, msgs)

	var handled, blocked, driftCalls atomic.Int64
	itx, _ := kafkaadapter.NewInterceptor(kafkaadapter.ConsumerConfig{
		Baseline: drift.BaselineConfig{LearningSamples: 5, AllowOptionalFields: false},
		Policy: drift.PolicyConfig{
			Policy:  drift.PolicyBlock,
			OnDrift: func(r drift.DriftReport) { driftCalls.Add(1) },
		},
		StoreDir: "/tmp/schemadrift-e2e/k2",
	})

	kconsume(ctx, t, 10, func(msg kafkago.Message) {
		err := itx.Handle(ctx, msg, func(ctx context.Context, m kafkago.Message) error {
			handled.Add(1)
			return nil
		})
		if err != nil {
			blocked.Add(1)
		}
	})

	if handled.Load() == 6 {
		pass("K2 6-normal-messages-reach-handler")
	} else {
		fail("K2 handler-count", fmt.Sprintf("want 6, got %d", handled.Load()))
	}
	if blocked.Load() == 4 {
		pass("K2 4-drifted-messages-blocked")
	} else {
		fail("K2 blocked-count", fmt.Sprintf("want 4, got %d", blocked.Load()))
	}
	if driftCalls.Load() == 4 {
		pass("K2 OnDrift-called-4x")
	} else {
		fail("K2 drift-count", fmt.Sprintf("want 4, got %d", driftCalls.Load()))
	}
}

// ── Kafka Test K3: optional+nullable fields don't trigger drift ───────────────

func testK3(ctx context.Context) {
	log.Println("--- K3: AllowOptionalFields+AllowNullable tolerance ---")
	type Full struct {
		ID    string   `json:"id"`
		Score float64  `json:"score"`
		Host  string   `json:"host"`
		Note  *string  `json:"note"`
		Tags  []string `json:"tags,omitempty"`
	}

	t := topic("tolerance")
	createTopic(ctx, t)

	note := "hello"
	var msgs [][]byte
	// 3 full + 3 partial (host absent, note null) → host=optional, note=nullable
	for i := 0; i < 3; i++ {
		msgs = append(msgs, mustJSON(Full{ID: fmt.Sprintf("t%d", i), Score: 1.0, Host: "h1", Note: &note, Tags: []string{"a"}}))
	}
	for i := 0; i < 3; i++ {
		msgs = append(msgs, mustJSON(struct {
			ID    string  `json:"id"`
			Score float64 `json:"score"`
			Note  *string `json:"note"`
		}{fmt.Sprintf("t%d", i+3), 2.0, nil}))
	}
	// Post-freeze: host absent + note null → should NOT drift
	for i := 0; i < 4; i++ {
		msgs = append(msgs, mustJSON(struct {
			ID    string  `json:"id"`
			Score float64 `json:"score"`
			Note  *string `json:"note"`
		}{fmt.Sprintf("t%d", i+6), 3.0, nil}))
	}
	kproduce(ctx, t, msgs)

	var driftCalls, handled atomic.Int64
	itx, _ := kafkaadapter.NewInterceptor(kafkaadapter.ConsumerConfig{
		Baseline: drift.BaselineConfig{
			LearningSamples:     6,
			AllowOptionalFields: true,
			AllowNullable:       true,
		},
		Policy: drift.PolicyConfig{
			Policy:  drift.PolicyBlock,
			OnDrift: func(r drift.DriftReport) { driftCalls.Add(1) },
		},
		StoreDir: "/tmp/schemadrift-e2e/k3",
	})

	kconsume(ctx, t, 10, func(msg kafkago.Message) {
		itx.Handle(ctx, msg, func(ctx context.Context, m kafkago.Message) error {
			handled.Add(1)
			return nil
		})
	})

	if driftCalls.Load() == 0 {
		pass("K3 optional+nullable fields cause zero drift")
	} else {
		fail("K3 unexpected-drift", fmt.Sprintf("%d drift events", driftCalls.Load()))
	}
	if handled.Load() == 10 {
		pass("K3 all-10-messages-handled")
	} else {
		fail("K3 handled-count", fmt.Sprintf("want 10, got %d", handled.Load()))
	}
}

// ── Redis helpers ─────────────────────────────────────────────────────────────

func xadd(ctx context.Context, rdb *goredis.Client, stream string, payload []byte) {
	rdb.XAdd(ctx, &goredis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"data": string(payload)},
	})
}

// xread reads up to n messages from a stream starting at id "0".
func xread(ctx context.Context, rdb *goredis.Client, stream string, n int, fn func(goredis.XMessage)) {
	lastID := "0"
	deadline := time.Now().Add(10 * time.Second)
	seen := 0
	for seen < n && time.Now().Before(deadline) {
		res, err := rdb.XRead(ctx, &goredis.XReadArgs{
			Streams: []string{stream, lastID},
			Count:   int64(n),
			Block:   time.Second,
		}).Result()
		if err != nil || len(res) == 0 {
			continue
		}
		for _, msg := range res[0].Messages {
			lastID = msg.ID
			fn(msg)
			seen++
		}
	}
}

// ── Redis Test R1: OrderV1→V2, PolicyQuarantine ───────────────────────────────

func testR1(ctx context.Context, rdb *goredis.Client) {
	log.Println("--- R1: OrderV1→V2 type+missing+new, PolicyQuarantine ---")
	stream := "schemadrift-e2e:orders:" + runStamp
	rdb.Del(ctx, stream)

	dlqPath := "/tmp/schemadrift-e2e/r1-dlq.jsonl"
	dlqFile, _ := os.Create(dlqPath)
	defer dlqFile.Close()

	var quarantined, handled, driftCalls atomic.Int64
	dlq := drift.DLQSinkFunc(func(ctx context.Context, msg drift.QuarantineMessage) error {
		line, _ := json.Marshal(msg)
		fmt.Fprintln(dlqFile, string(line))
		quarantined.Add(1)
		return nil
	})

	itx, _ := redisadapter.NewInterceptor(redisadapter.ConsumerConfig{
		Baseline: drift.BaselineConfig{LearningSamples: 5, AllowOptionalFields: false},
		Policy: drift.PolicyConfig{
			Policy:  drift.PolicyQuarantine,
			DLQ:     dlq,
			OnDrift: func(r drift.DriftReport) { driftCalls.Add(1) },
		},
		StoreDir:     "/tmp/schemadrift-e2e/r1",
		PayloadField: "data",
	})

	// Produce 6 V1 + 4 V2
	for i := 0; i < 6; i++ {
		xadd(ctx, rdb, stream, mustJSON(OrderV1{
			OrderID: fmt.Sprintf("o%d", i), CustomerID: "c1",
			Amount: float64(i+1) * 9.99, Currency: "USD", Status: "pending", Items: i + 1,
		}))
	}
	for i := 0; i < 4; i++ {
		xadd(ctx, rdb, stream, mustJSON(OrderV2{
			OrderID: fmt.Sprintf("o%d", i+6), CustomerID: "c1",
			Amount: "$9.99", Status: "paid", TaxAmount: 0.8,
		}))
	}

	xread(ctx, rdb, stream, 10, func(msg goredis.XMessage) {
		itx.Handle(ctx, stream, msg, func(ctx context.Context, m goredis.XMessage) error {
			handled.Add(1)
			return nil
		})
	})

	if handled.Load() == 6 {
		pass("R1 6-orders-reach-handler")
	} else {
		fail("R1 handler-count", fmt.Sprintf("want 6, got %d", handled.Load()))
	}
	if quarantined.Load() == 4 {
		pass("R1 4-orders-quarantined-to-DLQ")
	} else {
		fail("R1 quarantine-count", fmt.Sprintf("want 4, got %d", quarantined.Load()))
	}
	dlqFile.Close()
	data, _ := os.ReadFile(dlqPath)
	lines := 0
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.TrimSpace(line) != "" {
			lines++
		}
	}
	if lines == 4 {
		pass("R1 DLQ-file-has-4-JSONL-lines")
	} else {
		fail("R1 dlq-file-lines", fmt.Sprintf("want 4, got %d", lines))
	}
	// Verify DLQ entries are valid JSON with payload + report
	for i, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var qm drift.QuarantineMessage
		if err := json.Unmarshal([]byte(line), &qm); err != nil {
			fail(fmt.Sprintf("R1 dlq-line-%d-valid-json", i), err.Error())
			return
		}
		if len(qm.Report.Events) == 0 {
			fail(fmt.Sprintf("R1 dlq-line-%d-has-events", i), "empty events")
			return
		}
	}
	pass("R1 DLQ-entries-contain-valid-DriftReport")
}

// ── Redis Test R2: UserEventV1→V2, PolicyWarn ────────────────────────────────

func testR2(ctx context.Context, rdb *goredis.Client) {
	log.Println("--- R2: UserEventV1→V2 type+missing+new, PolicyWarn ---")
	stream := "schemadrift-e2e:users:" + runStamp
	rdb.Del(ctx, stream)

	var driftCalls, handled atomic.Int64
	itx, _ := redisadapter.NewInterceptor(redisadapter.ConsumerConfig{
		Baseline: drift.BaselineConfig{LearningSamples: 5, AllowOptionalFields: false},
		Policy: drift.PolicyConfig{
			Policy: drift.PolicyWarn,
			OnDrift: func(r drift.DriftReport) {
				driftCalls.Add(1)
				for _, ev := range r.Events {
					log.Printf("    R2 event: %s", ev)
				}
			},
		},
		StoreDir:     "/tmp/schemadrift-e2e/r2",
		PayloadField: "data",
	})

	for i := 0; i < 6; i++ {
		xadd(ctx, rdb, stream, mustJSON(UserEventV1{
			UserID: fmt.Sprintf("u%d", i), Action: "login",
			Timestamp: time.Now().Unix(), Success: true, IP: "10.0.0.1",
		}))
	}
	for i := 0; i < 4; i++ {
		xadd(ctx, rdb, stream, mustJSON(UserEventV2{
			UserID: fmt.Sprintf("u%d", i+6), Action: "login",
			Timestamp: time.Now().Unix(), Success: "true", DeviceID: "dev-abc",
		}))
	}

	xread(ctx, rdb, stream, 10, func(msg goredis.XMessage) {
		itx.Handle(ctx, stream, msg, func(ctx context.Context, m goredis.XMessage) error {
			handled.Add(1)
			return nil
		})
	})

	if handled.Load() == 10 {
		pass("R2 all-10-user-events-reach-handler (PolicyWarn)")
	} else {
		fail("R2 handler-count", fmt.Sprintf("want 10, got %d", handled.Load()))
	}
	if driftCalls.Load() == 4 {
		pass("R2 exactly-4-drift-callbacks")
	} else {
		fail("R2 drift-count", fmt.Sprintf("want 4, got %d", driftCalls.Load()))
	}
}

// ── Redis Test R3: nullable+optional tolerance ────────────────────────────────

func testR3(ctx context.Context, rdb *goredis.Client) {
	log.Println("--- R3: nullable+optional tolerance ---")
	stream := "schemadrift-e2e:tolerance:" + runStamp
	rdb.Del(ctx, stream)

	type EventFull struct {
		ID    string  `json:"id"`
		Score float64 `json:"score"`
		Host  string  `json:"host"`
		Note  *string `json:"note"`
	}
	type EventPartial struct {
		ID    string  `json:"id"`
		Score float64 `json:"score"`
		Note  *string `json:"note"`
	}

	note := "present"
	var driftCalls atomic.Int64
	itx, _ := redisadapter.NewInterceptor(redisadapter.ConsumerConfig{
		Baseline: drift.BaselineConfig{
			LearningSamples:     6,
			AllowOptionalFields: true,
			AllowNullable:       true,
		},
		Policy: drift.PolicyConfig{
			Policy:  drift.PolicyBlock,
			OnDrift: func(r drift.DriftReport) { driftCalls.Add(1) },
		},
		StoreDir:     "/tmp/schemadrift-e2e/r3",
		PayloadField: "data",
	})

	// 3 full (host present, note set) + 3 partial (host absent, note null)
	for i := 0; i < 3; i++ {
		xadd(ctx, rdb, stream, mustJSON(EventFull{fmt.Sprintf("e%d", i), 1.0, "h1", &note}))
	}
	for i := 0; i < 3; i++ {
		xadd(ctx, rdb, stream, mustJSON(EventPartial{fmt.Sprintf("e%d", i+3), 2.0, nil}))
	}
	// Post-freeze: host absent + note null → no drift expected
	for i := 0; i < 4; i++ {
		xadd(ctx, rdb, stream, mustJSON(EventPartial{fmt.Sprintf("e%d", i+6), 3.0, nil}))
	}

	xread(ctx, rdb, stream, 10, func(msg goredis.XMessage) {
		itx.Handle(ctx, stream, msg, func(ctx context.Context, m goredis.XMessage) error { return nil })
	})

	if driftCalls.Load() == 0 {
		pass("R3 optional+nullable fields cause zero drift")
	} else {
		fail("R3 unexpected-drift", fmt.Sprintf("%d events", driftCalls.Load()))
	}
}

// ── Redis Test R4: strict mode — new field IS a drift ─────────────────────────

func testR4(ctx context.Context, rdb *goredis.Client) {
	log.Println("--- R4: strict mode — new field detected as drift ---")
	stream := "schemadrift-e2e:strict:" + runStamp
	rdb.Del(ctx, stream)

	type BaseMsg struct {
		ID    string  `json:"id"`
		Score float64 `json:"score"`
	}
	type DriftedMsg struct {
		ID        string  `json:"id"`
		Score     float64 `json:"score"`
		ExtraField string `json:"extra_field"` // new field → drift
	}

	var driftCalls, blocked atomic.Int64
	itx, _ := redisadapter.NewInterceptor(redisadapter.ConsumerConfig{
		Baseline: drift.BaselineConfig{LearningSamples: 5, AllowOptionalFields: false},
		Policy: drift.PolicyConfig{
			Policy:  drift.PolicyBlock,
			OnDrift: func(r drift.DriftReport) { driftCalls.Add(1) },
		},
		StoreDir:     "/tmp/schemadrift-e2e/r4",
		PayloadField: "data",
	})

	for i := 0; i < 6; i++ {
		xadd(ctx, rdb, stream, mustJSON(BaseMsg{fmt.Sprintf("s%d", i), float64(i)}))
	}
	for i := 0; i < 4; i++ {
		xadd(ctx, rdb, stream, mustJSON(DriftedMsg{fmt.Sprintf("s%d", i+6), float64(i), "surprise"}))
	}

	xread(ctx, rdb, stream, 10, func(msg goredis.XMessage) {
		if err := itx.Handle(ctx, stream, msg, func(ctx context.Context, m goredis.XMessage) error {
			return nil
		}); err != nil {
			blocked.Add(1)
		}
	})

	if blocked.Load() == 4 {
		pass("R4 strict-new-field-blocks-4-messages")
	} else {
		fail("R4 blocked-count", fmt.Sprintf("want 4, got %d", blocked.Load()))
	}
	if driftCalls.Load() == 4 {
		pass("R4 OnDrift-called-4x")
	} else {
		fail("R4 drift-count", fmt.Sprintf("want 4, got %d", driftCalls.Load()))
	}
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime)

	log.Printf("Kafka broker : %s", *kafkaBroker)
	log.Printf("Redis address: %s", *redisAddr)
	log.Printf("Run stamp    : %s", runStamp)

	os.RemoveAll("/tmp/schemadrift-e2e")
	os.MkdirAll("/tmp/schemadrift-e2e", 0o755)

	ctx := context.Background()

	log.Println("\n=== Kafka Tests ===")
	testK1(ctx)
	testK2(ctx)
	testK3(ctx)

	log.Println("\n=== Redis Tests ===")
	rdb := goredis.NewClient(&goredis.Options{Addr: *redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}
	defer rdb.Close()

	testR1(ctx, rdb)
	testR2(ctx, rdb)
	testR3(ctx, rdb)
	testR4(ctx, rdb)

	// ── summary ───────────────────────────────────────────────────────────────
	log.Println("\n=== Summary ===")
	passed, failed := 0, 0
	for _, r := range results {
		if r.passed {
			passed++
		} else {
			failed++
			log.Printf("  FAIL: %s — %s", r.name, r.detail)
		}
	}
	log.Printf("Passed: %d / %d", passed, passed+failed)
	if failed > 0 {
		os.Exit(1)
	}
	log.Println("All tests passed ✅")
}
