package redissample

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func fixture() *Hook {
	return &Hook{instance: "fixture", process: "process01", deployment: "fixture-worker", pod: "worker-1", rows: map[string]*Row{}, totals: map[string]uint64{}, started: time.Now().Add(-time.Second)}
}
func record(h *Hook, ctx context.Context, cmd redis.Cmder) {
	out, _ := h.BeforeProcess(ctx, cmd)
	h.AfterProcess(out, cmd)
}
func TestNoValuesOrScriptBody(t *testing.T) {
	h := fixture()
	ctx := WithCategory(WithPath(context.Background(), "consumer.deliver"), "claim_due")
	cmd := redis.NewCmd(ctx, "eval", "return 'secret-value'", 1, "pre:follow_order:outbox:zset:{fo:25}", "secret-arg")
	record(h, ctx, cmd)
	b := h.flush(time.Now())
	raw, _ := json.Marshal(b)
	if strings.Contains(string(raw), "secret") || b.Rows[0].Path != "consumer.deliver/claim_due" || len(b.Rows[0].Script) != 40 {
		t.Fatal(string(raw))
	}
}
func TestPipelineCostOnce(t *testing.T) {
	h := fixture()
	ctx := context.Background()
	cmds := []redis.Cmder{redis.NewCmd(ctx, "get", "pre:ws:binding:42"), redis.NewCmd(ctx, "get", "pre:ws:binding:43")}
	out, _ := h.BeforeProcessPipeline(ctx, cmds)
	time.Sleep(time.Millisecond)
	h.AfterProcessPipeline(out, cmds)
	b := h.flush(time.Now())
	if len(b.Rows) != 1 || b.Rows[0].Calls != 2 || b.Rows[0].ClientUs != 0 || b.Rows[0].LatencyKnown != 0 || b.PipelineCount != 1 || b.PipelineUs < 1000 {
		t.Fatal(b)
	}
}
func TestAllCallsCountedNoSampling(t *testing.T) {
	h := fixture()
	ctx := context.Background()
	for i := 0; i < 10000; i++ {
		record(h, ctx, redis.NewCmd(ctx, "get", "pre:ws:binding:secret"))
	}
	b := h.flush(time.Now())
	if b.Totals["get"] != 10000 || b.Rows[0].Calls != 10000 || h.Dropped() != 0 {
		t.Fatal(b)
	}
}
func TestOverflowKeepsExactTotal(t *testing.T) {
	h := fixture()
	for i := 0; i < 600; i++ {
		ctx := WithPath(context.Background(), "code.path"+strconv.Itoa(i))
		record(h, ctx, redis.NewCmd(ctx, "get", "user:balance:42"))
	}
	b := h.flush(time.Now())
	var n, other uint64
	for _, r := range b.Rows {
		n += r.Calls
		if r.Command == "other" {
			other += r.Calls
		}
	}
	if len(b.Rows) != 500 || n != 600 || other != 101 || b.Totals["get"] != 600 || h.Dropped() != 0 {
		t.Fatal(len(b.Rows), n, other, b.Totals)
	}
}
func TestRandomStringIdsAreNotNamespaces(t *testing.T) {
	for _, k := range []string{"pre:ws:binding:secret-user", "pre:ws:binding:alice", "pre:follow_order:outbox:secret{fo:25}"} {
		if strings.Contains(keyShape(k), "secret") || strings.Contains(keyShape(k), "alice") {
			t.Fatal(keyShape(k))
		}
	}
}
func TestPlaintextRedirectAndInvalidConfigRejected(t *testing.T) {
	for _, endpoint := range []string{"http://example.invalid/api/ingest/redis-aggregates", "https://example.invalid/api/ingest/redis-samples", "https://example.invalid/api/ingest/redis-aggregates?token=x"} {
		if _, e := New("fixture", endpoint, strings.Repeat("x", 40), "worker", "pod"); e == nil {
			t.Fatal(endpoint)
		}
	}
	if _, e := New("fixture", "https://example.invalid/api/ingest/redis-aggregates", strings.Repeat("x", 40), "worker", "pod"); e != nil {
		t.Fatal(e)
	}
}
func TestBytesOnlyKnownResponses(t *testing.T) {
	h := fixture()
	ctx := context.Background()
	cmd := redis.NewStringCmd(ctx, "get", "user:balance:42")
	cmd.SetVal("secret-result")
	record(h, ctx, cmd)
	b := h.flush(time.Now())
	if b.Rows[0].BytesKnown != 1 || b.Rows[0].ResponseBytes == 0 {
		t.Fatal(b)
	}
	raw, _ := json.Marshal(b)
	if strings.Contains(string(raw), "secret-result") {
		t.Fatal(string(raw))
	}
}
func TestIdleReportsHeartbeatAndDrops(t *testing.T) {
	h := fixture()
	h.lost.Add(25)
	b := h.flush(time.Now())
	if len(b.Rows) != 1 || b.Rows[0].Command != "heartbeat" || b.DroppedCalls != 25 || len(b.Totals) != 0 {
		t.Fatal(b)
	}
}
func TestConcurrentCounters(t *testing.T) {
	h := fixture()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for k := 0; k < 500; k++ {
				record(h, ctx, redis.NewCmd(ctx, "get", "user:balance:42"))
			}
		}()
	}
	wg.Wait()
	if b := h.flush(time.Now()); b.Totals["get"] != 4000 {
		t.Fatal(b.Totals)
	}
}
func TestPostAcknowledgement(t *testing.T) {
	for _, status := range []int{202, 400, 409, 503} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
		h := fixture()
		h.endpoint = server.URL
		h.token = strings.Repeat("x", 40)
		ok, permanent := h.post(context.Background(), server.Client(), h.flush(time.Now()))
		server.Close()
		if ok != (status == 202) || permanent != (status == 400 || status == 409) {
			t.Fatal(status, ok, permanent)
		}
	}
}
func BenchmarkAggregate(b *testing.B) {
	h := fixture()
	ctx := context.Background()
	cmd := redis.NewCmd(ctx, "get", "pre:ws:binding:1234")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record(h, ctx, cmd)
	}
}
