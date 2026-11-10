// Package redissample aggregates ALL logical client calls; it does not measure server CPU.
package redissample

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/go-redis/redis/v8"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type labelKey struct{}
type labels struct {
	Path     string
	Category string
}

func WithCategory(ctx context.Context, category string) context.Context {
	if category != "claim_due" && category != "recover_timeout" && category != "sent_timeout" && category != "archive" {
		category = "unclassified"
	}
	v, _ := ctx.Value(labelKey{}).(labels)
	v.Category = category
	return context.WithValue(ctx, labelKey{}, v)
}

var identity = regexp.MustCompile(`^[A-Za-z0-9_.:/-]{1,160}$`)
var commandName = regexp.MustCompile(`^[a-z][a-z0-9_|-]{0,63}$`)

func WithPath(ctx context.Context, path string) context.Context {
	if !identity.MatchString(path) {
		path = "unclassified"
	}
	v, _ := ctx.Value(labelKey{}).(labels)
	v.Path = path
	return context.WithValue(ctx, labelKey{}, v)
}

type timerKey struct{ h *Hook }

var outbox = regexp.MustCompile(`^(pre:|alpha:)?follow_order:outbox:(processing:|sent_timeout:|acked:)?(zset|hash|ver):\{fo:([0-9]|[1-5][0-9]|6[0-3])\}$`)
var tokens = map[string]bool{"pre": true, "alpha": true, "follow_order": true, "outbox": true, "processing": true, "sent_timeout": true, "acked": true, "waiting_online": true, "wait_csr": true, "wait_any_csr": true, "zset": true, "hash": true, "ver": true, "ws": true, "pending": true, "index": true, "binding": true, "client": true, "shop": true, "shop_bindings": true, "broadcast_shop": true, "order_buyer_match": true, "user": true, "balance": true, "shop_qa_list": true, "customize_agent": true, "trigger_done": true, "user_robot_message": true, "invite": true, "other": true}
var shapeParts = regexp.MustCompile(`\{fo:([0-9]|[1-5][0-9]|6[0-3])\}|[^:]+`)
var bucketTag = regexp.MustCompile(`^\{fo:([0-9]|[1-5][0-9]|6[0-3])\}$`)

func keyShape(key string) string {
	if outbox.MatchString(key) {
		return key
	}
	scope := strings.HasPrefix(key, "follow_order:outbox:") || strings.HasPrefix(key, "pre:follow_order:outbox:") || strings.HasPrefix(key, "alpha:follow_order:outbox:")
	raw := strings.Split(key, ":")
	if strings.Contains(key, "{fo:") {
		raw = shapeParts.FindAllString(key, -1)
	}
	parts := make([]string, 0, 8)
	for _, p := range raw {
		if p == "" {
			continue
		}
		if !tokens[p] && !(scope && bucketTag.MatchString(p)) {
			p = "*"
		}
		parts = append(parts, p)
		if len(parts) == 8 {
			break
		}
	}
	if len(parts) == 0 {
		return "*"
	}
	return strings.Join(parts, ":")
}

type Row struct {
	Command       string `json:"command"`
	Shape         string `json:"shape"`
	Path          string `json:"path"`
	Script        string `json:"script"`
	Calls         uint64 `json:"calls"`
	Errors        uint64 `json:"errors"`
	ClientUs      uint64 `json:"client_us"`
	LatencyKnown  uint64 `json:"latency_known_calls"`
	RequestBytes  uint64 `json:"request_bytes"`
	ResponseBytes uint64 `json:"response_bytes"`
	BytesKnown    uint64 `json:"bytes_known_calls"`
}
type Batch struct {
	Instance      string            `json:"instance"`
	Process       string            `json:"process"`
	Deployment    string            `json:"deployment"`
	Pod           string            `json:"pod"`
	Batch         string            `json:"batch"`
	Start         float64           `json:"start"`
	End           float64           `json:"end"`
	Rows          []Row             `json:"rows"`
	Totals        map[string]uint64 `json:"totals"`
	DroppedCalls  uint64            `json:"dropped_calls"`
	PipelineCount uint64            `json:"pipeline_count"`
	PipelineUs    uint64            `json:"pipeline_client_us"`
}
type Hook struct {
	instance, endpoint, token, deployment, pod, process string
	mu                                                  sync.Mutex
	rows                                                map[string]*Row
	totals                                              map[string]uint64
	started                                             time.Time
	seq                                                 uint64
	pipelineCount, pipelineUs                           uint64
	lost                                                atomic.Uint64
}

func New(instance, endpoint, token, deployment, pod string) (*Hook, error) {
	u, e := url.Parse(endpoint)
	if e != nil || u.Scheme != "https" || u.User != nil || u.Host == "" || u.Path != "/api/ingest/redis-aggregates" || u.RawQuery != "" || len(token) < 32 || !identity.MatchString(deployment) || !identity.MatchString(pod) {
		return nil, errors.New("invalid Redis aggregate configuration")
	}
	var id [16]byte
	if _, e = rand.Read(id[:]); e != nil {
		return nil, e
	}
	return &Hook{instance: instance, endpoint: endpoint, token: token, deployment: deployment, pod: pod, process: hex.EncodeToString(id[:]), rows: map[string]*Row{}, totals: map[string]uint64{}, started: time.Now()}, nil
}
func (h *Hook) Dropped() uint64 { return h.lost.Load() }
func text(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	default:
		return ""
	}
}
func describe(ctx context.Context, cmd redis.Cmder) (Row, string) {
	r := Row{Command: strings.ToLower(cmd.Name()), Shape: "other", Path: "unclassified"}
	if !commandName.MatchString(r.Command) {
		r.Command = "other"
	}
	l, _ := ctx.Value(labelKey{}).(labels)
	if l.Path != "" {
		r.Path = l.Path
	}
	if l.Category != "" {
		r.Path += "/" + l.Category
	}
	if len(r.Path) > 160 {
		r.Path = "unclassified"
	}
	args := cmd.Args()
	keys := []string{}
	switch r.Command {
	case "eval", "evalsha", "eval_ro", "evalsha_ro":
		if len(args) >= 3 {
			n, e := strconv.Atoi(text(args[2]))
			if e == nil && n >= 0 && n <= len(args)-3 {
				for _, v := range args[3 : 3+n] {
					if len(keys) < 8 {
						keys = append(keys, keyShape(text(v)))
					}
				}
			}
			script := text(args[1])
			if strings.Contains(r.Command, "sha") {
				if v, e := hex.DecodeString(script); e == nil && len(v) == 20 {
					r.Script = strings.ToLower(script)
				}
			} else {
				sum := sha1.Sum([]byte(script))
				r.Script = hex.EncodeToString(sum[:])
			}
		}
	case "del", "unlink", "mget", "exists":
		for _, v := range args[1:] {
			if len(keys) < 8 {
				keys = append(keys, keyShape(text(v)))
			}
		}
	case "mset", "msetnx":
		for i := 1; i < len(args) && len(keys) < 8; i += 2 {
			keys = append(keys, keyShape(text(args[i])))
		}
	case "get", "set", "setex", "psetex", "hget", "hset", "hmget", "hmset", "hdel", "hgetall", "zrangebyscore", "zrange", "zadd", "zrem", "zscore", "expire", "pexpire", "ttl", "sadd", "smembers", "lpush", "rpush", "lpop", "rpop", "brpop", "blpop":
		if len(args) > 1 {
			keys = append(keys, keyShape(text(args[1])))
		}
	}
	if len(keys) > 0 {
		sort.Strings(keys)
		r.Shape = keys[0]
		for _, k := range keys[1:] {
			if k != strings.Split(r.Shape, "|")[len(strings.Split(r.Shape, "|"))-1] {
				r.Shape += "|" + k
			}
		}
		if len(r.Shape) > 512 {
			r.Shape = "other"
		}
	}
	sum := sha256.Sum256([]byte(r.Command + "\x00" + r.Shape + "\x00" + r.Path + "\x00" + r.Script))
	return r, hex.EncodeToString(sum[:])
}

// Protocol byte sizes are estimates of encoded arguments/results, not actual NIC traffic.
func scalarLen(v interface{}) (int, bool) {
	switch x := v.(type) {
	case string:
		return len(x), true
	case []byte:
		return len(x), true
	case int:
		return len(strconv.Itoa(x)), true
	case int64:
		return len(strconv.FormatInt(x, 10)), true
	case uint64:
		return len(strconv.FormatUint(x, 10)), true
	case float64:
		return len(strconv.FormatFloat(x, 'f', -1, 64)), true
	case bool:
		if x {
			return 1, true
		}
		return 1, true
	case nil:
		return 0, true
	default:
		return 0, false
	}
}
func requestBytes(args []interface{}) (uint64, bool) {
	n := 1 + len(strconv.Itoa(len(args))) + 2
	for _, v := range args {
		size, ok := scalarLen(v)
		if !ok {
			return 0, false
		}
		n += 1 + len(strconv.Itoa(size)) + 2 + size + 2
	}
	return uint64(n), true
}
func responseBytes(cmd redis.Cmder) (uint64, bool) {
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		return 0, false
	}
	switch x := cmd.(type) {
	case *redis.StringCmd:
		if x.Err() == redis.Nil {
			return 5, true
		}
		v := x.Val()
		return uint64(len(v) + len(strconv.Itoa(len(v))) + 5), true
	case *redis.StatusCmd:
		return uint64(len(x.Val()) + 3), true
	case *redis.IntCmd:
		return uint64(len(strconv.FormatInt(x.Val(), 10)) + 3), true
	case *redis.BoolCmd:
		return 4, true
	default:
		return 0, false
	}
}
func (h *Hook) record(ctx context.Context, cmd redis.Cmder, us uint64, known bool) {
	r, k := describe(ctx, cmd)
	r.Calls = 1
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		r.Errors = 1
	}
	if known {
		r.ClientUs = us
		r.LatencyKnown = 1
	}
	req, a := requestBytes(cmd.Args())
	res, b := responseBytes(cmd)
	if a && b {
		r.RequestBytes = req
		r.ResponseBytes = res
		r.BytesKnown = 1
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, known := h.totals[r.Command]; !known && len(h.totals) >= 511 {
		r.Command = "other"
		r.Shape = "other"
		r.Path = "other"
		r.Script = ""
		k = "other"
	}
	h.totals[r.Command]++
	dst := h.rows[k]
	if dst == nil {
		if len(h.rows) >= 499 {
			k = "other"
			r.Command = "other"
			r.Shape = "other"
			r.Path = "other"
			r.Script = ""
		}
		dst = h.rows[k]
		if dst == nil {
			dst = &Row{Command: r.Command, Shape: r.Shape, Path: r.Path, Script: r.Script}
			h.rows[k] = dst
		}
	}
	dst.Calls++
	dst.Errors += r.Errors
	dst.ClientUs += r.ClientUs
	dst.LatencyKnown += r.LatencyKnown
	dst.RequestBytes += r.RequestBytes
	dst.ResponseBytes += r.ResponseBytes
	dst.BytesKnown += r.BytesKnown
}
func (h *Hook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	return context.WithValue(ctx, timerKey{h}, time.Now()), nil
}
func (h *Hook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	t, ok := ctx.Value(timerKey{h}).(time.Time)
	us := uint64(0)
	if ok {
		us = uint64(time.Since(t).Microseconds())
	}
	h.record(ctx, cmd, us, ok)
	return nil
}
func (h *Hook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return context.WithValue(ctx, timerKey{h}, time.Now()), nil
}
func (h *Hook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	t, ok := ctx.Value(timerKey{h}).(time.Time)
	us := uint64(0)
	if ok {
		us = uint64(time.Since(t).Microseconds())
	}
	for _, cmd := range cmds {
		h.record(ctx, cmd, 0, false)
	}
	if ok {
		h.mu.Lock()
		h.pipelineCount++
		h.pipelineUs += us
		h.mu.Unlock()
	}
	return nil
}
func (h *Hook) flush(now time.Time) *Batch {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.rows) == 0 {
		h.rows["heartbeat"] = &Row{Command: "heartbeat", Shape: "other", Path: "other"}
	}
	h.seq++
	b := &Batch{Instance: h.instance, Process: h.process, Deployment: h.deployment, Pod: h.pod, Batch: h.process + "_" + strconv.FormatUint(h.seq, 10), Start: float64(h.started.UnixNano()) / 1e9, End: float64(now.UnixNano()) / 1e9, Totals: h.totals, DroppedCalls: h.lost.Swap(0), PipelineCount: h.pipelineCount, PipelineUs: h.pipelineUs, Rows: []Row{}}
	for _, r := range h.rows {
		b.Rows = append(b.Rows, *r)
	}
	sort.Slice(b.Rows, func(i, j int) bool {
		return b.Rows[i].Command+b.Rows[i].Shape+b.Rows[i].Path+b.Rows[i].Script < b.Rows[j].Command+b.Rows[j].Shape+b.Rows[j].Path+b.Rows[j].Script
	})
	h.rows = map[string]*Row{}
	h.totals = map[string]uint64{}
	h.pipelineCount = 0
	h.pipelineUs = 0
	h.started = now
	return b
}
func calls(b *Batch) uint64 {
	var n uint64
	for _, v := range b.Totals {
		n += v
	}
	return n
}

func (h *Hook) post(ctx context.Context, client *http.Client, b *Batch) (bool, bool) {
	body, e := json.Marshal(b)
	if e != nil {
		return false, true
	}
	req, e := http.NewRequestWithContext(ctx, "POST", h.endpoint, bytes.NewReader(body))
	if e != nil {
		return false, true
	}
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("Content-Type", "application/json")
	r, e := client.Do(req)
	if e != nil {
		return false, false
	}
	defer r.Body.Close()
	return r.StatusCode == 202, r.StatusCode == 400 || r.StatusCode == 409
}

// Run retries one immutable pending batch; only two windows are retained. Loss is
// carried forward explicitly. Network sends never execute on a business request.
func (h *Hook) Run(ctx context.Context) {
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	phase := sha256.Sum256([]byte(h.process))
	delay := time.Duration((int(phase[0])*256+int(phase[1]))%20000+1) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	var pending *Batch
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-timer.C:
			timer.Reset(20 * time.Second)
			current := h.flush(now)
			if pending == nil {
				pending = current
				current = nil
			}
			if pending == nil {
				continue
			}
			accepted, permanent := h.post(ctx, client, pending)
			if permanent {
				h.lost.Add(calls(pending) + pending.DroppedCalls)
				pending = current
			} else if accepted {
				pending = current
			} else if current != nil {
				h.lost.Add(calls(current) + current.DroppedCalls)
			}
			// Never retry a stale >1-day window forever or claim it was delivered.
			if pending != nil && now.Unix()-int64(pending.End) > 86400 {
				h.lost.Add(calls(pending) + pending.DroppedCalls)
				pending = nil
			}
		}
	}
}
