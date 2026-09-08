package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server unavailable")
	}
	dir := t.TempDir()
	socket := filepath.Join(dir, "r.sock")
	log, err := os.Create(filepath.Join(dir, "redis.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "--port", "0", "--unixsocket", socket, "--unixsocketperm", "700", "--save", "", "--appendonly", "no", "--dir", dir)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); _ = log.Close() })
	c := redis.NewClient(&redis.Options{Network: "unix", Addr: socket, MaxRetries: -1, DialTimeout: 100 * time.Millisecond})
	t.Cleanup(func() { _ = c.Close() })
	until := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(socket); err != nil {
			if time.Now().After(until) {
				t.Fatal("private Redis socket did not appear")
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if c.Ping(context.Background()).Err() == nil {
			return c
		}
		if time.Now().After(until) {
			t.Fatal("private Redis did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func startServer(t *testing.T, c *redis.Client, handler func(context.Context, string, json.RawMessage) (any, error)) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, c, handler) }()
	t.Cleanup(cancel)
	return cancel, done
}

func stopServer(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server failed to stop promptly")
	}
}

// Drop the EVAL response only after Redis executed it. A retry would enqueue
// a duplicate, even though the original caller never observed success.
type lostEnqueueReply struct {
	net.Conn
	drop bool
}

func (c *lostEnqueueReply) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte(enqueueScript)) {
		c.drop = true
	}
	return c.Conn.Write(p)
}

func (c *lostEnqueueReply) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if c.drop && n > 0 {
		c.drop = false
		_ = c.Close()
		return 0, io.EOF
	}
	return n, err
}

func TestRPCEnqueueResponseLostNotRetried(t *testing.T) {
	c := testRedis(t)
	opts := *c.Options()
	opts.MaxRetries = 10
	opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &lostEnqueueReply{Conn: conn}, nil
	}
	supplied := redis.NewClient(&opts)
	defer supplied.Close()
	_, err := Call[Empty, Empty](context.Background(), supplied, MethodAdd, Empty{})
	if err == nil || !strings.Contains(err.Error(), "show/list before retry") {
		t.Fatalf("got %v", err)
	}
	if n := c.LLen(context.Background(), Channel).Val(); n != 1 {
		t.Fatalf("enqueue count %d", n)
	}
}

func TestRPCUntrustedReplyLimit(t *testing.T) {
	c := testRedis(t)
	done := make(chan error, 1)
	go func() {
		values, err := c.BRPop(context.Background(), time.Second, Channel).Result()
		if err != nil {
			done <- err
			return
		}
		var req rpcRequest
		if err = json.Unmarshal([]byte(values[1]), &req); err != nil {
			done <- err
			return
		}
		done <- c.Publish(context.Background(), req.ReplyChannel, strings.Repeat("x", MaxReplyBytes+1)).Err()
	}()
	_, err := Call[Empty, Empty](context.Background(), c, MethodList, Empty{})
	if err == nil || !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("got %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRPCSuccessAndSubscribeRace(t *testing.T) {
	c := testRedis(t)
	cancel, done := startServer(t, c, func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		if method != MethodShow {
			return nil, errors.New("wrong method")
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > callTimeout {
			return nil, errors.New("missing bounded deadline")
		}
		var req ShowRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, err
		}
		return req, nil
	})
	defer stopServer(t, cancel, done)
	for i := 0; i < 50; i++ {
		got, err := Call[ShowRequest, ShowRequest](context.Background(), c, MethodShow, ShowRequest{Name: "example"})
		if err != nil || got.Name != "example" {
			t.Fatalf("call %d: %+v %v", i, got, err)
		}
	}
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("supplied client closed: %v", err)
	}
}

func TestRPCHandlerErrorsAndReplyLimit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result any
		err    error
		want   string
	}{
		{name: "handler", err: errors.New("revision conflict"), want: "revision conflict"},
		{name: "oversize", result: strings.Repeat("x", MaxReplyBytes), want: "size limit"},
		{name: "oversize error", err: errors.New(strings.Repeat("x", MaxReplyBytes)), want: "size limit"},
		{name: "unencodable", result: make(chan int), want: "cannot encode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testRedis(t)
			cancel, done := startServer(t, c, func(context.Context, string, json.RawMessage) (any, error) { return tc.result, tc.err })
			defer stopServer(t, cancel, done)
			_, err := Call[Empty, any](context.Background(), c, MethodList, Empty{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestRPCQueueFullAndExpiry(t *testing.T) {
	c := testRedis(t)
	ctx := context.Background()
	for i := 0; i < MaxQueuedRequests; i++ {
		n, err := c.Eval(ctx, enqueueScript, []string{Channel}, "queued", MaxQueuedRequests).Int()
		if err != nil || n != 1 {
			t.Fatalf("enqueue %d: %d %v", i, n, err)
		}
	}
	_, err := Call[Empty, Empty](ctx, c, MethodList, Empty{})
	if err == nil || !strings.Contains(err.Error(), "queue is full") {
		t.Fatalf("got %v", err)
	}
	if n := c.LLen(ctx, Channel).Val(); n != MaxQueuedRequests {
		t.Fatalf("depth %d", n)
	}
	if ttl := c.TTL(ctx, Channel).Val(); ttl < 29*time.Second || ttl > 30*time.Second {
		t.Fatalf("ttl %v", ttl)
	}
	// Accelerate expiry on this private datastore instead of spending 30s asleep.
	if err := c.PExpire(ctx, Channel, time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if n := c.Exists(ctx, Channel).Val(); n != 0 {
		t.Fatal("queue did not expire")
	}
}

func TestRPCRequestLimitAndCanceledCall(t *testing.T) {
	c := testRedis(t)
	_, err := Call[string, Empty](context.Background(), c, MethodAdd, strings.Repeat("x", MaxRequestBytes))
	if err == nil || !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Call[Empty, Empty](ctx, c, MethodList, Empty{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if n := c.LLen(context.Background(), Channel).Val(); n != 0 {
		t.Fatalf("unexpected enqueue: %d", n)
	}
}

func TestRPCNoServiceDeadlineAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		timeout     time.Duration
		cancelEarly bool
	}{
		{"short deadline", 100 * time.Millisecond, false},
		{"cancel while waiting", time.Second, true},
		{"hard maximum", 10 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testRedis(t)
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			if tc.cancelEarly {
				timer := time.AfterFunc(100*time.Millisecond, cancel)
				defer timer.Stop()
			}
			start := time.Now()
			_, err := Call[Empty, Empty](ctx, c, MethodList, Empty{})
			if err == nil || !strings.Contains(err.Error(), "show/list before retry") {
				t.Fatalf("got %v", err)
			}
			limit := tc.timeout + time.Second
			if tc.name == "hard maximum" {
				limit = 6 * time.Second
			}
			if tc.cancelEarly {
				limit = time.Second
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("got %v", err)
				}
			}
			if !tc.cancelEarly && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline error: %v", err)
			}
			if time.Since(start) > limit {
				t.Fatalf("slow cancellation: %v", time.Since(start))
			}
			if n := c.LLen(context.Background(), Channel).Val(); n != 1 {
				t.Fatalf("request retried: %d", n)
			}
			var env rpcRequest
			if err := json.Unmarshal([]byte(c.LIndex(context.Background(), Channel, 0).Val()), &env); err != nil {
				t.Fatal(err)
			}
			if env.Deadline > start.Add(callTimeout+20*time.Millisecond).UnixMilli() {
				t.Fatal("deadline not capped")
			}
		})
	}
}

func TestRPCMalformedAndExpiredNeverMutate(t *testing.T) {
	c := testRedis(t)
	ctx := context.Background()
	id := strings.Repeat("a", 32)
	base := rpcRequest{id, MethodAdd, Channel + ":reply:" + id, time.Now().Add(time.Second).UnixMilli(), json.RawMessage(`{}`)}
	bad := []string{"{", "null", strings.Repeat("x", MaxRequestBytes+1)}
	missingPayload, _ := json.Marshal(base)
	var fields map[string]any
	_ = json.Unmarshal(missingPayload, &fields)
	delete(fields, "payload")
	missingPayload, _ = json.Marshal(fields)
	bad = append(bad, string(missingPayload))
	changes := []func(*rpcRequest){
		func(r *rpcRequest) { r.ID = "bad"; r.ReplyChannel = Channel + ":reply:bad" },
		func(r *rpcRequest) { r.ReplyChannel = "unrelated:topic" },
		func(r *rpcRequest) { r.Deadline = time.Now().Add(-time.Second).UnixMilli() },
		func(r *rpcRequest) { r.Deadline = time.Now().Add(time.Minute).UnixMilli() },
		func(r *rpcRequest) { r.Deadline = 0 },
		func(r *rpcRequest) { r.Method = "v2.add" },
		func(r *rpcRequest) { r.Payload = json.RawMessage(`"` + strings.Repeat("x", MaxRequestBytes) + `"`) },
	}
	for _, change := range changes {
		req := base
		change(&req)
		body, _ := json.Marshal(req)
		bad = append(bad, string(body))
	}
	sub := c.PSubscribe(ctx, "*")
	defer sub.Close()
	if _, err := sub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	for _, body := range bad {
		if err := c.LPush(ctx, Channel, body).Err(); err != nil {
			t.Fatal(err)
		}
	}
	var mutations atomic.Int32
	cancel, done := startServer(t, c, func(context.Context, string, json.RawMessage) (any, error) { mutations.Add(1); return Empty{}, nil })
	until := time.Now().Add(time.Second)
	for c.LLen(ctx, Channel).Val() != 0 {
		if time.Now().After(until) {
			t.Fatal("queue not drained")
		}
		time.Sleep(5 * time.Millisecond)
	}
	stopServer(t, cancel, done)
	if mutations.Load() != 0 {
		t.Fatalf("mutated %d times", mutations.Load())
	}
	if msg, err := sub.ReceiveTimeout(ctx, 50*time.Millisecond); err == nil {
		t.Fatalf("malformed request published: %v", msg)
	}
}

func TestRPCSequentialQueueExpiry(t *testing.T) {
	c := testRedis(t)
	id := strings.Repeat("b", 32)
	now := time.Now()
	first := rpcRequest{id, MethodAdd, Channel + ":reply:" + id, now.Add(200 * time.Millisecond).UnixMilli(), json.RawMessage(`{}`)}
	second := first
	second.Deadline = now.Add(50 * time.Millisecond).UnixMilli()
	for _, req := range []rpcRequest{first, second} {
		body, _ := json.Marshal(req)
		if err := c.LPush(context.Background(), Channel, body).Err(); err != nil {
			t.Fatal(err)
		}
	}
	var calls atomic.Int32
	cancel, done := startServer(t, c, func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
		calls.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	until := time.Now().Add(time.Second)
	for c.LLen(context.Background(), Channel).Val() != 0 {
		if time.Now().After(until) {
			t.Fatal("queue did not drain")
		}
		time.Sleep(5 * time.Millisecond)
	}
	stopServer(t, cancel, done)
	if calls.Load() != 1 {
		t.Fatalf("expired queued request executed: %d calls", calls.Load())
	}
	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if err := Serve(canceled, c, func(context.Context, string, json.RawMessage) (any, error) {
		t.Error("handler after cancellation")
		return nil, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestRPCShutdownAndHandlerCancellation(t *testing.T) {
	c := testRedis(t)
	// An indefinite supplied ReadTimeout must not prevent shutdown.
	opts := *c.Options()
	opts.ReadTimeout = -1
	supplied := redis.NewClient(&opts)
	defer supplied.Close()
	cancel, done := startServer(t, supplied, func(context.Context, string, json.RawMessage) (any, error) {
		t.Error("unexpected handler")
		return nil, nil
	})
	time.Sleep(20 * time.Millisecond)
	stopServer(t, cancel, done)
	if err := supplied.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	var mutations atomic.Int32
	entered := make(chan struct{})
	cancel, done = startServer(t, c, func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
		mutations.Add(1)
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	callDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		_, err := Call[Empty, Empty](ctx, c, MethodAdd, Empty{})
		callDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler not entered")
	}
	select {
	case err := <-callDone:
		if err == nil {
			t.Fatal("expected ambiguous timeout")
		}
	case <-time.After(time.Second):
		t.Fatal("call stuck")
	}
	stopServer(t, cancel, done)
	if mutations.Load() != 1 {
		t.Fatalf("mutations %d", mutations.Load())
	}
	if n := c.LLen(context.Background(), Channel).Val(); n != 0 {
		t.Fatalf("retried: %d", n)
	}
}
