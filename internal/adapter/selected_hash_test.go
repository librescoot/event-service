package adapter

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func selectedHashRedis(t *testing.T) *redis.Client {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server unavailable")
	}
	dir := t.TempDir()
	socket := filepath.Join(dir, "r.sock")
	logFile, err := os.Create(filepath.Join(dir, "redis.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "--port", "0", "--unixsocket", socket, "--unixsocketperm", "700", "--save", "", "--appendonly", "no", "--dir", dir)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); _ = logFile.Close() })
	client := redis.NewClient(&redis.Options{Network: "unix", Addr: socket, MaxRetries: -1, DialTimeout: 100 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	until := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil && client.Ping(context.Background()).Err() == nil {
			return client
		}
		if time.Now().After(until) {
			t.Fatal("private Redis did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type selectedHashEvent struct{ field, value string }

func selectedHashNext(t *testing.T, events <-chan selectedHashEvent, want ...selectedHashEvent) {
	t.Helper()
	for _, expected := range want {
		select {
		case got := <-events:
			if got != expected {
				t.Fatalf("callback = %#v, want %#v", got, expected)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for callback")
		}
	}
}

func selectedHashQuiet(t *testing.T, events <-chan selectedHashEvent) {
	t.Helper()
	select {
	case event := <-events:
		t.Fatalf("unexpected callback: %#v", event)
	case <-time.After(75 * time.Millisecond):
	}
}

func TestSelectedHashSeedActivationAndSnapshots(t *testing.T) {
	client := selectedHashRedis(t)
	ctx := context.Background()
	if err := client.HSet(ctx, "custom", "a", "old", "b", "companion", "ignored", "secret").Err(); err != nil {
		t.Fatal(err)
	}
	events := make(chan selectedHashEvent, 32)
	w := newSelectedHashWatcher(client, "custom", map[string]int{"missing": 32, "b": 32, "a": 32}, func(f, v string) { events <- selectedHashEvent{f, v} }, func(f string) { events <- selectedHashEvent{f, "INVALID"} })
	t.Cleanup(func() { _ = w.Stop() })
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// No goroutine has started: these callbacks must already have happened.
	if len(events) != 3 {
		t.Fatalf("seed callbacks not synchronous: %d", len(events))
	}
	selectedHashNext(t, events, selectedHashEvent{"a", "old"}, selectedHashEvent{"b", "companion"}, selectedHashEvent{"missing", ""})
	if err := client.HSet(ctx, "custom", "a", "new", "b", "changed").Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := client.Publish(ctx, "custom", "a").Result(); err != nil || n != 1 {
		t.Fatalf("subscription not confirmed: subscribers=%d err=%v", n, err)
	}
	selectedHashQuiet(t, events)
	w.Activate()
	w.Activate()
	selectedHashNext(t, events, selectedHashEvent{"a", "new"}, selectedHashEvent{"b", "changed"}, selectedHashEvent{"missing", ""})
	if err := client.HDel(ctx, "custom", "b").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Publish(ctx, "custom", "unselected-field").Err(); err != nil {
		t.Fatal(err)
	}
	selectedHashNext(t, events, selectedHashEvent{"a", "new"}, selectedHashEvent{"b", ""}, selectedHashEvent{"missing", ""})
	for _, notification := range []string{"cleared", "replaced", "deleted"} {
		if err := client.Del(ctx, "custom").Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.Publish(ctx, "custom", notification).Err(); err != nil {
			t.Fatal(err)
		}
		selectedHashNext(t, events, selectedHashEvent{"a", ""}, selectedHashEvent{"b", ""}, selectedHashEvent{"missing", ""})
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if n := client.Publish(ctx, "custom", "a").Val(); n != 0 {
		t.Fatalf("subscription leaked after stop: %d", n)
	}
	selectedHashQuiet(t, events)
}

func TestSelectedHashOversizeNeverFetched(t *testing.T) {
	client := selectedHashRedis(t)
	ctx := context.Background()
	if err := client.HSet(ctx, "custom", "large", strings.Repeat("x", 4<<20), "ignored", strings.Repeat("y", 4<<20), "exact", "1234").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ConfigResetStat(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	var got []selectedHashEvent
	w := newSelectedHashWatcher(client, "custom", map[string]int{"large": 4, "exact": 4, "missing": 0}, func(f, v string) { got = append(got, selectedHashEvent{f, v}) }, func(f string) { got = append(got, selectedHashEvent{f, "INVALID"}) })
	t.Cleanup(func() { _ = w.Stop() })
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	want := []selectedHashEvent{{"exact", "1234"}, {"large", "INVALID"}, {"missing", ""}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("callbacks = %#v, want %#v", got, want)
	}
	stats, err := client.Info(ctx, "commandstats").Result()
	if err != nil {
		t.Fatal(err)
	}
	// Redis counts commands executed inside Lua. Exactly two HGETs prove the
	// large selected value was rejected before reading, not merely truncated.
	if !strings.Contains(stats, "cmdstat_hget:calls=2,") || !strings.Contains(stats, "cmdstat_hstrlen:calls=3,") || strings.Contains(stats, "cmdstat_hgetall:") {
		t.Fatalf("unexpected read commands: %s", stats)
	}
}

func TestSelectedHashCancellationAndStartupFailure(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-activation", true: "active"}[active], func(t *testing.T) {
			client := selectedHashRedis(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w := newSelectedHashWatcher(client, "custom", map[string]int{"a": 10}, func(string, string) {}, func(string) {})
			t.Cleanup(func() { _ = w.Stop() })
			if err := w.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if active {
				w.Activate()
			}
			cancel()
			select {
			case <-w.cleaned:
			case <-time.After(time.Second):
				t.Fatal("cancellation did not release subscription")
			}
			w.Activate()
			if err := w.Stop(); err != nil {
				t.Fatal(err)
			}
			if n := client.Publish(context.Background(), "custom", "a").Val(); n != 0 {
				t.Fatalf("subscription leaked: %d", n)
			}
		})
	}
	t.Run("wrong-type", func(t *testing.T) {
		client := selectedHashRedis(t)
		ctx := context.Background()
		if err := client.Set(ctx, "custom", "not a hash", 0).Err(); err != nil {
			t.Fatal(err)
		}
		w := newSelectedHashWatcher(client, "custom", map[string]int{"a": 10}, func(string, string) { t.Error("callback on error") }, func(string) { t.Error("invalidate on error") })
		if err := w.Start(ctx); err == nil {
			t.Fatal("expected startup error")
		}
		_ = w.Stop()
		if n := client.Publish(ctx, "custom", "a").Val(); n != 0 {
			t.Fatalf("subscription leaked on startup failure: %d", n)
		}
	})
}

func TestSelectedHashRefreshErrorAndRecovery(t *testing.T) {
	client := selectedHashRedis(t)
	ctx := context.Background()
	events := make(chan selectedHashEvent, 16)
	w := newSelectedHashWatcher(client, "custom", map[string]int{"a": 4}, func(f, v string) { events <- selectedHashEvent{f, v} }, func(f string) { events <- selectedHashEvent{f, "INVALID"} })
	t.Cleanup(func() { _ = w.Stop() })
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	selectedHashNext(t, events, selectedHashEvent{"a", ""})
	w.Activate()
	if err := client.Set(ctx, "custom", "wrong type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Publish(ctx, "custom", "a").Err(); err != nil {
		t.Fatal(err)
	}
	selectedHashQuiet(t, events)
	if err := client.Del(ctx, "custom").Err(); err != nil {
		t.Fatal(err)
	}
	for _, step := range []selectedHashEvent{{"a", "12345"}, {"a", "good"}} {
		if err := client.HSet(ctx, "custom", step.field, step.value).Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.Publish(ctx, "custom", "a").Err(); err != nil {
			t.Fatal(err)
		}
		if len(step.value) > 4 {
			step.value = "INVALID"
		}
		selectedHashNext(t, events, step)
	}
}

func TestSelectedHashReadDeadline(t *testing.T) {
	client := selectedHashRedis(t)
	ctx := context.Background()
	w := newSelectedHashWatcher(client, "custom", map[string]int{"a": 10}, func(string, string) {}, func(string) {})
	t.Cleanup(func() { _ = w.Stop() })
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Do(ctx, "CLIENT", "PAUSE", "3000", "ALL").Err(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := w.refresh(ctx); err == nil {
		t.Fatal("expected refresh timeout")
	}
	if elapsed := time.Since(start); elapsed > selectedHashReadTimeout+300*time.Millisecond {
		t.Fatalf("refresh exceeded deadline: %v", elapsed)
	}
}

func TestSelectedHashInvalidBudgets(t *testing.T) {
	client := selectedHashRedis(t)
	for _, fields := range []map[string]int{nil, {"a": -1}, {"a": selectedHashMaxBytes + 1}, {"a": selectedHashMaxBytes, "b": 1}} {
		w := newSelectedHashWatcher(client, "custom", fields, func(string, string) { t.Error("unexpected callback") }, func(string) {})
		if err := w.Start(context.Background()); err == nil {
			t.Fatalf("accepted invalid budget: %v", fields)
		}
		_ = w.Stop()
	}
}
