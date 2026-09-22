package main

import (
	"context"
	"encoding/json"
	"flag"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/librescoot/event-service/api"
	"github.com/librescoot/eventbus"
	"github.com/redis/go-redis/v9"
)

// Re-exec the test binary so main owns its flags, signals and goroutines, and
// race-enabled test runs also instrument the actual service process.
func TestConfiguredInputsLiveHelper(t *testing.T) {
	if os.Getenv("EVENT_SERVICE_INPUTS_HELPER") != "1" {
		return
	}
	addr, dir := os.Getenv("EVENT_SERVICE_INPUTS_REDIS"), os.Getenv("EVENT_SERVICE_INPUTS_RULES")
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host != "127.0.0.1" || port == "" || dir == "" {
		t.Fatal("helper requires explicit private Redis address and rules directory")
	}
	flag.CommandLine = flag.NewFlagSet("event-service", flag.ExitOnError)
	os.Args = []string{"event-service", "--redis=" + addr, "--rules-dir=" + dir}
	main()
}

func inputsLiveProcess(t *testing.T, cmd *exec.Cmd, logPath string) {
	t.Helper()
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("%s exited: %v", cmd.Path, err)
			}
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Errorf("%s did not shut down after SIGTERM", cmd.Path)
		}
		_ = logFile.Close()
		if t.Failed() {
			body, _ := os.ReadFile(logPath)
			t.Logf("%s:\n%s", logPath, body)
		}
	})
}

func inputsLiveRedis(t *testing.T) *redis.Client {
	t.Helper()
	server, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server is not installed")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	inputsLiveProcess(t, exec.Command(server, "--bind", "127.0.0.1", "--port", strconv.Itoa(port),
		"--save", "", "--appendonly", "no", "--dir", dir), filepath.Join(dir, "redis.log"))
	client := redis.NewClient(&redis.Options{
		Addr: addr, MaxRetries: -1, DialTimeout: 100 * time.Millisecond,
		ReadTimeout: 4 * time.Second, WriteTimeout: time.Second,
		ContextTimeoutEnabled: true,
	})
	t.Cleanup(func() { _ = client.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err = client.Ping(context.Background()).Err(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("private Redis did not start: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return client
}

func TestConfiguredInputsLive(t *testing.T) {
	client := inputsLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	must := func(t *testing.T, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(t, client.HSet(ctx, "live:sensor", "trigger", "seed", "companion", "old").Err())
	must(t, client.HSet(ctx, "live:dependency", "allowed", "yes").Err())
	commands := []string{"queued-first", "queued-second"}
	must(t, client.RPush(ctx, "live:raw", commands).Err())

	dir := t.TempDir()
	must(t, os.WriteFile(filepath.Join(dir, "inputs.toml"), []byte(`
[[rule]]
name = "hash"
on = ["input.live.hash"]
when = 'data.field == "trigger"'
[[rule.input]]
hash = "live:sensor"
fields = ["trigger"]
topic = "input.live.hash"
[[rule.input]]
hash = "live:sensor"
fields = ["companion"]
[[rule.step]]
do = "redis"
list = "live:actions"
push = "hash"

[[rule]]
name = "batch"
on = ["input.live.hash"]
when = 'data.field == "trigger" and from == "seed" and to == "changed"'
[[rule.step]]
when = 'state("live:sensor", "companion") == "new"'
do = "redis"
list = "live:actions"
push = "batch"

[[rule]]
name = "string"
on = ["input.live.string"]
when = 'data.channel == "live:raw" and data.payload == "go:now"'
[[rule.input]]
channel = "live:raw"
topic = "input.live.string"
format = "string"
[[rule.step]]
do = "redis"
list = "live:actions"
push = "string"

[[rule]]
name = "json"
on = ["input.live.json"]
when = 'data.channel == "live:json" and data.payload.ready == true and data.payload.count == 7'
[[rule.input]]
channel = "live:json"
topic = "input.live.json"
format = "json"
[[rule.step]]
do = "redis"
list = "live:actions"
push = "json"

[[rule]]
name = "dependency"
on = ["vehicle.state"]
when = 'to == "parked" and state("live:dependency", "allowed") == "yes"'
[[rule.input]]
hash = "live:dependency"
fields = ["allowed"]
[[rule.step]]
do = "redis"
list = "live:actions"
push = "dependency"
`), 0600))

	binary, err := os.Executable()
	must(t, err)
	cmd := exec.Command(binary, "-test.run=^TestConfiguredInputsLiveHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "EVENT_SERVICE_INPUTS_HELPER=1",
		"EVENT_SERVICE_INPUTS_REDIS="+client.Options().Addr, "EVENT_SERVICE_INPUTS_RULES="+dir)
	inputsLiveProcess(t, cmd, filepath.Join(dir, "service.log"))

	// Only the read-only status RPC is retried; test stimuli are sent once.
	deadline := time.Now().Add(8 * time.Second)
	for {
		callCtx, stop := context.WithTimeout(ctx, 300*time.Millisecond)
		status, err := api.Call[api.Empty, api.StatusResponse](callCtx, client, api.MethodStatus, api.Empty{})
		stop()
		if err == nil {
			if status.Counters["rules"] != "5" {
				t.Fatalf("loaded rules: %+v", status)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("management not ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	publish := func(t *testing.T, channel, payload string) {
		t.Helper()
		n, err := client.Publish(ctx, channel, payload).Result()
		must(t, err)
		if n == 0 {
			t.Fatalf("no subscriber on %s", channel)
		}
	}
	noAction := func(t *testing.T) {
		t.Helper()
		result, err := client.BRPop(ctx, time.Second, "live:actions").Result()
		if err != redis.Nil {
			t.Fatalf("unexpected action/error: %v, %v", result, err)
		}
	}
	actions := func(t *testing.T, want ...string) {
		t.Helper()
		got := make(map[string]int)
		for range want {
			result, err := client.BRPop(ctx, 3*time.Second, "live:actions").Result()
			if err != nil {
				t.Fatalf("waiting for %v (received %v): %v", want, got, err)
			}
			got[result[1]]++
		}
		expected := make(map[string]int)
		for _, value := range want {
			expected[value]++
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("actions = %v, want %v", got, expected)
		}
	}

	t.Run("seed is silent", func(t *testing.T) {
		noAction(t)
		// main starts the engine after seeding, so actions alone cannot detect
		// accidental seed emissions. The real bus stream also must be empty.
		n, err := client.XLen(ctx, eventbus.StreamKey).Result()
		if err != nil || n != 0 {
			t.Fatalf("seed emitted bus events: count=%d err=%v", n, err)
		}
	})
	t.Run("selected hash batch reaches action condition", func(t *testing.T) {
		must(t, client.HSet(ctx, "live:sensor", "trigger", "changed", "companion", "new").Err())
		publish(t, "live:sensor", "trigger")
		actions(t, "hash", "batch")
	})
	t.Run("raw string does not consume command list", func(t *testing.T) {
		publish(t, "live:raw", "not-a-match")
		noAction(t)
		publish(t, "live:raw", "go:now")
		actions(t, "string")
		got, err := client.LRange(ctx, "live:raw", 0, -1).Result()
		if err != nil || !reflect.DeepEqual(got, commands) {
			t.Fatalf("pubsub consumed command data: got %v, want %v, err=%v", got, commands, err)
		}
	})
	t.Run("JSON conditions and invalid payload", func(t *testing.T) {
		publish(t, "live:json", `{"ready":false,"count":7}`)
		noAction(t)
		before, err := client.XLen(ctx, eventbus.StreamKey).Result()
		must(t, err)
		publish(t, "live:json", `{"ready":true,"count":7`)
		noAction(t)
		after, err := client.XLen(ctx, eventbus.StreamKey).Result()
		if err != nil || after != before {
			t.Fatalf("invalid JSON emitted an event: before=%d after=%d err=%v", before, after, err)
		}
		publish(t, "live:json", `{"ready":true,"count":7}`)
		actions(t, "json")
	})
	t.Run("state-only dependency seeded for external canonical event", func(t *testing.T) {
		event := eventbus.New("vehicle.state", "live-test")
		event.From, event.To = "standby", "parked"
		body, err := json.Marshal(event)
		must(t, err)
		publish(t, eventbus.ChannelPrefix+event.Topic, string(body))
		actions(t, "dependency")
		noAction(t)
	})
}
