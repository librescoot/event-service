// This file is the rule the whole plan exists for, wired the way main wires
// it: rule files on disk, loaded and compiled, a real worker pool, a real
// scheduler, a real pending store, and a live datastore taking the pushes.
//
// It is an external test package because it needs the engine, and the engine
// imports seq. Same directory, no import cycle.
package seq_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/engine"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/event-service/internal/sched"
	"github.com/librescoot/event-service/internal/seq"
	"github.com/librescoot/eventbus"
	ipc "github.com/librescoot/redis-ipc"
)

// shortAfter stands in for the rule's thirty seconds. It has to be long
// enough that "the first push is not delayed" is a claim about the code
// rather than about how loaded the machine is under the race detector, and
// short enough that a handful of tests waiting several of them out is still
// quick. TestTheDocumentedRuleLoadsAndCompiles covers the same file with the
// thirty seconds it is actually documented with.
const shortAfter = 120 * time.Millisecond

type nopLog struct{}

func (nopLog) Printf(string, ...any) {}

type nopPusher struct{}

func (nopPusher) LPush(string, ...any) (int64, error) { return 1, nil }

// memHasher is the pending hash, in memory. The pushes these tests assert on
// go to a live datastore under a name unique to the run, but the pending hash
// cannot: seq.NewPendingStore writes to one fixed hash, so two packages'
// tests running at once would be reading each other's records out of it.
//
// It counts writes, which is what the zero-rules path needs: "adds no work"
// means the hash is not written to at all, not merely that it ends up empty.
type memHasher struct {
	mu     sync.Mutex
	fields map[string]string
	writes int
}

func newMemHasher() *memHasher { return &memHasher{fields: make(map[string]string)} }

func (h *memHasher) HSet(_, field string, value any) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fields[field] = fmt.Sprint(value)
	h.writes++
	return nil
}

func (h *memHasher) HGetAll(string) (map[string]string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]string, len(h.fields))
	for k, v := range h.fields {
		out[k] = v
	}
	return out, nil
}

func (h *memHasher) HDel(_ string, fields ...string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, f := range fields {
		delete(h.fields, f)
	}
	return nil
}

func (h *memHasher) records() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.fields)
}

// listSeq keeps every test's datastore list to itself, so two runs of this
// package at once cannot read each other's pushes.
var listSeq struct {
	mu sync.Mutex
	n  int
}

func testListName(t *testing.T) string {
	t.Helper()
	listSeq.mu.Lock()
	listSeq.n++
	n := listSeq.n
	listSeq.mu.Unlock()
	return fmt.Sprintf("test:event-service:blinker:%d:%d", time.Now().UnixNano(), n)
}

// liveClient returns a datastore client with the test's list removed again
// when it ends.
func liveClient(t *testing.T, list string) *ipc.Client {
	t.Helper()
	client, err := ipc.New(
		ipc.WithURL("localhost:6379"),
		ipc.WithDialTimeout(2*time.Second),
		ipc.WithCodec(ipc.StringCodec{}),
	)
	if err != nil {
		t.Skipf("datastore not reachable at localhost:6379: %v", err)
	}
	t.Cleanup(func() {
		_, _ = client.Do("DEL", list)
		_ = client.Close()
	})
	return client
}

// pushed returns what has landed on the list, oldest first. LPUSH prepends,
// so the datastore hands it back the other way round.
func pushed(t *testing.T, client *ipc.Client, list string) []string {
	t.Helper()
	got, err := client.Raw().LRange(client.Context(), list, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRANGE %s: %v", list, err)
	}
	out := make([]string, 0, len(got))
	for i := len(got) - 1; i >= 0; i-- {
		out = append(out, got[i])
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	if !eventually(2*time.Second, cond) {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// eventually reports whether cond became true inside d without failing the
// test, so a caller can say what a timeout means in its own words and put the
// counts that explain it in the message.
func eventually(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

// hazardsRule is the rule this plan was written for. list and after are
// substituted so that one test can run it in milliseconds and another can
// check that the thirty-second original loads and compiles; everything else
// is the file as a user would write it.
func hazardsRule(list, after string) string {
	return fmt.Sprintf(`
[[rule]]
name        = "hazards-on-alarm"
on          = ["alarm.triggered"]
concurrency = "restart"
cancel-on   = ["alarm.disarmed"]

  [[rule.step]]
  do   = "redis"
  list = %q
  push = "both"

  [[rule.step]]
  after = %q
  do    = "redis"
  list  = %q
  push  = "off"
`, list, after, list)
}

func rulesDir(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hazards.toml"), []byte(body), 0644); err != nil {
		t.Fatalf("write rule file: %v", err)
	}
	return dir
}

// newEngine is the wiring main does, over a rule directory the test wrote.
// Two calls with the same directory and the same hash are two starts of the
// service against the same rules and the same pending records.
func newEngine(t *testing.T, dir string, pusher action.Pusher, h seq.Hasher) *engine.Engine {
	t.Helper()

	cfg, loadErrs := rules.Load(dir)
	if len(loadErrs) != 0 {
		t.Fatalf("load: %v", loadErrs)
	}
	compiled, compileErrs := rules.Compile(cfg.Rules, func(string, string) string { return "" })
	if len(compileErrs) != 0 {
		t.Fatalf("compile: %v", compileErrs)
	}

	sch := sched.New()
	pool := action.NewPool(2, 256, nopLog{})
	pool.Start()
	store := seq.NewPendingStore(h, nopLog{})

	en, buildErrs := engine.New(compiled, pool, sch, store, pusher, nopLog{})
	if len(buildErrs) != 0 {
		t.Fatalf("engine.New: %v", buildErrs)
	}
	// The order main shuts down in: the engine stops handing steps to the
	// pool before the pool goes away.
	t.Cleanup(func() {
		en.Stop()
		sch.Stop()
		pool.Stop()
	})
	return en
}

func alarmTriggered() eventbus.Event {
	return eventbus.Event{Topic: "alarm.triggered", Src: "alarm", To: "level-2-triggered"}
}

// TestHazardsBlinkThenStopAfterTheDelay is the sentence the plan set out to
// make true: on alarm, blink, and thirty seconds later stop. The timings are
// the assertion, not just the order: the first push must land well inside the
// delay, and the second must not land before it is out.
func TestHazardsBlinkThenStopAfterTheDelay(t *testing.T) {
	list := testListName(t)
	client := liveClient(t, list)
	en := newEngine(t, rulesDir(t, hazardsRule(list, shortAfter.String())), client, newMemHasher())

	fired := time.Now()
	en.Handle(alarmTriggered())

	waitFor(t, "the hazards to come on", func() bool { return len(pushed(t, client, list)) >= 1 })
	if took := time.Since(fired); took >= shortAfter {
		t.Errorf("the first push took %v, which is past the %v delay; the first step must not wait for anything", took, shortAfter)
	}
	if got := pushed(t, client, list); !equal(got, []string{"both"}) {
		t.Fatalf("pushed %v, want both", got)
	}

	waitFor(t, "the hazards to go off again", func() bool { return len(pushed(t, client, list)) >= 2 })
	if took := time.Since(fired); took < shortAfter {
		t.Errorf("the second push landed after %v, inside the %v delay; the step must wait its after out", took, shortAfter)
	}
	if got := pushed(t, client, list); !equal(got, []string{"both", "off"}) {
		t.Errorf("pushed %v, want both, off", got)
	}
}

// TestARefireMidWaitRestartsRatherThanDoublePushing: the rule's concurrency
// is restart, so a second alarm while the first run is still counting down
// blinks again and pushes the stop back. Two runs in flight would push off
// twice, the second one against hazards the rider has since had turned on
// again by something else.
func TestARefireMidWaitRestartsRatherThanDoublePushing(t *testing.T) {
	list := testListName(t)
	client := liveClient(t, list)
	en := newEngine(t, rulesDir(t, hazardsRule(list, shortAfter.String())), client, newMemHasher())

	en.Handle(alarmTriggered())
	waitFor(t, "the first blink", func() bool { return len(pushed(t, client, list)) >= 1 })

	refired := time.Now()
	en.Handle(alarmTriggered())
	waitFor(t, "the second blink", func() bool { return len(pushed(t, client, list)) >= 2 })

	waitFor(t, "the stop push", func() bool { return len(pushed(t, client, list)) >= 3 })
	if took := time.Since(refired); took < shortAfter {
		t.Errorf("the stop push landed %v after the second alarm, inside the %v delay; a restart arms a new wait rather than inheriting the old one", took, shortAfter)
	}

	// Long enough for a second run's own stop push to show up if the first
	// run was left in flight beside the replacement.
	time.Sleep(2 * shortAfter)
	want := []string{"both", "both", "off"}
	if got := pushed(t, client, list); !equal(got, want) {
		t.Errorf("pushed %v, want %v; a restart replaces the live run rather than running a second copy of it", got, want)
	}
}

// TestADisarmMidWaitCancelsTheStopPush is the case the plan is named for. The
// rider disarms while the hazards are blinking, something else turns them off,
// and thirty seconds later this rule must not push off into a vehicle that has
// moved on. Its record has to go with it, or the next boot replays exactly the
// push the rider cancelled.
func TestADisarmMidWaitCancelsTheStopPush(t *testing.T) {
	list := testListName(t)
	client := liveClient(t, list)
	h := newMemHasher()
	en := newEngine(t, rulesDir(t, hazardsRule(list, shortAfter.String())), client, h)

	en.Handle(alarmTriggered())
	waitFor(t, "the hazards to come on", func() bool { return len(pushed(t, client, list)) >= 1 })
	waitFor(t, "the stop step to be recorded", func() bool { return h.records() == 1 })

	en.Handle(eventbus.Event{Topic: "alarm.disarmed", Src: "alarm", To: "disarmed"})

	// Well past the delay, so a stop push that was only late rather than
	// cancelled has had every chance to arrive.
	time.Sleep(3 * shortAfter)
	if got := pushed(t, client, list); !equal(got, []string{"both"}) {
		t.Errorf("pushed %v, want only both; a disarm mid-wait must stop the run before its stop push", got)
	}
	if n := h.records(); n != 0 {
		t.Errorf("%d pending record(s) after the disarm, want 0; a record that outlives the cancel replays the cancelled push at the next start", n)
	}
}

// TestARestartMidWaitStillStopsTheHazards is the other half of the same
// hazard. The service going down between the two steps must not leave the
// hazards blinking with nothing left in the system that will ever turn them
// off, so the second start picks the recorded step up and finishes the rule.
func TestARestartMidWaitStillStopsTheHazards(t *testing.T) {
	list := testListName(t)
	client := liveClient(t, list)
	h := newMemHasher()
	dir := rulesDir(t, hazardsRule(list, shortAfter.String()))

	first := newEngine(t, dir, client, h)
	first.Handle(alarmTriggered())
	waitFor(t, "the hazards to come on", func() bool { return len(pushed(t, client, list)) >= 1 })
	waitFor(t, "the stop step to be recorded", func() bool { return h.records() == 1 })

	// The service goes down mid-wait.
	first.Stop()
	if n := h.records(); n != 1 {
		t.Fatalf("%d pending record(s) after shutdown, want 1; without it nothing will ever turn the hazards off", n)
	}
	if got := pushed(t, client, list); !equal(got, []string{"both"}) {
		t.Fatalf("pushed %v before the restart, want only both", got)
	}

	// The next start, reading the same files and the same records.
	second := newEngine(t, dir, client, h)
	if n := second.Replay(5 * time.Minute); n != 1 {
		t.Fatalf("Replay resumed %d step(s), want 1", n)
	}

	waitFor(t, "the resumed run to push the stop", func() bool { return len(pushed(t, client, list)) >= 2 })
	if got := pushed(t, client, list); !equal(got, []string{"both", "off"}) {
		t.Errorf("pushed %v, want both, off", got)
	}
	if n := h.records(); n != 0 {
		t.Errorf("%d pending record(s) once the resumed step fired, want 0", n)
	}
}

// TestTheDocumentedRuleLoadsAndCompiles runs the same file with the thirty
// seconds it is written with everywhere else, without firing it. The rest of
// this file proves the behaviour with a delay short enough to wait out; this
// proves the version anybody will actually copy is a file the loader accepts.
func TestTheDocumentedRuleLoadsAndCompiles(t *testing.T) {
	dir := rulesDir(t, hazardsRule("scooter:blinker", "30s"))

	cfg, loadErrs := rules.Load(dir)
	if len(loadErrs) != 0 {
		t.Fatalf("load: %v", loadErrs)
	}
	if len(cfg.Rules) != 1 {
		t.Fatalf("loaded %d rule(s), want 1", len(cfg.Rules))
	}

	compiled, compileErrs := rules.Compile(cfg.Rules, func(string, string) string { return "" })
	if len(compileErrs) != 0 {
		t.Fatalf("compile: %v", compileErrs)
	}
	if len(compiled) != 1 {
		t.Fatalf("compiled %d rule(s), want 1", len(compiled))
	}

	r := compiled[0]
	if r.Name != "hazards-on-alarm" {
		t.Errorf("rule name is %q, want hazards-on-alarm", r.Name)
	}
	if r.Concurrency != rules.PolicyRestart {
		t.Errorf("concurrency is %q, want restart", r.Concurrency)
	}
	if len(r.CancelOn) != 1 || r.CancelOn[0] != "alarm.disarmed" {
		t.Errorf("cancel-on is %v, want alarm.disarmed", r.CancelOn)
	}
	if len(r.Steps) != 2 {
		t.Fatalf("rule has %d step(s), want 2", len(r.Steps))
	}
	if r.Steps[0].After != 0 {
		t.Errorf("step 0 waits %v, want no wait", r.Steps[0].After)
	}
	if r.Steps[1].After != 30*time.Second {
		t.Errorf("step 1 waits %v, want 30s", r.Steps[1].After)
	}
	if !r.Steps[1].Durable {
		t.Error("step 1 must be durable, or a restart during its thirty seconds loses the stop push")
	}

	en, buildErrs := engine.New(compiled, action.NewPool(1, 8, nopLog{}), sched.New(), nil, nopPusher{}, nopLog{})
	if len(buildErrs) != 0 {
		t.Fatalf("engine.New: %v", buildErrs)
	}
	if en.RuleCount() != 1 {
		t.Fatalf("%d rule(s) live, want 1", en.RuleCount())
	}
	want := []string{eventbus.ChannelPrefix + "alarm.triggered", eventbus.ChannelPrefix + "alarm.disarmed"}
	if got := en.Patterns(); !equal(got, want) {
		t.Errorf("subscribes to %v, want %v; the cancelling topic needs a subscription of its own", got, want)
	}
}

// TestRepeatWithDebounceDispatchesOnceAndRunsEveryPass is the combination
// neither feature's own tests reach. Debounce decides whether and with which
// event the rule fires at all; repeat is bookkeeping inside the one run that
// firing creates. Five events inside one quiet window must therefore produce
// one run of three passes, not five runs, and not one pass.
func TestRepeatWithDebounceDispatchesOnceAndRunsEveryPass(t *testing.T) {
	const (
		window = 60 * time.Millisecond
		passes = 3
	)
	list := testListName(t)
	client := liveClient(t, list)
	en := newEngine(t, rulesDir(t, fmt.Sprintf(`
[[rule]]
name     = "chirp-on-motion"
on       = ["sensor.motion"]
debounce = %q
repeat   = { count = %d, every = "10ms" }

  [[rule.step]]
  do   = "redis"
  list = %q
  push = "chirp"
`, window.String(), passes, list)), client, newMemHasher())

	// Five events, none more than a third of the window apart, so the quiet
	// window never runs out while the source is still flapping.
	for i := 0; i < 5; i++ {
		en.Handle(eventbus.Event{Topic: "sensor.motion", Src: "motion", Data: map[string]any{"n": i}})
		time.Sleep(window / 3)
	}
	if got := pushed(t, client, list); len(got) != 0 {
		t.Errorf("pushed %v while the source was still flapping, want nothing; debounce holds the fire until it goes quiet", got)
	}

	if !eventually(2*time.Second, func() bool { return len(pushed(t, client, list)) >= passes }) {
		t.Fatalf("pushed %v once the source went quiet, want %d chirps; the debounced dispatch runs the whole repeat count",
			pushed(t, client, list), passes)
	}

	// Long enough for a second dispatch's passes to arrive if each event got
	// its own run.
	time.Sleep(4 * window)
	if got := pushed(t, client, list); len(got) != passes {
		t.Errorf("pushed %d time(s) (%v), want %d: one dispatch of a %d-pass repeat", len(got), got, passes, passes)
	}
}
