package main

import (
	"strconv"
	"testing"
	"time"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/engine"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/event-service/internal/sched"
	"github.com/librescoot/eventbus"
)

type nopLog struct{}

func (nopLog) Printf(string, ...any) {}

type nopPusher struct{}

func (nopPusher) LPush(string, ...any) (int64, error) { return 1, nil }

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met within 2s")
	}
}

// TestBuildSnapshotKeepsDroppedAndRefusedApart is the field-mapping test
// dropped and refused need: dropped must come from the pool alone and
// refused from the runner alone, with neither leaking into the other. The
// two are driven to different, nonzero, mutually distinguishable counts so
// a swap between them is not accidentally invisible.
func TestBuildSnapshotKeepsDroppedAndRefusedApart(t *testing.T) {
	// A one-slot, unstarted pool: the first Submit fills the queue, the
	// second finds it full and is dropped. Unstarted, so there is nothing
	// to Stop.
	pool := action.NewPool(1, 1, nopLog{})
	act, err := action.Build(action.Spec{Do: "redis", List: "x", Push: "1"}, nopPusher{})
	if err != nil {
		t.Fatalf("action.Build: %v", err)
	}
	pool.Submit(act, eventbus.Event{}, "filler", nil)
	pool.Submit(act, eventbus.Event{}, "filler", nil) // dropped: queue full

	if got := pool.Stats().Dropped; got != 1 {
		t.Fatalf("pool dropped %d job(s) setting up the test, want 1", got)
	}

	// A queue-policy rule whose only step parks for an hour, so the live
	// run never ends for the length of the test. maxQueued is 8
	// (internal/seq): the first Handle starts the run, the next 8 fill the
	// backlog, and the remaining 5 are refused.
	sch := sched.New()
	defer sch.Stop()
	rs, errs := rules.Compile([]rules.RuleConfig{{
		Name: "r", On: []string{"x.y"}, Concurrency: "queue",
		Steps: []rules.StepConfig{{Do: "redis", List: "a", Push: "1", After: "1h"}},
	}}, func(string, string) string { return "" })
	if len(errs) != 0 {
		t.Fatalf("compile: %v", errs)
	}
	runnerPool := action.NewPool(1, 8, nopLog{})
	runnerPool.Start()
	defer runnerPool.Stop()
	en, buildErrs := engine.New(rs, runnerPool, sch, nil, nopPusher{}, nopLog{})
	if len(buildErrs) != 0 {
		t.Fatalf("engine.New: %v", buildErrs)
	}
	defer en.Stop()

	for i := 0; i < 1+8+5; i++ {
		en.Handle(eventbus.Event{Topic: "x.y"})
	}
	waitFor(t, func() bool { return en.Refused() == 5 })

	got := buildSnapshot(pool, sch, en, "v1.2.3")

	if got["dropped"] != strconv.FormatUint(pool.Stats().Dropped, 10) {
		t.Errorf(`snapshot["dropped"] = %q, want the pool's own Dropped (%d)`, got["dropped"], pool.Stats().Dropped)
	}
	if got["refused"] != strconv.FormatUint(en.Refused(), 10) {
		t.Errorf(`snapshot["refused"] = %q, want the runner's own Refused (%d)`, got["refused"], en.Refused())
	}
	if got["dropped"] == got["refused"] {
		t.Fatalf("dropped and refused came out equal (%q); the test cannot tell a swap from a correct mapping this way", got["dropped"])
	}
	if got["dropped"] != "1" {
		t.Errorf(`snapshot["dropped"] = %q, want "1"; it must not include the runner's refusals`, got["dropped"])
	}
	if got["refused"] != "5" {
		t.Errorf(`snapshot["refused"] = %q, want "5"; it must not include the pool's drops`, got["refused"])
	}
}
