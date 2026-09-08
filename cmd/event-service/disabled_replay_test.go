package main

import (
	"testing"
	"time"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/engine"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/event-service/internal/sched"
	"github.com/librescoot/event-service/internal/seq"
	"github.com/librescoot/eventbus"
)

type channelPusher struct{ pushes chan string }

func (p channelPusher) LPush(_ string, values ...any) (int64, error) {
	p.pushes <- values[0].(string)
	return 1, nil
}

func TestDisabledRuleReplaysCleanupWithoutSubscribingOrRetriggering(t *testing.T) {
	enabled := false
	cfg := rules.RuleConfig{Name: "cleanup", Source: "cleanup.toml", On: []string{"test.start"},
		CancelOn: []string{"test.cancel"}, Enabled: &enabled,
		Steps: []rules.StepConfig{
			{Do: "redis", List: "test:cleanup", Push: "start"},
			{Do: "redis", List: "test:cleanup", Push: "finish", After: "1s"},
		}}
	compiled, errs := rules.CompileForRuntime([]rules.RuleConfig{cfg}, func(string, string) string { return "" })
	if len(errs) != 0 || len(compiled) != 1 || !compiled[0].ReplayOnly {
		t.Fatalf("compile disabled: %v %v", compiled, errs)
	}
	hash := newMemPending()
	store := seq.NewPendingStore(hash, nopLog{})
	if err := store.Put(seq.Pending{ID: "old-run", Rule: cfg.Name, Source: cfg.Source, Step: 1,
		FireAt: time.Now().Add(100 * time.Millisecond).UnixMilli(), Fingerprint: compiled[0].Steps[1].Fingerprint,
		Event: eventbus.Event{Topic: "test.start"}}); err != nil {
		t.Fatal(err)
	}
	pool := action.NewPool(1, 4, nopLog{})
	pool.Start()
	defer pool.Stop()
	sch := sched.New()
	defer sch.Stop()
	pusher := channelPusher{pushes: make(chan string, 4)}
	en, errs := engine.New(compiled, pool, sch, store, pusher, nopLog{})
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	defer en.Stop()
	stop := startRules(en, time.Minute, func([]string) func() {
		t.Error("replay-only rule must not subscribe")
		return func() {}
	}, nopLog{})
	defer stop()
	if en.RuleCount() != 0 || len(en.Patterns()) != 0 {
		t.Fatal("disabled rule exposed active triggers")
	}
	en.Handle(eventbus.Event{Topic: "test.start"})
	en.Handle(eventbus.Event{Topic: "test.cancel"})
	select {
	case got := <-pusher.pushes:
		if got != "finish" {
			t.Fatalf("disabled rule restarted instead of resuming cleanup: %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("disabled cleanup was lost")
	}
	waitFor(t, func() bool { return en.Active() == 0 })
	if len(pusher.pushes) != 0 {
		t.Fatal("disabled rule fired extra actions")
	}
	pending, err := hash.HGetAll(seq.PendingHash)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending records: %v %v", pending, err)
	}
	info := en.RuleInfo(cfg.Name)
	if info.Loaded || info.LastFire == 0 || info.Errors != 0 {
		t.Fatalf("disabled replay metrics: %+v", info)
	}
}
