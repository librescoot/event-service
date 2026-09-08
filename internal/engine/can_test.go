package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/brutella/can"
	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/eventbus"
)

type channelCAN struct{ frames chan can.Frame }

func (s channelCAN) Send(ctx context.Context, _ string, f can.Frame) error {
	select {
	case s.frames <- f:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestCANRulesLoadAndRunSequence(t *testing.T) {
	dir := t.TempDir()
	const toml = `[[rule]]
name = "can-test"
on = ["test.can"]
[[rule.step]]
do = "can"
iface = "can0"
id = "0x123"
data = "01 02 03"
[[rule.step]]
after = "1ms"
durable = false
do = "can"
iface = "can0"
id = "0x12345"
rtr = true
dlc = 8
`
	if err := os.WriteFile(filepath.Join(dir, "can.toml"), []byte(toml), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, errs := rules.Load(dir)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	compiled := compileRules(t, cfg.Rules...)
	pool := action.NewPool(1, 2, nopLog{})
	pool.Start()
	defer pool.Stop()
	sch := testSched(t)
	sender := channelCAN{frames: make(chan can.Frame, 2)}
	en, errs := New(compiled, pool, sch, nil, nil, nopLog{}, action.WithCAN(sender))
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	defer en.Stop()
	if len(sender.frames) != 0 {
		t.Fatal("building rules must not send CAN")
	}
	en.Handle(eventbus.Event{Topic: "test.can"})
	for i, want := range []struct {
		id     uint32
		length uint8
	}{{0x123, 3}, {0xc0012345, 8}} {
		select {
		case f := <-sender.frames:
			if f.ID != want.id || f.Length != want.length {
				t.Fatalf("step %d frame=%+v", i, f)
			}
		case <-time.After(time.Second):
			t.Fatalf("step %d did not execute", i)
		}
	}
}
