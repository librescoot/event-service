package action

import (
	"context"
	"sync"
	"testing"

	"github.com/librescoot/eventbus"
)

type fakePusher struct {
	mu  sync.Mutex
	got []string
	err error
}

func (f *fakePusher) LPush(key string, values ...any) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	for _, v := range values {
		f.got = append(f.got, key+"="+v.(string))
	}
	return int64(len(f.got)), nil
}

func TestRedisActionPushesTheConfiguredValue(t *testing.T) {
	p := &fakePusher{}
	a, err := NewRedisAction(p, "scooter:blinker", "both")
	if err != nil {
		t.Fatalf("NewRedisAction: %v", err)
	}
	if a.Kind() != "redis" {
		t.Errorf("Kind() = %q, want redis", a.Kind())
	}
	if err := a.Do(context.Background(), eventbus.Event{Topic: "alarm.triggered"}); err != nil {
		t.Fatalf("Do: %v", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.got) != 1 || p.got[0] != "scooter:blinker=both" {
		t.Errorf("pushed %v, want [scooter:blinker=both]", p.got)
	}
}

func TestRedisActionRequiresListAndPush(t *testing.T) {
	if _, err := NewRedisAction(&fakePusher{}, "", "both"); err == nil {
		t.Error("empty list must be rejected at construction, not at fire time")
	}
	if _, err := NewRedisAction(&fakePusher{}, "scooter:horn", ""); err == nil {
		t.Error("empty push must be rejected at construction")
	}
}
