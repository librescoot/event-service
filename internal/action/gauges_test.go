package action

import (
	"context"
	"io"
	"log"
	"testing"
	"time"

	"github.com/librescoot/eventbus"
)

func TestPoolReportsActualQueueAndBusyWorkers(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	first := actionFunc(func(ctx context.Context, _ eventbus.Event) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	pool := NewPool(1, 1, log.New(io.Discard, "", 0))
	pool.Start()
	defer pool.Stop()
	done := make(chan error, 2)
	callback := func(err error) { done <- err }
	if !pool.Submit(first, eventbus.Event{}, "test", callback) {
		t.Fatal("first refused")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker not started")
	}
	if !pool.Submit(actionFunc(func(context.Context, eventbus.Event) error { return nil }), eventbus.Event{}, "test", callback) {
		t.Fatal("queued job refused")
	}
	if pool.Workers() != 1 || pool.BusyWorkers() != 1 || pool.QueueDepth() != 1 || pool.QueueCapacity() != 1 {
		t.Fatal("worker gauges do not reflect active action and queued work")
	}
	close(release)
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("job did not complete")
		}
	}
	pool.Stop()
	if pool.BusyWorkers() != 0 || pool.QueueDepth() != 0 {
		t.Fatal("gauges did not drain")
	}
}
