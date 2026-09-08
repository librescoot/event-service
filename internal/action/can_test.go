package action

import (
	"bytes"
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/brutella/can"
	"github.com/librescoot/eventbus"
)

type recordingCAN struct {
	iface string
	frame can.Frame
	calls int
	err   error
}

func (s *recordingCAN) Send(ctx context.Context, iface string, frame can.Frame) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.iface, s.frame = iface, frame
	s.calls++
	return s.err
}

func TestBuildCANIsLazyAndSendsFrame(t *testing.T) {
	sender := &recordingCAN{}
	a, err := Build(Spec{Do: "can", Iface: "can0", ID: "0x123", Data: "01 02"}, nil, WithCAN(sender))
	if err != nil {
		t.Fatal(err)
	}
	if sender.calls != 0 || a.Kind() != "can" {
		t.Fatal("building a CAN action must not send")
	}
	if err := a.Do(t.Context(), eventbus.Event{}); err != nil {
		t.Fatal(err)
	}
	if sender.calls != 1 || sender.iface != "can0" || sender.frame.ID != 0x123 || sender.frame.Length != 2 || sender.frame.Data[1] != 2 {
		t.Fatalf("unexpected CAN send: %+v", sender)
	}
}

func TestBuildCANRejectsBadFrameBeforeExecution(t *testing.T) {
	sender := &recordingCAN{}
	if _, err := Build(Spec{Do: "can", Iface: "can0", ID: "20000000"}, nil, WithCAN(sender)); err == nil {
		t.Fatal("over-wide ID accepted")
	}
	if sender.calls != 0 {
		t.Fatal("invalid configuration sent a frame")
	}
	if _, err := Build(Spec{Do: "can", Iface: "can0", ID: "123"}, nil); err == nil {
		t.Fatal("missing sender accepted")
	}
}

func TestCANFailureIsCountedWithoutInfoLogging(t *testing.T) {
	sender := &recordingCAN{err: errors.New("bus unavailable")}
	a, err := Build(Spec{Do: "can", Iface: "can0", ID: "123"}, nil, WithCAN(sender))
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	pool := NewPool(1, 1, log.New(&logs, "", 0))
	pool.Start()
	t.Cleanup(pool.Stop)
	done := make(chan error, 1)
	if !pool.Submit(a, eventbus.Event{}, "test", func(err error) { done <- err }) {
		t.Fatal("submit refused")
	}
	select {
	case err := <-done:
		if !errors.Is(err, sender.err) {
			t.Fatalf("error lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not complete")
	}
	pool.Stop()
	if pool.Stats().Failed != 1 || pool.Stats().Dispatched != 1 || logs.Len() != 0 {
		t.Fatalf("stats=%+v logs=%q", pool.Stats(), logs.String())
	}
}
