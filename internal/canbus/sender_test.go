package canbus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/brutella/can"
)

type fakeConn struct {
	writes, closes     int
	payload            []byte
	writeErr, closeErr error
	short              bool
}

func (c *fakeConn) Write(p []byte) (int, error) {
	c.writes++
	c.payload = append([]byte(nil), p...)
	if c.short {
		return len(p) - 1, c.writeErr
	}
	return len(p), c.writeErr
}
func (c *fakeConn) Close() error { c.closes++; return c.closeErr }

func testSender() (*Sender, *[]*fakeConn) {
	s := New(nil)
	connections := new([]*fakeConn)
	s.dial = func(string) (connection, error) {
		c := &fakeConn{}
		*connections = append(*connections, c)
		return c, nil
	}
	return s, connections
}

func TestSenderLazyReuseAndClose(t *testing.T) {
	s, conns := testSender()
	if len(*conns) != 0 {
		t.Fatal("opened eagerly")
	}
	ctx := context.Background()
	frame := can.Frame{ID: 0x123, Length: 2, Data: [8]byte{0xaa, 0xbb}}
	for _, iface := range []string{"can0", "can0", "can1"} {
		if err := s.Send(ctx, iface, frame); err != nil {
			t.Fatal(err)
		}
	}
	if len(*conns) != 2 || (*conns)[0].writes != 2 || (*conns)[1].writes != 1 {
		t.Fatal("did not reuse per-interface sockets")
	}
	var got can.Frame
	if err := can.Unmarshal((*conns)[0].payload, &got); err != nil || got != frame {
		t.Fatalf("wire frame %+v: %v", got, err)
	}
	if got := s.Stats(); got != (Stats{Sent: 3}) {
		t.Fatal(got)
	}
	for range 2 {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range *conns {
		if c.closes != 1 {
			t.Fatalf("closed %d times", c.closes)
		}
	}
	if err := s.Send(ctx, "can0", frame); err == nil {
		t.Fatal("sent after close")
	}
	if got := s.Stats(); got != (Stats{Sent: 3, Errors: 1}) {
		t.Fatal(got)
	}
}

func TestSenderInvalidatesWithoutRetry(t *testing.T) {
	failure := errors.New("write failed")
	cleanup := errors.New("close failed")
	for _, short := range []bool{false, true} {
		t.Run(fmt.Sprint(short), func(t *testing.T) {
			s, conns := testSender()
			bad := &fakeConn{writeErr: failure, closeErr: cleanup, short: short}
			if short {
				bad.writeErr = nil
			}
			nextDial := s.dial
			s.dial = func(string) (connection, error) { s.dial = nextDial; return bad, nil }
			err := s.Send(context.Background(), "can0", can.Frame{})
			want := failure
			if short {
				want = io.ErrShortWrite
			}
			if !errors.Is(err, want) || !errors.Is(err, cleanup) {
				t.Fatalf("missing error: %v", err)
			}
			if bad.writes != 1 || bad.closes != 1 || len(*conns) != 0 {
				t.Fatal("retried failed write or failed to close")
			}
			if err := s.Send(context.Background(), "can0", can.Frame{}); err != nil {
				t.Fatal(err)
			}
			if len(*conns) != 1 || (*conns)[0].writes != 1 {
				t.Fatal("next send did not reopen")
			}
			if got := s.Stats(); got != (Stats{Sent: 1, Errors: 1}) {
				t.Fatal(got)
			}
			_ = s.Close()
		})
	}
}

func TestSenderDialFailureAndCleanupError(t *testing.T) {
	s, _ := testSender()
	failure := errors.New("dial failed")
	dials := 0
	s.dial = func(string) (connection, error) { dials++; return nil, failure }
	for range 2 {
		if err := s.Send(context.Background(), "can0", can.Frame{}); !errors.Is(err, failure) {
			t.Fatal(err)
		}
	}
	if dials != 2 || len(s.connections) != 0 || s.Stats().Errors != 2 {
		t.Fatal("dial failure cached or uncounted")
	}
	c := &fakeConn{closeErr: failure}
	s.connections["can0"] = c
	for range 2 {
		if err := s.Close(); !errors.Is(err, failure) {
			t.Fatal(err)
		}
	}
	if c.closes != 1 {
		t.Fatal("retried close")
	}
}

func TestCancelledContextsNeverWrite(t *testing.T) {
	s, conns := testSender()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Send(ctx, "can0", can.Frame{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(*conns) != 0 {
		t.Fatal("cancelled send opened socket")
	}
	ctx, cancel = context.WithCancel(context.Background())
	original := s.dial
	s.dial = func(iface string) (connection, error) { c, err := original(iface); cancel(); return c, err }
	if err := s.Send(ctx, "can0", can.Frame{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if (*conns)[0].writes != 0 {
		t.Fatal("wrote after cancellation during dial")
	}
	if s.Stats().Errors != 2 {
		t.Fatal(s.Stats())
	}
	_ = s.Close()
}

// observedContext synchronizes exactly with the initial, pre-lock context check.
type observedContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (c *observedContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.checked) })
	return err
}

func TestCancellationWhileWaitingForMutex(t *testing.T) {
	s, conns := testSender()
	ctx, cancel := context.WithCancel(context.Background())
	observed := &observedContext{Context: ctx, checked: make(chan struct{})}
	s.mu.Lock()
	done := make(chan error, 1)
	go func() { done <- s.Send(observed, "can0", can.Frame{}) }()
	<-observed.checked
	cancel()
	s.mu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(*conns) != 0 {
		t.Fatal("opened after cancellation waiting for mutex")
	}
}

func TestDirectFrameValidation(t *testing.T) {
	s, conns := testSender()
	for _, frame := range []can.Frame{
		{Length: 9}, {Length: 255}, {Flags: 1}, {Res0: 1}, {Res1: 1}, {ID: errFlag}, {ID: 0x800}, {ID: rtrFlag, Data: [8]byte{1}},
	} {
		if err := s.Send(context.Background(), "can0", frame); err == nil {
			t.Fatalf("accepted %+v", frame)
		}
	}
	if err := s.Send(context.Background(), "", can.Frame{}); err == nil {
		t.Fatal("accepted empty interface")
	}
	if len(*conns) != 0 || s.Stats().Errors != 9 {
		t.Fatal("invalid input dialed or uncounted")
	}
}

func TestSenderSocketBound(t *testing.T) {
	s, conns := testSender()
	for i := 0; i < maxOpenInterfaces; i++ {
		if err := s.Send(context.Background(), fmt.Sprintf("can%d", i), can.Frame{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Send(context.Background(), "overflow", can.Frame{}); err == nil {
		t.Fatal("unbounded sockets")
	}
	if len(*conns) != maxOpenInterfaces {
		t.Fatal(len(*conns))
	}
	if err := s.Send(context.Background(), "can0", can.Frame{}); err != nil {
		t.Fatal("limit blocked reuse", err)
	}
	_ = s.Close()
}

func TestDebugCallbackOutsideMutex(t *testing.T) {
	s, _ := testSender()
	calls := 0
	s.debugf = func(string, ...any) { calls++; _ = s.Stats(); _ = s.Close() }
	if err := s.Send(context.Background(), "can0", can.Frame{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Send(context.Background(), "can0", can.Frame{}); err == nil {
		t.Fatal("callback did not close")
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}

func TestConcurrentSendAndClose(t *testing.T) {
	s, conns := testSender()
	var callbacks atomic.Int64
	s.debugf = func(string, ...any) { callbacks.Add(1); _ = s.Stats() }
	const sends = 200
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < sends; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; _ = s.Send(context.Background(), "can0", can.Frame{}) }()
	}
	for range 10 {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; _ = s.Close() }()
	}
	close(start)
	wg.Wait()
	stats := s.Stats()
	if stats.Sent+stats.Errors != sends || callbacks.Load() != sends {
		t.Fatal(stats, callbacks.Load())
	}
	var writes uint64
	for _, c := range *conns {
		writes += uint64(c.writes)
		if c.closes != 1 {
			t.Fatal("double close")
		}
	}
	if writes != stats.Sent {
		t.Fatal(writes, stats)
	}
}
