package adapter

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/librescoot/event-service/internal/shadow"
	"github.com/librescoot/eventbus"
	ipc "github.com/librescoot/redis-ipc"
)

// These tests exercise Start() against a real datastore instead of faking
// the ipc.Client, because the two bugs they guard against (a fake transition
// on every seeded field, and a name silently double-dispatched as both a
// hash and a channel) only show up once HashWatcher.StartWithSync and the
// pubsub mux are actually driving dispatchField and dispatchMessage.

func newLiveClient(t *testing.T) *ipc.Client {
	t.Helper()
	client, err := ipc.New(
		ipc.WithURL("localhost:6379"),
		ipc.WithDialTimeout(2*time.Second),
		ipc.WithCodec(ipc.StringCodec{}),
	)
	if err != nil {
		t.Skipf("redis not reachable at localhost:6379: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

type collectingEmitter struct {
	mu     sync.Mutex
	events []eventbus.Event
}

func (e *collectingEmitter) Emit(ev eventbus.Event) error {
	e.mu.Lock()
	e.events = append(e.events, ev)
	e.mu.Unlock()
	return nil
}

func (e *collectingEmitter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.events)
}

type fieldCall struct {
	hash, field, value, prev string
}

type messageCall struct {
	channel, payload string
}

// recordingSource records every OnField/OnMessage call verbatim and returns
// one placeholder event per call, so a test can check both the derived
// event count and the exact arguments the adapter passed in.
type recordingSource struct {
	hashes   []string
	channels []string

	mu       sync.Mutex
	fields   []fieldCall
	messages []messageCall
}

func (s *recordingSource) Hashes() []string   { return s.hashes }
func (s *recordingSource) Channels() []string { return s.channels }

func (s *recordingSource) OnField(hash, field, value, prev string) []eventbus.Event {
	s.mu.Lock()
	s.fields = append(s.fields, fieldCall{hash, field, value, prev})
	s.mu.Unlock()
	return []eventbus.Event{eventbus.New("test.field", "test")}
}

func (s *recordingSource) OnMessage(channel, payload string) []eventbus.Event {
	s.mu.Lock()
	s.messages = append(s.messages, messageCall{channel, payload})
	s.mu.Unlock()
	return []eventbus.Event{eventbus.New("test.message", "test")}
}

func (s *recordingSource) fieldCallsSnapshot() []fieldCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]fieldCall, len(s.fields))
	copy(out, s.fields)
	return out
}

func (s *recordingSource) messageCallsSnapshot() []messageCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]messageCall, len(s.messages))
	copy(out, s.messages)
	return out
}

// TestStartSeedsExistingFieldsWithoutEmittingEvents is the regression test
// for Finding 1: fields that already existed in the hash before Start() must
// populate the shadow store but must not be reported as transitions.
func TestStartSeedsExistingFieldsWithoutEmittingEvents(t *testing.T) {
	client := newLiveClient(t)
	const hash = "event-service-test:adapter:seed-hash"
	t.Cleanup(func() { _, _ = client.Del(hash) })

	if err := client.HSet(hash, "state", "parked"); err != nil {
		t.Fatalf("HSet state: %v", err)
	}
	if err := client.HSet(hash, "kickstand", "down"); err != nil {
		t.Fatalf("HSet kickstand: %v", err)
	}

	em := &collectingEmitter{}
	sh := shadow.NewStore()
	src := &recordingSource{hashes: []string{hash}}
	a := New(client, em, sh)
	a.Register(src)

	if err := a.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(a.Stop)

	if n := em.count(); n != 0 {
		t.Fatalf("emitted %d events for pre-existing fields, want 0", n)
	}
	if calls := src.fieldCallsSnapshot(); len(calls) != 0 {
		t.Fatalf("OnField called %d times during seeding, want 0: %+v", len(calls), calls)
	}
	if got := sh.Get(hash, "state"); got != "parked" {
		t.Errorf("shadow state = %q, want %q", got, "parked")
	}
	if got := sh.Get(hash, "kickstand"); got != "down" {
		t.Errorf("shadow kickstand = %q, want %q", got, "down")
	}
}

// TestFieldChangedAfterStartEmitsEventWithSeededFrom checks the other half
// of Finding 1: once Start() has returned, a real field change must produce
// exactly one event, and "from" must be the value seeded at startup rather
// than empty.
func TestFieldChangedAfterStartEmitsEventWithSeededFrom(t *testing.T) {
	client := newLiveClient(t)
	const hash = "event-service-test:adapter:live-hash"
	t.Cleanup(func() { _, _ = client.Del(hash) })

	if err := client.HSet(hash, "state", "parked"); err != nil {
		t.Fatalf("HSet state: %v", err)
	}

	em := &collectingEmitter{}
	sh := shadow.NewStore()
	src := &recordingSource{hashes: []string{hash}}
	a := New(client, em, sh)
	a.Register(src)

	if err := a.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(a.Stop)

	pub := client.NewHashPublisher(hash)
	if err := pub.Set("state", "moving", ipc.Sync()); err != nil {
		t.Fatalf("publish field change: %v", err)
	}

	if !waitFor(2*time.Second, func() bool { return em.count() == 1 }) {
		t.Fatalf("emitted %d events after live change, want 1", em.count())
	}

	calls := src.fieldCallsSnapshot()
	if len(calls) != 1 {
		t.Fatalf("OnField called %d times, want 1: %+v", len(calls), calls)
	}
	if calls[0].value != "moving" || calls[0].prev != "parked" {
		t.Fatalf("OnField(hash=%q field=%q value=%q prev=%q), want value=%q prev=%q",
			calls[0].hash, calls[0].field, calls[0].value, calls[0].prev, "moving", "parked")
	}
}

// TestMessageOnChannelReachesOnMessage checks raw pub/sub delivery: a
// message on a subscribed channel must reach OnMessage with the payload
// verbatim, which is why main.go must configure StringCodec.
func TestMessageOnChannelReachesOnMessage(t *testing.T) {
	client := newLiveClient(t)
	const channel = "event-service-test:adapter:message-channel"

	em := &collectingEmitter{}
	sh := shadow.NewStore()
	src := &recordingSource{channels: []string{channel}}
	a := New(client, em, sh)
	a.Register(src)

	if err := a.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(a.Stop)

	if _, err := client.Publish(channel, "horn:tap", ipc.Sync()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if !waitFor(2*time.Second, func() bool { return len(src.messageCallsSnapshot()) == 1 }) {
		t.Fatalf("OnMessage called %d times, want 1", len(src.messageCallsSnapshot()))
	}

	calls := src.messageCallsSnapshot()
	if calls[0].channel != channel || calls[0].payload != "horn:tap" {
		t.Fatalf("OnMessage(channel=%q payload=%q), want channel=%q payload=%q",
			calls[0].channel, calls[0].payload, channel, "horn:tap")
	}
}

// TestStartErrorsWhenNameIsBothHashAndChannel is the regression test for
// Finding 2: one source declaring a name as a hash and another declaring
// the same name as a channel is a programming error and must fail loudly
// instead of silently wiring up both a HashWatcher and a raw Subscribe.
func TestStartErrorsWhenNameIsBothHashAndChannel(t *testing.T) {
	const dup = "event-service-test:adapter:dup-key"

	a := New(nil, nil, shadow.NewStore())
	a.Register(&recordingSource{hashes: []string{dup}})
	a.Register(&recordingSource{channels: []string{dup}})

	err := a.Start()
	if err == nil {
		t.Fatal("Start() = nil, want an error naming the colliding key")
	}
	if !strings.Contains(err.Error(), dup) {
		t.Fatalf("Start() error = %q, want it to mention %q", err.Error(), dup)
	}
}
