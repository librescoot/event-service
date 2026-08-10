package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/adapter"
	"github.com/librescoot/event-service/internal/engine"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/event-service/internal/sched"
	"github.com/librescoot/event-service/internal/seq"
	"github.com/librescoot/event-service/internal/shadow"
	"github.com/librescoot/eventbus"
	ipc "github.com/librescoot/redis-ipc"
)

var version = "dev"

// pendingHash gives the pending store the three hash operations it asks for.
// The client has no HDel of its own, so that one goes through the raw command
// interface.
type pendingHash struct{ c *ipc.Client }

func (h pendingHash) HSet(key, field string, value any) error {
	return h.c.HSet(key, field, value)
}

func (h pendingHash) HGetAll(key string) (map[string]string, error) {
	return h.c.HGetAll(key)
}

func (h pendingHash) HDel(key string, fields ...string) error {
	args := make([]any, 0, len(fields)+1)
	args = append(args, key)
	for _, f := range fields {
		args = append(args, f)
	}
	_, err := h.c.Do("HDEL", args...)
	return err
}

func main() {
	var (
		redisAddr = flag.String("redis", "localhost:6379", "datastore address")
		logLevel  = flag.String("log-level", "info", "log level: debug, info, warn, error")
		rulesDir  = flag.String("rules-dir", "/data/extensions", "directory of rule TOML files")
		workers   = flag.Int("workers", 2, "action worker count")
		queue     = flag.Int("queue", 256, "action queue depth")
		replayWin = flag.Duration("replay-window", 5*time.Minute, "how far past due a recorded step may be and still run at start")
	)
	flag.Parse()

	// journald stamps its own timestamps, so ours would be duplicated.
	if os.Getenv("JOURNAL_STREAM") != "" {
		log.SetFlags(0)
	} else {
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	}

	log.Printf("event-service %s starting (log-level=%s)", version, *logLevel)

	client, err := ipc.New(
		ipc.WithURL(*redisAddr),
		ipc.WithDialTimeout(2*time.Second),
		ipc.WithCodec(ipc.StringCodec{}),
		ipc.WithOnConnect(func() { log.Printf("datastore connected") }),
		ipc.WithOnDisconnect(func(err error) { log.Printf("datastore disconnected: %v", err) }),
	)
	if err != nil {
		log.Fatalf("cannot reach datastore at %s: %v", *redisAddr, err)
	}
	defer client.Close()

	sh := shadow.NewStore()
	pub := eventbus.NewPublisher(client, "adapter")

	ad := adapter.New(client, pub, sh)
	ad.Register(adapter.NewVehicleSource())
	ad.Register(adapter.NewInputSource())
	ad.Register(adapter.NewBatterySource())
	ad.Register(adapter.NewMiscSource(adapter.NewLiveLookup(client)))

	log.Printf("subscribing to: %v", ad.Subscriptions())
	if err := ad.Start(); err != nil {
		log.Fatalf("adapter start: %v", err)
	}
	defer ad.Stop()

	cfg, loadErrs := rules.Load(*rulesDir)
	for _, err := range loadErrs {
		log.Printf("rules: %v", err)
	}

	compiled, compileErrs := rules.Compile(cfg.Rules, sh.Get)
	for _, err := range compileErrs {
		log.Printf("rules: %v", err)
	}

	sch := sched.New()
	defer sch.Stop()

	pool := action.NewPool(*workers, *queue, log.Default())
	pool.Start()
	defer pool.Stop()

	store := seq.NewPendingStore(pendingHash{c: client}, log.Default())

	en, buildErrs := engine.New(compiled, pool, sch, store, client, log.Default())
	for _, err := range buildErrs {
		log.Printf("rules: %v", err)
	}
	// Registered after pool.Stop so it runs before it: a sequence must stop
	// handing steps to the pool before the pool goes away.
	defer en.Stop()

	log.Printf("%d rules live from %s", en.RuleCount(), *rulesDir)

	// Before the subscription, never after: a step picked up here must not
	// race a live event re-firing the same rule.
	if n := en.Replay(*replayWin); n > 0 {
		log.Printf("rules: resumed %d pending step(s) from before the restart", n)
	}

	if en.RuleCount() > 0 {
		patterns := en.Patterns()
		log.Printf("rules subscribing to: %v", patterns)
		psub := client.Raw().PSubscribe(client.Context(), patterns...)
		defer psub.Close()
		go func() {
			for msg := range psub.Channel() {
				var e eventbus.Event
				if err := json.Unmarshal([]byte(msg.Payload), &e); err != nil {
					log.Printf("rules: bad event payload on %s: %v", msg.Channel, err)
					continue
				}
				en.Handle(e)
			}
		}()
	} else {
		log.Printf("rules: no rules live, not subscribing to the bus")
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")
}
