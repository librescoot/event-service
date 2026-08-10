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
	"github.com/librescoot/event-service/internal/shadow"
	"github.com/librescoot/eventbus"
	ipc "github.com/librescoot/redis-ipc"
)

var version = "dev"

func main() {
	var (
		redisAddr = flag.String("redis", "localhost:6379", "datastore address")
		logLevel  = flag.String("log-level", "info", "log level: debug, info, warn, error")
		rulesDir  = flag.String("rules-dir", "/data/extensions", "directory of rule TOML files")
		workers   = flag.Int("workers", 2, "action worker count")
		queue     = flag.Int("queue", 256, "action queue depth")
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

	pool := action.NewPool(*workers, *queue, log.Default())
	pool.Start()
	defer pool.Stop()

	en, buildErrs := engine.New(compiled, pool, client, log.Default())
	for _, err := range buildErrs {
		log.Printf("rules: %v", err)
	}
	// Registered after pool.Stop so it runs before it: a sequence must stop
	// handing steps to the pool before the pool goes away.
	defer en.Stop()

	log.Printf("%d rules live from %s", en.RuleCount(), *rulesDir)

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
