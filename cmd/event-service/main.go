package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/librescoot/event-service/internal/action"
	"github.com/librescoot/event-service/internal/adapter"
	"github.com/librescoot/event-service/internal/engine"
	"github.com/librescoot/event-service/internal/rules"
	"github.com/librescoot/event-service/internal/sched"
	"github.com/librescoot/event-service/internal/seq"
	"github.com/librescoot/event-service/internal/shadow"
	"github.com/librescoot/event-service/internal/stats"
	"github.com/librescoot/eventbus"
	ipc "github.com/librescoot/redis-ipc"
)

var version = "dev"

// defaultStatsInterval is how often the counters in the extensions hash are
// refreshed. Only fields whose value changed are written, so an idle rule set
// costs one wakeup at this interval and no datastore traffic at all.
const defaultStatsInterval = 10 * time.Second

// logger is the small slice of logging this file needs, the same interface
// every internal package takes.
type logger interface {
	Printf(format string, v ...any)
}

// buildSnapshot builds the counter map published to the extensions hash.
// dropped and refused are kept apart on purpose: dropped is action.Pool
// turning away work because its queue is full or it is stopping, the lever
// for which is --workers or --queue, while refused is one rule's own
// concurrency backlog saturating, the lever for which is inside that rule's
// sequence. Merging them into one number would hide which of two unrelated
// problems an operator is looking at.
//
// Pulled out of main so its field mapping has something other than main
// itself to exercise it: main is not otherwise reached by the test suite.
func buildSnapshot(pool *action.Pool, sch *sched.Scheduler, en *engine.Engine, version string) map[string]string {
	ps := pool.Stats()
	return map[string]string{
		"rules":       strconv.Itoa(en.RuleCount()),
		"dispatched":  strconv.FormatUint(ps.Dispatched, 10),
		"dropped":     strconv.FormatUint(ps.Dropped, 10),
		"refused":     strconv.FormatUint(en.Refused(), 10),
		"failed":      strconv.FormatUint(ps.Failed, 10),
		"pending":     strconv.Itoa(sch.Pending()),
		"runs-active": strconv.Itoa(en.Active()),
		"version":     version,
	}
}

// startRules resumes the steps recorded before the last shutdown and then, if
// any rule has something to listen for, opens the bus subscription. It returns
// the function that closes that subscription again; with no rules live there
// is no subscription and the returned function does nothing.
//
// The order is why this is a function of its own rather than four lines in
// main. A step recorded before the restart has to be back in the runner before
// the bus can deliver anything: subscribe first and a live event can fire the
// same rule while its resumed tail is not registered yet, so the rule's
// concurrency policy is applied against a run that is not there, nothing is
// restarted or dropped, and both the resumed tail and the fresh run act on the
// vehicle.
func startRules(en *engine.Engine, replayWindow time.Duration, subscribe func(patterns []string) (stop func()), log logger) func() {
	if n := en.Replay(replayWindow); n > 0 {
		log.Printf("rules: resumed %d pending step(s) from before the restart", n)
	}

	// No rules means no subscription at all, which is what keeps an idle
	// scooter at zero events per second: nothing here wakes the process on bus
	// traffic that nothing could match anyway.
	if en.RuleCount() == 0 {
		log.Printf("rules: no rules live, not subscribing to the bus")
		return func() {}
	}

	patterns := en.Patterns()
	log.Printf("rules subscribing to: %v", patterns)
	return subscribe(patterns)
}

func main() {
	var (
		redisAddr = flag.String("redis", "localhost:6379", "datastore address")
		logLevel  = flag.String("log-level", "info", "log level: debug, info, warn, error")
		rulesDir  = flag.String("rules-dir", "/data/extensions", "directory of rule TOML files")
		workers   = flag.Int("workers", 2, "action worker count")
		queue     = flag.Int("queue", 256, "action queue depth")
		replayWin = flag.Duration("replay-window", 5*time.Minute, "how far past due a recorded step may be and still run at start; zero or less replays only steps still in the future")
		statsEach = flag.Duration("stats-interval", defaultStatsInterval, "how often to refresh the counters in the extensions hash")
	)
	flag.Parse()

	// journald stamps its own timestamps, so ours would be duplicated.
	if os.Getenv("JOURNAL_STREAM") != "" {
		log.SetFlags(0)
	} else {
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	}

	log.Printf("event-service %s starting (log-level=%s)", version, *logLevel)

	statsInterval := *statsEach
	if statsInterval <= 0 {
		// A ticker of zero panics, and refusing to start over a counter
		// interval would take the rules down with it on a vehicle.
		log.Printf("stats: --stats-interval must be positive, using %v", defaultStatsInterval)
		statsInterval = defaultStatsInterval
	}

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

	cfg, loadErrs := rules.Load(*rulesDir)
	for _, err := range loadErrs {
		log.Printf("rules: %v", err)
	}

	compiled, compileErrs := rules.Compile(cfg.Rules, sh.Get)
	for _, err := range compileErrs {
		log.Printf("rules: %v", err)
	}

	sch := sched.New()

	pool := action.NewPool(*workers, *queue, log.Default())
	pool.Start()

	store := seq.NewPendingStore(seq.NewClientHasher(client), log.Default())

	en, buildErrs := engine.New(compiled, pool, sch, store, client, log.Default())
	for _, err := range buildErrs {
		log.Printf("rules: %v", err)
	}

	log.Printf("%d rules live from %s", en.RuleCount(), *rulesDir)

	// The publisher only reads counters, all of which are safe to read at any
	// point in the process's life, so it goes up before the bus opens and the
	// hash is populated before anything can move.
	statsPub := stats.NewPublisher(client, statsInterval, log.Default())
	statsPub.Start(func() map[string]string {
		return buildSnapshot(pool, sch, en, version)
	})

	stopSub := startRules(en, *replayWin, func(patterns []string) func() {
		psub := client.Raw().PSubscribe(client.Context(), patterns...)
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
		return func() { _ = psub.Close() }
	}, log.Default())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")

	// Shutdown is one ordered list rather than a stack of defers: the order is
	// the point, and reading it out of a LIFO stack spread over sixty lines is
	// how it came to be wrong in the first place. Each line below is what the
	// line after it needs to have happened already.
	//
	// The bus closes first, so nothing new can start a run. The engine then
	// abandons the runs that are in flight, cutting loose every pending tail
	// and debounce timer, which is what stops a sequence handing another step
	// to a pool that is about to go away; the steps still waiting out an after
	// keep their records, so the next start finishes them. The scheduler goes
	// after the engine, since by then nothing is left to arm a new timer. The
	// pool then cancels whatever its workers are running and waits for them.
	// Stats goes after all of that, so the last snapshot it may be writing
	// reports counters that have stopped moving. The adapter and the client go
	// last, because everything above writes through them.
	stopSub()
	en.Stop()
	sch.Stop()
	pool.Stop()
	statsPub.Stop()
	ad.Stop()
	if err := client.Close(); err != nil {
		log.Printf("datastore close: %v", err)
	}
}
