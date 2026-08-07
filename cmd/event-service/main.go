package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	ipc "github.com/librescoot/redis-ipc"
)

var version = "dev"

func main() {
	var (
		redisAddr = flag.String("redis", "localhost:6379", "datastore address")
		logLevel  = flag.String("log-level", "info", "log level: debug, info, warn, error")
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
		ipc.WithOnConnect(func() { log.Printf("datastore connected") }),
		ipc.WithOnDisconnect(func(err error) { log.Printf("datastore disconnected: %v", err) }),
	)
	if err != nil {
		log.Fatalf("cannot reach datastore at %s: %v", *redisAddr, err)
	}
	defer client.Close()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")
}
