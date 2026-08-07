# event-service

Turns Librescoot system state changes into a normalised event bus, and runs
user-defined rules against it.

See `EVENT-SERVICE-DESIGN.md` in the librescoot tree for the design.

## Build

    make build        # ARM, for the MDB
    make build-host   # native, for development
    make test

## Run

    event-service --redis localhost:6379 --log-level info

## Observing the bus

    redis-cli psubscribe 'ev:*'
    redis-cli xrevrange events + - COUNT 10
