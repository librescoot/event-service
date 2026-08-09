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

## Rules

Drop `*.toml` files into the extensions directory (`--rules-dir`, default
`/data/extensions`) and event-service loads them at startup and runs them
against the bus. With no files present it does not subscribe to anything
extra, so a scooter with no extensions installed pays nothing for this
feature.

Example, `/data/extensions/demo.toml`:

    [[rule]]
    name = "demo"
    on   = ["alarm.triggered"]
    when = "to == 'level-2-triggered'"

      [[rule.step]]
      do   = "redis"
      list = "test:fired"
      push = "yes"

`on` matches against event topics: an exact topic, `*` for everything, or
`prefix.*` for anything starting with `prefix.`. `when` is
an expression evaluated against the event: `topic`, `src`, `from`, `to`,
`data`, and `state("hash", "field")` for reading current values out of the
datastore that the event itself does not carry. A rule with no `when` fires
on every event matching `on`.

`cooldown` (a duration, e.g. `"30s"`) suppresses repeat firing of a rule
within the given window after it last fired.

Supported `do` kinds for `[[rule.step]]`:

- `redis`: push a value onto a list with `list` and `push`.
- `exec`: run a command with `command` and an optional `timeout`.

Not supported yet: multiple steps per rule, `after`, `concurrency`,
`cancel-on`, `repeat`, `debounce`, and the `can`, `lua`, and `http` step
kinds. A rule using any of these fails to load with an error naming the rule
and file, rather than silently doing nothing.

`durable` is not a recognised key at all yet, on either a rule or a step. TOML
decoding does not reject unknown keys, so writing `durable = true` loads
without error and is simply ignored. Do not rely on it.
