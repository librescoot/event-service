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
`data`, and `state("hash", "field")` for reading the last observed value of a
hash field that the event itself does not carry. `state` reads event-service's
own in-memory shadow store, not the datastore directly: only the handful of
hashes an adapter watches are in it, and only fields seen since the process
started. A field that exists in the datastore but has not changed since
startup, or that belongs to a hash nothing watches, reads back as `""`,
indistinguishable from a genuinely empty value. A rule with no `when` fires on
every event matching `on`.

`cooldown` (a duration, e.g. `"30s"`) suppresses repeat firing of a rule
within the given window after it last fired.

Supported `do` kinds for `[[rule.step]]`:

- `redis`: push a value onto a list with `list` and `push`.
- `exec`: run a command with `command` and an optional `timeout`.

A rule can have several `[[rule.step]]` blocks. They run in order, and a step
starts only once the one before it has finished. A step that fails ends the
run and the steps after it do not run: a sequence is a recipe, so carrying on
would act on a state the failed step never established.

A step can carry its own `when`, checked immediately before that step runs
and evaluated against the event that triggered the rule, with `state()`
reading whatever is current at that moment. A false step `when` ends the run
cleanly; it does not skip ahead to the next step.

Not supported yet: `after`, `concurrency`, `cancel-on`, `repeat`, `debounce`,
and the `can`, `lua`, and `http` step kinds. A rule using any of these fails
to load with an error naming the rule, the file, and the step it is on,
rather than silently doing nothing. This includes writing the empty
form of a feature (`cancel-on = []`, `repeat = {}`, `debounce = "0s"`): the
key being present is what is rejected, not whether its value would have done
anything.

`durable` is not a recognised key at all, on either a rule or a step, and
neither is any key not listed above. A file containing one fails to load: the
error names the file and the offending key, and the rest of that file's rules
do not load either. Other files in the extensions directory are unaffected.

## A note on safety

There is no allowlist, no rate limit, and no interlock on what a rule can do.
A `redis` step can `LPUSH` onto `scooter:state`, `scooter:horn`,
`scooter:blinker`, `scooter:seatbox`, or any other command queue, as freely as
vehicle-service's legitimate callers can. Two rules can watch each other's
output topics and cycle a command back and forth indefinitely, including
through the steering lock; nothing here detects or breaks that loop. This is
the same position `EVENT-SERVICE-DESIGN.md` takes on the `can` step kind: the
extension subsystem is a power-user feature, and it is deliberately not
event-service's job to second-guess what a rule tells the vehicle to do.
Write rules with that in mind.
