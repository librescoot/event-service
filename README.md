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

A step can also carry `after` (a duration), which runs it that long after the
step before it finished. A step waiting out its delay holds no worker and no
thread: it sits on a timer, so a rule can say "and thirty seconds later, turn
it off" without occupying anything for thirty seconds.

`concurrency` decides what a fresh trigger does to a run of the same rule that
has not finished yet:

- `restart`, the default and what an omitted key means: drop the pending tail
  of the live run and start the sequence over.
- `drop`: ignore the trigger while a run is live. The rule fires again
  normally once that run has ended.
- `queue`: hold the trigger and run the sequence again once the live run has
  finished, so runs go back to back rather than side by side. The backlog is
  capped at 8 per rule; triggers past that are refused, counted and logged,
  because an unbounded queue behind a flapping trigger is a memory leak.

`cancel-on` takes topics in the same form as `on`, and an event matching one
of them drops every live run of that rule: pending timers are cancelled, the
queued backlog is thrown away, and no further step is submitted. It is applied
before matching, so a single event can cancel one rule and fire another, which
is how "blink the hazards, stop 30s later" is made to stop early when the
rider disarms at second five.

A step that is already executing when the cancel arrives is **not**
interrupted. The worker owns it and runs it to its end; a `redis` push or an
`exec` command in flight will complete. What cancelling guarantees is that
nothing after that step runs.

Not supported yet: `repeat`, `debounce`, and the `can`, `lua`, and `http` step
kinds. A rule using any of these fails to load rather than silently doing
nothing. The error always names the rule and the file; where the offending key
belongs to a step (`after`, and any problem with a step's `do` or `when`) it
also names the step index. `repeat` and `debounce` sit on the rule itself, so
their errors have no step to name. This includes writing the empty form of a
feature (`repeat = {}`, `debounce = "0s"`): the key being present is what is
rejected, not whether its value would have done anything. An unrecognised
`concurrency` is rejected the same way, naming the rule, the file and the
three values it accepts.

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
