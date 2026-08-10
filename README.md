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

A step with `after` is also `durable` unless it says otherwise. The waiting
step is written to the `extensions:pending` hash when it is scheduled and
removed when it fires or is cancelled, so a service restart in the middle of
the wait does not strand the vehicle half-changed: "hazards on, hazards off
thirty seconds later" still turns them off if the service goes down at second
five. On start, a recorded step whose delay has run out is run straight away
and one still in the future waits out what is left of it, both before the
first event is handled. A rule with `repeat` comes back on the pass it was on
and finishes the passes it had left, rather than starting its count over.

A record is thrown away instead, with a line saying why, if its rule is gone,
if its rule no longer has that step, if the step at that index is not the one
the record was written for any more, or if it is more than `--replay-window`
(5 minutes by default) past due. Editing a rule file while the service is down
is expected, and a record identifies its step by what that step was configured
to do, so reordering or rewriting steps drops the record rather than firing
whatever ended up at the same index. A window of zero or less replays only
what is still in the future: a scooter that was off for a week must not come
back up acting on what it was doing then.
Write `durable = false` on the step to opt out, and note that nothing is
recorded for a `repeat` gap or for a trigger sitting in a `queue` backlog:
neither has acted on the vehicle yet. `durable` on a step without `after`
fails to load rather than doing nothing quietly.

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

A step that has already been handed to the worker pool when the cancel arrives
is **not** interrupted, and that covers both a step a worker is running and one
still waiting its turn in the pool's queue. A `redis` push or an `exec` command
already accepted will complete. What cancelling guarantees is that nothing
after that step runs.

A rule's `name` must be unique across every file in the directory. It is the
handle a rule's runs are grouped under, so two rules sharing one would share a
concurrency policy, a cancel-on list and a queue, and either could cancel the
other's runs on a topic it never mentions. The second definition fails to load
with an error naming both files; the first still loads, as does everything
else. A disabled rule holds no name, so keeping the old copy around with
`enabled = false` while a variant is tried works as expected.

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

`durable` belongs to a step and nowhere else; on a rule it is not a
recognised key, and neither is any key not listed above. A file containing one
fails to load: the error names the file and the offending key, and the rest of
that file's rules do not load either. Other files in the extensions directory
are unaffected.

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
