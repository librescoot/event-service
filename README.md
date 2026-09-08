# event-service

Turns Librescoot system state changes into a normalised event bus, and runs
user-defined rules against it.

The service runs on the MDB. It is being integrated into nightly images ahead
of Librescoot 1.4.0; it is not included in 1.3.1 stable images. No automation
rules are installed by default.

See the [technical reference](https://github.com/librescoot/unu-tech-reference/blob/main/services/librescoot-events.md)
for the event catalogue and Redis interface.

## Build

Requires Go 1.25.7. The Makefile selects that toolchain automatically.

```sh
make build        # Linux ARMv7: bin/event-service (also make build-arm)
make build-host   # native: bin/event-service-host
make test
make lint         # requires golangci-lint
```

Live datastore tests require a disposable Redis/Valkey at `localhost:6379`
and skip when it is unavailable. Do not point the test suite at a vehicle.

The optional SocketCAN integration test uses a virtual interface in an isolated
Linux network namespace, never the vehicle's CAN bus:

```sh
GOTOOLCHAIN=go1.25.7 go test -c -o /tmp/event-service-canbus-tests ./internal/canbus
sudo unshare -n sh -ec '
  ip link add vcan-evtest type vcan
  ip link set vcan-evtest up
  EVENT_SERVICE_TEST_VCAN=1 /tmp/event-service-canbus-tests -test.run TestVcanIntegration -test.v
'
```

Requires kernel vcan support, `ip`, `unshare`, and permission to create network
namespaces. The test skips during ordinary `make test` runs.

## Run

```sh
./bin/event-service-host --redis localhost:6379
```

| Flag | Default | Meaning |
|---|---|---|
| `--redis` | `localhost:6379` | Datastore address |
| `--rules-dir` | `/data/extensions` | Rule directory |
| `--workers` | `2` | Action workers |
| `--queue` | `256` | Action queue capacity |
| `--replay-window` | `5m` | Maximum lateness for pending-step replay |
| `--stats-interval` | `10s` | Counter refresh interval; only changes are written |
| `--log-level` | `info` | `debug` enables per-frame CAN logging; other service logs are not currently filtered |

### On the MDB

The image recipe installs `/usr/bin/event-service` and enables
[`librescoot-events.service`](https://github.com/librescoot/meta-librescoot/blob/wrynose/recipes-core/event-service/files/librescoot-events.service).
The unit waits for `/data` and starts after Valkey. A missing rules directory
is normal and loads zero rules.

Install only trusted rules and executable scripts: the packaged service runs
as root. Keep the directory and its contents writable only by trusted users.

```sh
install -d -m 0700 /data/extensions
# Install your reviewed .toml files and executable scripts, then:
systemctl restart librescoot-events
journalctl -u librescoot-events -n 50 --no-pager
redis-cli HGETALL extensions
```

Files are loaded non-recursively, in filename order, from lowercase `*.toml`
files. There is no hot reload, SIGHUP reload, or validation-only CLI; restart
for changes to take effect. Invalid files/rules are logged and skipped while
valid rules can still run, so check the logs and loaded-rule count, not just
whether the unit is active. `lsc ext` is not implemented yet.

## Observing the bus

```sh
redis-cli psubscribe 'ev:*'
redis-cli xrevrange events + - COUNT 10
```

| Interface | Contents |
|---|---|
| `ev:<topic>` | Live JSON event envelopes |
| `events` | Recent adapter events; stream fields `topic` and `e` (JSON), approximately 2000 entries |
| `extensions` | Version, rule count and runtime counters; read with `HGETALL` |
| `extensions:pending` | Internal pending-step records; do not edit while running |

Rules consume live Pub/Sub, not the stream. Triggers missed while the service
is down or disconnected are not replayed. The adapter observes notified hash
values rather than atomic producer transitions; rapid intermediate values can
be missed. It does not provide an authoritative vehicle transition log.

## Rules

Drop `*.toml` files into the extensions directory (`--rules-dir`, default
`/data/extensions`) and event-service loads them at startup and runs them
against the bus. With no files present it does not subscribe to anything
extra. Workers and the statistics infrastructure still exist even with zero
rules.

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
in-memory shadow store, not the datastore directly. Watched hashes are seeded
at startup without emitting transitions; later notifications update them.
Unwatched or missing fields return `""`, indistinguishable from an empty value.
Changes without a corresponding notification can leave the shadow stale. A
rule with no `when` fires on every event matching `on`.

Supported `do` kinds for `[[rule.step]]`:

- `redis`: push a value onto a list with `list` and `push`.
- `exec`: run a command with `command` and an optional `timeout`, default
  `10s`. The event is on stdin as JSON, plus `LS_TOPIC`, `LS_SRC`, `LS_FROM`,
  `LS_TO`, `LS_ID` and one `LS_DATA_<KEY>` per scalar `data` field, so a short
  shell script needs no JSON parser. `command` is an executable name or path,
  not shell text or an argument string. Put arguments and pipelines in an
  executable wrapper script. Standard output is discarded; failed-command
  stderr is included in the error log.
- `can`: send a classic CAN frame directly through SocketCAN, using `iface`,
  `id`, and `data`. No helper process or receive loop is started.

### CAN frames

```toml
[[rule.step]]
do = "can"
iface = "can0"
id = "0x123"
data = "01 02 03 04"
```

This illustrates syntax, not an ECU command to send. IDs are hexadecimal, with
or without `0x`. IDs above `0x7ff` use extended frames, up to `0x1fffffff`.
Payloads accept contiguous hex (`"01020304"`) or whitespace-separated byte
pairs, with at most eight bytes; empty data sends a zero-length frame.

An optional remote-request frame uses `rtr = true` and `dlc = 0` through `8`
(default `0`), with no payload. It requests a response of that length rather
than carrying data. Ordinary frames derive their length from `data` and reject
an explicit `dlc`. No ECU behavior or support for RTR is assumed.

The service opens one socket per interface lazily and reuses it, retaining at
most 64 sockets. Sends are
nonblocking; a full transmit queue or down interface fails the step rather
than holding a worker or retrying a potentially delivered command. A failed
socket is closed so a later action can reopen it. Successful send means the
kernel accepted the frame, not that the ECU received or acknowledged it.

`extensions[can-sent]` and `[can-errors]` count sends and failures. Per-frame
logging, including transport errors, is enabled only by `--log-level=debug`;
failures still count in the action pool's `failed` total and end the sequence.
There is no bus-rate limit or riding-state interlock. Do not send arbitrary
frames to the ECU; high-rate rules can interfere with its normal traffic.

### Step sequences

A rule can have several `[[rule.step]]` blocks. They run in order, and a step
starts only once the one before it has finished. A step that fails ends the
run and the steps after it do not run: a sequence is a recipe, so carrying on
would act on a state the failed step never established.

A step can carry its own `when`, checked before it is submitted to the worker
pool and evaluated against the event that triggered the rule, with `state()`
reading whatever is current at that moment. A queued action may run later. A
false step `when` ends the run cleanly; it does not skip ahead to the next step.

A step can also carry `after` (a duration), which runs it that long after the
step before it finished. A step waiting out its delay holds no worker and no
thread: it sits on a timer, so a rule can say "and thirty seconds later, turn
it off" without occupying anything for thirty seconds.

Here is the rule this feature was built for, exactly as it loads:

    [[rule]]
    name        = "hazards-on-alarm"
    on          = ["alarm.triggered"]
    concurrency = "restart"
    cancel-on   = ["alarm.disarmed"]

      [[rule.step]]
      do   = "redis"
      list = "scooter:blinker"
      push = "both"

      [[rule.step]]
      after = "30s"
      do    = "redis"
      list  = "scooter:blinker"
      push  = "off"

On the alarm, the hazards go on, and thirty seconds later the second step
requests off. Disarming cancels that delayed step; cancellation does not undo
an action that already ran. To request off on disarm, add a companion rule:

    [[rule]]
    name = "hazards-off-on-disarm"
    on   = ["alarm.disarmed"]

      [[rule.step]]
      do   = "redis"
      list = "scooter:blinker"
      push = "off"

Actions already accepted by the worker pool are not interrupted, so these
rules do not guarantee ordering against an in-flight action or another caller
of the blinker queue. They are examples, not a hardware safety interlock.

### Durability

A step with a positive `after` is also `durable` unless it says otherwise.
The waiting step is written to `extensions:pending` when scheduled and removed
when its action starts or the run is cancelled. This supports recovery across
an **event-service process restart while Valkey retains its data**. The image's
Valkey configuration disables disk persistence, so this is not recovery across
a vehicle reboot, power loss, or datastore restart.

On start, overdue recorded steps are submitted and future steps have their
remaining delay rearmed before the rule subscription opens. Replay does not
wait for those actions to complete. A rule with `repeat` comes back on the
pass it was on and finishes the passes it had left, rather than starting its
count over.

A record is thrown away instead, with a line saying why, if its rule is gone,
if its rule no longer has that step, if the step at that index is not the one
the record was written for any more, if it is more than `--replay-window`
(5 minutes by default) past due, or if it is dated further ahead than the
step's own `after` could put it, which is what a clock that ran backwards over
the restart leaves behind. Editing a rule file while the service is down
is expected, and a record identifies its step by what that step was configured
to do, so reordering or rewriting steps drops the record rather than firing
whatever ended up at the same index. A window of zero or less replays only
what is still in the future: a scooter that was off for a week must not come
back up acting on what it was doing then.

A replayed record goes through its rule's `concurrency` policy the same way a
live trigger does, so a rule that ends up with two records comes back with one
run rather than two. A step that comes due when the action pool has no room
for it keeps its record instead: it provably did not run, so the next start is
what runs it, and the same goes for a step still sitting in the pool's queue
when the service is stopped.

Write `durable = false` on the step to opt out. Nothing is recorded for a
`repeat` gap or for a trigger sitting in a `queue` backlog. Earlier repeat
passes may already have acted. Explicit `durable` without a positive `after`
fails to load; `after = "0s"` by itself is immediate and non-durable.

Recovery is not exactly-once execution. If recording fails, the error is logged
but the step continues without a durable record. Failed deletion can cause a
later replay; a crash after deletion but before successful action completion
can lose the action. Use idempotent actions and account for those failure
windows.

### Concurrency and cancellation

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
before matching, so a single event can cancel one rule and fire another.
Cancellation does not issue an off command or undo prior actions; use an
explicit cleanup rule where appropriate.

A step that has already been handed to the worker pool when the cancel arrives
is **not** interrupted, and that covers both a step a worker is running and one
still waiting its turn in the pool's queue. A `redis` push or an `exec` command
already accepted will complete. What cancelling guarantees is that nothing
after that step runs.

### Repeat

`repeat = { count = 3, every = "700ms" }` at rule level runs the whole step
sequence again once it finishes, `count` times in total, waiting `every`
between one pass finishing and the next starting. With no `repeat` key a rule
runs one pass, which is also what `count = 1` means. `every` is only checked,
and only has to be positive, once `count` is greater than 1, since a single
pass has nothing to wait between. Writing the key at all commits to the
feature: `repeat = {}` decodes to `count = 0`, rejected the same as any other
bad count rather than read as "no repeat".

The gap between passes is not durable; only a step's own `after` is. A
restart during the gap simply ends the run there, on the pass it had reached.

### Cooldown and debounce

`cooldown` (a duration, e.g. `"30s"`) suppresses repeat firing of a rule
within the given window after it last fired. This is a leading edge: the
first event of a burst fires immediately, and everything else inside the
window is ignored outright.

`debounce` (a duration) is the opposite, a trailing edge: nothing dispatches
while matching events keep arriving. Each one restarts the quiet window, and
once the window elapses without a new match the rule fires exactly once,
carrying the most recent event seen, not the one that opened the window. A
`debounce` must be positive if the key is written at all; an omitted key means
no debounce, same as `cooldown`.

The two compose rather than conflict. With both set, `cooldown` is checked
against the debounced dispatch itself, not against each event that only
restarted the window, so a burst that never goes quiet long enough to satisfy
`debounce` never reaches `cooldown` at all.

### Naming and errors

A rule's `name` must be unique across every file in the directory. It is the
handle a rule's runs are grouped under, so two rules sharing one would share a
concurrency policy, a cancel-on list and a queue, and either could cancel the
other's runs on a topic it never mentions. The second definition fails to load
with an error naming both files; the first still loads, as does everything
else. A disabled rule (`enabled = false`) claims no name, and neither does a
rule that fails to compile, so keeping an old copy around while a variant is
tried, or fixing a broken rule under the name a working one already took,
works as expected.

Not supported yet: the `lua` and `http` step kinds. A rule using any
of these fails to load rather than silently doing nothing. The error always
names the rule and the file; where the offending key belongs to a step
(`do`, `after`, or a step's `when`) it also names the step index. `repeat`,
`debounce` and `concurrency` sit on the rule itself, so their errors have no
step to name. An unrecognised `concurrency` is rejected the same way the step
kinds are, naming the rule, the file and the three values it accepts.

`durable` belongs to a step and nowhere else; on a rule it is not a
recognised key, and neither is any key not listed above. A file containing one
fails to load: the error names the file and the offending key, and the rest of
that file's rules do not load either. Other files in the extensions directory
are unaffected.

### Keys, at a glance

Rule level:

| Key | Default |
|---|---|
| `name` | required, unique across every file |
| `on` | required |
| `when` | none: fires on every event `on` matches |
| `concurrency` | `restart` |
| `cancel-on` | none |
| `cooldown` | none |
| `debounce` | none; must be positive if set |
| `repeat` | none (one pass); `count` at least 1, `every` positive and required once `count` > 1 |
| `enabled` | `true` |

Step level:

| Key | Default |
|---|---|
| `do` | required: `redis`, `exec`, or `can` |
| `when` | none: step always runs |
| `after` | none: step runs as soon as it is reached |
| `durable` | `true` if `after` is positive; explicitly setting it otherwise is a load error |
| `list`, `push` | required for `do = "redis"` |
| `command` | required for `do = "exec"` |
| `timeout` | `10s`, for `do = "exec"` |
| `iface`, `id` | required for `do = "can"`; interface name and hexadecimal ID |
| `data` | empty; up to eight hex bytes for CAN data frames |
| `rtr` | `false`; send a CAN remote-request frame if true |
| `dlc` | `0` for RTR, range 0–8; rejected on ordinary data frames |

## A note on safety

There is no allowlist, no rate limit, and no interlock on what a rule can do.
A `redis` step can `LPUSH` onto `scooter:state`, `scooter:horn`,
`scooter:blinker`, `scooter:seatbox`, or any other command queue, as freely as
vehicle-service's legitimate callers can. Two rules can watch each other's
output topics and cycle a command back and forth indefinitely, including
through the steering lock; nothing here detects or breaks that loop. The
extension subsystem is a power-user feature; event-service does not
second-guess what a rule tells the vehicle to do. The unit's CPU weight,
memory limit and task limit reduce contention, but are not a security sandbox
or a guarantee against datastore flooding.
Write rules with that in mind.

Durability extends the same stance across a process restart. A recorded step
with a positive `after` can come back on the next service start,
so a rule nobody retriggered this session, sitting on a wait from before the
restart, can still `LPUSH` onto a command queue once the service is back up,
without any event happening in between that the rider watching the vehicle
now would connect to it. That is deliberate: durability exists so a sequence
that already told the vehicle to do half of something finishes the other
half, and there is no separate check asking whether it still should. Do not
write an `after` step onto a command queue you would not want fired by
something that happened before the current rider ever saw the vehicle.

## License

[GNU AGPL-3.0](LICENSE).
