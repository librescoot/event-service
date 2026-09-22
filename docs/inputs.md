# Configurable inputs

Rules can subscribe to selected existing datastore notifications without adding
Go handlers. Inputs are opt-in `[[rule.input]]` blocks. They are loaded with the
rule catalogue; adding, changing, enabling or disabling them takes effect only
**after restarting event-service**, just like rule actions. `lsc ext add/show`
preserves the input blocks. A preview never subscribes, reads Redis, or generates
an input event: it uses the existing shadow snapshot and supplied event.

Existing canonical events and their friendly names are unchanged. Input topics
must begin with `input.` to keep configured mappings separate from those names.
Topics are not automatically added to a rule's `on`: specify the ones that should
trigger it. Other rules can also listen to these generated topics.

## Notified hash fields

```toml
[[rule]]
name = "engine-power-reset"
on = ["input.engine-power"]
when = "to == 'off'"

[[rule.input]]
hash = "vehicle"
fields = ["engine-power"]
topic = "input.engine-power"

[[rule.step]]
do = "exec"
command = "/data/extensions/bin/reset-local-state"
```

A generated event has `src = "adapter"`, the observed `from` and `to` strings,
and `data.hash` / `data.field`. A single input can select several fields; use
`data.field` in the condition to distinguish them. Identical observed values do
not fire. Startup seeds state without firing. A first *live* value after startup
can fire with `from = ""`; do not confuse this with a measured hardware boot.

The example observes the **commanded** engine-power field, not ECU supply
voltage, a brownout, or a controller reset while power stays commanded on.

## State-only dependencies

Omit `topic` from a hash input to populate `state()` without publishing events:

```toml
[[rule]]
name = "conditional-notification"
on = ["input.notice"]
when = 'state("accessory", "mode") == "enabled"'

[[rule.input]]
hash = "accessory"
fields = ["mode"]

[[rule.input]]
channel = "accessory:notice"
topic = "input.notice"

[[rule.step]]
do = "exec"
command = "/data/extensions/bin/handle-notice"
```

`state()` itself still performs no I/O and does not infer subscriptions from
expressions. Declare every additional dependency explicitly. Missing and empty
values both read as `""`. Disabled rules do not register input event producers;
their hash dependencies can remain as state-only watches for retained delayed
cleanup. Cleanup dependencies take priority over new inputs when enforcing
aggregate budgets.

## Raw pub/sub

```toml
[[rule]]
name = "accessory-fault"
on = ["input.accessory-report"]
when = 'data.payload.status == "fault"'

[[rule.input]]
channel = "accessory:report"
format = "json"
max-bytes = 4096
topic = "input.accessory-report"

[[rule.step]]
do = "exec"
command = "/data/extensions/bin/report-accessory-fault"
```

Events carry `data.channel` and `data.payload`. The default `format = "string"`
keeps the payload as a string. `format = "json"` accepts JSON objects, arrays,
scalars and null; conditions must match the chosen shape. Numbers have the same
JSON numeric semantics as ordinary event envelopes. Invalid JSON and oversized
payloads are skipped, with a diagnostic that does not log the payload. Incoming
JSON is data, never trusted as an event envelope.

These are **pub/sub subscriptions**, not list consumers. Watching a channel with
the same name as a command/RPC list does not observe list operations and never
removes requests from that list. Observing command handling requires a separate
producer notification. No Redis `MONITOR`, broad subscription or keyspace
notification configuration is enabled.

## Limits and boundaries

- Exact source names only: no wildcard discovery, `ev:` feedback subscriptions,
  or Redis `__key*` notification subscriptions. Names are limited to 128 bytes
  without whitespace/control characters or glob metacharacters.
- At most 16 input declarations per rule, 64 distinct declarations overall,
  and 64 selected fields per hash. Reused declarations share subscriptions.
- `max-bytes` defaults to 4096 (also when zero), with a maximum of 65536. It
  limits raw payloads and observed hash values used for generated events.
- Configured selected-field budgets total at most 1 MiB. New hashes use bounded
  selected-field reads, not `HGETALL`. Oversized selected values become
  unavailable in the shadow rather than leaving a stale valid value.
- Hashes already watched by built-in sources retain their existing watcher and
  whole-hash shadow behavior. Their configured event values are size-checked,
  but this does not retrofit a memory limit onto the built-in watcher.
- New selected-field watches refresh all their selected fields on each hash
  notification, including `cleared`/`replaced` and companion-field updates.
  Built-in shared hashes retain the legacy notification behavior.
- A conflicting or over-budget rule is rejected with a startup diagnostic;
  unrelated valid rules continue. Hash/raw-channel interpretation conflicts
  with built-in sources are also rejected. No truncation of subscriptions.
- Silent writes cannot produce events. Field-name notifications require reading
  current values and can coalesce rapid transitions. These events describe
  observations, not a complete transaction log.
- Startup and reconnect are not reliable event replay boundaries. Existing
  startup delivery windows and lack of reconnect shadow reconciliation remain;
  missed input events are not replayed from the event stream.
- Payload checks do not bound the size of messages already received by the Redis
  client. Use trusted producers. Select high-rate sources deliberately: rule
  cooldown reduces action dispatch, not input traffic or decoding cost.

Catch-all rules such as `on = ["*"]` also receive newly configured input events.
Do not use broad action rules unless that behavior is intended.
