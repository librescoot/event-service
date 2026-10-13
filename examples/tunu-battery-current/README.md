# TUNU dual-battery input-current limit

Opt-in example for a TUNU VESC at CAN node 70 on `can0`. Requires Python 3,
`redis-cli`, and the `tunu-flash.py` transport from the sibling `tunu-configs`
checkout. Nothing is installed automatically.

- Both slots `present=true` and `state=active`: **150 A** total input limit.
  This example caps the aggregate limit at 150 A; review the controller's limits
  before changing it.
- Otherwise, including missing/unknown battery state: **80 A** total.
- Engine power not `on`: no CAN command.

The script reads an atomic Redis snapshot, sets only `l-in-current-max` via
LispBM REPL, and checks its readback. It changes RAM configuration, not flash.
It does not touch phase current, regen, engine power, or vehicle lock state.
A file lock serializes concurrent invocations, and battery state is rechecked
after the command. Other VESC configuration/upload tools must not run concurrently:
the VESC fragmented-command receive buffer is shared.

Rules reconcile on battery state/presence changes and make three attempts after
commanded engine power-on, allowing boot time. Failed attempts are logged by
event-service. There is no continuous polling or ECU firmware change.

## Install (after reviewing the limits)

From this repository, stage the files on the intended scooter:

```sh
scp examples/tunu-battery-current/battery-current.py \
    examples/tunu-battery-current/battery-current.toml \
    ../tunu-configs/tunu-flash.py root@<scooter-address>:/data/
```

On the scooter:

```sh
install -d -m 0700 /data/extensions
install -m 0700 /data/battery-current.py /data/extensions/battery-current.py
install -m 0600 /data/tunu-flash.py /data/extensions/tunu-flash.py
install -m 0600 /data/battery-current.toml /data/extensions/battery-current.toml
systemctl restart librescoot-events
journalctl -u librescoot-events -n 50 --no-pager
# Initial observations do not emit events. Reconcile explicitly if already powered:
/data/extensions/battery-current.py
```

Check that two rules loaded and that there are no execution/readback failures.
Restarting event-service requires another explicit reconciliation when the ECU
is already on. An uncommanded ECU reboot is not detected by engine-power events.
Verify CAN REPL/readback compatibility on the target; tests mock the transport.
A failed step ends the rule's repeat sequence, so the three passes do not retry
failed commands.

## Limits and failure modes

150 A is an aggregate limit, **not an enforceable 80 A ceiling on each pack**.
Parallel packs may share current unevenly. Confirm battery/BMS, controller,
cabling, connectors, and fusing can sustain the requested currents.

This notification-driven extension is not a hardware safety interlock. Missed
notifications, stale producer state, datastore/service outages, queue overflow,
or CAN/REPL failures can leave the last applied limit in place (including 150 A).
There is a race between reading battery state and setting the limit; rechecking
reduces but does not eliminate it. Unknown state falls back to 80 A only when the
script successfully runs; Redis errors abort rather than guessing. A true
fail-safe dual-pack grant requires an ECU-side expiring grant/watchdog and a
conservative persisted boot limit. The checked-in motor configuration boots at
60 A; this example does not change that.

Removing the rules does not undo a RAM limit. With the vehicle safely parked,
restore the desired input limit explicitly or power-cycle the ECU through the
normal vehicle shutdown/startup path. Do not use the flashing tool's default
engine-control mode while riding.

## Test

```sh
python3 -m unittest discover -s examples/tunu-battery-current -p 'test_*.py'
GOTOOLCHAIN=go1.25.7 go test ./internal/rules
```
