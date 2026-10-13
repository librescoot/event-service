#!/usr/bin/env python3
import fcntl
import importlib.util
import pathlib
import re
import subprocess


SNAPSHOT = """
local values = {redis.call('HGET', 'vehicle', 'engine-power') or ''}
for slot = 0, 1 do
  local key = 'battery:' .. slot
  table.insert(values, redis.call('HGET', key, 'present') or '')
  table.insert(values, redis.call('HGET', key, 'state') or '')
end
return values
"""


def read_state():
    result = subprocess.run(
        ['redis-cli', '--raw', 'EVAL', SNAPSHOT, '0'],
        check=True, capture_output=True, text=True, timeout=2,
    )
    values = result.stdout.splitlines()
    if len(values) != 5:
        raise RuntimeError('invalid battery snapshot: ' + result.stdout.strip())
    return values


def target_current(values):
    dual = values[1:] == ['true', 'active', 'true', 'active']
    return 150 if dual else 80


def load_transport():
    path = pathlib.Path(__file__).with_name('tunu-flash.py')
    spec = importlib.util.spec_from_file_location('tunu_transport', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def apply_current(transport, amps):
    connection = transport.VescCan('can0', 70, 252, 1.0)
    try:
        expression = "(progn (conf-set 'l-in-current-max %d.0) (conf-get 'l-in-current-max))" % amps
        lines = transport.cmd_repl(connection, expression, collect=2.0)
        confirmed = any(
            re.fullmatch(r'\s*>\s*' + str(amps) + r'(?:\.0+)?(?:f32)?\s*', line)
            for line in lines
        )
        if not confirmed:
            raise RuntimeError('input-current readback not confirmed: ' + repr(lines))
    finally:
        connection.close()


def reconcile(transport):
    for _ in range(3):
        before = read_state()
        if before[0] != 'on':
            return
        amps = target_current(before)
        apply_current(transport, amps)
        after = read_state()
        if after[0] != 'on' or target_current(after) == amps:
            return
    raise RuntimeError('battery state kept changing during reconciliation')


def main():
    with open('/run/lock/tunu-battery-current.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        reconcile(load_transport())


if __name__ == '__main__':
    main()
