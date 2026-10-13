import importlib.util
import pathlib
import unittest
from unittest.mock import Mock, patch


spec = importlib.util.spec_from_file_location(
    'battery_current', pathlib.Path(__file__).with_name('battery-current.py')
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

DUAL = ['on', 'true', 'active', 'true', 'active']
SINGLE = ['on', 'true', 'active', 'true', 'asleep']


class BatteryCurrentTest(unittest.TestCase):
    def test_limits(self):
        self.assertEqual(module.target_current(DUAL), 150)
        for state in [SINGLE, ['on', '', '', '', ''],
                      ['on', 'false', 'active', 'true', 'active'],
                      ['on', 'true', 'asleep', 'true', 'active']]:
            self.assertEqual(module.target_current(state), 80)

    def test_off_does_not_send(self):
        with patch.object(module, 'read_state', return_value=['off'] + DUAL[1:]), \
             patch.object(module, 'apply_current') as apply:
            module.reconcile(Mock())
        apply.assert_not_called()

    def test_rechecks_after_send(self):
        with patch.object(module, 'read_state', side_effect=[DUAL, SINGLE, SINGLE, SINGLE]), \
             patch.object(module, 'apply_current') as apply:
            transport = Mock()
            module.reconcile(transport)
        self.assertEqual([call.args[1] for call in apply.call_args_list], [150, 80])

    def test_readback_and_regen_preservation(self):
        transport = Mock()
        transport.cmd_repl.return_value = ['> 150.000000f32']
        module.apply_current(transport, 150)
        expression = transport.cmd_repl.call_args.args[1]
        self.assertIn("conf-set 'l-in-current-max 150.0", expression)
        self.assertNotIn("l-in-current-min", expression)
        transport.VescCan.return_value.close.assert_called_once()

    def test_failed_readback_closes_socket(self):
        for reply in [[], ['> 60.0'], ['error']]:
            transport = Mock()
            transport.cmd_repl.return_value = reply
            with self.assertRaises(RuntimeError):
                module.apply_current(transport, 160)
            transport.VescCan.return_value.close.assert_called_once()

    def test_invalid_snapshot(self):
        with patch.object(module.subprocess, 'run', return_value=Mock(stdout='ERR unavailable\n')):
            with self.assertRaises(RuntimeError):
                module.read_state()


if __name__ == '__main__':
    unittest.main()
