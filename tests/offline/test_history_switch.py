"""Exercise the real deployment state machine; Docker, APIs and DB use doubles."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[2]


class HistorySwitchTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('history_switch', ROOT / 'deploy/switch_history_contract.py')
        self.m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.m)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.m.ATTEMPT = Path(temp.name)
        self.m.STATE = {'stage': 'init', 'events': []}
        self.m.PAYLOAD = {'check_only': False, 'config_hashes': {'before': self.m.digest(self.m.canonical({}))}}
        self.m.MANIFEST = {'source_head': 'test'}
        self.m.save('frontend-before.json', {'index': {'sha256': 'same'}})
        self.calls = []
        for name in ['preflight', 'seal_check', 'healthy', 'check_running', 'module_hashes', 'smoke', 'audit']:
            setattr(self.m, name, Mock(return_value={}))
        self.m.config = Mock(return_value={})
        self.m.db_probe = Mock(return_value={'hashes': {'history': 'same'}, 'preferences': []})
        self.m.api_probe = Mock(return_value={'index': {'sha256': 'same'}})
        self.m.stop_backend = Mock(side_effect=lambda: self.calls.append('stop'))
        self.m.up = Mock(side_effect=lambda new: self.calls.append(('up', new)))

    def test_preflight_failure_keeps_service(self):
        self.m.preflight.side_effect = RuntimeError('bad_preflight')
        self.assertEqual(self.m.main(), 1)
        self.assertEqual(self.calls, [])

    def test_check_only_keeps_service(self):
        self.m.PAYLOAD['check_only'] = True
        self.assertEqual(self.m.main(), 0)
        self.assertEqual(self.calls, [])

    def test_success_stops_then_recreates_and_checks_frontend(self):
        self.assertEqual(self.m.main(), 0)
        self.assertEqual(self.calls, ['stop', ('up', True)])
        self.m.api_probe.assert_any_call('frontend-after.json', frontend=True)
        self.m.smoke.assert_called_once()
        self.assertEqual(self.m.STATE['stage'], 'complete')

    def test_stop_timeout_also_rolls_back(self):
        self.m.stop_backend.side_effect = [RuntimeError('timeout'), None]
        self.assertEqual(self.m.main(), 1)
        self.m.up.assert_called_once_with(False)
        self.assertEqual(self.m.STATE['stage'], 'rolled_back_v12')

    def test_new_backend_failure_restores_only_v12(self):
        self.m.up.side_effect = [RuntimeError('failed'), None]
        self.assertEqual(self.m.main(), 1)
        self.assertEqual([call.args for call in self.m.up.call_args_list], [(True,), (False,)])
        self.assertEqual(self.m.STATE['stage'], 'rolled_back_v12')

    def test_frontend_failure_requires_rollback(self):
        self.m.api_probe.side_effect = [dict(), RuntimeError('bad_frontend'), dict(), dict()]
        self.assertEqual(self.m.main(), 1)
        self.assertEqual(self.calls, ['stop', ('up', True), 'stop', ('up', False)])
        self.assertEqual(self.m.STATE['stage'], 'rolled_back_v12')

    def test_changed_business_content_rolls_back(self):
        self.m.db_probe.side_effect = [dict(hashes={'history': 'before'}, preferences=[]),
                                      dict(hashes={'history': 'after'}, preferences=[]), {}]
        self.assertEqual(self.m.main(), 1)
        self.assertEqual(self.m.STATE['stage'], 'rolled_back_v12')

    def test_unsafe_rollback_keeps_backend_stopped(self):
        self.m.up.side_effect = RuntimeError('new_backend_failed')
        self.m.db_probe.side_effect = [dict(hashes={}, preferences=[]), RuntimeError('active_job')]
        self.assertEqual(self.m.main(), 1)
        self.m.up.assert_called_once_with(True)
        self.assertEqual(self.m.stop_backend.call_count, 3)
        self.assertEqual(self.m.STATE['stage'], 'rollback_failed_stop_requested')

    def test_active_or_missing_historical_job_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'job_active_or_missing'):
            self.m.check_jobs({'success': True, 'data': []})


if __name__ == '__main__':
    unittest.main()
