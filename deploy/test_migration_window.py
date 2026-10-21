"""Mocked outage/recovery tests: never runs Docker or changes a real database."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import migration_window as window


class WindowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='migration-window-fixture-')
        self.root = Path(self.temp.name)
        self.source = {name: {'id': 'fixture-' + name, 'image_id': 'fixture-image',
                             'running': True, 'started_at': 'fixture-time'}
                       for name in ('mongodb', 'redis', 'backend', 'frontend')}
        self.state = {'schema': 1, 'stage': 'prepared', 'root': str(self.root), 'source': self.source,
                      'original_ttl': True, 'ttl_change_attempted': False, 'snapshot_complete': False}
        window.save_state(self.root, self.state)

    def tearDown(self):
        self.temp.cleanup()

    def test_snapshot_needs_all_approvals(self):
        for flags in ({}, {'approved': True}, {'approved': True, 'drained': True}):
            with patch.object(window, 'ttl_state') as ttl:
                with self.assertRaises(ValueError):
                    window.snapshot(self.root, **flags)
                ttl.assert_not_called()

    def snapshot(self, failure=False):
        events = []
        def ttl(desired=None):
            events.append(('ttl', desired))
            if desired is not None:
                self.assertTrue(window.load_state(self.root)['ttl_change_attempted'])
            return True if desired is None else desired
        def do_backup(*args, **kwargs):
            events.append(('backup', kwargs))
            if failure:
                raise TimeoutError('synthetic')
        inventory = {'git_commit': window.COMMIT, 'git_status': ''}
        with patch.object(window, 'identities', return_value=self.source), \
             patch.object(window.backup, 'inspect', return_value=inventory), \
             patch.object(window, 'capacity_check', return_value={'fixture': True}), \
             patch.object(window, 'runtime_configuration', return_value={'schema': 1}), \
             patch.object(window, 'activity', return_value={'drained': True}), \
             patch.object(window, 'ttl_state', side_effect=ttl), \
             patch.object(window.backup, 'backup', side_effect=do_backup), \
             patch.object(window.backup, 'run') as process:
            if failure:
                with self.assertRaises(TimeoutError):
                    window.snapshot(self.root, approved=True, drained=True, external_writers_isolated=True)
            else:
                window.snapshot(self.root, approved=True, drained=True, external_writers_isolated=True)
            process.assert_not_called()
        return events

    def test_success_keeps_source_stopped_and_ttl_paused(self):
        events = self.snapshot()
        self.assertEqual(events[0:2], [('ttl', None), ('ttl', False)])
        self.assertEqual(events[2][1]['success_state'], 'keep-stopped')
        self.assertEqual(events[2][1]['failure_state'], 'keep-stopped')
        self.assertEqual(window.load_state(self.root)['stage'], 'snapshot_complete_source_stopped')

    def test_failure_is_durable_and_does_not_restart(self):
        self.snapshot(failure=True)
        state = window.load_state(self.root)
        self.assertEqual(state['stage'], 'failed_manual_recovery_required')
        self.assertFalse(state['snapshot_complete'])
        self.assertTrue(state['ttl_change_attempted'])

    def test_rollback_requires_target_isolated(self):
        with patch.object(window, 'ttl_state') as ttl:
            with self.assertRaises(ValueError):
                window.rollback(self.root, approved=True)
            ttl.assert_not_called()

    def test_rollback_restores_original_ttl_before_starting_exact_containers(self):
        for original in (True, False):
            with self.subTest(original=original):
                self.state.update(stage='snapshot_complete_source_stopped', original_ttl=original)
                window.save_state(self.root, self.state)
                events = []
                with patch.object(window, 'identities', return_value=self.source), \
                     patch.object(window, 'ttl_state', side_effect=lambda value: events.append(('ttl', value))), \
                     patch.object(window.backup, 'run', side_effect=lambda argv, **kw: events.append(('start', argv[-1]))):
                    result = window.rollback(self.root, approved=True, target_inactive_confirmed=True)
                self.assertEqual(events, [('ttl', original), ('start', 'fixture-redis'),
                                          ('start', 'fixture-backend'), ('start', 'fixture-frontend')])
                self.assertTrue(result['new_final_snapshot_required'])

    def test_ttl_restore_error_blocks_application_start(self):
        self.state['stage'] = 'failed_manual_recovery_required'
        window.save_state(self.root, self.state)
        with patch.object(window, 'identities', return_value=self.source), \
             patch.object(window, 'ttl_state', side_effect=TimeoutError), \
             patch.object(window.backup, 'run') as process:
            with self.assertRaises(TimeoutError):
                window.rollback(self.root, approved=True, target_inactive_confirmed=True)
            process.assert_not_called()

    def test_recreated_container_rejected(self):
        current = json.loads(json.dumps(self.source))
        current['backend']['id'] = 'replacement'
        with self.assertRaises(ValueError):
            window.assert_same_containers(self.source, current)

    def test_state_is_private(self):
        self.assertEqual((self.root / 'window-state.json').stat().st_mode & 0o777, 0o600)
        self.assertEqual(len(list(self.root.glob('*.tmp'))), 0)

    def test_active_task_blocks_before_any_ttl_mutation(self):
        with patch.object(window, 'identities', return_value=self.source), \
             patch.object(window.backup, 'inspect', return_value={'git_commit': window.COMMIT, 'git_status': ''}), \
             patch.object(window, 'capacity_check', return_value={}), \
             patch.object(window, 'activity', return_value={'drained': False}), \
             patch.object(window, 'ttl_state', return_value=True) as ttl, \
             patch.object(window.backup, 'backup') as backup:
            with self.assertRaises(ValueError):
                window.snapshot(self.root, approved=True, drained=True, external_writers_isolated=True)
            ttl.assert_called_once_with()
            backup.assert_not_called()
            self.assertEqual(window.load_state(self.root)['stage'], 'prepared')


if __name__ == '__main__':
    unittest.main(verbosity=2)
