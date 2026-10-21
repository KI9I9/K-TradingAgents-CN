"""Synthetic prepare-only tests. Never contacts the user's source deployment."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import migration_prepare as prepare
import migration_window as window


class PrepareTests(unittest.TestCase):
    def test_no_mutating_cli_action_accepted(self):
        with patch.object(window, 'prepare') as action, contextlib.redirect_stderr(io.StringIO()):
            for flag in ('snapshot', '--approved-outage', '--approved-rollback'):
                with self.assertRaises(SystemExit):
                    prepare.main(['--window', '/unused', flag])
            action.assert_not_called()

    def test_error_body_is_not_printed(self):
        output = io.StringIO()
        def failure(*args, report_stage):
            report_stage('source_baseline')
            raise RuntimeError('FAKE_SECRET_MUST_NOT_APPEAR')
        with patch.object(window, 'prepare', side_effect=failure), contextlib.redirect_stdout(output):
            self.assertEqual(prepare.main(['--window', '/unused']), 1)
        self.assertNotIn('FAKE_SECRET', output.getvalue())
        self.assertEqual(json.loads(output.getvalue())['failed_stage'], 'source_baseline')

    def test_capacity_failure_reports_only_budget_and_category(self):
        budget = {'windows_free_bytes': {'C': 42, 'D': 73}}
        output = io.StringIO()
        with patch.object(window, 'prepare', side_effect=window.CapacityError(budget)), contextlib.redirect_stdout(output):
            self.assertEqual(prepare.main(['--window', '/unused']), 1)
        self.assertEqual(json.loads(output.getvalue())['capacity'], budget)

    def test_prepare_reads_ttl_and_never_calls_backup_or_mutation(self):
        with tempfile.TemporaryDirectory(prefix='prepare-only-fixture-') as temporary:
            parent = Path(temporary)
            root = parent / 'source'
            root.mkdir()
            (root / '.env').write_text('FAKE_ONLY=synthetic\n')
            (root / '.env').chmod(0o600)
            destination = parent / 'window'
            identities = {name: {'id': 'fixture-' + name, 'image_id': 'fixture-image', 'running': True}
                          for name in ('mongodb', 'redis', 'backend', 'frontend')}
            stages = []
            with patch.object(window.backup, 'inspect', return_value={'git_commit': window.COMMIT, 'git_status': ''}), \
                 patch.object(window, 'identities', return_value=identities), \
                 patch.object(window, 'capacity_check', return_value={'fixture': True}), \
                 patch.object(window, 'ttl_state', return_value=True) as ttl, \
                 patch.object(window.backup, 'backup') as backup, \
                 patch.object(window.backup, 'run') as process:
                result = window.prepare(root, destination, report_stage=stages.append)
            self.assertEqual(result['status'], 'prepared')
            ttl.assert_called_once_with()
            backup.assert_not_called()
            process.assert_not_called()
            self.assertEqual(window.load_state(destination)['stage'], 'prepared')
            self.assertEqual((destination / 'window-state.json').stat().st_mode & 0o777, 0o600)
            self.assertEqual(stages[-1], 'write_private_preparation_record')


if __name__ == '__main__':
    unittest.main(verbosity=2)
