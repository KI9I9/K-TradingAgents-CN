"""Mocked backup controls. No Docker, source service, or real backup is accessed."""
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import types
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('migration_backup', Path(__file__).with_name('migration_backup.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class BackupSafetyTests(unittest.TestCase):
    def run_fixture(self, success, failure, fail_dump=False):
        calls = []
        def fake_run(args, **kwargs):
            calls.append(args)
            if fail_dump and 'mongodump' in args:
                raise subprocess.CalledProcessError(2, args)
            return types.SimpleNamespace(stdout=b'', stderr=b'', returncode=0)
        inventory = {'services': {name: {'id': name, 'running': True, 'image': 'fixture-image', 'image_id': 'fixture-image-id'}
                                 for name in ('frontend', 'backend', 'redis', 'mongodb')}}
        with tempfile.TemporaryDirectory(prefix='migration-backup-fixture-') as directory:
            root = Path(directory)
            (root / '.env').write_text('FIXTURE_ONLY=no-real-secret\n')
            destination = root / 'backup'
            with patch.object(module, 'run', side_effect=fake_run), \
                 patch.object(module, 'prepare_mongo_tools_config', return_value='/dev/shm/k-tradingagents-mongo-Ab1234/tools.json'), \
                 patch.object(module, 'save_source', side_effect=lambda root, dest: dest.write_bytes(b'fixture')), \
                 patch.object(module, 'save_backend_cache', side_effect=lambda dest: dest.write_bytes(b'fixture')), \
                 patch.object(module, 'mongo_baseline', return_value=[{'name': 'fixture', 'count': 1}]), \
                 patch.object(module, 'confirm_stopped'), \
                 patch.object(module, 'output', side_effect=lambda argv: json.dumps([{'Name': argv[-1]}])), \
                 patch.object(module, 'seal', side_effect=lambda dest: (dest / 'COMPLETE').write_text('fixture-only')), \
                 contextlib.redirect_stdout(io.StringIO()):
                if fail_dump:
                    with self.assertRaises(subprocess.CalledProcessError):
                        module.backup(root, destination, inventory, success_state=success, failure_state=failure,
                                      write_freeze_confirmed=True, runtime_config={'schema': 1})
                else:
                    module.backup(root, destination, inventory, success_state=success, failure_state=failure,
                                  write_freeze_confirmed=True, runtime_config={'schema': 1})
                    self.assertTrue((destination / 'COMPLETE').is_file())
                self.assertEqual((destination / '.env').stat().st_mode & 0o777, 0o600)
                self.assertTrue((destination / 'temporary-credential-files.json').is_file())
        return calls

    def test_final_snapshot_success_keeps_writers_stopped(self):
        calls = self.run_fixture('keep-stopped', 'keep-stopped')
        self.assertEqual([c[-1] for c in calls if c[:2] == ['docker', 'stop']], ['frontend', 'backend', 'redis'])
        self.assertFalse(any('start' in c for c in calls))

    def test_rehearsal_restarts_only_when_explicit(self):
        calls = self.run_fixture('restart-source', 'keep-stopped')
        self.assertEqual([c[-1] for c in calls if c[:2] == ['docker', 'start']], ['redis', 'backend', 'frontend'])

    def test_failure_keep_stopped_and_restart_are_separate_choices(self):
        calls = self.run_fixture('restart-source', 'keep-stopped', fail_dump=True)
        self.assertFalse(any('start' in c for c in calls))
        calls = self.run_fixture('keep-stopped', 'restart-source', fail_dump=True)
        self.assertEqual([c[-1] for c in calls if c[:2] == ['docker', 'start']], ['redis', 'backend', 'frontend'])

    def test_mongo_arguments_have_config_and_application_scope(self):
        calls = self.run_fixture('keep-stopped', 'keep-stopped')
        commands = [c for c in calls if 'mongodump' in c or 'mongorestore' in c]
        self.assertEqual(len(commands), 2)
        for argv in commands:
            self.assertFalse(any('--password' in word for word in argv))
            self.assertNotIn('sh', argv)
            self.assertTrue(any(word.startswith('--config=/dev/shm/') for word in argv))
            self.assertTrue(any('k_tradingagents' in word for word in argv))

    def test_mongo_config_stdout_is_strictly_validated(self):
        good = '/dev/shm/k-tradingagents-mongo-Ab1234/tools.json'
        output = 'Harmless startup notice\nTA_MIGRATION_CONFIG=' + json.dumps({'path': good}) + '\n'
        with patch.object(module, 'run', return_value=types.SimpleNamespace(stdout=output)) as run:
            self.assertEqual(module.prepare_mongo_tools_config('fixture-mongo'), good)
            argv = run.call_args.args[0]
            self.assertNotIn('MONGO_INITDB_ROOT_PASSWORD', ' '.join(argv))
            self.assertIn('process.env.MONGO_INITDB_ROOT_PASSWORD', run.call_args.kwargs['input'])
        for bad in ('/tmp/wrong', '/dev/shm/k-tradingagents-mongo-Ab1234/../tools.json', None):
            with patch.object(module, 'run', return_value=types.SimpleNamespace(stdout='TA_MIGRATION_CONFIG=' + json.dumps({'path': bad}))):
                with self.assertRaises(RuntimeError):
                    module.prepare_mongo_tools_config('fixture-mongo')
        with patch.object(module, 'run', return_value=types.SimpleNamespace(stdout=output + output)):
            with self.assertRaises(RuntimeError):
                module.prepare_mongo_tools_config('fixture-mongo')

    def test_streaming_checksum_works_without_file_digest(self):
        with tempfile.TemporaryDirectory(prefix='migration-hash-fixture-') as directory:
            path = Path(directory) / 'fixture'
            path.write_bytes(b'synthetic-content')
            self.assertEqual(module.file_sha256(path), hashlib.sha256(b'synthetic-content').hexdigest())

    def test_freeze_gate_rejects_before_any_command(self):
        with patch.object(module, 'run') as run:
            with self.assertRaises(ValueError):
                module.backup(Path('/unused'), Path('/unused'), {}, success_state='keep-stopped', failure_state='keep-stopped')
            run.assert_not_called()

    def test_volume_mount_is_readonly_without_copyup(self):
        calls = self.run_fixture('keep-stopped', 'keep-stopped')
        mounts = [c for c in calls if '--mount' in c]
        self.assertEqual(len(mounts), 8)
        for argv in mounts:
            self.assertIn('readonly,volume-nocopy', argv[argv.index('--mount') + 1])
            self.assertIn('fixture-image-id', argv)
            self.assertIn('--read-only', argv)

    def test_metadata_protocol_and_ttl_gate(self):
        self.assertIn('ttl.ttlMonitorEnabled !== false', module.MONGO_BASELINE_SCRIPT)
        self.assertNotIn('setParameter', module.MONGO_BASELINE_SCRIPT)
        self.assertIn("info.name !== 'system.views'", module.MONGO_BASELINE_SCRIPT)
        self.assertIn('process.exit(7)', module.MONGO_BASELINE_SCRIPT)
        self.assertIn('scheduler_executions', module.MONGO_BASELINE_SCRIPT)
        self.assertIn('options: info.options', module.MONGO_BASELINE_SCRIPT)
        result = 'notice\nTA_MIGRATION_BASELINE=[{"name":"fixture","count":2}]\n'
        with patch.object(module, 'run', return_value=types.SimpleNamespace(stdout=result)):
            self.assertEqual(module.mongo_baseline('fixture'), [{'name': 'fixture', 'count': 2}])
        for bad in ('notice', result + result, 'TA_MIGRATION_BASELINE=[]'):
            with patch.object(module, 'run', return_value=types.SimpleNamespace(stdout=bad)):
                with self.assertRaises((ValueError, RuntimeError)):
                    module.mongo_baseline('fixture')


if __name__ == '__main__':
    unittest.main(verbosity=2)
