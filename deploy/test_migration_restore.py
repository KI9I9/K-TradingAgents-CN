"""Offline target restore guards. All process calls are mocked; no real data."""
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import types
import unittest
from unittest.mock import patch

import migration_restore as restore
from migration_archive import write_json_new
from test_migration_redis_history import policy


class RestoreTests(unittest.TestCase):
    def test_restore_requires_both_explicit_confirmations(self):
        with patch.object(restore, 'run') as command, patch.object(restore, 'assert_accepted') as verify:
            for flags in ({}, {'approved': True}, {'source_frozen': True}):
                with self.assertRaises(ValueError):
                    restore.restore('/unused', '/unused', **flags)
            verify.assert_not_called()
            command.assert_not_called()

    def test_review_and_stop_require_separate_confirmations(self):
        with patch.object(restore, 'load') as load:
            for action in (restore.review, restore.quiesce):
                with self.assertRaises(ValueError):
                    action('/unused')
            load.assert_not_called()

    def test_failed_restore_cannot_start_backend(self):
        with patch.object(restore, 'load', return_value={'stage': 'failed_retained_for_review', 'restore_passed': False}), \
             patch.object(restore, 'compose') as compose:
            with self.assertRaises(ValueError):
                restore.review('/unused', approved=True)
            compose.assert_not_called()

    def test_acceptance_requires_age_record(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(restore, 'verify'):
            with self.assertRaises(FileNotFoundError):
                restore.assert_accepted(Path(temporary) / 'fixture')

    def test_original_runtime_mismatch_or_missing_signing_key_rejected(self):
        fixture = {'backend_environment': {'MONGODB_USERNAME': 'u', 'MONGODB_PASSWORD': 'p', 'REDIS_PASSWORD': 'r',
                    'JWT_SECRET': 'j', 'CSRF_SECRET': 'c', 'MONGODB_DATABASE': 'k_tradingagents'},
                   'service_credentials': {'mongodb_username': 'u', 'mongodb_password': 'p', 'redis_password': 'r'}}
        with patch.object(restore, 'private_json', return_value=fixture):
            self.assertEqual(restore.checked_runtime(Path('/unused')), fixture)
            fixture['backend_environment']['REDIS_PASSWORD'] = 'different'
            with self.assertRaises(ValueError): restore.checked_runtime(Path('/unused'))
            fixture['backend_environment']['REDIS_PASSWORD'] = 'r'
            del fixture['backend_environment']['JWT_SECRET']
            with self.assertRaises(ValueError): restore.checked_runtime(Path('/unused'))

    def test_archive_root_and_binary_file_metadata_preserved(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            archive = parent / 'tree.tar.gz'
            with tarfile.open(archive, 'w:gz') as stream:
                for name, kind, mode, body in (('.', tarfile.DIRTYPE, 0o750, b''),
                                              ('payload', tarfile.REGTYPE, 0o640, b'\0\xfffixture')):
                    entry = tarfile.TarInfo(name)
                    entry.type, entry.mode, entry.uid, entry.gid = kind, mode, os.getuid(), os.getgid()
                    entry.size = len(body)
                    stream.addfile(entry, io.BytesIO(body) if body else None)
            with patch.object(restore.os, 'geteuid', return_value=0):
                result = restore.restore_tree(archive, parent / 'restored')
            self.assertEqual(result, {'files': 1, 'contents_and_ownership_equal': True})
            self.assertEqual((parent / 'restored').stat().st_mode & 0o777, 0o750)
            self.assertEqual(parent.stat().st_mode & 0o777, 0o700)

    def test_missing_archive_root_rejected_without_guessing(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            archive = parent / 'tree.tar.gz'
            with tarfile.open(archive, 'w:gz') as stream:
                entry = tarfile.TarInfo('only-file')
                stream.addfile(entry, io.BytesIO())
            with self.assertRaises(ValueError):
                restore.restore_tree(archive, parent / 'restored')
            self.assertFalse((parent / 'restored').exists())

    def test_mismatched_container_config_path_not_stopped(self):
        metadata = {'Config': {'Labels': {'com.docker.compose.project': 'ta-restore-fixture',
                                         'com.docker.compose.project.config_files': '/different/compose.json'}}}
        with patch.object(restore, 'run', side_effect=[types.SimpleNamespace(stdout=b'fixture-id\n'),
                         types.SimpleNamespace(stdout=json.dumps([metadata]).encode())]) as run:
            with self.assertRaises(ValueError):
                restore.stop_project(Path('/expected'), {'project': 'ta-restore-fixture'})
            self.assertEqual(run.call_count, 2)

    def test_stop_preserves_volumes_and_does_not_start_source(self):
        record = {'Id': 'exact-fixture-id', 'State': {'Running': True}}
        stopped = {'Id': 'exact-fixture-id', 'State': {'Running': False}}
        with patch.object(restore, 'project_containers', side_effect=[[record], [stopped]]), \
             patch.object(restore, 'run') as run:
            self.assertTrue(restore.stop_project(Path('/unused'), {}))
            run.assert_called_once_with(['docker', 'stop', '--time', '20', 'exact-fixture-id'], timeout=45)

    def test_exception_bound_to_snapshot_and_rehearsal_before_target_creation(self):
        for rehearsal, digest in ((True, 'b' * 64), (False, 'a' * 64)):
            with tempfile.TemporaryDirectory() as temporary:
                parent = Path(temporary)
                target = parent / 'never-created'
                with patch.object(restore.os, 'geteuid', return_value=0), \
                     patch.object(restore, 'assert_accepted'), \
                     patch.object(restore, 'file_sha256', return_value=digest), \
                     patch.object(restore, 'private_json', return_value=policy()), \
                     patch.object(restore, 'checked_runtime') as runtime, patch.object(restore, 'run') as run:
                    with self.assertRaises(ValueError):
                        restore.restore(parent / 'snapshot', target, approved=True, rehearsal_only=rehearsal,
                            source_frozen=not rehearsal, approved_terminal_exception=parent / 'policy.json')
                    runtime.assert_not_called()
                    run.assert_not_called()
                    self.assertFalse(target.exists())

    def test_exception_file_is_mounted_read_only_without_values_in_arguments(self):
        state = {'project': 'ta-restore-fixture', 'terminal_exception_approved': True}
        with patch.object(restore, 'run', return_value=types.SimpleNamespace(stdout=b'{}')) as run, \
             patch.object(restore.subprocess, 'run'):
            restore.client(Path('/private/fixture'), state, 'redis')
        argv = run.call_args.args[0]
        self.assertIn('type=bind,src=/private/fixture/terminal-exception.json,dst=/terminal-exception.json,readonly', argv)
        self.assertNotIn('cancelled-fixture', ' '.join(argv))

    def test_final_policy_reaches_runtime_gate_but_never_starts_target_in_test(self):
        final = policy()
        final['purpose'] = 'cutover_staging'
        final['snapshot_manifest_sha256'] = 'c' * 64
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            target = parent / 'not-created'
            with patch.object(restore.os, 'geteuid', return_value=0), \
                 patch.object(restore, 'assert_accepted'), \
                 patch.object(restore, 'file_sha256', return_value='c' * 64), \
                 patch.object(restore, 'private_json', return_value=final), \
                 patch.object(restore, 'checked_runtime', side_effect=RuntimeError('fixture-stop')) as runtime, \
                 patch.object(restore, 'run') as run:
                with self.assertRaisesRegex(RuntimeError, '^fixture-stop$'):
                    restore.restore(parent / 'snapshot', target, approved=True, source_frozen=True,
                                    approved_terminal_exception=parent / 'policy.json')
                runtime.assert_called_once()
                run.assert_not_called()
                self.assertFalse(target.exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
