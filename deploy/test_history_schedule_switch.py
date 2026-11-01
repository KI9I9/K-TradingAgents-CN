"""Exercise the generated controller's switch/failure/rollback branches offline."""
import json
import hashlib
import tempfile
from pathlib import Path
import unittest

from run_history_schedule_switch import controller


class SwitchTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.ns = {'__name__': 'offline_controller'}
        exec(compile(controller(), '<frozen-controller>', 'exec'), self.ns)
        self.events, self.actions = [], []
        self.ns.update(PAYLOAD={'check_only': False}, MANIFEST={'source_head': 'synthetic'}, STATE={},
                       ATTEMPT=Path(self.directory.name))
        self.ns['event'] = self.events.append
        self.ns['save'] = lambda *_: None
        for name in ('preflight', 'seal_check', 'module_hashes', 'protected_check', 'audit', 'smoke', 'ingress_probe', 'check_running'):
            self.ns[name] = lambda *_, **kw: None
        self.ns['stop_backend'] = lambda: self.actions.append('stop')
        self.ns['up'] = lambda new: self.actions.append(('up', new))
        self.ns['healthy'] = lambda: None
        self.ns['in_backend'] = lambda _: 'IMPORT_OK\n'
        self.ns['db_probe'] = lambda _: {'hashes': {'synthetic': 'unchanged'}, 'preferences': []}
        self.ns['api_probe'] = lambda *_, **kw: {'index': 'same'}
        (Path(self.directory.name) / 'frontend-before.json').write_text(json.dumps({'index': 'same'}))
        self.ns['config'] = lambda _: {'synthetic': 'original'}
        self.ns['PAYLOAD']['config_hashes'] = {'before': self.ns['digest'](self.ns['canonical']({'synthetic': 'original'}))}

    def test_success_installs_once_and_does_not_run_recovery(self):
        self.assertEqual(self.ns['main'](), 0)
        self.assertEqual(self.actions, ['stop', ('up', True)])
        self.assertEqual(self.events[-1], 'complete')

    def test_preflight_only_never_stops_application(self):
        self.ns['PAYLOAD']['check_only'] = True
        self.assertEqual(self.ns['main'](), 0)
        self.assertEqual(self.actions, [])

    def test_smoke_failure_restores_original_compose_chain(self):
        self.ns['smoke'] = lambda: (_ for _ in ()).throw(RuntimeError('synthetic_install_failure'))
        self.assertEqual(self.ns['main'](), 1)
        self.assertEqual(self.actions, ['stop', ('up', True), 'stop', ('up', False)])
        self.assertEqual(self.events[-1], 'rolled_back_history_release')

    def test_changed_data_fails_acceptance_and_rolls_back_application(self):
        self.ns['db_probe'] = lambda name: {'hashes': {'synthetic': 'changed' if name == 'db-after.json' else 'unchanged'}, 'preferences': []}
        self.assertEqual(self.ns['main'](), 1)
        self.assertEqual(self.ns['STATE']['failure_code'], 'guarded_business_content_changed')
        self.assertEqual(self.actions[-1], ('up', False))

    def test_failed_rollback_requests_backend_stop(self):
        def fail():
            raise RuntimeError('synthetic_health_failure')
        self.ns['healthy'] = fail
        self.assertEqual(self.ns['main'](), 1)
        self.assertEqual(self.events[-1], 'rollback_failed_stop_requested')
        self.assertEqual(self.actions[-1], 'stop')

    def test_no_downtime_if_backup_fails(self):
        def fail():
            raise RuntimeError('synthetic_backup_failure')
        self.ns['preflight'] = fail
        self.assertEqual(self.ns['main'](), 1)
        self.assertEqual(self.actions, [])

    def test_running_mount_count_depends_on_release(self):
        # Run the real check_running function, bypassing only external Docker IO.
        ns = {'__name__': 'offline_running'}
        exec(compile(controller(), '<frozen-controller>', 'exec'), ns)
        cfg = {'services': {'backend': {'image': 'synthetic-image'}}}
        h = ns['digest'](ns['canonical'](cfg))
        ns.update(PAYLOAD={'config_hashes': {'before': h, 'after': h}, 'image_id': 'synthetic-id'})
        ns['config'] = lambda _: cfg
        ns['run'] = lambda _: '[{}]'
        ns['VERIFY'] = type('Verifier', (), {'compare_running': staticmethod(lambda *_: [])})
        for new, count in ((False, 40), (True, 43)):
            ns['inspect'] = lambda count=count: {'Image': 'synthetic-id', 'Mounts': [None] * count}
            ns['check_running'](new)
            with self.assertRaisesRegex(RuntimeError, 'mount_count_mismatch'):
                ns['check_running'](not new)

    def test_unreadable_stage_refused_before_any_backend_operation(self):
        ns = {'__name__': 'offline_preflight'}
        exec(compile(controller(), '<frozen-controller>', 'exec'), ns)
        stage = Path(self.directory.name) / 'staged'
        stage.mkdir()
        module = stage / 'worker.py'
        module.write_bytes(b'pass\n')
        checksums = stage / 'SHA256SUMS'
        checksums.write_text(hashlib.sha256(module.read_bytes()).hexdigest() + '  worker.py\n')
        ns.update(STAGED=stage, PAYLOAD={'checksums_sha256': hashlib.sha256(checksums.read_bytes()).hexdigest()})
        def first_external_operation():
            raise RuntimeError('reached_backend_inspect')
        ns['inspect'] = first_external_operation
        module.chmod(0o600)
        with self.assertRaisesRegex(RuntimeError, 'staged_file_mode_changed'):
            ns['preflight']()
        module.chmod(0o644)
        with self.assertRaisesRegex(RuntimeError, 'reached_backend_inspect'):
            ns['preflight']()


if __name__ == '__main__':
    unittest.main()
