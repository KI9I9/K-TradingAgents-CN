"""Check reader delivery preserves bind inode and survives failed app startup."""
from datetime import datetime
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT/'deploy'))
import paper_supply_continuity as delivery
from app.services.quote_contract import CN_TZ


class Continuity(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root/'reader.py'; self.old = b'# original\n'; self.new = b'# renewed\n'
        self.source.write_bytes(self.old); self.source.chmod(0o644)
        self.runtime = self.root/'runtime'; self.runtime.mkdir()
        self.permit = self.runtime/'paper-quote-supply.json'; self.permit.write_bytes(b'original permit')
        self.request = dict(modules={delivery.TARGET: delivery.digest(self.old)},
            candidate=self.new.decode(), candidate_sha256=delivery.digest(self.new),
            previous_permit_sha256=delivery.digest(self.permit.read_bytes()))
        self.value = dict(Mounts=[dict(Destination='/app/runtime', Source=str(self.runtime), Type='bind')])
        self.attempt = self.root/'attempt'; self.attempt.mkdir()
        for attribute, value in dict(SOURCE=self.source, ROOT=self.root,
            ATTEMPT=self.attempt, REQUEST=self.request, PAYLOAD={'attempt':'test','check_only':False},
            HELPER=types.SimpleNamespace(native_import_argv=lambda *a: ['isolated-import']), STATE={}).items():
            patcher = patch.object(delivery, attribute, value, create=True)
            patcher.start(); self.addCleanup(patcher.stop)

    def snapshot(self, new=False, backup=None):
        return dict(modules={'reader': delivery.digest(self.source.read_bytes())},
            runtime={delivery.PERMIT: self.request['previous_permit_sha256']},
            collections={'paper_orders':dict(count=0,sha256='same',raw_sha256='same')}, used=11178)

    def test_in_place_write_and_restore_keep_mount_inode_and_permissions(self):
        before = self.source.stat()
        delivery.overwrite(self.old, self.new)
        self.assertEqual(self.source.read_bytes(), self.new)
        self.assertEqual(self.source.stat().st_ino, before.st_ino)
        self.assertEqual(self.source.stat().st_mode, before.st_mode)
        delivery.overwrite(self.new, self.old)
        self.assertEqual(self.source.read_bytes(), self.old)

    def test_drift_and_symlink_do_not_modify_unknown_source(self):
        with self.assertRaisesRegex(RuntimeError, 'source_changed'):
            delivery.overwrite(b'wrong', self.new)
        other = self.root/'other'; self.source.rename(other); self.source.symlink_to(other)
        with self.assertRaisesRegex(RuntimeError, 'source_symlink'):
            delivery.overwrite(self.old, self.new)
        self.assertEqual(other.read_bytes(), self.old)

    def test_failed_start_can_restore_without_docker_exec_in_stopped_container(self):
        stopped = []
        now = datetime(2026,11,3,14,tzinfo=CN_TZ)
        with patch.object(delivery,'running',return_value=(self.value,{})), \
             patch.object(delivery,'jobs'), patch.object(delivery,'run',return_value='PAPER_IMPORT_OK:'), \
             patch.object(delivery,'snapshot',side_effect=self.snapshot) as snapshot, \
             patch.object(delivery,'call_renewal',return_value={'ok':True}) as renewal, \
             patch.object(delivery,'stop',side_effect=lambda:stopped.append(True)), \
             patch.object(delivery,'start',side_effect=[RuntimeError('backend_exited'),None]) as start, \
             patch.object(delivery,'datetime') as clock:
            clock.now.return_value = now
            self.assertEqual(delivery.main(),1)
        self.assertEqual(self.source.read_bytes(),self.old)
        self.assertEqual(start.call_count,2)
        self.assertEqual(delivery.STATE['stage'],'rolled_back')
        # No snapshot under the new source while the container is stopped.
        self.assertFalse(any(call.args and call.args[0] is True for call in snapshot.call_args_list))
        self.assertEqual(renewal.call_count,1)  # Precheck only; actual renewal was never reached.

    def test_reader_precheck_always_snapshots_old_disk_bytes(self):
        payload = dict(delivery.PAYLOAD, auth={'password':'SECRET_CANARY'})
        extra = dict(expires_at='2026-11-08T14:00:00+08:00',runtime='runtime',
            snapshot_code='snapshot',query_code='query',validate_code='validate')
        delivery.REQUEST.update(extra)
        with patch.object(delivery,'PAYLOAD',payload):
            before = delivery.renewal_request(True); after = delivery.renewal_request(False)
        self.assertEqual(before['modules'][delivery.TARGET],delivery.digest(self.old))
        self.assertEqual(after['modules'][delivery.TARGET],delivery.digest(self.new))


if __name__ == '__main__':
    unittest.main()
