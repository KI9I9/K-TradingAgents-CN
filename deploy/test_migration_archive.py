"""Synthetic fixtures only: no business files, Docker or network."""
import gzip
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import migration_archive as archive


def make_tar(path, entries=None):
    if entries is None:
        entries = [('nested/file.txt', b'fixture', tarfile.REGTYPE, 0o640)]
    with tarfile.open(path, 'w:gz') as stream:
        for name, content, kind, mode in entries:
            info = tarfile.TarInfo(name)
            info.type, info.mode = kind, mode
            info.size = len(content) if kind == tarfile.REGTYPE else 0
            if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                info.linkname = '../../escape'
            stream.addfile(info, io.BytesIO(content) if info.isfile() else None)
    path.chmod(0o600)


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='migration-archive-fixture-')
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def snapshot(self):
        destination = self.root / 'snapshot'
        destination.mkdir(mode=0o700)
        for name in archive.PAYLOADS:
            path = destination / name
            if name.endswith('.tar.gz'):
                make_tar(path)
            elif name.endswith('.gz'):
                with gzip.open(path, 'wb') as stream:
                    stream.write(b'not-a-real-mongo-dump')
            else:
                path.write_text('fixture-only')
            path.chmod(0o600)
        (destination / 'snapshot-metadata.json').write_text(json.dumps({
            'schema': 1, 'writers_stopped': True, 'ttl_monitor_enabled': False,
            'before': [{'name': 'fixture', 'count': 1}], 'after': [{'name': 'fixture', 'count': 1}],
        }))
        (destination / 'runtime-config.json').write_text(json.dumps({'schema': 1,
            'backend_environment': {'FIXTURE_ONLY': 'not-real'}, 'service_credentials': {
                'mongodb_username': 'fixture', 'mongodb_password': 'fixture', 'redis_password': 'fixture'}}))
        return destination

    def test_seal_and_verify(self):
        destination = self.snapshot()
        self.assertEqual(archive.seal(destination)['payload_files'], 17)
        self.assertEqual(archive.verify(destination)['status'], 'verified')

    def test_missing_payload_prevents_complete(self):
        destination = self.snapshot()
        (destination / 'backend-cache.tar.gz').unlink()
        with self.assertRaises(ValueError):
            archive.seal(destination)
        self.assertFalse((destination / 'COMPLETE').exists())

    def test_missing_complete_is_not_an_accepted_snapshot(self):
        destination = self.snapshot()
        archive.seal(destination)
        (destination / 'COMPLETE').unlink()
        with self.assertRaises(ValueError):
            archive.verify(destination)

    def test_extra_and_modified_files_rejected(self):
        destination = self.snapshot()
        archive.seal(destination)
        extra = destination / 'unexpected'
        extra.write_bytes(b'extra')
        with self.assertRaises(ValueError):
            archive.verify(destination)
        extra.unlink()
        (destination / '.env').write_text('changed-fixture')
        with self.assertRaises(ValueError):
            archive.verify(destination)

    def test_permissions_symlinks_and_hardlinks_rejected(self):
        destination = self.snapshot()
        archive.seal(destination)
        path = destination / '.env'
        path.chmod(0o644)
        with self.assertRaises(ValueError):
            archive.verify(destination)
        path.chmod(0o600)
        os.link(path, self.root / 'external-hardlink')
        with self.assertRaises(ValueError):
            archive.verify(destination)
        path.unlink()
        path.symlink_to(self.root / 'external-hardlink')
        with self.assertRaises(ValueError):
            archive.verify(destination)

    def test_duplicate_manifest_fields_rejected(self):
        path = self.root / 'duplicate.json'
        path.write_text('{"schema":2,"schema":2}')
        with self.assertRaises(ValueError):
            archive.strict_json(path)

    def test_changed_baseline_cannot_be_completed(self):
        destination = self.snapshot()
        path = destination / 'snapshot-metadata.json'
        value = json.loads(path.read_text())
        value['after'][0]['count'] = 2
        path.write_text(json.dumps(value))
        with self.assertRaises(ValueError):
            archive.seal(destination)
        self.assertFalse((destination / 'COMPLETE').exists())

    def test_dangerous_tar_entries_rejected(self):
        for name, kind, mode in [('../escape', tarfile.REGTYPE, 0o600),
                                 ('/escape', tarfile.REGTYPE, 0o600),
                                 ('link', tarfile.SYMTYPE, 0o600),
                                 ('hard', tarfile.LNKTYPE, 0o600),
                                 ('device', tarfile.CHRTYPE, 0o600),
                                 ('pipe', tarfile.FIFOTYPE, 0o600),
                                 ('suid', tarfile.REGTYPE, 0o4600)]:
            with self.subTest(name=name):
                path = self.root / 'danger.tar.gz'
                make_tar(path, [(name, b'x', kind, mode)])
                with self.assertRaises(ValueError):
                    archive.validate_tar(path)

    def test_duplicate_members_and_parent_conflicts_rejected(self):
        for names in [('x', './x'), ('x', 'x/y'), ('x/y', 'x')]:
            path = self.root / 'conflict.tar.gz'
            make_tar(path, [(name, b'x', tarfile.REGTYPE, 0o600) for name in names])
            with self.assertRaises(ValueError):
                archive.validate_tar(path)

    def test_truncated_and_oversized_compression_rejected(self):
        path = self.root / 'bad.tar.gz'
        make_tar(path)
        with self.assertRaises(ValueError):
            archive.validate_gzip(path, limit=32)
        with self.assertRaises(ValueError):
            archive.validate_tar(path, max_members=0)
        path.write_bytes(path.read_bytes()[:-5])
        with self.assertRaises((EOFError, OSError)):
            archive.validate_tar(path)

    def test_extract_preserves_mode_and_refuses_existing_destination(self):
        path = self.root / 'valid.tar.gz'
        make_tar(path)
        destination = self.root / 'new'
        archive.extract_new(path, destination)
        self.assertEqual((destination / 'nested/file.txt').read_bytes(), b'fixture')
        self.assertEqual((destination / 'nested/file.txt').stat().st_mode & 0o777, 0o640)
        self.assertEqual(destination.stat().st_mode & 0o777, 0o700)
        with self.assertRaises(FileExistsError):
            archive.extract_new(path, destination)

    def test_unsafe_tar_does_not_create_target(self):
        path = self.root / 'evil.tar.gz'
        make_tar(path, [('../escaped', b'bad', tarfile.REGTYPE, 0o600)])
        destination = self.root / 'new'
        with self.assertRaises(ValueError):
            archive.extract_new(path, destination)
        self.assertFalse(destination.exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
