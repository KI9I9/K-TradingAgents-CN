"""Optional real age roundtrip, using only temporary synthetic keys/data.

Run separately: MIGRATION_TEST_AGE=1 python3 -B deploy/test_migration_transport.py
The logging regression deliberately denies subprocesses, so do not combine them.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import unittest
from unittest.mock import patch
import test_migration_archive as fixtures
import migration_archive as archive
import migration_transport as transport


@unittest.skipUnless(os.environ.get('MIGRATION_TEST_AGE') == '1' and shutil.which('age'), 'Explicit synthetic age test only')
class TransportTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ArchiveTests()
        self.fixture.setUp()
        self.root = self.fixture.root
        self.snapshot = self.fixture.snapshot()
        archive.seal(self.snapshot)
        self.key = self.root / 'synthetic.agekey'
        subprocess.run(['age-keygen', '-o', str(self.key)], capture_output=True, check=True)
        self.key.chmod(0o600)
        self.recipient = subprocess.run(['age-keygen', '-y', str(self.key)], capture_output=True, text=True, check=True).stdout.strip()
        self.cipher = self.root / 'fixture.age'
        transport.encrypt(self.snapshot, self.cipher, self.recipient)
        self.receipt = self.cipher.with_suffix('.age.transport.json')

    def tearDown(self):
        self.fixture.tearDown()

    def test_real_age_roundtrip_preserves_all_payloads(self):
        target = self.root / 'accepted'
        with patch.object(transport, 'RECIPIENT', self.recipient):
            result = transport.decrypt(self.cipher, self.receipt, target, self.key)
        self.assertEqual(result['status'], 'accepted')
        for name in transport.FILES:
            self.assertEqual((target / name).read_bytes(), (self.snapshot / name).read_bytes())
        self.assertTrue((self.root / 'accepted.accepted.json').is_file())

    def test_authenticated_eof_required_even_if_transport_hash_matches(self):
        data = bytearray(self.cipher.read_bytes())
        data[-1] ^= 1
        self.cipher.write_bytes(data)
        receipt = json.loads(self.receipt.read_text())
        receipt['ciphertext_sha256'] = archive.file_sha256(self.cipher)
        self.receipt.write_text(json.dumps(receipt))
        target = self.root / 'rejected'
        with patch.object(transport, 'RECIPIENT', self.recipient):
            with self.assertRaises((ValueError, EOFError, tarfile.ReadError)):
                transport.decrypt(self.cipher, self.receipt, target, self.key)
        self.assertFalse((target / 'COMPLETE').exists())
        self.assertFalse((self.root / 'rejected.accepted.json').exists())

    def test_receipt_mismatch_rejects_before_plaintext_directory(self):
        self.cipher.write_bytes(self.cipher.read_bytes()[:-1])
        target = self.root / 'not-created'
        with patch.object(transport, 'RECIPIENT', self.recipient):
            with self.assertRaises(ValueError):
                transport.decrypt(self.cipher, self.receipt, target, self.key)
        self.assertFalse(target.exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
