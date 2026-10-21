"""Synthetic-only tests. Does not inspect containers, credentials, or databases."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from urllib.parse import unquote, urlsplit

spec = importlib.util.spec_from_file_location('migration_credentials', Path(__file__).with_name('migration_credentials.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class CredentialsTests(unittest.TestCase):
    def values(self):
        return {'mongodb_username': 'fixture@user:/中文',
                'mongodb_password': 'fixture-mongo-$"\\\n中文',
                'redis_password': 'fixture-redis-$"\\\n中文'}

    def test_mongo_roundtrip_keeps_exact_existing_value(self):
        original = self.values()
        config = json.loads(module.mongo_tools_config(original))
        self.assertEqual(config['password'], original['mongodb_password'])
        parsed = urlsplit(config['uri'])
        self.assertEqual(unquote(parsed.username), original['mongodb_username'])
        self.assertIsNone(parsed.password)
        self.assertEqual(parsed.hostname, '127.0.0.1')

    def test_redis_hash_matches_exact_utf8_password(self):
        values = self.values()
        acl = module.redis_acl(values)
        self.assertIn('#' + hashlib.sha256(values['redis_password'].encode()).hexdigest(), acl)
        self.assertNotIn(values['redis_password'], acl)
        self.assertNotIn('nopass', acl)

    def test_arguments_have_no_credentials_and_scope_app_db(self):
        values = self.values()
        for operation in ('dump', 'restore-dry-run'):
            argv = module.command_for_mongo(operation, '/run/migration/mongo-tools.json')
            for secret in values.values():
                self.assertNotIn(secret, ' '.join(argv))
            self.assertFalse(any(item.startswith('--password') for item in argv))
            self.assertTrue(any('k_tradingagents' in item for item in argv))
        with self.assertRaises(ValueError):
            module.command_for_mongo('restore-all', '/fixture')

    def test_restricted_files_and_no_overwrite(self):
        with tempfile.TemporaryDirectory(prefix='migration-secret-fixture-') as directory:
            destination = Path(directory) / 'new'
            files = module.write_files(self.values(), destination)
            self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o700)
            for name in files:
                self.assertEqual(stat.S_IMODE((destination / name).stat().st_mode), 0o600)
            with self.assertRaises(FileExistsError):
                module.write_files(self.values(), destination)
            target = Path(directory) / 'alias'
            target.symlink_to(destination, target_is_directory=True)
            with self.assertRaises(FileExistsError):
                module.write_files(self.values(), target)

    def test_missing_empty_and_nul_are_rejected(self):
        for field in self.values():
            for invalid in ('', None, 123, 'bad\0value'):
                values = self.values()
                values[field] = invalid
                with self.assertRaises(ValueError):
                    module.validate(values)

    def test_input_permissions_and_type(self):
        with tempfile.TemporaryDirectory(prefix='migration-input-fixture-') as directory:
            source = Path(directory) / 'fixture.json'
            source.write_text(json.dumps(self.values()))
            source.chmod(0o600)
            self.assertEqual(module.read_restricted_input(source), self.values())
            source.chmod(0o644)
            with self.assertRaises(ValueError):
                module.read_restricted_input(source)
            alias = Path(directory) / 'alias'
            alias.symlink_to(source)
            with self.assertRaises(ValueError):
                module.read_restricted_input(alias)

    def test_bootstrap_newline_rejected_before_creating_files(self):
        with tempfile.TemporaryDirectory(prefix='migration-newline-fixture-') as directory:
            values = self.values()
            values['mongodb_password'] += '\n'
            destination = Path(directory) / 'must-not-exist'
            with self.assertRaises(ValueError):
                module.write_files(values, destination)
            self.assertFalse(destination.exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
