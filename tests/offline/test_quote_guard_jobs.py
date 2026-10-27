"""鉴权取证工具离线测试，不连接服务器、不执行真实登录。"""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('query_jobs', ROOT / 'deploy/query_quote_guard_jobs.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
remote = {'__name__': 'offline_remote'}
exec(module.REMOTE, remote)


def jobs():
    return [{'id': ident, 'paused': True, 'has_running_execution': False, 'next_run_time': None}
            for ident in module.TARGET_JOBS if ident != 'quotes_ingestion_service']


class JobsTests(unittest.TestCase):
    def test_token_only_calls_jobs_and_filters_private_fields(self):
        calls = []
        def fetch(path, **kwargs):
            calls.append((path, kwargs))
            return {'success': True, 'data': [dict(j, kwargs={'secret': 'CANARY'}) for j in jobs()]}
        result = remote['query']({'auth': {'token': 'TOKEN_CANARY'}, 'jobs': module.TARGET_JOBS}, fetch)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], '/api/scheduler/jobs')
        self.assertNotIn('CANARY', json.dumps(result))
        self.assertEqual(module.check_jobs(result), [])

    def test_credentials_use_normal_login_once(self):
        calls = []
        auth = {'username': 'operator', 'password': 'quote" slash\\'}
        def fetch(path, **kwargs):
            calls.append((path, kwargs))
            if path.endswith('/login'):
                return {'success': True, 'data': {'access_token': 'TOKEN_CANARY', 'refresh_token': 'PRIVATE'}}
            self.assertEqual(kwargs['token'], 'TOKEN_CANARY')
            return {'success': True, 'data': jobs()}
        result = remote['query']({'auth': auth, 'jobs': module.TARGET_JOBS}, fetch)
        self.assertEqual([c[0] for c in calls], ['/api/auth/login', '/api/scheduler/jobs'])
        self.assertEqual(calls[0][1]['body'], auth)
        self.assertNotIn('CANARY', json.dumps(result))

    def test_login_failure_does_not_retry_or_request_jobs(self):
        calls = []
        def fetch(path, **kwargs):
            calls.append(path)
            return {'success': False}
        with self.assertRaises(ValueError):
            remote['query']({'auth': {'username': 'u', 'password': 'p'}, 'jobs': module.TARGET_JOBS}, fetch)
        self.assertEqual(calls, ['/api/auth/login'])

    def test_api_failure_or_duplicates_fail_closed(self):
        for response in ({'success': False, 'data': jobs()}, {'success': True, 'data': jobs() + jobs()}):
            with self.subTest(response=response), self.assertRaises(ValueError):
                remote['query']({'auth': {'token': 't'}, 'jobs': module.TARGET_JOBS}, lambda *a, **k: response)

    def test_missing_false_fields_are_not_idle(self):
        for key in ('paused', 'has_running_execution', 'next_run_time'):
            data = jobs()
            del data[0][key]
            self.assertTrue(module.check_jobs({'data': data}))
        self.assertTrue(module.check_jobs({'data': []}))

    def test_credential_permissions_and_symlinks(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'creds'
            path.write_text('operator\npassword\n')
            path.chmod(0o600)
            self.assertEqual(module.read_auth(path, True)['username'], 'operator')
            link = Path(temp) / 'link'
            link.symlink_to(path)
            with self.assertRaises(OSError):
                module.read_auth(link, True)
            path.chmod(0o644)
            with self.assertRaises(ValueError):
                module.read_auth(path, True)

    def test_token_sent_only_via_stdin_output_private(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'token'
            path.write_text('TOKEN_CANARY')
            path.chmod(0o600)
            output = Path(temp) / 'jobs.json'
            report = {'success': True, 'data': jobs(), 'missing_jobs': ['quotes_ingestion_service']}
            def run(args, **kwargs):
                self.assertNotIn('TOKEN_CANARY', ' '.join(args))
                self.assertEqual(json.loads(kwargs['input'])['auth']['token'], 'TOKEN_CANARY')
                return types.SimpleNamespace(returncode=0, stdout=json.dumps(report))
            argv = ['query', '--token-file', str(path), '--output', str(output), '--ssh-config', '/config',
                    '--host', 'production', '--container', 'backend']
            with patch('sys.argv', argv), patch.object(module.subprocess, 'run', run), patch('builtins.print'):
                self.assertEqual(module.main(), 0)
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            self.assertNotIn('CANARY', output.read_text())


if __name__ == '__main__':
    unittest.main(verbosity=2)
