"""Synthetic-only tests of the inactive launch planner and explicit startup gate."""
import copy
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from subprocess import CompletedProcess
from unittest.mock import patch

import migration_launch_plan as plan
import migration_production_entrypoint as entry

FAKE = 'SYNTHETIC_SECRET_NEVER_A_REAL_CREDENTIAL'
DIRECTORY = Path('/private/final-restore')
OUTPUT = Path('/private/pending-launch')
IMAGE = 'sha256:' + 'e' * 64


def fixture(debug='false'):
    state = {'schema': 1, 'kind': 'isolated_target_restore', 'directory': str(DIRECTORY),
             'snapshot': '/private/new-final-snapshot', 'purpose': 'cutover_staging',
             'requires_new_final_snapshot': False, 'source_frozen_confirmed': True,
             'restore_passed': True, 'stage': 'target_stopped_retained', 'target_inactive_confirmed': True,
             'production_activation_allowed': False, 'project': 'ta-restore-123456abcdef',
             'manifest_sha256': 'b' * 64}
    review = {'volumes': {key: {'name': state['project'] + '_' + key}
        for key in ('review_mongodb_data', 'review_mongodb_config', 'review_redis_data')}}
    runtime = {'schema': 1, 'backend_environment': {'MONGODB_USERNAME': 'fixture_user',
        'MONGODB_PASSWORD': FAKE + 'mongo', 'REDIS_PASSWORD': FAKE + 'redis',
        'JWT_SECRET': FAKE + 'jwt', 'CSRF_SECRET': FAKE + 'csrf', 'MODEL_API_KEY': FAKE + 'model',
        'MONGODB_DATABASE': 'k_tradingagents', 'REDIS_DB': '0', 'DEBUG': debug,
        'WATCHLIST_ANALYSIS_ENABLED': 'true', 'PROXY_ENABLED': 'false'},
        'service_credentials': {'mongodb_username': 'fixture_user', 'mongodb_password': FAKE + 'mongo',
                                'redis_password': FAKE + 'redis'}}
    return state, review, json.dumps(runtime).encode()


def rendered(debug='false', **kwargs):
    state, review, runtime = fixture(debug)
    opts = {'backend_image': IMAGE, 'original_ttl': True, 'origin': entry.APPROVED_ORIGIN,
            'tls_files': {'certificate': '/private/local.crt', 'key': '/private/local.key'}}
    opts.update(kwargs)
    return runtime, plan.render(DIRECTORY, state, review, runtime, OUTPUT, **opts)


def approved(context):
    return {'schema': 1, 'purpose': 'activate_final_snapshot',
            'snapshot_manifest_sha256': context['snapshot_manifest_sha256'],
            'target_project': context['project'], 'access_origin': context['access_origin'],
            'original_ttl_enabled': context['original_ttl_enabled'],
            **{name: True for name in entry.CONFIRMATIONS}}


class LaunchPlanTests(unittest.TestCase):
    def test_01_no_host_ports_and_frontend_and_db_have_no_egress(self):
        _, (_, config, _) = rendered()
        for name, service in config['services'].items():
            self.assertNotIn('ports', service)
            self.assertNotIn('network_mode', service)
            self.assertTrue(service['read_only'])
            self.assertEqual(service['profiles'], ['cutover-approved'])
            self.assertEqual(service['logging'], {'driver': 'none'})
            self.assertEqual(service['cap_drop'], ['ALL'])
            self.assertIn('no-new-privileges:true', service['security_opt'])
            self.assertNotIn('env_file', service)
        self.assertEqual(config['services']['frontend']['user'], '1000:1000')
        for name in ('mongodb', 'redis'):
            self.assertEqual(config['services'][name]['networks'], ['db_internal'])
        self.assertEqual(config['services']['frontend']['networks'], ['web_internal'])
        self.assertTrue(config['networks']['db_internal']['internal'])
        self.assertTrue(config['networks']['web_internal']['internal'])
        self.assertNotIn(FAKE, json.dumps(config))

    def test_02_original_mounts_and_external_volumes_no_cold_redis_or_new_empty_data(self):
        _, (context, config, _) = rendered()
        self.assertEqual(context['project'], 'ta-live-123456abcdef')
        self.assertNotIn('redis-cold', config['services'])
        for alias, volume in config['volumes'].items():
            self.assertTrue(volume['external'])
            self.assertEqual(volume['name'], 'ta-restore-123456abcdef_' + alias)
        for name, target in plan.restore.MOUNTS.items():
            self.assertIn(plan.bind(DIRECTORY / 'mounts' / name, target, False), config['services']['backend']['volumes'])
        self.assertNotIn('/private/final-restore/mounts/redis_data', json.dumps(config))

    def test_03_every_service_requires_existing_approval_file(self):
        _, (_, config, _) = rendered()
        for service in config['services'].values():
            mounts = [v for v in service['volumes'] if v.get('target') == '/run/secrets/activation-approval.json']
            self.assertEqual(len(mounts), 1)
            self.assertTrue(mounts[0]['read_only'])
            self.assertFalse(mounts[0]['bind']['create_host_path'])
        self.assertEqual(config['services']['backend']['command'], [])

    def test_04_rehearsal_unverified_running_or_unfrozen_state_rejected(self):
        original, review, runtime = fixture()
        for key, value in (('purpose', 'rehearsal_only'), ('requires_new_final_snapshot', True),
                           ('source_frozen_confirmed', False), ('restore_passed', False),
                           ('stage', 'limited_review_running'), ('target_inactive_confirmed', False),
                           ('production_activation_allowed', True), ('manifest_sha256', '*')):
            state = {**original, key: value}
            with self.subTest(key=key), self.assertRaises(ValueError):
                plan.render(DIRECTORY, state, review, runtime, OUTPUT, backend_image=IMAGE,
                            original_ttl=True, origin=entry.APPROVED_ORIGIN)

    def test_05_rehearsal_prepare_refused_before_runtime_read_or_docker(self):
        state, _, _ = fixture()
        state['purpose'] = 'rehearsal_only'
        with patch.object(plan.restore, 'load', return_value=state), \
             patch.object(plan.restore, 'project_containers') as containers, \
             patch.object(plan.restore, 'checked_runtime') as credentials:
            with self.assertRaises(ValueError):
                plan.prepare(DIRECTORY, OUTPUT, backend_image=IMAGE, original_ttl=True,
                             origin=entry.APPROVED_ORIGIN)
            containers.assert_not_called()
            credentials.assert_not_called()

    def test_06_volume_identity_pinning_and_explicit_ttl(self):
        state, review, raw = fixture()
        review['volumes']['review_redis_data']['name'] = 'unrelated'
        with self.assertRaises(ValueError):
            plan.render(DIRECTORY, state, review, raw, OUTPUT, backend_image=IMAGE,
                        original_ttl=True, origin=entry.APPROVED_ORIGIN,
                        tls_files={'certificate': '/private/local.crt', 'key': '/private/local.key'})
        for opts in ({'backend_image': 'backend:latest'}, {'original_ttl': 1},
                     {'origin': 'https://public.invalid'}, {'origin': 'http://localhost:8088/path'}):
            with self.subTest(opts=opts), self.assertRaises(ValueError): rendered(**opts)
        _, (_, config, _) = rendered(original_ttl=False)
        self.assertIn('ttlMonitorEnabled=false', config['services']['mongodb']['command'])

    def test_07_production_cookies_require_tls_without_changing_debug(self):
        with self.assertRaises(ValueError): rendered('false', tls_files=None)
        with self.assertRaises(ValueError): rendered('false', tls_files={'certificate': '/private/local.crt'})
        runtime, (context, config, nginx) = rendered('false')
        env = entry.validated_environment(runtime, context, approved(context))
        self.assertEqual(env['DEBUG'], 'false')
        self.assertIn('listen 8080 ssl;', nginx)
        self.assertIn('ssl_protocols TLSv1.2 TLSv1.3;', nginx)
        self.assertIn('/run/secrets/tls-key.pem', json.dumps(config['services']['frontend']['volumes']))

    def test_18_only_verified_https_origin_and_old_image_gate_rejected(self):
        for origin in ('http://127.0.0.1:18088', 'https://127.0.0.1:8088',
                       'https://localhost:18088', 'https://127.0.0.1:18088/',
                       'https://public.invalid:18088'):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                rendered(origin=origin)
        metadata = {'Id': IMAGE, 'Architecture': 'arm64', 'Config': {'Labels': {
            'migration.launch.patch': 'final-cutover-gate-https-20261017-v1'}}}
        with self.assertRaises(ValueError): plan.validate_image(metadata, IMAGE, backend=True)

    def test_08_reverse_proxy_preserves_api_sse_websocket_and_origin(self):
        _, (_, _, nginx) = rendered()
        for part in ('location /api/', 'proxy_pass http://backend:8000;', 'proxy_set_header Host $http_host;',
                     'proxy_set_header X-Forwarded-Proto $scheme;', 'proxy_buffering off;',
                     'proxy_set_header Upgrade $http_upgrade;', 'try_files $uri $uri/ /index.html;'):
            self.assertIn(part, nginx)
        self.assertNotIn('listen 80;', nginx)

    def test_16_backend_gate_image_label_required(self):
        data = {'Id': IMAGE, 'Architecture': 'arm64', 'Config': {'Labels': {
            'migration.launch.patch': plan.BACKEND_LABEL}}}
        plan.validate_image(data, IMAGE, backend=True)
        for wrong in ({**data, 'Architecture': 'amd64'}, {**data, 'Id': 'sha256:' + '0' * 64},
                      {**data, 'Config': {'Labels': {}}}):
            with self.assertRaises(ValueError): plan.validate_image(wrong, IMAGE, backend=True)

    def test_17_any_running_consumer_of_restored_volumes_blocks_prepare(self):
        state, review, _ = fixture()
        with patch.object(plan.restore, 'run', return_value=CompletedProcess([], 0, stdout=b'')) as run:
            plan.assert_no_running_volume_users(review, state['project'])
            self.assertEqual(run.call_count, 3)
            self.assertTrue(all('volume=' in c.args[0][-1] for c in run.call_args_list))
        with patch.object(plan.restore, 'run', return_value=CompletedProcess([], 0, stdout=b'container-id')):
            with self.assertRaises(ValueError): plan.assert_no_running_volume_users(review, state['project'])


class StartupGateTests(unittest.TestCase):
    def test_approved_cloudflare_host_is_added_without_wildcard_or_origin_change(self):
        runtime, (context, _, _) = rendered()
        env = entry.validated_environment(runtime, context, approved(context))
        self.assertEqual(json.loads(env['ALLOWED_HOSTS']),
                         ['localhost', '127.0.0.1', 'backend', 'trading.ai42.de5.net'])
        self.assertEqual(json.loads(env['ALLOWED_ORIGINS']), ['https://127.0.0.1:18088'])
        self.assertNotIn('*', json.loads(env['ALLOWED_HOSTS']))

    def test_domain_change_does_not_bypass_runtime_digest(self):
        runtime, (context, _, _) = rendered()
        changed = json.loads(runtime)
        changed['backend_environment']['ALLOWED_HOSTS'] = '["trading.ai42.de5.net"]'
        with self.assertRaises(ValueError):
            entry.validated_environment(json.dumps(changed).encode(), context, approved(context))

    def test_09_original_credentials_and_task_flags_preserved(self):
        runtime, (context, _, _) = rendered()
        env = entry.validated_environment(runtime, context, approved(context))
        original = json.loads(runtime)['backend_environment']
        for key in ('MONGODB_USERNAME', 'MONGODB_PASSWORD', 'REDIS_PASSWORD', 'JWT_SECRET', 'CSRF_SECRET',
                    'MODEL_API_KEY', 'WATCHLIST_ANALYSIS_ENABLED', 'PROXY_ENABLED', 'DEBUG'):
            self.assertEqual(env[key], original[key])
        self.assertEqual(env['MONGODB_HOST'], 'mongodb')
        self.assertEqual(env['REDIS_HOST'], 'redis')
        self.assertEqual(env['TA_MIGRATION_REVIEW'], '0')
        self.assertEqual(env['DOTENV_FILE'], '/dev/null')
        self.assertEqual(context['access_origin'], 'https://127.0.0.1:18088')
        self.assertEqual(json.loads(env['ALLOWED_ORIGINS']), ['https://127.0.0.1:18088'])
        self.assertNotIn(FAKE, json.dumps(context))

    def test_10_each_approval_required_and_not_a_truthy_value(self):
        runtime, (context, _, _) = rendered()
        for key in entry.CONFIRMATIONS:
            for value in (False, 1, 'true', None):
                approval = approved(context)
                approval[key] = value
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    entry.validated_environment(runtime, context, approval)

    def test_11_approval_bound_to_snapshot_project_origin_and_original_ttl(self):
        runtime, (context, _, _) = rendered()
        for key, value in (('snapshot_manifest_sha256', 'a' * 64), ('target_project', 'ta-live-abcdef123456'),
                           ('access_origin', 'https://127.0.0.1:8088'), ('original_ttl_enabled', False),
                           ('purpose', 'rehearsal_only'), ('schema', True)):
            approval = {**approved(context), key: value}
            with self.subTest(key=key), self.assertRaises(ValueError):
                entry.validated_environment(runtime, context, approval)

    def test_12_changed_runtime_or_credentials_refused_without_replacements(self):
        raw, (context, _, _) = rendered()
        with self.assertRaises(ValueError): entry.validated_environment(raw + b' ', context, approved(context))
        for key in ('MONGODB_PASSWORD', 'REDIS_PASSWORD', 'JWT_SECRET', 'CSRF_SECRET'):
            runtime = json.loads(raw)
            runtime['backend_environment'][key] = ''
            changed = json.dumps(runtime).encode()
            updated = {**context, 'runtime_file_sha256': hashlib.sha256(changed).hexdigest()}
            with self.subTest(key=key), self.assertRaises(ValueError):
                entry.validated_environment(changed, updated, approved(updated))

    def test_13_private_files_no_symlinks_hardlinks_or_loose_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'fake.json'
            path.write_text('{}'); path.chmod(0o600)
            self.assertEqual(entry.private_bytes(path), b'{}')
            path.chmod(0o644)
            with self.assertRaises(ValueError): entry.private_bytes(path)
            path.chmod(0o600)
            link = Path(directory) / 'alias.json'
            link.symlink_to(path)
            with self.assertRaises(OSError): entry.private_bytes(link)
            hardlink = Path(directory) / 'hard.json'
            os.link(path, hardlink)
            with self.assertRaises(ValueError): entry.private_bytes(path)
        with self.assertRaises(ValueError): entry.parse('{"schema":1,"schema":1}')

    def test_14_missing_approval_does_not_import_app_or_change_environment(self):
        raw, (context, _, _) = rendered()
        before = dict(os.environ)
        fake_uvicorn = types.SimpleNamespace(run=lambda *a, **kw: self.fail('must not start'))
        with patch.object(entry, 'private_bytes', side_effect=[raw, json.dumps(context).encode(), FileNotFoundError(FAKE)]), \
             patch.dict(sys.modules, {'uvicorn': fake_uvicorn}), contextlib.redirect_stdout(io.StringIO()) as capture:
            self.assertEqual(entry.main(), 1)
        self.assertEqual(os.environ, before)
        self.assertNotIn(FAKE, capture.getvalue())
        self.assertEqual(json.loads(capture.getvalue())['error_category'], 'FileNotFoundError')

    def test_15_approved_gate_uses_normal_lifespan_one_worker_no_business_call(self):
        raw, (context, _, _) = rendered()
        calls = []
        fake_uvicorn = types.SimpleNamespace(run=lambda *args, **kwargs: calls.append((args, kwargs)))
        old_path = list(sys.path)
        try:
            with patch.object(entry, 'private_bytes', side_effect=[raw, json.dumps(context).encode(), json.dumps(approved(context)).encode()]), \
                 patch.dict(sys.modules, {'uvicorn': fake_uvicorn}), patch.dict(os.environ, {}, clear=True):
                self.assertEqual(entry.main(), 0)
                self.assertEqual(os.environ['TA_MIGRATION_REVIEW'], '0')
            self.assertEqual(calls[0][0], ('app.main:app',))
            self.assertEqual(calls[0][1]['workers'], 1)
        finally:
            sys.path[:] = old_path


if __name__ == '__main__':
    unittest.main(verbosity=2)
