"""SYNTHETIC ONLY: new fixtures, fake age key, same actual restore coordinator.

Creates no business backup and never opens the Osaka receiver identity. Deletes
only the random fixture projects it creates; private fake files/reports remain.
"""
import copy
import io
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tarfile
import tempfile
from unittest.mock import patch

import migration_restore as restore
import migration_transport as transport
from migration_archive import APP_VOLUMES, PAYLOADS, seal, write_json_new, file_sha256
from migration_credentials import write_files
import migration_backup as backup

ROOT = Path('/home/ubuntu/tradingagents-arm64-build-20261016')


SEED = r'''
import datetime, json, time
from pathlib import Path
from bson import ObjectId
from pymongo import MongoClient
from redis import Redis
import bcrypt
values=json.loads(Path('/runtime-config.json').read_text())['service_credentials']
c=MongoClient('mongodb',username=values['mongodb_username'],password=values['mongodb_password'],authSource='admin',serverSelectionTimeoutMS=1000)
r=Redis(host='redis',password=values['redis_password'],socket_timeout=3)
for _ in range(45):
    try: c.admin.command('ping'); r.ping(); break
    except Exception: time.sleep(1)
else: raise TimeoutError('SyntheticSourceNotReady')
db=c.k_tradingagents
if db.list_collection_names() or r.dbsize(): raise ValueError('NotEmpty')
now=datetime.datetime.now(datetime.timezone.utc)
db.users.insert_one({'_id':ObjectId('0123456789abcdef01234567'),'username':'pipeline-fixture',
    'email':'pipeline@example.invalid','hashed_password':bcrypt.hashpw(b'fixture-pass-only',bcrypt.gensalt()).decode(),
    'is_active':True,'is_verified':True,'is_admin':False,'created_at':now,'updated_at':now,'preferences':{}})
db.users.create_index('username',unique=True)
db.user_sessions.insert_one({'_id':'expired-fixture','expires_at':datetime.datetime(2000,1,1)})
db.user_sessions.create_index('expires_at',expireAfterSeconds=0)
for number in range(5):
    task='pipeline-task-'+str(number)
    status='completed' if number<4 else 'failed'
    collection='analysis_tasks' if number%2==0 else 'unified_analysis_tasks'
    # Deliberately give Mongo _id a different value to catch the old assumption.
    db[collection].insert_one({'_id':'not-the-task-id-'+str(number),'task_id':task,'status':status,'opaque':'原样保留'})
    r.hset('qa:task:'+task,mapping={'status':status,'extra':b'\x00\xfffixture','id':task,'result':'合成内容'})
    r.sadd('qa:'+status,task)
db.create_collection('completed_view',viewOn='analysis_tasks',pipeline=[{'$match':{'status':'completed'}}])
r.set('fixture:transient','not-replayed',ex=600)
print(json.dumps({'seeded':True}))
'''

VERIFY_LOGIN = r'''
import json, urllib.request, urllib.error
from pathlib import Path
from pymongo import MongoClient
from redis import Redis
import bcrypt, jwt
runtime=json.loads(Path('/runtime-config.json').read_text())
v=runtime['service_credentials']; env=runtime['backend_environment']
c=MongoClient('mongodb',username=v['mongodb_username'],password=v['mongodb_password'],authSource='admin',serverSelectionTimeoutMS=2000)
db=c.k_tradingagents
r=Redis(host='redis',password=v['redis_password'],socket_timeout=3)
checks={}
user=db.users.find_one({'username':'pipeline-fixture'})
original_hash=user['hashed_password']
checks['original_password_valid']=bcrypt.checkpw(b'fixture-pass-only',original_hash.encode())
checks['expired_session_preserved_during_review']=db.user_sessions.count_documents({'_id':'expired-fixture'})==1
checks['mongo_original_document_fields_preserved']=all(row['opaque']=='原样保留' for name in ('analysis_tasks','unified_analysis_tasks') for row in db[name].find())
checks['redis_five_terminal_hashes_and_two_sets']=r.dbsize()==7
checks['redis_binary_and_extra_fields_preserved']=all(r.hget('qa:task:pipeline-task-'+str(i),'extra')==b'\x00\xfffixture' for i in range(5))
checks['transient_key_and_queues_not_replayed']=not r.exists('fixture:transient','qa:ready','qa:processing')
q=urllib.request.Request('http://backend:8000/api/auth/login',data=json.dumps({'username':'pipeline-fixture','password':'fixture-pass-only'}).encode(),headers={'Content-Type':'application/json'})
with urllib.request.urlopen(q,timeout=10) as response: body=json.load(response)
checks['restored_account_login_succeeds']=body.get('success') is True
token=body['data']['access_token']
jwt.decode(token,env['JWT_SECRET'],algorithms=['HS256'])
checks['original_signing_secret_preserved']=True
q=urllib.request.Request('http://backend:8000/api/auth/me',headers={'Authorization':'Bearer '+token})
with urllib.request.urlopen(q,timeout=10) as response: checks['restored_identity_route_succeeds']=response.status==200
checks['password_hash_not_changed_by_login']=db.users.find_one({'username':'pipeline-fixture'})['hashed_password']==original_hash
checks['target_ttl_still_paused']=c.admin.command({'getParameter':1,'ttlMonitorEnabled':1})['ttlMonitorEnabled'] is False
print(json.dumps({'checks':checks,'all_passed':all(checks.values())}))
if not all(checks.values()): raise SystemExit(1)
'''


def helper(directory, state, code):
    name = state['project'] + '-synthetic-client'
    try:
        result = restore.run(['docker', 'run', '--rm', '-i', '--name', name,
            '--network', state['project'] + '_review_internal', '--read-only', '--user', '1000:1000',
            '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true', '--memory', '512m', '--pids-limit', '128',
            '--log-driver', 'none', '--mount', f'type=bind,src={directory / "runtime-config.json"},dst=/runtime-config.json,readonly',
            '--entrypoint', 'python', restore.BACKEND, '-B', '-'], input=code.encode(), timeout=180)
        return json.loads(result.stdout)
    finally:
        subprocess.run(['docker', 'rm', '-f', name], capture_output=True, timeout=30)


def secrets_for_fixture(directory, runtime):
    write_files(runtime['service_credentials'], directory / 'secrets')
    write_json_new(directory / 'runtime-config.json', runtime)
    os.chown(directory / 'runtime-config.json', 1000, 1000)
    for image, account, names in ((restore.MONGO, 'mongodb', ('mongo-root-username', 'mongo-root-password', 'mongo-tools.json')),
                                 (restore.REDIS, 'redis', ('redis.conf', 'redis-users.acl'))):
        uid, gid = [int(restore.run(['docker', 'run', '--rm', '--network', 'none', '--entrypoint', 'id', image, flag, account]).stdout)
                    for flag in ('-u', '-g')]
        for name in names:
            os.chown(directory / 'secrets' / name, uid, gid)


def fake_tree(path):
    with tarfile.open(path, 'w:gz') as stream:
        for name, kind, mode, body in (('.', tarfile.DIRTYPE, 0o755, b''),
                                      ('fixture.dat', tarfile.REGTYPE, 0o640, b'\x00\xffpreserve-me'),
                                      ('empty', tarfile.DIRTYPE, 0o750, b'')):
            member = tarfile.TarInfo(name)
            member.type, member.mode, member.uid, member.gid = kind, mode, 1000, 1000
            member.size = len(body)
            stream.addfile(member, io.BytesIO(body) if body else None)
    path.chmod(0o600)


def main():
    if os.geteuid() != 0:
        raise ValueError('Synthetic rehearsal requires root for fixture ownership')
    os.umask(0o077)
    directory = Path(tempfile.mkdtemp(prefix='pipeline-fixture-', dir=ROOT))
    source = directory / 'source'
    source.mkdir(mode=0o700)
    state = {'schema': 1, 'kind': 'isolated_target_restore', 'directory': str(source),
             'project': 'ta-restore-fixture-' + secrets.token_hex(5), 'restore_passed': False}
    restore.save_state(source, state)
    created = [source]
    cache_container = state['project'] + '-backend-1'
    report = {'real_data_used': False, 'real_age_identity_read': False, 'all_passed': False, 'checks': {},
              'directory': str(directory)}
    step = 'synthetic_source_setup'
    try:
        credentials = {'mongodb_username': 'pipeline-fixture', 'mongodb_password': secrets.token_hex(24), 'redis_password': secrets.token_hex(24)}
        runtime = {'schema': 1, 'service_credentials': credentials, 'backend_environment': {
            'MONGODB_USERNAME': credentials['mongodb_username'], 'MONGODB_PASSWORD': credentials['mongodb_password'],
            'REDIS_PASSWORD': credentials['redis_password'], 'MONGODB_HOST': 'mongodb', 'MONGODB_PORT': '27017',
            'MONGODB_DATABASE': 'k_tradingagents', 'MONGODB_AUTH_SOURCE': 'admin', 'REDIS_HOST': 'redis',
            'REDIS_PORT': '6379', 'REDIS_DB': '0', 'JWT_SECRET': secrets.token_hex(32), 'CSRF_SECRET': secrets.token_hex(32),
            'DEBUG': 'false', 'ALLOWED_HOSTS': '["*"]'}}
        secrets_for_fixture(source, runtime)
        config = restore.render_compose(source, state['project'])
        config['services'].pop('backend'); config['services'].pop('redis-cold')
        write_json_new(source / 'compose.json', config)
        print(json.dumps({'stage': step}), flush=True)
        restore.compose(source, state, 'up', '-d', '--pull', 'never', '--no-build', timeout=120)
        helper(source, state, SEED)
        restore.isolation_check(source, state)
        # Validate the last-moment active-task gate against a real mongosh process.
        connection_code = SEED.split('db=c.k_tradingagents')[0] + '\ndb=c.k_tradingagents\n'
        helper(source, state, connection_code + "db.analysis_tasks.insert_one({'_id':'fixture-active','task_id':'fixture-active','status':'processing'}); print('{}')")
        try:
            restore.target_baseline(source, state)
        except subprocess.CalledProcessError as error:
            report['checks']['active_task_blocks_frozen_baseline'] = error.returncode == 7
        else:
            raise ValueError('ActiveTaskGateFailed')
        helper(source, state, connection_code + "db.analysis_tasks.delete_one({'_id':'fixture-active'}); print('{}')")
        snapshot = directory / 'synthetic-snapshot'
        snapshot.mkdir(mode=0o700)
        write_json_new(snapshot / 'runtime-config.json', runtime)
        (snapshot / '.env').write_text('SYNTHETIC_ONLY=1\n')
        write_json_new(snapshot / 'inventory.json', {'fixture_only': True})
        write_json_new(snapshot / 'temporary-credential-files.json', {'fixture_only': True})
        (snapshot / 'restore-dry-run.log').write_text('Synthetic fixture, not a business backup.\n')
        fake_tree(snapshot / 'source.tar.gz')
        baseline = restore.target_baseline(source, state)
        write_json_new(snapshot / 'snapshot-metadata.json', {'schema': 1, 'writers_stopped': True,
            'ttl_monitor_enabled': False, 'before': baseline, 'after': baseline})
        step = 'synthetic_database_archive'
        with (snapshot / 'mongodb.archive.gz').open('xb') as output:
            restore.run(['docker', 'exec', restore.container(source, state, 'mongodb'), 'mongodump',
                '--config=/run/secrets/mongo-tools.json', '--db=k_tradingagents', '--archive', '--gzip'],
                stdout=output, stderr=subprocess.PIPE, timeout=120)
        restore.compose(source, state, 'stop', '--timeout', '20', 'redis', timeout=45)
        with (snapshot / 'redis_data.tar.gz').open('xb') as output:
            restore.run(['docker', 'run', '--rm', '--network', 'none', '--read-only', '--user', '0',
                '--cap-drop', 'ALL', '--cap-add', 'DAC_OVERRIDE', '--security-opt', 'no-new-privileges:true',
                '--mount', f'type=volume,src={state["project"]}_review_redis_data,dst=/volume,readonly,volume-nocopy',
                '--entrypoint', 'tar', restore.BACKEND, '-czf', '-', '-C', '/volume', '.'],
                stdout=output, stderr=subprocess.PIPE, timeout=90)
        for name in APP_VOLUMES:
            if name != 'redis_data': fake_tree(snapshot / (name + '.tar.gz'))
        # Exercise the real stopped-container docker cp cache archive layout.
        cache = directory / 'cache'
        cache.mkdir(mode=0o755)
        (cache / 'fixture.dat').write_bytes(b'\0\xffcache')
        os.chown(cache, 1000, 1000); os.chown(cache / 'fixture.dat', 1000, 1000)
        # This directory also contains an importable Python package! Preserve the
        # existing image files, then add a synthetic writable-layer cache entry.
        # An empty bind mount would hide get_cache and create an invalid fixture.
        restore.run(['docker', 'create', '--name', cache_container, '--network', 'none', restore.BACKEND])
        restore.run(['docker', 'cp', '-a', str(cache / 'fixture.dat'),
                     cache_container + ':/app/tradingagents/dataflows/cache/fixture.dat'])
        with patch.object(backup, 'PROJECT', state['project']):
            backup.save_backend_cache(snapshot / 'backend-cache.tar.gz')
        for path in snapshot.iterdir(): path.chmod(0o600)
        seal(snapshot)
        step = 'fake_age_authenticated_transfer'
        identity = directory / 'fixture.agekey'
        restore.run(['age-keygen', '-o', str(identity)])
        recipient = restore.run(['age-keygen', '-y', str(identity)]).stdout.decode().strip()
        cipher = directory / 'fixture.tar.age'
        transport.encrypt(snapshot, cipher, recipient=recipient)
        accepted = directory / 'received'
        with patch.object(transport, 'RECIPIENT', recipient):
            transport.decrypt(cipher, cipher.with_suffix(cipher.suffix + '.transport.json'), accepted, identity=identity)
        report['checks']['authenticated_fake_age_snapshot_accepted'] = True
        before_hashes = {name: file_sha256(accepted / name) for name in PAYLOADS}
        step = 'integrated_restore'
        target = directory / 'target'
        created.append(target)
        result = restore.restore(accepted, target, approved=True, rehearsal_only=True)
        report['checks']['rehearsal_explicitly_requires_new_final_snapshot'] = result['requires_new_final_snapshot']
        report['checks']['nine_file_scopes_content_and_owner_equal'] = result['file_scopes'] == 9 and result['files_equal']
        report['checks']['mongo_frozen_baseline_equal'] = result['mongo']['baseline_equal']
        report['checks']['redis_terminal_history_equal'] = result['redis'] == {'tasks': 5, 'keys': 7, 'all_fields_equal': True}
        target_state = restore.load(target)
        report['checks']['no_backend_before_restore_acceptance'] = not any(
            item['Config']['Labels'].get('com.docker.compose.service') == 'backend'
            for item in restore.project_containers(target, target_state))
        step = 'review_start_and_original_account_login'
        review = restore.review(target, approved=True)
        report['checks']['limited_review_guard_active'] = review['limited_review_ready'] and review['side_effect_route_blocked']
        report['checks'].update(helper(target, target_state, VERIFY_LOGIN)['checks'])
        report['checks']['accepted_snapshot_unchanged'] = before_hashes == {name: file_sha256(accepted / name) for name in PAYLOADS}
        step = 'target_stop_without_deletion'
        stopped = restore.quiesce(target, approved=True)
        report['checks']['target_stop_confirmed_without_source_restart'] = stopped['target_inactive_confirmed'] and not stopped['source_restart_performed']
        report['checks']['target_files_retained_for_recovery'] = (target / 'mounts/app_data/fixture.dat').is_file()
        report['checks']['target_volumes_retained_for_recovery'] = bool(restore.run(['docker', 'volume', 'ls', '-q', '--filter', 'label=com.docker.compose.project=' + target_state['project']]).stdout.strip())
        step = 'injected_mid_restore_failure'
        failed = directory / 'failed-target'
        created.append(failed)
        real_client = restore.client
        def injected(*args):
            if args[-1] == 'redis': raise RuntimeError('SyntheticInjectedFailure')
            return real_client(*args)
        with patch.object(restore, 'client', side_effect=injected):
            try:
                restore.restore(accepted, failed, approved=True, rehearsal_only=True)
            except RuntimeError: pass
            else: raise ValueError('InjectedFailureNotDetected')
        failed_state = restore.load(failed)
        report['checks']['failure_stops_only_target_and_retains_data'] = (
            failed_state['stage'] == 'failed_retained_for_review' and failed_state['target_inactive_confirmed'] and
            (failed / 'mounts/app_data/fixture.dat').is_file())
        report['checks']['original_fixture_database_not_stopped_by_failure'] = any(
            item['State']['Running'] and item['Config']['Labels'].get('com.docker.compose.service') == 'mongodb'
            for item in restore.project_containers(source, state))
        report['all_passed'] = all(report['checks'].values())
    except BaseException as error:
        report.update(failed_stage=step, error_category=type(error).__name__)
        if isinstance(error, subprocess.CalledProcessError):
            raw = (error.stdout or b'') + (error.stderr or b'')
            report['diagnostic_categories'] = [key for key in ('PermissionError', 'ValueError', 'ModuleNotFoundError', 'TimeoutError',
                'Read-only file system', 'Permission denied', 'AuthenticationError', 'OperationFailure') if key.encode() in raw]
            # These are exclusively synthetic fixture diagnostics; still private.
            (directory / 'synthetic-diagnostic.txt').write_bytes(raw)
    finally:
        subprocess.run(['docker', 'rm', '-f', cache_container], capture_output=True, timeout=30)
        clean = True
        for path in reversed(created):
            if not (path / 'compose.json').is_file(): continue
            try:
                item = restore.load(path)
                restore.project_containers(path, item)  # identity gate before any removal
                restore.compose(path, item, '--profile', 'review', 'down', '--volumes', '--timeout', '20', timeout=120)
            except Exception: clean = False
        report['synthetic_projects_removed'] = clean
        if not clean: report['all_passed'] = False
        write_json_new(directory / 'result.json', report)
        print(json.dumps(report), flush=True)
    return 0 if report['all_passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
