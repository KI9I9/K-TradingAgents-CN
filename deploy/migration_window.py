"""Explicit source window coordinator. Prepare is read-only to the deployment.

snapshot requires an approved outage and drained/external-writer confirmations.
It keeps local writers stopped on SUCCESS AND FAILURE. Recovery is a separate
command, requiring confirmation that the target cannot write. Never auto-failover.
Only fixed localhost DB operations, never secret values in argv or diagnostics.
"""
import argparse
import json
import math
import os
from pathlib import Path
import shutil
import stat
import subprocess
import time
import uuid

import migration_backup as backup
from migration_archive import restricted_directory, strict_json, write_json_new

COMMIT = 'd3ca3ec561c5800bcfbdfb2c7537601a0972289c'
GIB = 1024**3
PROJECT = backup.PROJECT


class CapacityError(ValueError):
    def __init__(self, budget):
        super().__init__('Physical capacity below conservative snapshot budget')
        self.budget = budget

AUTH_JS = r'''
const username = process.env.MONGO_INITDB_ROOT_USERNAME;
const password = process.env.MONGO_INITDB_ROOT_PASSWORD;
if (!username || !password) process.exit(2);
const connection = new Mongo('mongodb://' + encodeURIComponent(username) + ':' +
  encodeURIComponent(password) + '@127.0.0.1:27017/?authSource=admin');
const admin = connection.getDB('admin');
'''


def tagged_mongo(body):
    script = 'try {\n' + AUTH_JS + body + r'''
process.stdout.write('TA_WINDOW=' + JSON.stringify(result) + '\n');
process.exit(0);
} catch (_) { process.exit(3); }
'''
    result = backup.run(['docker', 'exec', '--user', '0', '-i', f'{PROJECT}-mongodb-1',
                         'mongosh', '--nodb', '--quiet', '--file', '/dev/stdin'],
                        input=script, capture_output=True, text=True, timeout=60)
    lines = [line[len('TA_WINDOW='):] for line in result.stdout.splitlines() if line.startswith('TA_WINDOW=')]
    if len(lines) != 1:
        raise ValueError('Mongo operation did not return one tagged result')
    return json.loads(lines[0])


def ttl_state(desired=None):
    body = ''
    if desired is not None:
        if type(desired) is not bool:
            raise ValueError('TTL value must be boolean')
        body = 'if (admin.runCommand({setParameter:1,ttlMonitorEnabled:' + str(desired).lower() + '}).ok !== 1) process.exit(4);\n'
    body += 'const response = admin.runCommand({getParameter:1,ttlMonitorEnabled:1});\n'
    body += 'if (response.ok !== 1) process.exit(5); const result={enabled:response.ttlMonitorEnabled};\n'
    result = tagged_mongo(body)
    if type(result.get('enabled')) is not bool or (desired is not None and result['enabled'] is not desired):
        raise ValueError('TTL readback failed')
    return result['enabled']


def identities():
    result = {}
    for service in ('mongodb', 'redis', 'backend', 'frontend'):
        info = json.loads(backup.output(['docker', 'inspect', f'{PROJECT}-{service}-1']))[0]
        labels = info['Config'].get('Labels') or {}
        if labels.get('com.docker.compose.project') != PROJECT or labels.get('com.docker.compose.service') != service:
            raise ValueError('Source identity mismatch')
        result[service] = {'id': info['Id'], 'image_id': info['Image'],
                           'running': info['State']['Running'], 'started_at': info['State']['StartedAt']}
    return result


def assert_same_containers(original, current):
    if set(original) != set(current):
        raise ValueError('Source service set changed')
    for service in original:
        if any(original[service][field] != current[service][field] for field in ('id', 'image_id')):
            raise ValueError('Source containers recreated; manual recovery review required')


def runtime_configuration():
    """Only called inside an approved snapshot; the result is never printed."""
    environments = {}
    for service in ('mongodb', 'redis', 'backend'):
        info = json.loads(backup.output(['docker', 'inspect', f'{PROJECT}-{service}-1']))[0]
        values = {}
        for item in info['Config'].get('Env', []):
            key, value = item.split('=', 1)
            if key in values:
                raise ValueError('Duplicate runtime environment name')
            values[key] = value
        environments[service] = values
    mongo, redis, app = (environments[name] for name in ('mongodb', 'redis', 'backend'))
    credentials = {'mongodb_username': mongo.get('MONGO_INITDB_ROOT_USERNAME'),
                   'mongodb_password': mongo.get('MONGO_INITDB_ROOT_PASSWORD'),
                   'redis_password': redis.get('REDIS_PASSWORD')}
    for key, environment_key in (('mongodb_username', 'MONGODB_USERNAME'),
                                  ('mongodb_password', 'MONGODB_PASSWORD'), ('redis_password', 'REDIS_PASSWORD')):
        if not credentials[key] or app.get(environment_key) != credentials[key]:
            raise ValueError('Runtime credential source mismatch')
    if not app.get('JWT_SECRET') or not app.get('CSRF_SECRET'):
        raise ValueError('Original signing secrets absent; do not generate replacements')
    if app.get('MONGODB_DATABASE') != 'k_tradingagents' or app.get('REDIS_DB', '0') != '0':
        raise ValueError('Unreviewed database selection')
    return {'schema': 1, 'backend_environment': app, 'service_credentials': credentials}


ACTIVITY_CODE = r'''
import json, os
from pymongo import MongoClient
from redis import Redis
try:
    env=os.environ
    c=MongoClient(env['MONGODB_HOST'],int(env.get('MONGODB_PORT','27017')),
        username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],
        authSource=env.get('MONGODB_AUTH_SOURCE','admin'),serverSelectionTimeoutMS=3000,socketTimeoutMS=5000)
    r=Redis(host=env['REDIS_HOST'],port=int(env.get('REDIS_PORT','6379')),password=env['REDIS_PASSWORD'],
        db=int(env.get('REDIS_DB','0')),socket_timeout=3,socket_connect_timeout=3)
    statuses=['queued','pending','running','processing','cancelling','scheduled']
    names=['analysis_tasks','unified_analysis_tasks','stock_sync_tasks','favorites_sync_tasks','scheduler_executions']
    counts={name:c[env['MONGODB_DATABASE']][name].count_documents({'status':{'$in':statuses}}) for name in names}
    queues={'ready':r.llen('qa:ready'),'processing':r.scard('qa:processing')}
    print(json.dumps({'active_tasks':counts,'queues':queues,'drained':not any([*counts.values(),*queues.values()])}))
except Exception as error:
    print(json.dumps({'error_category':type(error).__name__}))
    raise SystemExit(1)
'''


def activity():
    result = backup.run(['docker', 'exec', '-i', f'{PROJECT}-backend-1', 'python', '-B', '-'],
                        input=ACTIVITY_CODE, capture_output=True, text=True, timeout=45)
    value = json.loads(result.stdout)
    if type(value.get('drained')) is not bool:
        raise ValueError('Activity check did not return expected status')
    return value


SIZE_CODE = r'''
import json, os, stat
paths = ['/app/data','/app/logs','/app/runtime','/app/results','/app/reports','/app/uploads','/app/config',
         '/app/tradingagents/dataflows/cache']
total = count = 0
for root in paths:
    if not os.path.isdir(root) or os.path.islink(root): raise ValueError('Unsupported root')
    for directory, dirs, files in os.walk(root, followlinks=False, onerror=lambda error: (_ for _ in ()).throw(error)):
        for name in dirs + files:
            info = os.lstat(os.path.join(directory, name)); count += 1
            if count > 200000: raise ValueError('Size review required')
            if stat.S_ISREG(info.st_mode): total += info.st_size
print(json.dumps({'bytes':total, 'entries':count}))
'''


def estimate_bytes(root):
    stats = tagged_mongo("const s=connection.getDB('k_tradingagents').stats(1); if(s.ok!==1)process.exit(6); const result={bytes:s.dataSize};")
    files = json.loads(backup.run(['docker', 'exec', '--user', '0', '-i', f'{PROJECT}-backend-1',
                                   'python', '-B', '-'], input=SIZE_CODE, capture_output=True,
                                  text=True, timeout=90).stdout)
    # Redis data is a small cold backup; du apparent bytes via the existing backend
    # image in an isolated read-only helper, never create a misspelled source volume.
    volume = f'{PROJECT}_redis_data'
    backup.output(['docker', 'volume', 'inspect', volume])
    redis_size = int(backup.output(['docker', 'run', '--rm', '--network', 'none', '--read-only',
        '--cap-drop', 'ALL', '--cap-add', 'DAC_OVERRIDE', '--user', '0', '--pids-limit', '64',
        '--memory', '256m', '--security-opt', 'no-new-privileges:true',
        '--mount', f'type=volume,src={volume},dst=/volume,readonly,volume-nocopy',
        '--entrypoint', 'du', identities()['backend']['image_id'], '-sb', '/volume']).split()[0])
    names = backup.run(['git', '-C', str(root), 'ls-files', '-z', '--cached', '--others', '--exclude-standard'],
                       capture_output=True, timeout=30).stdout.decode().split('\0')
    source_size = 0
    for name in set(names) - {''}:
        path = Path(name)
        if path.is_absolute() or '..' in path.parts:
            raise ValueError('Invalid source path')
        info = (root / path).lstat()
        if stat.S_ISREG(info.st_mode):
            source_size += info.st_size
    raw = int(stats['bytes']) + int(files['bytes']) + redis_size + source_size
    # Compression cannot be relied on. Includes metadata/tar overhead and growth.
    archive_bound = math.ceil(raw * 1.5) + 256 * 1024**2
    return {'observed_logical_bytes': raw, 'archive_budget_bytes': archive_bound,
            'required_wsl_host_free_bytes': archive_bound * 2 + 5 * GIB,
            'required_docker_host_free_bytes': 5 * GIB}


def capacity_check(root, destination_parent):
    if not destination_parent.is_dir() or root.stat().st_dev != destination_parent.stat().st_dev:
        raise ValueError('Backup must be on the reviewed WSL filesystem, not a Windows-mounted directory')
    executable = shutil.which('powershell.exe')
    fallback = Path('/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe')
    if not executable and fallback.is_file():
        executable = str(fallback)
    if not executable:
        raise ValueError('Cannot verify Windows physical free space; no source changes made')
    script = "[Console]::OutputEncoding=[System.Text.UTF8Encoding]::new(); $r=@{}; foreach($d in @('C','D')){$r[$d]=[long](Get-PSDrive -Name $d -ErrorAction Stop).Free}; $r|ConvertTo-Json -Compress"
    free = json.loads(backup.run([executable, '-NoProfile', '-NonInteractive', '-Command', script],
                                 capture_output=True, text=True, timeout=20).stdout.lstrip('\ufeff'))
    budget = estimate_bytes(root)
    budget['windows_free_bytes'] = free
    budget['wsl_filesystem_free_bytes'] = shutil.disk_usage(destination_parent).free
    # C/D placement is from the user's verified source report, not inferred from
    # the WSL virtual maximum. Re-review if the VHD locations are ever changed.
    if min(free['D'], budget['wsl_filesystem_free_bytes']) < budget['required_wsl_host_free_bytes'] or free['C'] < budget['required_docker_host_free_bytes']:
        raise CapacityError(budget)
    return budget


def save_state(directory, state):
    temporary = directory / ('state-' + uuid.uuid4().hex + '.tmp')
    write_json_new(temporary, state)
    os.replace(temporary, directory / 'window-state.json')
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def prepare(root, directory, *, report_stage=lambda stage: None):
    report_stage('paths_and_permissions')
    root, directory = Path(root).resolve(), Path(directory).absolute()
    if not directory.parent.is_dir() or directory.exists() or directory.is_symlink():
        raise ValueError('Use a new window directory under an existing private parent')
    restricted_directory(directory.parent)
    if directory.parent.resolve() != directory.parent or directory.is_relative_to(root):
        raise ValueError('Window must be outside source checkout, with no symlink parent')
    env_info = (root / '.env').lstat()
    if not stat.S_ISREG(env_info.st_mode) or stat.S_IMODE(env_info.st_mode) != 0o600:
        raise ValueError('Original .env must be a regular 600 file')
    report_stage('source_baseline')
    inventory = backup.inspect(root)
    if inventory['git_commit'] != COMMIT or inventory['git_status']:
        raise ValueError('Source baseline changed; review before snapshot')
    source = identities()
    if not all(value['running'] for value in source.values()):
        raise ValueError('Expected four running source services')
    report_stage('physical_capacity')
    budget = capacity_check(root, directory.parent)
    report_stage('read_original_ttl')
    original_ttl = ttl_state()
    report_stage('write_private_preparation_record')
    directory.mkdir(mode=0o700)
    save_state(directory, {'schema': 1, 'stage': 'prepared', 'root': str(root), 'source': source,
        'original_ttl': original_ttl, 'budget': budget, 'prepared_at': time.time(),
        'ttl_change_attempted': False, 'snapshot_complete': False})
    return {'status': 'prepared', 'capacity': budget, 'ttl_will_be_temporarily_paused': original_ttl}


def load_state(directory):
    restricted_directory(directory)
    path = directory / 'window-state.json'
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
        raise ValueError('Invalid window state file')
    state = strict_json(path)
    if state.get('schema') != 1 or type(state.get('original_ttl')) is not bool:
        raise ValueError('Invalid window state schema')
    return state


def snapshot(directory, *, approved=False, drained=False, external_writers_isolated=False):
    if not (approved and drained and external_writers_isolated):
        raise ValueError('Approved outage, drained tasks and external writer isolation required')
    state = load_state(directory)
    if state['stage'] != 'prepared':
        raise ValueError('Window already attempted; inspect and recover, do not repeat')
    root = Path(state['root'])
    assert_same_containers(state['source'], identities())
    inventory = backup.inspect(root)
    if inventory['git_commit'] != COMMIT or inventory['git_status']:
        raise ValueError('Source baseline changed')
    state['budget'] = capacity_check(root, directory.parent)
    if ttl_state() is not state['original_ttl']:
        raise ValueError('Source TTL changed since preparation')
    if not activity()['drained']:
        raise ValueError('Active work exists; no TTL change or outage started')
    state.update(stage='ttl_pause_attempted', ttl_change_attempted=True)
    save_state(directory, state)  # journal before mutation, including timeout ambiguity
    try:
        ttl_state(False)
        state['stage'] = 'snapshot_running'
        save_state(directory, state)
        backup.backup(root, directory / 'snapshot', inventory, success_state='keep-stopped',
                      failure_state='keep-stopped', write_freeze_confirmed=True,
                      runtime_config=runtime_configuration())
        state.update(stage='snapshot_complete_source_stopped', snapshot_complete=True)
        save_state(directory, state)
        return {'status': 'snapshot_complete', 'source_writers_stopped': True, 'source_ttl_paused': True}
    except BaseException:
        state['stage'] = 'failed_manual_recovery_required'
        save_state(directory, state)
        raise


def rollback(directory, *, target_inactive_confirmed=False, approved=False):
    if not (target_inactive_confirmed and approved):
        raise ValueError('Explicit rollback approval and inactive target required')
    state = load_state(directory)
    if state['stage'] not in {'ttl_pause_attempted', 'snapshot_running', 'failed_manual_recovery_required',
                              'snapshot_complete_source_stopped', 'rollback_started'}:
        raise ValueError('No interrupted snapshot to recover')
    assert_same_containers(state['source'], identities())
    state['stage'] = 'rollback_started'
    save_state(directory, state)
    # Restore the exact ORIGINAL value, not an assumed true. On readback failure
    # do not resume application writers. Idempotent after interruption.
    ttl_state(state['original_ttl'])
    for service in ('redis', 'backend', 'frontend'):
        if state['source'][service]['running']:
            backup.run(['docker', 'start', state['source'][service]['id']], timeout=60)
    current = identities()
    if not all(current[service]['running'] == state['source'][service]['running'] for service in current):
        raise ValueError('Container recovery incomplete; inspect without leaking logs')
    state['stage'] = 'rolled_back_snapshot_not_for_cutover'
    save_state(directory, state)
    return {'status': 'source_restarted', 'ttl_original_value_restored': True,
            'container_health_still_requires_check': True, 'new_final_snapshot_required': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'snapshot', 'rollback', 'status', 'activity'])
    parser.add_argument('--window', type=Path, required=True)
    parser.add_argument('--root', type=Path, default=Path('/home/marvin/project/K-TradingAgents-CN'))
    parser.add_argument('--approved-outage', action='store_true')
    parser.add_argument('--tasks-drained', action='store_true')
    parser.add_argument('--external-writers-isolated', action='store_true')
    parser.add_argument('--approved-rollback', action='store_true')
    parser.add_argument('--target-inactive-confirmed', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.action == 'activity':
            result = activity()
        elif args.action == 'prepare':
            result = prepare(args.root, args.window)
        elif args.action == 'snapshot':
            result = snapshot(args.window, approved=args.approved_outage, drained=args.tasks_drained,
                              external_writers_isolated=args.external_writers_isolated)
        elif args.action == 'rollback':
            result = rollback(args.window, approved=args.approved_rollback,
                              target_inactive_confirmed=args.target_inactive_confirmed)
        else:
            state = load_state(args.window)
            result = {key: state[key] for key in ('stage', 'snapshot_complete', 'original_ttl')}
        print(json.dumps(result))
    except BaseException as error:
        print(json.dumps({'status': 'failed', 'error_category': type(error).__name__,
                          'action': args.action, 'no_automatic_source_restart': True}))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
