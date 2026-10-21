"""Fresh isolated target restore, never production activation or source recovery.

Explicit approval + source-frozen confirmation OR rehearsal-only purpose are mandatory. No archive is
restored in-place. Failure stops only this newly recorded project, retains files
and volumes, never restarts WSL or clears queues. No secret/record/error body is
printed. Runtime environment comes only from the authenticated restricted bundle.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import secrets
import shutil
import stat
import subprocess
import tarfile

from migration_archive import (APP_VOLUMES, BoundedTarInfo, SCHEMA, extract_new,
    file_sha256, restricted_directory, strict_json, validate_gzip, validate_tar,
    verify, write_json_new)
from migration_credentials import write_files
from migration_window import save_state
from migration_backup import MONGO_BASELINE_SCRIPT
from migration_redis_history import parse_terminal_exception

TOOLS = Path(__file__).resolve().parent
MONGO = 'mongo@sha256:65e9d974435d072874cf08922f26ecdf7d45b54662ce632509750b5c23ba5c5f'
REDIS = 'redis@sha256:1f09a89a207d794a8c61d9edfc26e7c58427de10ccef7c5d18d638df79a63b85'
BACKEND = 'sha256:f3e12357a61d9be7f591d944d9c52a578689397a2632876b2ee89493017f818b'
MOUNTS = {name: '/app/' + name.removeprefix('app_') for name in APP_VOLUMES if name != 'redis_data'}
MOUNTS['backend-cache'] = '/app/tradingagents/dataflows/cache'


def run(argv, **kwargs):
    return subprocess.run(argv, check=True, capture_output=not any(k in kwargs for k in ('stdout', 'stderr')),
                          timeout=kwargs.pop('timeout', 60), **kwargs)


def private_json(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
        raise ValueError('Private input must be a regular unlinked 600 file')
    return strict_json(path)


def load(directory):
    directory = Path(directory).absolute()
    restricted_directory(directory)
    if directory.resolve() != directory or ',' in str(directory):
        raise ValueError('Unsupported work path')
    state = private_json(directory / 'window-state.json')
    if state.get('schema') != 1 or state.get('kind') != 'isolated_target_restore' or state.get('directory') != str(directory):
        raise ValueError('Invalid target journal')
    if not state.get('project', '').startswith('ta-restore-'):
        raise ValueError('Unexpected target project')
    return state


def record(directory, state, stage):
    state['stage'] = stage
    save_state(directory, state)
    print(json.dumps({'stage': stage}), flush=True)


def compose(directory, state, *args, **kwargs):
    return run(['docker', 'compose', '-p', state['project'], '-f', str(directory / 'compose.json'), *args], **kwargs)


def project_containers(directory, state):
    ids = run(['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project=' + state['project']]).stdout.decode().split()
    records = []
    for identifier in ids:
        info = json.loads(run(['docker', 'inspect', identifier]).stdout)[0]
        labels = info['Config'].get('Labels') or {}
        if (labels.get('com.docker.compose.project') != state['project'] or
                labels.get('com.docker.compose.project.config_files') != str(directory / 'compose.json')):
            raise ValueError('Project container identity mismatch')
        records.append(info)
    return records


def container(directory, state, service):
    records = [item for item in project_containers(directory, state)
               if item['Config']['Labels'].get('com.docker.compose.service') == service]
    if len(records) != 1:
        raise ValueError('Expected one target service')
    return records[0]['Id']


def stop_project(directory, state):
    # No down, rm or volume deletion for real restores. Labels + exact config
    # path bind the stop to this journal's new project, including partial startup.
    records = project_containers(directory, state)
    for item in records:
        if item['State']['Running']:
            run(['docker', 'stop', '--time', '20', item['Id']], timeout=45)
    return not any(item['State']['Running'] for item in project_containers(directory, state))


def checked_runtime(snapshot):
    value = private_json(snapshot / 'runtime-config.json')
    environment, credentials = value['backend_environment'], value['service_credentials']
    for key, field in (('MONGODB_USERNAME', 'mongodb_username'), ('MONGODB_PASSWORD', 'mongodb_password'),
                       ('REDIS_PASSWORD', 'redis_password')):
        if environment.get(key) != credentials[field]:
            raise ValueError('Original runtime credentials disagree')
    if not all(environment.get(key) for key in ('JWT_SECRET', 'CSRF_SECRET')):
        raise ValueError('Original signing secrets missing')
    if environment.get('MONGODB_DATABASE') != 'k_tradingagents' or environment.get('REDIS_DB', '0') != '0':
        raise ValueError('Unexpected source database selection')
    return value


def assert_accepted(snapshot):
    verify(snapshot)
    acceptance = private_json(snapshot.parent / (snapshot.name + '.accepted.json'))
    if acceptance.get('age_authenticated') is not True or acceptance.get('schema') != SCHEMA or acceptance.get('status') != 'verified':
        raise ValueError('Authenticated age acceptance is required')


def restore_tree(archive, destination):
    """Preserve volume-root metadata INSIDE a 700 host-only staging parent.

    Generic archive extraction intentionally keeps its root 700; a runtime bind
    mount must instead retain the archived root UID/GID/mode for the app user.
    No source archive or existing runtime directory is changed.
    """
    roots = []
    with tarfile.open(archive, 'r|gz', tarinfo=BoundedTarInfo) as stream:
        for member in stream:
            if member.name.rstrip('/') in ('.', '') and member.isdir():
                roots.append(member)
    if len(roots) != 1:
        raise ValueError('Archive root metadata missing or duplicated; do not guess ownership')
    extract_new(archive, destination, preserve_owner=True)
    root = roots[0]
    os.chown(destination, root.uid, root.gid)
    os.chmod(destination, root.mode & 0o777)
    os.utime(destination, (root.mtime, root.mtime))
    count = 0
    expected = set()
    with tarfile.open(archive, 'r|gz', tarinfo=BoundedTarInfo) as stream:
        for member in stream:
            name = str(Path(member.name))
            expected.add(name)
            path = destination / name
            info = path.lstat()
            if (info.st_uid != member.uid or info.st_gid != member.gid or
                    stat.S_IMODE(info.st_mode) != member.mode & 0o777):
                raise ValueError('Restored file metadata differs')
            if member.isfile():
                if not stat.S_ISREG(info.st_mode) or info.st_size != member.size:
                    raise ValueError('Restored file type or size differs')
                with stream.extractfile(member) as source, path.open('rb') as target:
                    while True:
                        block = source.read(1024 * 1024)
                        if target.read(len(block) if block else 1) != block:
                            raise ValueError('Restored file contents differ')
                        if not block:
                            break
                count += 1
            elif not stat.S_ISDIR(info.st_mode):
                raise ValueError('Restored directory type differs')
    actual = {'.'} | {str(path.relative_to(destination)) for path in destination.rglob('*')}
    if actual != expected:
        raise ValueError('Restored directory member set differs')
    return {'files': count, 'contents_and_ownership_equal': True}


def render_compose(directory, project):
    config = json.loads(run(['docker', 'compose', '-p', project, '-f', str(TOOLS / 'compose.restore-review.yaml'),
                             'config', '--format', 'json'],
                            env={**os.environ, 'MIGRATION_SECRET_DIR': str(directory / 'secrets')}).stdout)
    config['name'] = project
    for name, volume in config['volumes'].items():
        volume['name'] = project + '_' + name
    config['networks']['review_internal']['name'] = project + '_review_internal'
    cold = copy.deepcopy(config['services']['redis'])
    cold['volumes'] = [{'type': 'bind', 'source': str(directory / 'mounts/redis_data'), 'target': '/data'}]
    config['services']['redis-cold'] = cold
    config['services']['backend'] = {
        'image': BACKEND, 'profiles': ['review'], 'restart': 'no', 'user': '1000:1000',
        'read_only': True, 'networks': ['review_internal'], 'security_opt': ['no-new-privileges:true'],
        'cap_drop': ['ALL'], 'mem_limit': '2g', 'cpus': 2, 'pids_limit': 256,
        'logging': {'driver': 'none'}, 'tmpfs': ['/tmp:rw,nosuid,nodev,size=128m'],
        'volumes': [{'type': 'bind', 'source': str(directory / 'runtime-config.json'),
                     'target': '/run/secrets/runtime-config.json', 'read_only': True}] +
                   [{'type': 'bind', 'source': str(directory / 'mounts' / name), 'target': target}
                    for name, target in MOUNTS.items()]}
    # Database diagnostics may contain operation details; never persist raw logs
    # when processing a real backup. The coordinator keeps sanitized stage reports.
    for service in config['services'].values():
        service['logging'] = {'driver': 'none'}
    return config


CLIENT = r'''
import json, sys, time, urllib.request, urllib.error
from pathlib import Path
from pymongo import MongoClient
from redis import Redis
sys.path.insert(0, '/restore-tools')
from migration_redis_history import copy_terminal_history, mongo_terminal_status, parse_terminal_exception
v=json.loads(Path('/runtime-config.json').read_text())['service_credentials']
c=MongoClient('mongodb',username=v['mongodb_username'],password=v['mongodb_password'],authSource='admin',
              serverSelectionTimeoutMS=1000,connectTimeoutMS=1000,socketTimeoutMS=5000)
src=Redis(host='redis-cold',password=v['redis_password'],socket_timeout=3,socket_connect_timeout=3)
dst=Redis(host='redis',password=v['redis_password'],socket_timeout=3,socket_connect_timeout=3)
try:
    mode=sys.argv[1]
    if mode=='ready':
        for _ in range(45):
            try: c.admin.command('ping'); src.ping(); dst.ping(); break
            except Exception: time.sleep(1)
        else: raise TimeoutError('DatabaseReadiness')
        if c.k_tradingagents.list_collection_names() or dst.dbsize(): raise ValueError('FreshTargetRequired')
        print(json.dumps({'fresh_databases_ready':True}))
    elif mode=='redis':
        exception=None
        policy_path=Path('/terminal-exception.json')
        if policy_path.exists():
            context=json.loads(policy_path.read_text())
            exception=parse_terminal_exception(context['policy'],context['manifest_sha256'],
                rehearsal_only=context['rehearsal_only'],source_frozen=context.get('source_frozen',False))
        result=copy_terminal_history(src,dst,
            lambda task: mongo_terminal_status(c.k_tradingagents,task,exception=exception),exception=exception)
        if exception is not None:
            if mongo_terminal_status(c.k_tradingagents,exception.task_id,exception=exception)!=b'cancelled':
                raise ValueError('Historical Mongo status changed')
            result['mongo_exception_status_unchanged']=True
        print(json.dumps(result))
    elif mode=='health':
        for _ in range(60):
            try:
                with urllib.request.urlopen('http://backend:8000/api/health',timeout=2) as response: body=json.load(response)
                if body.get('mode')=='migration_review' and body.get('production_ready') is False: break
            except Exception: pass
            time.sleep(1)
        else: raise TimeoutError('ReviewReadiness')
        try: urllib.request.urlopen('http://backend:8000/api/stocks/refresh',timeout=2)
        except urllib.error.HTTPError as error:
            if error.code != 423: raise ValueError('GuardNotActive')
        else: raise ValueError('GuardNotActive')
        print(json.dumps({'limited_review_ready':True,'production_ready':False,'side_effect_route_blocked':True}))
    else: raise ValueError('UnsupportedClientMode')
except Exception as error:
    print(json.dumps({'status':'failed','error_category':type(error).__name__}))
    raise SystemExit(1)
'''


def client(directory, state, mode):
    name = state['project'] + '-client-' + secrets.token_hex(3)
    flags = ['docker', 'run', '--rm', '-i', '--name', name, '--label', 'ta.migration.helper=' + state['project'],
        '--network', state['project'] + '_review_internal', '--user', '1000:1000', '--read-only',
        '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true', '--memory', '512m', '--cpus', '1',
        '--pids-limit', '128', '--log-driver', 'none',
        '--mount', f'type=bind,src={directory / "runtime-config.json"},dst=/runtime-config.json,readonly',
        '--mount', f'type=bind,src={directory / "client-tools"},dst=/restore-tools,readonly']
    if state.get('terminal_exception_approved'):
        flags += ['--mount', f'type=bind,src={directory / "terminal-exception.json"},dst=/terminal-exception.json,readonly']
    flags += ['--entrypoint', 'python', BACKEND, '-B', '-', mode]
    try:
        result = run(flags, input=CLIENT.encode(), timeout=180)
        return json.loads(result.stdout)
    finally:
        # Only this random, stateless helper; no user volumes are removed.
        subprocess.run(['docker', 'rm', '-f', name], capture_output=True, timeout=30)


def target_baseline(directory, state):
    script = MONGO_BASELINE_SCRIPT.replace("process.env.MONGO_INITDB_ROOT_USERNAME",
        "require('fs').readFileSync('/run/secrets/mongo-root-username','utf8')").replace(
        "process.env.MONGO_INITDB_ROOT_PASSWORD", "require('fs').readFileSync('/run/secrets/mongo-root-password','utf8')")
    result = run(['docker', 'exec', '-i', container(directory, state, 'mongodb'), 'mongosh',
                  '--nodb', '--quiet', '--file', '/dev/stdin'], input=script.encode(), timeout=300)
    entries = [line[len('TA_MIGRATION_BASELINE='):] for line in result.stdout.decode().splitlines()
               if line.startswith('TA_MIGRATION_BASELINE=')]
    if len(entries) != 1:
        raise ValueError('Target baseline unavailable')
    return json.loads(entries[0])


def isolation_check(directory, state):
    network = state['project'] + '_review_internal'
    info = json.loads(run(['docker', 'network', 'inspect', network]).stdout)[0]
    if info['Internal'] is not True:
        raise ValueError('Restore network is not internal')
    for item in project_containers(directory, state):
        if item['HostConfig']['PortBindings'] or set(item['NetworkSettings']['Networks']) != {network}:
            raise ValueError('Unexpected exposed port or network')
        if not item['HostConfig']['ReadonlyRootfs'] or 'no-new-privileges:true' not in item['HostConfig']['SecurityOpt']:
            raise ValueError('Restore runtime isolation differs')
    return True


def restore(snapshot, directory, *, approved=False, source_frozen=False, rehearsal_only=False,
            approved_terminal_exception=None):
    if not approved or not (source_frozen or rehearsal_only) or (source_frozen and rehearsal_only):
        raise ValueError('Approval and exactly one of frozen-source or rehearsal-only purpose required')
    if os.geteuid() != 0:
        raise ValueError('Root needed to preserve numeric file ownership')
    snapshot, directory = Path(snapshot).absolute(), Path(directory).absolute()
    restricted_directory(directory.parent)
    if (directory.exists() or directory.is_symlink() or directory.parent.resolve() != directory.parent
            or snapshot.resolve() != snapshot or directory.is_relative_to(snapshot) or ',' in str(directory)):
        raise ValueError('Use a new private target outside the authenticated snapshot')
    assert_accepted(snapshot)
    manifest_sha256 = file_sha256(snapshot / 'checksums.json')
    exception_policy = None
    if approved_terminal_exception is not None:
        exception_policy = private_json(Path(approved_terminal_exception))
        parse_terminal_exception(exception_policy, manifest_sha256,
                                 rehearsal_only=rehearsal_only, source_frozen=source_frozen)
    runtime = checked_runtime(snapshot)
    for image in (MONGO, REDIS, BACKEND):
        metadata = json.loads(run(['docker', 'image', 'inspect', image]).stdout)[0]
        if metadata['Architecture'] != 'arm64':
            raise ValueError('Expected existing native ARM64 image')
    sizes = {name: validate_tar(snapshot / (name + '.tar.gz'))['file_bytes']
             for name in (*MOUNTS, 'redis_data')}
    required = sum(sizes.values()) * 2 + validate_gzip(snapshot / 'mongodb.archive.gz') * 4 + 5 * 1024**3
    if shutil.disk_usage(directory.parent).free < required:
        raise ValueError('Target capacity below conservative restore budget')
    directory.mkdir(mode=0o700)
    state = {'schema': 1, 'kind': 'isolated_target_restore', 'directory': str(directory),
        'project': 'ta-restore-' + secrets.token_hex(6), 'snapshot': str(snapshot),
        'manifest_sha256': manifest_sha256, 'restore_passed': False,
        'source_frozen_confirmed': source_frozen, 'purpose': 'rehearsal_only' if rehearsal_only else 'cutover_staging',
        'requires_new_final_snapshot': rehearsal_only, 'production_activation_allowed': False,
        'target_capacity_budget_bytes': required,
        'terminal_exception_approved': exception_policy is not None}
    started = False
    try:
        record(directory, state, 'restore_files')
        (directory / 'mounts').mkdir(mode=0o700)
        state['file_checks'] = {name: restore_tree(snapshot / (name + '.tar.gz'), directory / 'mounts' / name)
                                for name in (*MOUNTS, 'redis_data')}
        write_files(runtime['service_credentials'], directory / 'secrets')
        write_json_new(directory / 'runtime-config.json', runtime)
        os.chown(directory / 'runtime-config.json', 1000, 1000)
        if exception_policy is not None:
            write_json_new(directory / 'terminal-exception.json', {'policy': exception_policy,
                'manifest_sha256': manifest_sha256, 'rehearsal_only': rehearsal_only,
                'source_frozen': source_frozen})
            os.chown(directory / 'terminal-exception.json', 1000, 1000)
        for image, account, names in ((MONGO, 'mongodb', ('mongo-root-username', 'mongo-root-password', 'mongo-tools.json')),
                                     (REDIS, 'redis', ('redis.conf', 'redis-users.acl'))):
            ids = [int(run(['docker', 'run', '--rm', '--network', 'none', '--read-only', '--entrypoint', 'id', image, flag, account]).stdout)
                   for flag in ('-u', '-g')]
            for name in names:
                os.chown(directory / 'secrets' / name, *ids)
        # Code-only read-only mount, accessible to the nonroot client UID; no keys.
        (directory / 'client-tools').mkdir(mode=0o755)
        (directory / 'client-tools').chmod(0o755)  # undo private umask for this code-only mount
        shutil.copyfile(TOOLS / 'migration_redis_history.py', directory / 'client-tools/migration_redis_history.py')
        (directory / 'client-tools/migration_redis_history.py').chmod(0o644)
        write_json_new(directory / 'compose.json', render_compose(directory, state['project']))
        record(directory, state, 'start_isolated_databases')
        started = True  # journal precedes up; interruption may leave partial startup
        compose(directory, state, 'up', '-d', '--no-build', '--pull', 'never', 'mongodb', 'redis', 'redis-cold', timeout=120)
        isolation_check(directory, state)
        client(directory, state, 'ready')
        record(directory, state, 'restore_mongodb')
        with (snapshot / 'mongodb.archive.gz').open('rb') as stream:
            run(['docker', 'exec', '-i', container(directory, state, 'mongodb'), 'mongorestore',
                 '--config=/run/secrets/mongo-tools.json', '--nsInclude=k_tradingagents.*',
                 '--archive', '--gzip', '--stopOnError'], stdin=stream, timeout=1200)
        baseline = target_baseline(directory, state)
        expected = strict_json(snapshot / 'snapshot-metadata.json')['before']
        if baseline != expected:
            raise ValueError('Mongo collections/views/indexes/counts do not match frozen source')
        state['mongo'] = {'collections_and_views': len(baseline), 'baseline_equal': True, 'ttl_paused': True}
        record(directory, state, 'restore_redis_terminal_history')
        state['redis'] = client(directory, state, 'redis')
        run(['docker', 'stop', '--time', '20', container(directory, state, 'redis-cold')], timeout=45)
        if file_sha256(snapshot / 'checksums.json') != state['manifest_sha256']:
            raise ValueError('Snapshot manifest changed during restore')
        state['restore_passed'] = True
        record(directory, state, 'restored_inactive')
        return {'status': 'restored_inactive', 'file_scopes': len(state['file_checks']),
                'files_equal': True, 'mongo': state['mongo'], 'redis': state['redis'],
                'backend_started': False, 'production_ready': False,
                'requires_new_final_snapshot': rehearsal_only}
    except BaseException as error:
        state['failed_stage'] = state.get('stage')
        state['error_category'] = type(error).__name__
        try:
            state['target_inactive_confirmed'] = stop_project(directory, state) if started else True
        except Exception:
            state['target_inactive_confirmed'] = False
        record(directory, state, 'failed_retained_for_review')
        raise


def review(directory, *, approved=False):
    if not approved:
        raise ValueError('Explicit limited review start approval required')
    directory = Path(directory).absolute()
    state = load(directory)
    if state['stage'] != 'restored_inactive' or not state['restore_passed']:
        raise ValueError('Cannot start review before complete restore verification')
    if target_baseline(directory, state) != strict_json(Path(state['snapshot']) / 'snapshot-metadata.json')['before']:
        raise ValueError('Target changed before review start')
    try:
        record(directory, state, 'review_starting')
        compose(directory, state, '--profile', 'review', 'up', '-d', '--no-deps', '--no-build', '--pull', 'never', 'backend', timeout=90)
        isolation_check(directory, state)
        result = client(directory, state, 'health')
        record(directory, state, 'limited_review_running')
        return result
    except BaseException as error:
        state['error_category'] = type(error).__name__
        try:
            state['target_inactive_confirmed'] = stop_project(directory, state)
        except Exception:
            state['target_inactive_confirmed'] = False
        record(directory, state, 'review_failed_retained')
        raise


def quiesce(directory, *, approved=False):
    if not approved:
        raise ValueError('Explicit target stop approval required')
    directory = Path(directory).absolute()
    state = load(directory)
    inactive = stop_project(directory, state)
    state['target_inactive_confirmed'] = inactive
    record(directory, state, 'target_stopped_retained' if inactive else 'target_stop_failed')
    if not inactive:
        raise ValueError('Target inactivity not confirmed')
    return {'target_inactive_confirmed': True, 'files_and_volumes_retained': True,
            'source_restart_performed': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('restore', 'review', 'quiesce', 'status'))
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--snapshot', type=Path)
    parser.add_argument('--approved-isolated-restore', action='store_true')
    parser.add_argument('--source-frozen-confirmed', action='store_true')
    parser.add_argument('--snapshot-for-rehearsal-only', action='store_true')
    parser.add_argument('--approved-limited-review', action='store_true')
    parser.add_argument('--approved-target-stop', action='store_true')
    parser.add_argument('--approved-terminal-exception', type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.action == 'restore':
            if args.snapshot is None:
                raise ValueError('Snapshot is required')
            result = restore(args.snapshot, args.directory, approved=args.approved_isolated_restore,
                             source_frozen=args.source_frozen_confirmed, rehearsal_only=args.snapshot_for_rehearsal_only,
                             approved_terminal_exception=args.approved_terminal_exception)
        elif args.action == 'review':
            result = review(args.directory, approved=args.approved_limited_review)
        elif args.action == 'quiesce':
            result = quiesce(args.directory, approved=args.approved_target_stop)
        else:
            state = load(args.directory)
            result = {key: state[key] for key in ('stage', 'restore_passed')}
        print(json.dumps(result))
        return 0
    except BaseException as error:
        print(json.dumps({'status': 'failed', 'error_category': type(error).__name__,
                          'action': args.action, 'no_source_restart': True, 'partial_data_retained': True}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
