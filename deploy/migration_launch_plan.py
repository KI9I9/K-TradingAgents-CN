"""Prepare a new, INACTIVE production specification from a final verified restore.

No start/stop/apply command exists. Never creates activation approval. Real use
requires a new final snapshot, quiesced restore, explicit source-side TTL value,
and a separately built/pinned backend image. Rehearsal restores are rejected.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import stat

import migration_restore as restore
from migration_archive import write_json_new, restricted_directory
from migration_production_entrypoint import parse, validate_origin

FRONTEND = 'sha256:61236673b74b89beb7c34dd2c1d5e893204fd989a6aab0d2f3166e5fd24c77ee'
BACKEND_LABEL = 'final-cutover-gate-https-18088-20261018-v2'


def validate_image(metadata, identifier, *, backend=False):
    if (metadata.get('Id') != identifier or metadata.get('Architecture') != 'arm64'
            or (backend and (metadata.get('Config', {}).get('Labels') or {}).get(
                'migration.launch.patch') != BACKEND_LABEL)):
        raise ValueError('Expected pinned ARM image with the approved startup gate')


def assert_no_running_volume_users(review, project):
    for alias in ('review_mongodb_data', 'review_mongodb_config', 'review_redis_data'):
        expected = project + '_' + alias
        if review.get('volumes', {}).get(alias, {}).get('name') != expected:
            raise ValueError('Restored volume identity mismatch')
        # Also checks consumers outside the restore project. IDs are not printed.
        if restore.run(['docker', 'ps', '-q', '--filter', 'volume=' + expected]).stdout.strip():
            raise ValueError('Restored volume has a running consumer')


def bind(source, target, readonly=True):
    return {'type': 'bind', 'source': str(source), 'target': target, 'read_only': readonly,
            'bind': {'create_host_path': False}}


def nginx_config(tls):
    ssl_lines = ('ssl_certificate /run/secrets/tls-cert.pem;\n'
                 '    ssl_certificate_key /run/secrets/tls-key.pem;\n'
                 '    ssl_protocols TLSv1.2 TLSv1.3;') if tls else ''
    return '''pid /tmp/nginx.pid;
error_log /dev/stderr warn;
events { worker_connections 1024; }
http {
    include /etc/nginx/mime.types;
    default_type application/octet-stream;
    access_log off;
    client_body_temp_path /tmp/client_temp;
    proxy_temp_path /tmp/proxy_temp;
    fastcgi_temp_path /tmp/fastcgi_temp;
    uwsgi_temp_path /tmp/uwsgi_temp;
    scgi_temp_path /tmp/scgi_temp;
    map $http_upgrade $connection_upgrade { default upgrade; '' close; }
    server {
        listen 8080''' + (' ssl' if tls else '') + ''';
        server_name localhost 127.0.0.1;
        ''' + ssl_lines + '''
        root /usr/share/nginx/html;
        index index.html;
        client_max_body_size 10m;
        location /api/ {
            proxy_pass http://backend:8000;
            proxy_http_version 1.1;
            proxy_set_header Host $http_host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
            proxy_buffering off;
            proxy_cache off;
            proxy_read_timeout 900s;
            proxy_send_timeout 900s;
        }
        location / { try_files $uri $uri/ /index.html; }
    }
}
'''


def validate_target_state(directory, state):
    if (state.get('kind') != 'isolated_target_restore' or state.get('directory') != str(directory)
            or state.get('purpose') != 'cutover_staging' or state.get('requires_new_final_snapshot') is not False
            or state.get('source_frozen_confirmed') is not True or state.get('restore_passed') is not True
            or state.get('stage') != 'target_stopped_retained' or state.get('target_inactive_confirmed') is not True
            or state.get('production_activation_allowed') is not False
            or not re.fullmatch(r'ta-restore-[0-9a-f]{12}', state.get('project', ''))
            or not re.fullmatch(r'[0-9a-f]{64}', state.get('manifest_sha256', ''))):
        raise ValueError('Only a final verified and stopped restore may be planned')


def render(directory, state, review, runtime_raw, output, *, backend_image, original_ttl,
           origin, tls_files=None):
    """Pure renderer. Parameters are data; does not touch files, Docker or sockets."""
    validate_target_state(directory, state)
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', backend_image) or type(original_ttl) is not bool:
        raise ValueError('Pinned backend and explicit original TTL required')
    # Runtime bytes stay in existing restricted files; the plan contains only
    # an internal integrity binding, never copies credentials into Compose.
    runtime = parse(runtime_raw)
    validate_origin(origin, runtime['backend_environment'])
    tls = origin.startswith('https://')
    if tls != bool(tls_files) or (tls and set(tls_files) != {'certificate', 'key'}):
        raise ValueError('HTTPS requires explicit protected certificate and key files')
    restore_project = state['project']
    project = restore_project.replace('ta-restore-', 'ta-live-', 1)
    context = {'schema': 1, 'kind': 'pending_cutover_launch', 'startup_mode': 'preserve_original',
               'project': project, 'snapshot_manifest_sha256': state['manifest_sha256'],
               'runtime_file_sha256': hashlib.sha256(runtime_raw).hexdigest(),
               'backend_image': backend_image, 'access_origin': origin,
               'original_ttl_enabled': original_ttl}
    base = {'profiles': ['cutover-approved'], 'platform': 'linux/arm64', 'restart': 'unless-stopped',
            'read_only': True, 'cap_drop': ['ALL'], 'security_opt': ['no-new-privileges:true'],
            'logging': {'driver': 'none'}, 'tmpfs': ['/tmp:rw,nosuid,nodev,size=128m'],
            'pids_limit': 256, 'cpus': 2, 'mem_limit': '2g'}
    approval = bind(output / 'activation-approval.json', '/run/secrets/activation-approval.json')
    services = {}
    for name, image in (('mongodb', restore.MONGO), ('redis', restore.REDIS)):
        services[name] = {**copy.deepcopy(base), 'image': image, 'networks': ['db_internal'],
            'volumes': [copy.deepcopy(approval)], 'stop_grace_period': '60s',
            'cap_add': ['CHOWN', 'SETUID', 'SETGID', 'DAC_OVERRIDE', 'FOWNER']}
    services['mongodb'].update(mem_limit='4g', command=['mongod', '--bind_ip_all', '--auth',
        '--setParameter', 'ttlMonitorEnabled=' + str(original_ttl).lower()],
        environment={'MONGO_INITDB_ROOT_USERNAME_FILE': '/run/secrets/mongo-root-username',
                     'MONGO_INITDB_ROOT_PASSWORD_FILE': '/run/secrets/mongo-root-password', 'TZ': 'Asia/Shanghai'},
        secrets=['mongo-root-username', 'mongo-root-password'])
    services['redis'].update(mem_limit='512m', cpus=1, command=['redis-server', '/run/secrets/redis.conf'],
                             secrets=['redis.conf', 'redis-users.acl'])
    volumes = {}
    for alias, target, service in (('review_mongodb_data', '/data/db', 'mongodb'),
                                   ('review_mongodb_config', '/data/configdb', 'mongodb'),
                                   ('review_redis_data', '/data', 'redis')):
        expected = restore_project + '_' + alias
        if review.get('volumes', {}).get(alias, {}).get('name') != expected:
            raise ValueError('Restored volume identity mismatch')
        volumes[alias] = {'external': True, 'name': expected}
        services[service]['volumes'].append({'type': 'volume', 'source': alias, 'target': target,
                                            'volume': {'nocopy': True}})
    backend = {**copy.deepcopy(base), 'image': backend_image, 'user': '1000:1000',
        'entrypoint': ['python', '-B', '/app/deploy/migration_production_entrypoint.py'], 'command': [],
        'networks': ['db_internal', 'web_internal', 'backend_egress'],
        'depends_on': {'mongodb': {'condition': 'service_started'}, 'redis': {'condition': 'service_started'}},
        'volumes': [copy.deepcopy(approval),
                    bind(directory / 'runtime-config.json', '/run/secrets/runtime-config.json'),
                    bind(output / 'launch-context.json', '/run/secrets/launch-context.json')]
            + [bind(directory / 'mounts' / name, target, False) for name, target in restore.MOUNTS.items()],
        'healthcheck': {'test': ['CMD', 'python', '-c',
            "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/health',timeout=4).close()"],
            'interval': '30s', 'timeout': '5s', 'retries': 5, 'start_period': '120s'}}
    frontend = {**copy.deepcopy(base), 'image': FRONTEND, 'user': '1000:1000',
        'entrypoint': ['nginx'], 'command': ['-c', '/etc/nginx/nginx.conf', '-g', 'daemon off;'],
        'networks': ['web_internal'], 'cpus': 1, 'mem_limit': '256m',
        # Do not inherit an image healthcheck aimed at the original port 80.
        'healthcheck': {'disable': True},
        'depends_on': {'backend': {'condition': 'service_healthy'}},
        # SSH forwards to the verified container address:8080 on web_internal.
        # Publishing on an internal-only network was not reachable on this host.
        # Do not add host ports or an egress network to make the frontend reachable.
        'volumes': [copy.deepcopy(approval), bind(output / 'nginx.conf', '/etc/nginx/nginx.conf')]}
    if tls:
        frontend['volumes'] += [bind(tls_files['certificate'], '/run/secrets/tls-cert.pem'),
                               bind(tls_files['key'], '/run/secrets/tls-key.pem')]
    services.update(backend=backend, frontend=frontend)
    secrets = {name: {'file': str(directory / 'secrets' / name)} for name in
               ('mongo-root-username', 'mongo-root-password', 'redis.conf', 'redis-users.acl')}
    config = {'name': project, 'services': services, 'secrets': secrets, 'volumes': volumes,
              'networks': {'db_internal': {'internal': True}, 'web_internal': {'internal': True},
                           'backend_egress': {'internal': False}}}
    return context, config, nginx_config(tls)


def prepare(directory, output, *, backend_image, original_ttl, origin, tls_files=None):
    directory, output = Path(directory).absolute(), Path(output).absolute()
    state = restore.load(directory)
    validate_target_state(directory, state)
    restricted_directory(output.parent)
    if (output.exists() or output.is_symlink() or output.parent.resolve() != output.parent
            or output.is_relative_to(directory) or output.is_relative_to(Path(state['snapshot']))):
        raise ValueError('New launch directory required')
    # Bind mounts must not make Compose create empty files or borrow a live volume.
    rows = restore.project_containers(directory, state)
    if not rows or any(c['State']['Running'] for c in rows):
        raise ValueError('Restore project must be stopped')
    restore.isolation_check(directory, state)
    if restore.run(['docker', 'ps', '-q', '--filter', 'label=ta.migration.helper=' + state['project']]).stdout.strip():
        raise ValueError('Restore helper still running')
    review = restore.private_json(directory / 'compose.json')
    assert_no_running_volume_users(review, state['project'])
    restore.checked_runtime(directory)  # Validate existing restricted runtime and unchanged credentials.
    runtime_raw = (directory / 'runtime-config.json').read_bytes()
    for image in (backend_image, FRONTEND):
        data = json.loads(restore.run(['docker', 'image', 'inspect', image]).stdout)[0]
        validate_image(data, image, backend=image == backend_image)
    if tls_files:
        for raw in tls_files.values():
            path = Path(raw).absolute()
            info = path.lstat()
            if (path.resolve() != path or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or info.st_uid != 1000 or stat.S_IMODE(info.st_mode) != 0o600):
                raise ValueError('Explicit protected TLS files readable by frontend UID required')
    context, config, nginx = render(directory, state, review, runtime_raw, output,
        backend_image=backend_image, original_ttl=original_ttl, origin=origin, tls_files=tls_files)
    output.mkdir(mode=0o700)
    write_json_new(output / 'launch-context.json', context)
    os.chown(output / 'launch-context.json', 1000, 1000)
    write_json_new(output / 'compose.production.pending.json', config)
    fd = os.open(output / 'nginx.conf', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
    with os.fdopen(fd, 'w') as stream:
        stream.write(nginx)
    (output / 'nginx.conf').chmod(0o644)  # Code/config only; no credentials.
    return {'status': 'prepared_not_activated', 'services': 4, 'approval_created': False,
            'source_changed': False, 'containers_started': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--final-restore-directory', type=Path, required=True)
    parser.add_argument('--new-launch-directory', type=Path, required=True)
    parser.add_argument('--backend-image', required=True)
    parser.add_argument('--original-ttl', choices=['enabled', 'disabled'], required=True)
    parser.add_argument('--browser-origin', required=True)
    parser.add_argument('--tls-certificate', type=Path)
    parser.add_argument('--tls-key', type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        tls = {'certificate': args.tls_certificate, 'key': args.tls_key} if (args.tls_certificate or args.tls_key) else None
        if tls and not all(tls.values()): raise ValueError('Both TLS paths required')
        result = prepare(args.final_restore_directory, args.new_launch_directory,
            backend_image=args.backend_image, original_ttl=args.original_ttl == 'enabled',
            origin=args.browser_origin, tls_files=tls)
        print(json.dumps(result))
        return 0
    except BaseException as error:
        print(json.dumps({'status': 'failed', 'error_category': type(error).__name__, 'activation_performed': False}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
