"""Normal app startup ONLY after a separately approved, snapshot-bound cutover.

This module never creates approval, changes passwords, or contacts the source.
Source-stop claims require fresh external confirmation by the cutover operator.
No production configuration is embedded in the image or in Docker environment.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

CONFIRMATIONS = ('normal_startup_approved', 'configured_outbound_approved',
                 'automatic_tasks_mcp_embedding_approved', 'source_stopped_confirmed',
                 'restore_verified')
APPROVED_ORIGIN = 'https://127.0.0.1:18088'


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate configuration field')
        result[key] = value
    return result


def private_bytes(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_uid != os.geteuid()
                or not 0 < info.st_size <= 4 * 1024 * 1024):
            raise ValueError('Restricted file required')
        return stream.read()


def parse(raw):
    return json.loads(raw, object_pairs_hook=unique_pairs)


def validate_origin(origin, environment):
    if origin != APPROVED_ORIGIN:
        raise ValueError('Only the approved loopback origin is supported')
    debug = environment.get('DEBUG', 'true').strip().lower()
    if debug not in {'true', 'false', '1', '0', 'yes', 'no', 'on', 'off'}:
        raise ValueError('Original DEBUG value is not understood')
    if debug in {'false', '0', 'no', 'off'} and not origin.startswith('https://'):
        raise ValueError('Secure session cookies require browser HTTPS; do not weaken DEBUG')


def validated_environment(runtime_raw, context, approval):
    if (not isinstance(context, dict) or type(context.get('schema')) is not int
            or context['schema'] != 1 or context.get('kind') != 'pending_cutover_launch'
            or context.get('startup_mode') != 'preserve_original'
            or not re.fullmatch(r'ta-live-[0-9a-f]{12}', context.get('project', ''))
            or not re.fullmatch(r'[0-9a-f]{64}', context.get('snapshot_manifest_sha256', ''))
            or type(context.get('original_ttl_enabled')) is not bool
            or hashlib.sha256(runtime_raw).hexdigest() != context.get('runtime_file_sha256')):
        raise ValueError('Launch context mismatch')
    expected_keys = {'schema', 'purpose', 'snapshot_manifest_sha256', 'target_project',
                     'access_origin', 'original_ttl_enabled', *CONFIRMATIONS}
    if (not isinstance(approval, dict) or set(approval) != expected_keys
            or type(approval['schema']) is not int or approval['schema'] != 1
            or approval['purpose'] != 'activate_final_snapshot'
            or any(approval[k] is not True for k in CONFIRMATIONS)
            or approval['snapshot_manifest_sha256'] != context['snapshot_manifest_sha256']
            or approval['target_project'] != context['project']
            or approval['access_origin'] != context['access_origin']
            or approval['original_ttl_enabled'] is not context['original_ttl_enabled']):
        raise ValueError('Explicit cutover approval missing or mismatched')
    runtime = parse(runtime_raw)
    env = runtime.get('backend_environment')
    credentials = runtime.get('service_credentials')
    if (type(runtime.get('schema')) is not int or runtime['schema'] != 1
            or not isinstance(env, dict) or not isinstance(credentials, dict)
            or not all(isinstance(k, str) and isinstance(v, str) and k and '=' not in k
                       and '\0' not in k + v for k, v in env.items())):
        raise ValueError('Original runtime format invalid')
    for key, field in (('MONGODB_USERNAME', 'mongodb_username'), ('MONGODB_PASSWORD', 'mongodb_password'),
                       ('REDIS_PASSWORD', 'redis_password')):
        if not env.get(key) or env[key] != credentials.get(field):
            raise ValueError('Original service credentials disagree')
    if (not env.get('JWT_SECRET') or not env.get('CSRF_SECRET')
            or env.get('MONGODB_DATABASE') != 'k_tradingagents' or env.get('REDIS_DB', '0') != '0'):
        raise ValueError('Original signing keys or database selection invalid')
    validate_origin(context['access_origin'], env)
    # Only infrastructure/access overrides, never credential replacement or
    # task/embedding/MCP preference changes. DEBUG and Secure cookies are preserved.
    result = dict(env)
    result.update({'MONGODB_HOST': 'mongodb', 'MONGODB_PORT': '27017',
                   'REDIS_HOST': 'redis', 'REDIS_PORT': '6379', 'DOTENV_FILE': '/dev/null',
                   'TA_MIGRATION_REVIEW': '0',
                   'ALLOWED_ORIGINS': json.dumps([context['access_origin']]),
                   'ALLOWED_HOSTS': json.dumps(['localhost', '127.0.0.1', 'backend', 'trading.ai42.de5.net'])})
    return result


def main():
    try:
        root = Path('/run/secrets')
        runtime = private_bytes(root / 'runtime-config.json')
        context = parse(private_bytes(root / 'launch-context.json'))
        approval = parse(private_bytes(root / 'activation-approval.json'))
        env = validated_environment(runtime, context, approval)
        os.environ.update(env)
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        # Normal lifespan is intentional and may write DBs, restore MCP and make
        # paid model/Embedding calls. Approval above explicitly covers this.
        import uvicorn
        uvicorn.run('app.main:app', host='0.0.0.0', port=8000, workers=1,
                    log_config=None, access_log=False)
        return 0
    except BaseException as error:
        print(json.dumps({'status': 'startup_refused_or_failed', 'error_category': type(error).__name__}), flush=True)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
