"""Render existing service credentials to restricted files; never generate replacements.

This preparation utility is not a backup/cutover command. The input must contain
resolved runtime values collected into a restricted JSON file, not shell-expanded
arguments or a naively parsed .env. Do not run on real data before the snapshot plan.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
from urllib.parse import quote


def validate(values):
    if not isinstance(values, dict) or set(values) != {'mongodb_username', 'mongodb_password', 'redis_password'}:
        raise ValueError('Unexpected credential field set')
    for value in values.values():
        if not isinstance(value, str) or not value or '\0' in value:
            raise ValueError('Credential value missing or unsupported')


def mongo_tools_config(values):
    validate(values)
    # JSON is a YAML subset accepted by MongoDB Database Tools --config. Password is
    # a separate supported config field; it never appears in the URI or process argv.
    return json.dumps({
        'uri': 'mongodb://' + quote(values['mongodb_username'], safe='') + '@127.0.0.1:27017/?authSource=admin',
        'password': values['mongodb_password'],
    }, ensure_ascii=True) + '\n'


def redis_acl(values):
    validate(values)
    # Redis ACL SHA-256 preserves the existing password, including quote/$/Unicode
    # characters, without putting plaintext in redis-server arguments or config.
    digest = hashlib.sha256(values['redis_password'].encode('utf-8')).hexdigest()
    return 'user default on #' + digest + ' ~* &* +@all\n'


def command_for_mongo(operation, config_path):
    """Only application namespace; database users in admin are never exported/restored."""
    config_path = str(config_path)
    if operation == 'dump':
        return ['mongodump', '--config=' + config_path, '--db=k_tradingagents', '--archive', '--gzip']
    if operation == 'restore-dry-run':
        return ['mongorestore', '--config=' + config_path, '--nsInclude=k_tradingagents.*', '--archive', '--gzip', '--dryRun']
    raise ValueError('Unsupported MongoDB operation')


def write_files(values, destination):
    validate(values)
    if any(values[key].endswith('\n') for key in ('mongodb_username', 'mongodb_password')):
        raise ValueError('Mongo *_FILE bootstrap cannot preserve a trailing newline')
    destination = Path(destination)
    # Use a newly created directory and exclusive files. No replacements or chmod
    # of user-owned existing targets; reject symlinks/existing paths.
    destination.mkdir(mode=0o700)
    destination.chmod(0o700)
    contents = {
        'mongo-tools.json': mongo_tools_config(values),
        # Used by the official Mongo image's *_FILE bootstrap. No newline added.
        # Shell command substitution in that entrypoint trims trailing newlines;
        # reject that case below rather than silently changing a supplied value.
        'mongo-root-username': values['mongodb_username'],
        'mongo-root-password': values['mongodb_password'],
        'redis-users.acl': redis_acl(values),
        'redis.conf': 'appendonly yes\naclfile /run/secrets/redis-users.acl\n',
    }
    for name, content in contents.items():
        fd = os.open(destination / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            stream.write(content)
    return tuple(contents)


def read_restricted_input(path):
    path = Path(path)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600 or info.st_uid != os.getuid():
        raise ValueError('Input must be an owned regular 600 file')
    parent = path.parent.lstat()
    if not stat.S_ISDIR(parent.st_mode) or stat.S_IMODE(parent.st_mode) != 0o700 or parent.st_uid != os.getuid():
        raise ValueError('Input parent must be an owned 700 directory')
    if not 0 < info.st_size <= 65536:
        raise ValueError('Input size invalid')
    return json.loads(path.read_text(encoding='utf-8'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-file', type=Path, required=True)
    parser.add_argument('--new-output-directory', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        files = write_files(read_restricted_input(args.input_file), args.new_output_directory)
    except Exception as error:
        # No exception body, input object, hash, username, or password in output.
        print(json.dumps({'status': 'failed', 'error_category': type(error).__name__}))
        raise SystemExit(1)
    print(json.dumps({'status': 'success', 'files_created': len(files), 'mode': '600'}))
