"""Fail-closed snapshot validation. No Docker access and no secret output.

Checksums detect damage, not sender authenticity: use age plus verified SSH.
Links/special files are rejected, not silently dropped. If found in a real
snapshot, review them before changing this policy. Extraction is never in-place.
"""
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tarfile

APP_VOLUMES = ('app_data', 'app_logs', 'app_runtime', 'app_results',
               'app_reports', 'app_uploads', 'app_config', 'redis_data')
PAYLOADS = frozenset({'.env', 'inventory.json', 'temporary-credential-files.json',
                      'runtime-config.json',
                      'snapshot-metadata.json', 'restore-dry-run.log',
                      'source.tar.gz', 'mongodb.archive.gz', 'backend-cache.tar.gz',
                      *(name + '.tar.gz' for name in APP_VOLUMES)})
SCHEMA = 3
MAX_JSON = 4 * 1024 * 1024
MAX_FILE = 16 * 1024**3
MAX_EXPANDED = 32 * 1024**3
MAX_MEMBERS = 200000


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def strict_json(path):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate JSON field')
            result[key] = value
        return result
    if not 0 < path.stat().st_size <= MAX_JSON:
        raise ValueError('Invalid JSON size')
    return json.loads(path.read_text(encoding='utf-8'), object_pairs_hook=pairs)


def restricted_directory(path):
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError('Snapshot directory must be a real 700 directory')


def validate_gzip(path, limit=MAX_EXPANDED):
    total = 0
    with gzip.open(path, 'rb') as stream:
        while True:
            block = stream.read(min(1024 * 1024, limit - total + 1))
            if not block:
                return total
            total += len(block)
            if total > limit:
                raise ValueError('Archive expansion limit exceeded')


class BoundedTarInfo(tarfile.TarInfo):
    # Bound extension headers before tarfile allocates their contents. Ordinary
    # file contents are streamed, never read into one object.
    def _proc_member(self, tar):
        if self.type in (tarfile.XHDTYPE, tarfile.XGLTYPE, tarfile.GNUTYPE_LONGNAME,
                         tarfile.GNUTYPE_LONGLINK) and self.size > 1024 * 1024:
            raise ValueError('Oversized tar extension header')
        return super()._proc_member(tar)


def member_name(member):
    raw = member.name
    if not raw or len(raw) > 4096 or '\\' in raw or any(ord(c) < 32 for c in raw):
        raise ValueError('Unsupported archive path')
    path = PurePosixPath(raw)
    if path.is_absolute() or '..' in path.parts:
        raise ValueError('Archive path escapes destination')
    name = str(path)
    if name == '.' and not member.isdir():
        raise ValueError('Archive root is not a directory')
    if not (member.isfile() or member.isdir()) or member.issparse():
        raise ValueError('Archive contains links, sparse or special files; review required')
    if member.mode & 0o7000 or member.size < 0 or member.size > MAX_FILE:
        raise ValueError('Unsupported archive mode or size')
    if member.uid < 0 or member.gid < 0 or member.uid > 2**31 - 1 or member.gid > 2**31 - 1:
        raise ValueError('Unsupported archive ownership')
    return name


def validate_tar(path, limit=MAX_EXPANDED, max_members=MAX_MEMBERS):
    validate_gzip(path, limit)
    seen, required_directories = {}, set()
    total = 0
    with tarfile.open(path, 'r|gz', tarinfo=BoundedTarInfo) as archive:
        for member in archive:
            name = member_name(member)
            if name in seen or len(seen) >= max_members:
                raise ValueError('Duplicate member or member limit exceeded')
            is_dir = member.isdir()
            if name in required_directories and not is_dir:
                raise ValueError('Archive file conflicts with directory')
            for parent in PurePosixPath(name).parents:
                parent = str(parent)
                if parent in seen and not seen[parent]:
                    raise ValueError('Archive parent is not a directory')
                required_directories.add(parent)
            seen[name] = is_dir
            total += member.size
            if total > limit:
                raise ValueError('Archive expansion limit exceeded')
    return {'members': len(seen), 'file_bytes': total}


def verify(destination, *, require_complete=True):
    destination = Path(destination)
    restricted_directory(destination)
    expected = PAYLOADS | {'checksums.json'} | ({'COMPLETE'} if require_complete else set())
    if {p.name for p in destination.iterdir()} != expected:
        raise ValueError('Snapshot file set differs from schema')
    for path in destination.iterdir():
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
            raise ValueError('Snapshot files must be unlinked regular 600 files')
    manifest = strict_json(destination / 'checksums.json')
    if not isinstance(manifest, dict) or set(manifest) != {'schema', 'files'} or manifest['schema'] != SCHEMA:
        raise ValueError('Unsupported manifest schema')
    if not isinstance(manifest['files'], dict) or set(manifest['files']) != PAYLOADS:
        raise ValueError('Required snapshot files missing or unexpected')
    if require_complete:
        complete = strict_json(destination / 'COMPLETE')
        if complete != {'schema': SCHEMA, 'manifest_sha256': file_sha256(destination / 'checksums.json')}:
            raise ValueError('Missing or inconsistent completion record')
    metadata = strict_json(destination / 'snapshot-metadata.json')
    if (not isinstance(metadata, dict) or metadata.get('schema') != 1
            or metadata.get('writers_stopped') is not True
            or metadata.get('ttl_monitor_enabled') is not False
            or not isinstance(metadata.get('before'), list)
            or not metadata['before'] or metadata['before'] != metadata.get('after')):
        raise ValueError('Frozen source metadata is missing or inconsistent')
    runtime = strict_json(destination / 'runtime-config.json')
    from migration_credentials import validate
    if not isinstance(runtime, dict) or set(runtime) != {'schema', 'backend_environment', 'service_credentials'} or runtime['schema'] != 1:
        raise ValueError('Runtime configuration missing or invalid')
    validate(runtime['service_credentials'])
    if not isinstance(runtime['backend_environment'], dict) or not runtime['backend_environment'] or not all(
            isinstance(k, str) and isinstance(v, str) and '\0' not in k + v and '=' not in k
            for k, v in runtime['backend_environment'].items()):
        raise ValueError('Runtime environment invalid')
    for name, item in manifest['files'].items():
        if (not isinstance(item, dict) or set(item) != {'bytes', 'sha256'}
                or type(item['bytes']) is not int or item['bytes'] <= 0
                or not isinstance(item['sha256'], str)
                or not re.fullmatch('[0-9a-f]{64}', item['sha256'])):
            raise ValueError('Invalid manifest entry')
        path = destination / name
        if path.stat().st_size != item['bytes'] or file_sha256(path) != item['sha256']:
            raise ValueError('Snapshot checksum mismatch')
        if name.endswith('.tar.gz'):
            validate_tar(path)
        elif name.endswith('.gz'):
            validate_gzip(path)
    return {'status': 'verified', 'payload_files': len(PAYLOADS), 'schema': SCHEMA}


def write_json_new(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=True, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def seal(destination):
    destination = Path(destination)
    restricted_directory(destination)
    if {p.name for p in destination.iterdir()} != PAYLOADS:
        raise ValueError('Cannot complete an incomplete snapshot')
    for path in destination.iterdir():
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
            raise ValueError('Snapshot files must be regular 600 files without hardlinks')
    files = {name: {'bytes': (destination / name).stat().st_size,
                    'sha256': file_sha256(destination / name)} for name in sorted(PAYLOADS)}
    write_json_new(destination / 'checksums.json', {'schema': SCHEMA, 'files': files})
    verify(destination, require_complete=False)
    write_json_new(destination / 'COMPLETE', {
        'schema': SCHEMA, 'manifest_sha256': file_sha256(destination / 'checksums.json')})
    return verify(destination)


def extract_new(path, destination, *, preserve_owner=False):
    """Extract one validated tar into a NEW directory under a private parent.

    Do not point at application volumes or running deployments. Root ownership
    preservation is explicit. On failure leave the private partial directory for
    inspection; never overwrite, follow links, or automatically delete it.
    """
    path, destination = Path(path), Path(destination)
    restricted_directory(destination.parent)
    if destination.parent.resolve() != destination.parent.absolute():
        raise ValueError('Extraction parent contains symlinks')
    if preserve_owner and os.geteuid() != 0:
        raise ValueError('Ownership preservation requires root')
    archive_info = validate_tar(path)
    if shutil.disk_usage(destination.parent).free < archive_info['file_bytes'] + 1024 * 1024:
        raise ValueError('Insufficient free space for staged extraction')
    destination.mkdir(mode=0o700)
    directories = []
    with tarfile.open(path, 'r|gz', tarinfo=BoundedTarInfo) as archive:
        for member in archive:
            name = member_name(member)
            target = destination / name
            target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            if member.isdir():
                target.mkdir(mode=0o700, exist_ok=True)
                directories.append((target, member))
                continue
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with archive.extractfile(member) as source, os.fdopen(fd, 'wb') as output:
                shutil.copyfileobj(source, output, 1024 * 1024)
            if preserve_owner:
                os.chown(target, member.uid, member.gid)
            os.chmod(target, member.mode & 0o777)
            os.utime(target, (member.mtime, member.mtime))
    for target, member in sorted(directories, key=lambda item: len(item[0].parts), reverse=True):
        if target == destination:
            continue  # keep the sensitive staging root 700 and owned by caller
        if preserve_owner:
            os.chown(target, member.uid, member.gid)
        os.chmod(target, member.mode & 0o777)
        os.utime(target, (member.mtime, member.mtime))
