"""Encrypt/decrypt the exact sealed snapshot. Never contact SSH automatically.

Partial ciphertext/plaintext is retained in private paths on failure. Only a fully
authenticated age stream followed by snapshot verification is accepted. No real
key values, secret hashes, database records or exception bodies are printed.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile

from migration_archive import (PAYLOADS, SCHEMA, MAX_FILE, BoundedTarInfo, file_sha256,
                               member_name, restricted_directory, verify, write_json_new)

RECIPIENT = 'age13ym5nza5sy6h3ja0y4v23acfjel00x4p7sfn5ltchx74yj5uz39qd3m7tr'
IDENTITY = Path('/home/ubuntu/k-tradingagents-migration-receiver-20261016/keys/receiver.agekey')
FILES = PAYLOADS | {'checksums.json', 'COMPLETE'}


def encrypt(snapshot, output, recipient=RECIPIENT):
    verify(snapshot)
    restricted_directory(output.parent)
    if output.exists() or output.is_symlink() or output.with_suffix(output.suffix + '.transport.json').exists():
        raise ValueError('Ciphertext target already exists')
    total = sum((snapshot / name).stat().st_size for name in FILES)
    if shutil.disk_usage(output.parent).free < total + 1024**3:
        raise ValueError('Insufficient room for ciphertext plus reserve')
    partial = output.with_suffix(output.suffix + '.partial')
    process = None
    try:
        fd = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as encrypted:
            process = subprocess.Popen(['age', '--recipient', recipient], stdin=subprocess.PIPE,
                                       stdout=encrypted, stderr=subprocess.DEVNULL)
            with tarfile.open(fileobj=process.stdin, mode='w|', format=tarfile.PAX_FORMAT) as stream:
                for name in sorted(FILES):
                    stream.add(snapshot / name, arcname=name, recursive=False)
            process.stdin.close()
            if process.wait(timeout=300) != 0:
                raise RuntimeError('AgeEncryptionFailed')
            encrypted.flush()
            os.fsync(encrypted.fileno())
        # Same private directory, refuse accidental replacement of prior artifacts.
        os.link(partial, output)
        partial.unlink()  # rename-like removal of an extra ciphertext name only
        receipt = {'schema': SCHEMA, 'ciphertext_bytes': output.stat().st_size,
                   'ciphertext_sha256': file_sha256(output), 'recipient': recipient,
                   'filename': output.name}
        write_json_new(output.with_suffix(output.suffix + '.transport.json'), receipt)
        return {'status': 'encrypted', 'ciphertext_bytes': receipt['ciphertext_bytes']}
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        if process is not None and process.stdin is not None and not process.stdin.closed:
            try:
                process.stdin.close()
            except BrokenPipeError:
                pass


def decrypt(ciphertext, receipt_path, destination, identity=IDENTITY):
    from migration_archive import strict_json
    import stat
    restricted_directory(destination.parent)
    info = identity.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
        raise ValueError('Age identity must be a regular 600 file')
    receipt = strict_json(receipt_path)
    if (receipt.get('schema') != SCHEMA or receipt.get('recipient') != RECIPIENT
            or ciphertext.stat().st_size != receipt.get('ciphertext_bytes')
            or file_sha256(ciphertext) != receipt.get('ciphertext_sha256')):
        raise ValueError('Ciphertext receipt mismatch')
    # Public receipt is an integrity/transfer aid, not proof of who sent it.
    destination.mkdir(mode=0o700)
    process = subprocess.Popen(['age', '--decrypt', '--identity', str(identity), str(ciphertext)],
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    seen = set()
    completion = None
    try:
        with tarfile.open(fileobj=process.stdout, mode='r|', tarinfo=BoundedTarInfo) as stream:
            for member in stream:
                name = member_name(member)
                if not member.isfile() or name not in FILES or name in seen or member.size > MAX_FILE:
                    raise ValueError('Unexpected outer snapshot member')
                if shutil.disk_usage(destination).free < member.size + 1024**3:
                    raise ValueError('Decryption space below reserve')
                seen.add(name)
                if name == 'COMPLETE':
                    if member.size > 16384:
                        raise ValueError('Oversized completion record')
                    with stream.extractfile(member) as source:
                        completion = source.read()
                    continue  # never expose COMPLETE before age authenticates EOF
                fd = os.open(destination / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, 'wb') as target, stream.extractfile(member) as source:
                    shutil.copyfileobj(source, target, 1024 * 1024)
        # Consume through age's authenticated final chunk, including tar padding.
        trailing = 0
        while True:
            block = process.stdout.read(65536)
            if not block:
                break
            trailing += len(block)
            if trailing > 1024 * 1024 or any(block):
                raise ValueError('Unexpected data following outer archive')
        process.stdout.close()
        if process.wait(timeout=300) != 0 or seen != FILES:
            raise ValueError('Age authentication or required file set failed')
        fd = os.open(destination / 'COMPLETE', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(completion)
        result = verify(destination)
        # Sibling record: never poison the strict snapshot file set.
        write_json_new(destination.parent / (destination.name + '.accepted.json'),
                       {'schema': SCHEMA, 'age_authenticated': True, **result})
        return {'status': 'accepted', 'payload_files': len(PAYLOADS)}
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        if process.stdout is not None and not process.stdout.closed:
            process.stdout.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['encrypt', 'decrypt'])
    parser.add_argument('--window', type=Path)
    parser.add_argument('--ciphertext', type=Path, required=True)
    parser.add_argument('--new-directory', type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.action == 'encrypt':
            from migration_window import load_state
            from migration_backup import confirm_stopped
            if args.window is None or load_state(args.window)['stage'] != 'snapshot_complete_source_stopped':
                raise ValueError('Only the frozen source snapshot can be encrypted for cutover')
            confirm_stopped()
            result = encrypt(args.window / 'snapshot', args.ciphertext)
        else:
            if args.new_directory is None:
                raise ValueError('New restricted destination required')
            result = decrypt(args.ciphertext, args.ciphertext.with_suffix(args.ciphertext.suffix + '.transport.json'), args.new_directory)
        print(json.dumps(result))
    except Exception as error:
        print(json.dumps({'status': 'failed', 'error_category': type(error).__name__,
                          'partial_files_retained_for_review': True}))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
