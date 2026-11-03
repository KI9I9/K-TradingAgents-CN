"""Explicit finite renewal inside the accepted backend; never creates orders.

The caller supplies a frozen absolute expiry and the exact previous hash. A
failed quote acceptance restores the previous bytes, without extending them.
An unexpected replacement is preserved for investigation. This is not a timer
and never automatically renews a permit.
"""
from contextlib import redirect_stdout
import fcntl
import hashlib
import io
import json
import math
import os
from pathlib import Path
import stat
import sys
import types
import uuid

PERMIT = Path('/app/runtime/paper-quote-supply.json')


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def current(path, max_bytes=4096):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and not info.st_mode & 0o077,
                'permit_not_private_regular_file')
        raw = stream.read(max_bytes + 1)
    require(len(raw) <= max_bytes, 'permit_too_large')
    return raw


def write_new(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    require(current(path, max_bytes=len(raw)) == raw, 'backup_readback_failed')
    sync_directory(path.parent)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def replace_owned(path, before, after):
    """Caller holds the operator lock; refuse a changed file before replacement."""
    temporary = path.with_name('.paper-renewal-' + uuid.uuid4().hex)
    try:
        write_new(temporary, after)
        require(current(path) == before, 'permit_changed_do_not_replace')
        os.replace(temporary, path)
        sync_directory(path.parent)
        require(current(path) == after, 'permit_readback_failed')
    finally:
        if temporary.exists():
            temporary.unlink()


def protected(snapshot):
    result = dict(snapshot)
    result.pop('modules', None)
    result['runtime'] = dict(result['runtime'])
    result['runtime'].pop(str(PERMIT), None)
    result['collections'] = {k: {key: value for key, value in row.items()
                                if key != 'raw_sha256'}
                             for k, row in result['collections'].items()}
    return result


def snapshot(payload, backup=None):
    data = dict(runtime=payload['runtime'], modules=payload['modules'],
                backup=str(backup) if backup else None)
    original = sys.stdin; output = io.StringIO()
    try:
        sys.stdin = io.StringIO(json.dumps(data))
        with redirect_stdout(output):
            exec(compile(payload['snapshot_code'], '<renewal-snapshot>', 'exec'),
                 {'__name__': 'renewal_snapshot'})
    finally:
        sys.stdin = original
    rows = [s.split(':', 1)[1] for s in output.getvalue().splitlines()
            if s.startswith('ETF_SNAPSHOT:')]
    require(len(rows) == 1, 'snapshot_missing')
    return json.loads(rows[0])


def accept_quotes(payload, now):
    helper = types.ModuleType('renewal_api')
    exec(payload['query_code'], helper.__dict__)
    auth = payload['auth']
    if 'token' in auth:
        token = auth['token']
    else:
        login = helper.request('/api/auth/login', body=auth)
        require(login.get('success') is True, 'login_failed')
        token = login['data']['access_token']
    validation = dict(require=require, math=math)
    exec(payload['validate_code'], validation)
    results = []
    for code, market in [('600000', 'CN'), ('159865', 'ETF')]:
        response = helper.request('/api/paper/quote/' + code + '?market=' + market, token=token)
        results.append(validation['validate_quote'](response, code, now()))
    return results


def renew(payload, path=PERMIT):
    from app.services.paper_quote_supply import cn_now, read_permit, in_continuous_session
    from app.services.quote_contract import parse_quote_time

    result = dict(ok=False, status='not_started', orders_created=0)
    old = new = None; attempted = False; lock = None
    try:
        require(os.geteuid() == 1000, 'backend_uid_changed')
        lock = os.open(path.with_suffix('.renewal.lock'), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        require(stat.S_ISREG(os.fstat(lock).st_mode) and not os.fstat(lock).st_mode & 0o077,
                'renewal_lock_invalid')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        now = cn_now()
        end = parse_quote_time(payload['expires_at'])
        require(end is not None and 0 < (end - now).total_seconds() <= 7 * 86400,
                'renewal_expiry_invalid')
        old = current(path)
        require(digest(old) == payload['previous_sha256'], 'previous_permit_changed')
        previous = json.loads(old)
        require(read_permit(now, path) == previous, 'previous_permit_not_active')
        require(end > parse_quote_time(previous['expires_at']), 'renewal_does_not_extend')
        backup = Path(payload['backup'])
        require(not os.path.lexists(backup), 'renewal_attempt_already_exists')
        before = snapshot(payload)
        require(before['runtime'].get(str(PERMIT)) == digest(old), 'snapshot_permit_mismatch')
        if payload.get('check_only') is True:
            return dict(result, ok=True, status='ready_no_renewal',
                        previous_sha256=digest(old), expires_at=end.isoformat())
        require(in_continuous_session(now), 'outside_continuous_session')
        require(now.hour * 60 + now.minute < (685 if now.hour < 12 else 895),
                'too_close_to_session_end')
        backup.mkdir(mode=0o700)
        write_new(backup / 'previous-permit.json', old)
        before = snapshot(payload, backup / 'protected')
        write_new(backup / 'before.json', encoded(before))
        new = encoded(dict(version=2, enabled=True, source='sina_l1', scope='paper_execution_sh_sz',
                           not_before=now.isoformat(), expires_at=end.isoformat(),
                           activation_id=uuid.uuid4().hex, renewal_of_sha256=digest(old)))
        write_new(backup / 'candidate-permit.json', new)
        # Set before replace: a directory-fsync failure may occur after publication.
        attempted = True
        replace_owned(path, old, new)
        require(read_permit(cn_now(), path) == json.loads(new), 'renewed_permit_invalid')
        quotes = accept_quotes(payload, cn_now)
        after = snapshot(payload)
        require(protected(before) == protected(after), 'protected_state_changed')
        require(current(path) == new and read_permit(cn_now(), path) == json.loads(new)
                and in_continuous_session(cn_now()), 'permit_or_session_changed')
        result.update(ok=True, status='renewed', previous_sha256=digest(old),
                      permit_sha256=digest(new), expires_at=end.isoformat(),
                      quotes=quotes, protected_state_unchanged=True, backup=str(backup))
        write_new(backup / 'completion.json', encoded(result))
    except BaseException as error:
        result.update(ok=False, status='failed_before_renewal',
                      reason=str(error) if type(error) is RuntimeError else type(error).__name__)
        if attempted:
            try:
                value = current(path)
                if value == new:
                    replace_owned(path, new, old)
                else:
                    require(value == old, 'unexpected_permit_preserved')
                result.update(status='previous_permit_restored', previous_sha256=digest(old))
            except BaseException as rollback_error:
                result.update(status='rollback_requires_attention',
                              rollback_reason=str(rollback_error) if type(rollback_error) is RuntimeError
                              else type(rollback_error).__name__)
    finally:
        if lock is not None:
            os.close(lock)
    return result


if __name__ == '__main__':
    import signal
    os.umask(0o077)
    def expired(*_):
        raise TimeoutError('renewal_deadline')
    signal.signal(signal.SIGALRM, expired); signal.alarm(110)
    report = renew(json.load(sys.stdin))
    signal.alarm(0)
    print('PAPER_RENEWAL_RESULT:' + json.dumps(report))
    raise SystemExit(0 if report['ok'] else 1)
