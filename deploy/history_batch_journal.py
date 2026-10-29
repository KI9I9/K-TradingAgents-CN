"""Durable batch orchestration core; no production adapters or execution CLI.

All batches in one workspace share a nonblocking OS lock. Request authority and
per-request budget accounting belong to an explicitly supplied transport adapter,
not to a plan or this journal. Never reset uncertain requests or writes.
"""
from contextlib import contextmanager
from copy import deepcopy
from datetime import date
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import uuid

MAX_JSON = 16 * 1024 * 1024
PHASES = ('pending', 'fetching', 'fetched', 'writing', 'committed')


class BatchStopped(RuntimeError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def _require(condition, reason):
    if not condition:
        raise BatchStopped(reason)


def _hash(value):
    return isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value) is not None


def _pairs(items):
    result = {}
    for key, value in items:
        _require(key not in result, 'duplicate_json_key')
        result[key] = value
    return result


def _private(path, directory=False):
    info = path.lstat()
    _require((stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode))
             and stat.S_IMODE(info.st_mode) == (0o700 if directory else 0o600)
             and info.st_uid == os.geteuid()
             and (directory or info.st_nlink == 1), 'unsafe_journal_path')


def _sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _read(path):
    _private(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        raw = stream.read(MAX_JSON + 1)
    _require(len(raw) <= MAX_JSON, 'journal_too_large')
    try:
        return json.loads(raw, object_pairs_hook=_pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError, TypeError):
        raise BatchStopped('invalid_journal_json') from None


def _atomic(path, value):
    raw = canonical(value)
    _require(len(raw) <= MAX_JSON, 'journal_too_large')
    temp = path.parent / ('.next-' + uuid.uuid4().hex)
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        _sync_dir(path.parent)
    finally:
        if temp.exists():
            temp.unlink()


def validate_plan(plan):
    """Structural validation, not approval or proof of the supplied inventory."""
    try:
        copy = deepcopy(plan)
        expected = copy.pop('plan_sha256')
        _require(_hash(expected) and digest(copy) == expected, 'plan_hash_mismatch')
        _require(plan['schema'] == 1 and plan['mode'] == 'incremental_daily_qfq'
                 and plan['execution_authorized'] is False and plan['full_market_ready'] is False,
                 'unsupported_plan')
        items, pool = plan['items'], plan['pool']
        _require(isinstance(pool, list) and pool and pool == sorted(set(pool))
                 and isinstance(items, list) and [i['code'] for i in items] == pool,
                 'invalid_plan_pool')
        _require(plan['blocked_count'] == 0 and plan['candidate_count'] == len(pool)
                 and plan['total_requests'] == 2 * len(pool), 'blocked_plan')
        _require(isinstance(plan['quota'], dict) and plan['quota'].get('evidence_ref'),
                 'quota_unverified')
        for item in items:
            _require(re.fullmatch(r'\d{6}\.(SH|SZ|BJ)', item['code']) is not None
                     and item['status'] == 'candidate' and item['reason'] is None
                     and item['request_cost'] == 2 and _hash(item['observation_sha256'])
                     and item['end'] == plan['end']
                     and 0 <= (date.fromisoformat(item['end'])-date.fromisoformat(item['start'])).days < 30,
                     'invalid_plan_item')
        return expected
    except (KeyError, ValueError, TypeError, AttributeError):
        raise BatchStopped('invalid_plan') from None


class Workspace:
    def __init__(self, directory):
        self.path = Path(directory)
        _require(self.path.is_absolute() and self.path.resolve() == self.path, 'canonical_path_required')
        self._held = False

    def provision(self):
        """Explicit local operation. Existing directories are never reset."""
        self.path.mkdir(mode=0o700)
        fd = os.open(self.path/'workspace.lock', os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        _sync_dir(self.path)
        _sync_dir(self.path.parent)

    @contextmanager
    def locked(self):
        _require(not self._held, 'busy')
        _private(self.path, directory=True)
        _private(self.path/'workspace.lock')
        fd = os.open(self.path/'workspace.lock', os.O_RDWR | os.O_NOFOLLOW)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise BatchStopped('busy') from None
            self._held = True
            yield
        finally:
            self._held = False
            os.close(fd)

    def stop(self):
        """Permanent workspace latch; does not wait for an in-flight call/lock."""
        _private(self.path, directory=True)
        try:
            fd = os.open(self.path/'STOP', os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        _sync_dir(self.path)

    def _not_stopped(self):
        try:
            (self.path/'STOP').lstat()
        except FileNotFoundError:
            return
        raise BatchStopped('workspace_stopped')

    def create(self, plan):
        """Freeze a run; no network/database access and no authority granted."""
        identity = validate_plan(plan)
        with self.locked():
            folder = self.path/identity
            folder.mkdir(mode=0o700)  # O_EXCL equivalent; never replace an old run
            _atomic(folder/'plan.json', plan)
            for item in plan['items']:
                _atomic(folder/(item['code']+'.json'), dict(
                    schema=1, plan_sha256=identity, code=item['code'], phase='pending',
                    fetch_attempts=0, payload=None, payload_sha256=None))
            _atomic(folder/'ready.json', {'plan_sha256': identity})
            _sync_dir(self.path)
        return identity

    def _load(self, identity):
        _require(self._held and _hash(identity), 'invalid_run_access')
        folder = self.path/identity
        _private(folder, directory=True)
        _require(_read(folder/'ready.json') == {'plan_sha256': identity}, 'run_not_ready')
        plan = _read(folder/'plan.json')
        _require(validate_plan(plan) == identity, 'plan_changed')
        states = {}
        for item in plan['items']:
            code = item['code']
            state = _read(folder/(code+'.json'))
            _require(isinstance(state, dict) and set(state) == {
                'schema', 'plan_sha256', 'code', 'phase', 'fetch_attempts', 'payload', 'payload_sha256'},
                'invalid_item_state')
            _require(state['schema'] == 1 and state['plan_sha256'] == identity
                     and state['code'] == code and state['phase'] in PHASES,
                     'item_identity_mismatch')
            _require(type(state['fetch_attempts']) is int
                     and state['fetch_attempts'] == (0 if state['phase'] == 'pending' else 1),
                     'invalid_fetch_attempts')
            if state['phase'] in ('pending', 'fetching'):
                _require(state['payload'] is None and state['payload_sha256'] is None, 'unexpected_payload')
            else:
                _require(isinstance(state['payload'], dict) and state['payload']
                         and _hash(state['payload_sha256'])
                         and digest(state['payload']) == state['payload_sha256'], 'payload_corrupt')
            states[code] = state
        return folder, plan, states

    def status(self, identity):
        with self.locked():
            _, _, states = self._load(identity)
            counts = {phase: sum(s['phase'] == phase for s in states.values()) for phase in PHASES}
            return dict(plan_sha256=identity, counts=counts,
                        complete=counts['committed'] == len(states))

    def advance(self, identity, adapter, *, max_items):
        """Run a bounded slice with explicit authority and side-effect adapters.

        Adapter methods are synchronous. authorize must return exactly True for
        each action (checks only, no budget reservation); fetch MUST independently journal and limit each HTTP request.
        validate must check full historical provenance/units/identity, preflight
        returns the current inventory observation hash, and matches must compare
        persisted business contents to the frozen payload (not merely row count).
        No production adapter is provided by this module.
        """
        _require(type(max_items) is int and 1 <= max_items <= 10000, 'invalid_slice')
        with self.locked():
            folder, plan, states = self._load(identity)
            # Refuse all further work when ANY earlier request is uncertain.
            _require(not any(s['phase'] == 'fetching' for s in states.values()), 'request_outcome_unknown')
            # Reconcile uncertain writes before fetching any new symbol.
            ordered = sorted(plan['items'], key=lambda i: states[i['code']]['phase'] != 'writing')
            processed = 0
            for item in ordered:
                state = states[item['code']]
                if state['phase'] == 'committed':
                    continue
                if processed >= max_items:
                    break
                path = folder/(item['code']+'.json')

                def allowed(action):
                    self._not_stopped()
                    _require(adapter.authorize(identity, deepcopy(item), action) is True, 'not_authorized')

                def validate():
                    _require(adapter.validate(deepcopy(item), deepcopy(state['payload'])) is True,
                             'payload_rejected')

                if state['phase'] == 'writing':
                    allowed('reconcile')
                    validate()
                    _require(adapter.matches(deepcopy(item), deepcopy(state['payload'])) is True,
                             'write_outcome_unresolved')
                else:
                    if state['phase'] == 'pending':
                        allowed('fetch')
                        _require(adapter.preflight(deepcopy(item)) == item['observation_sha256'],
                                 'inventory_changed')
                        state.update(phase='fetching', fetch_attempts=1)
                        _atomic(path, state)  # durable BEFORE external request
                        allowed('fetch')  # recheck after preflight and durable marker
                        payload = adapter.fetch(deepcopy(item))
                        _require(isinstance(payload, dict) and payload, 'invalid_payload')
                        state['payload'] = deepcopy(payload)
                        validate()
                        state.update(phase='fetched', payload_sha256=digest(state['payload']))
                        _atomic(path, state)  # response + phase in one atomic record
                    allowed('write')
                    validate()
                    _require(adapter.preflight(deepcopy(item)) == item['observation_sha256'],
                             'inventory_changed')
                    state['phase'] = 'writing'
                    _atomic(path, state)  # durable BEFORE any database side effect
                    allowed('write')  # stop/expiry may change during preflight
                    adapter.write(deepcopy(item), deepcopy(state['payload']))
                    allowed('reconcile')
                    _require(adapter.matches(deepcopy(item), deepcopy(state['payload'])) is True,
                             'write_outcome_unresolved')
                state['phase'] = 'committed'
                _atomic(path, state)
                processed += 1
            return dict(processed=processed, committed=sum(s['phase'] == 'committed' for s in states.values()),
                        total=len(states), complete=all(s['phase'] == 'committed' for s in states.values()))
