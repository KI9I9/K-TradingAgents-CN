"""Durable batch orchestration core; no production adapters or execution CLI.

All batches in one workspace share a nonblocking OS lock. Request authority and
per-request budget accounting belong to an explicitly supplied transport adapter,
not to a plan or this journal. Never reset uncertain requests or writes.
"""
from contextlib import contextmanager
from copy import deepcopy
from datetime import date
import asyncio
import inspect
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import uuid

MAX_JSON = 16 * 1024 * 1024
PHASES = ('pending', 'fetching', 'fetched', 'writing', 'committed', 'skipped')


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
        cost = 1 if plan['mode'] in ('incremental_daily_none', 'year_daily_none') else 2
        _require(plan['schema'] == 1 and plan['mode'] in ('incremental_daily_qfq', 'year_daily_qfq', 'incremental_daily_none', 'year_daily_none')
                 and plan['execution_authorized'] is False and plan['full_market_ready'] is False,
                 'unsupported_plan')
        items, pool = plan['items'], plan['pool']
        _require(isinstance(pool, list) and pool and pool == sorted(set(pool))
                 and isinstance(items, list) and [i['code'] for i in items] == pool,
                 'invalid_plan_pool')
        _require(plan['blocked_count'] == sum(i['status']=='blocked' for i in items)
                 and plan['candidate_count'] == len(pool)-plan['blocked_count']
                 and plan['candidate_requests'] == cost * plan['candidate_count']
                 and plan['total_requests'] == (None if plan['blocked_count'] else cost*len(pool)), 'blocked_plan')
        _require(isinstance(plan['quota'], dict) and plan['quota'].get('evidence_ref'),
                 'quota_unverified')
        for item in items:
            _require(re.fullmatch(r'\d{6}\.(SH|SZ|BJ)', item['code']) is not None
                     and ((item['status']=='candidate' and item['reason'] is None and item['request_cost']==cost)
                          or (item['status']=='blocked' and item['reason'] in
                              ('legacy_contract_unproven','cursor_after_requested_end','window_exceeds_current_transport')
                              and item['request_cost'] is None)) and _hash(item['observation_sha256'])
                     and item['end'] == plan['end']
                     and (item['status']=='blocked' or
                          0 <= (date.fromisoformat(item['end'])-date.fromisoformat(item['start'])).days < plan.get('max_window_days', 30) <= 366),
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
                    schema=1, plan_sha256=identity, code=item['code'], phase='skipped' if item['status']=='blocked' else 'pending',
                    fetch_attempts=0, payload=None, payload_sha256=None))
            _atomic(folder/'ready.json', {'plan_sha256': identity})
            _sync_dir(self.path)
        return identity

    def _load(self, identity, *, metadata_only=False):
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
                     and state['fetch_attempts'] == (0 if state['phase'] in ('pending','skipped') else 1),
                     'invalid_fetch_attempts')
            _require((state['phase']=='skipped') == (item['status']=='blocked'), 'invalid_skip')
            if state['phase'] in ('pending', 'fetching', 'skipped'):
                _require(state['payload'] is None and state['payload_sha256'] is None, 'unexpected_payload')
            else:
                _require(isinstance(state['payload'], dict) and state['payload']
                         and _hash(state['payload_sha256'])
                         and digest(state['payload']) == state['payload_sha256'], 'payload_corrupt')
            if metadata_only and state['phase']=='committed':
                state['payload']={k:v for k,v in state['payload'].items() if k in ('schema','outcome')}
            states[code] = state
        return folder, plan, states

    def status(self, identity):
        with self.locked():
            _, _, states = self._load(identity,metadata_only=True)
            counts = {phase: sum(s['phase'] == phase for s in states.values()) for phase in PHASES}
            return dict(plan_sha256=identity, counts=counts,
                        complete=counts['committed']+counts['skipped'] == len(states))

    def advance(self, identity, adapter, *, max_items):
        """Synchronous convenience entry; async applications use advance_async."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.advance_async(identity, adapter, max_items=max_items))
        raise BatchStopped('use_advance_async')

    async def advance_async(self, identity, adapter, *, max_items, on_progress=None):
        """Run a bounded slice with explicit authority and side-effect adapters.

        Adapter methods may be synchronous or async; all stay on the caller loop.
        authorize must return exactly True for
        each action (checks only, no budget reservation); fetch MUST independently journal and limit each HTTP request.
        validate must check full historical provenance/units/identity, preflight
        returns the current inventory observation hash, and matches must compare
        persisted business contents to the frozen payload (not merely row count).
        No production adapter is provided by this module.
        """
        async def call(method, *args):
            result = method(*args)
            return await result if inspect.isawaitable(result) else result

        _require(type(max_items) is int and 1 <= max_items <= 10000, 'invalid_slice')
        with self.locked():
            folder, plan, states = self._load(identity,metadata_only=True)
            def summary(processed):
                return dict(processed=processed,committed=sum(s['phase']=='committed' for s in states.values()),
                    skipped=sum(s['phase']=='skipped' for s in states.values()),total=len(states),
                    outcomes=[dict(code=c,reason=s['payload']['outcome']) for c,s in states.items()
                              if s['phase']=='committed' and s['payload'].get('schema') in (2,3)],
                    complete=all(s['phase'] in ('committed','skipped') for s in states.values()))
            # Refuse all further work when ANY earlier request is uncertain.
            _require(not any(s['phase'] == 'fetching' for s in states.values()), 'request_outcome_unknown')
            # Reconcile uncertain writes before fetching any new symbol.
            ordered = sorted(plan['items'], key=lambda i: states[i['code']]['phase'] != 'writing')
            processed = 0
            for item in ordered:
                state = states[item['code']]
                if state['phase'] in ('committed','skipped'):
                    continue
                if processed >= max_items:
                    break
                path = folder/(item['code']+'.json')

                async def allowed(action):
                    self._not_stopped()
                    _require(await call(adapter.authorize, identity, deepcopy(item), action) is True, 'not_authorized')

                async def validate():
                    _require(await call(adapter.validate, deepcopy(item), deepcopy(state['payload'])) is True,
                             'payload_rejected')

                if state['phase'] == 'writing':
                    await allowed('reconcile')
                    await validate()
                    _require(await call(adapter.matches, deepcopy(item), deepcopy(state['payload'])) is True,
                             'write_outcome_unresolved')
                else:
                    if state['phase'] == 'pending':
                        await allowed('fetch')
                        _require(await call(adapter.preflight, deepcopy(item)) == item['observation_sha256'],
                                 'inventory_changed')
                        state.update(phase='fetching', fetch_attempts=1)
                        _atomic(path, state)  # durable BEFORE external request
                        await allowed('fetch')  # recheck after preflight and durable marker
                        payload = await call(adapter.fetch, deepcopy(item))
                        _require(isinstance(payload, dict) and payload, 'invalid_payload')
                        state['payload'] = deepcopy(payload)
                        await validate()
                        state.update(phase='fetched', payload_sha256=digest(state['payload']))
                        _atomic(path, state)  # response + phase in one atomic record
                    await allowed('write')
                    await validate()
                    _require(await call(adapter.preflight, deepcopy(item)) == item['observation_sha256'],
                             'inventory_changed')
                    state['phase'] = 'writing'
                    _atomic(path, state)  # durable BEFORE any database side effect
                    await allowed('write')  # stop/expiry may change during preflight
                    await call(adapter.write, deepcopy(item), deepcopy(state['payload']))
                    await allowed('reconcile')
                    _require(await call(adapter.matches, deepcopy(item), deepcopy(state['payload'])) is True,
                             'write_outcome_unresolved')
                state['phase'] = 'committed'
                _atomic(path, state)
                state['payload']={k:v for k,v in state['payload'].items() if k in ('schema','outcome')}
                processed += 1
                if on_progress is not None:
                    await call(on_progress, summary(processed))
            return summary(processed)
