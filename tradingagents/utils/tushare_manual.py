"""Bounded manual A-share authorization. No credentials, network or app imports.

ContextVar carries authority through asyncio tasks/to_thread, never globally.
Each attempt is fsynced before send; a crash/failure requires human review.
State is explicitly provisioned by the deployment controller, never on request.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import re
import time
from uuid import uuid4

from tradingagents.utils.tushare_egress import (
    Permit, TushareEgressStopped, _pairs, _private_fd,
)

DIRECTORY = '/app/runtime/tushare-manual-daily-v1'
EXPIRY = 1792984622
DAILY_LIMIT = 10
MINUTE_LIMIT = 4
BJ = timezone(timedelta(hours=8))
CURRENT = ContextVar('tushare_manual_authority', default=None)
JOB_IDS = (
    'tushare_basic_info_sync', 'tushare_historical_sync',
    'tushare_financial_sync', 'tushare_status_check',
    'tushare_realtime_quotes_hourly',
)


def day_at(stamp):
    return datetime.fromtimestamp(stamp, BJ).date()


def stock_code(value):
    # Conservative explicit A-share families; exclude indices, B-shares and funds.
    if not isinstance(value, str):
        raise TushareEgressStopped('invalid_request')
    value = value.strip().upper()
    match = re.fullmatch(r'(\d{6})(?:\.(SH|SZ|BJ))?', value)
    if not match:
        raise TushareEgressStopped('invalid_request')
    code, suffix = match.groups()
    exchange = ('SH' if code.startswith(('600', '601', '603', '605', '688', '689'))
                else 'SZ' if code.startswith(('000', '001', '002', '003', '300', '301'))
                else 'BJ' if code.startswith(('430', '83', '87', '88', '920')) else None)
    if not exchange or (suffix and suffix != exchange):
        raise TushareEgressStopped('invalid_request')
    return code + '.' + exchange


def scope(payload, now=None):
    if (payload.get('data_source') != 'tushare' or payload.get('sync_historical') is not True
            or any(payload.get(key, False) is not False
                   for key in ('sync_realtime', 'sync_financial', 'sync_basic'))
            or type(payload.get('days')) is not int or not 1 <= payload['days'] <= 30):
        raise TushareEgressStopped('invalid_request')
    symbols = payload.get('symbols', [payload.get('symbol')])
    if not isinstance(symbols, list) or not 1 <= len(symbols) <= 5:
        raise TushareEgressStopped('invalid_request')
    codes = list(dict.fromkeys(stock_code(code) for code in symbols))
    end = day_at(time.time() if now is None else now)
    return dict(codes=codes, start=(end-timedelta(days=payload['days']-1)).strftime('%Y%m%d'),
                end=end.strftime('%Y%m%d'))


def _state(value):
    if not __debug__:
        raise TushareEgressStopped('state_invalid')
    try:
        assert set(value) == {'schema', 'mode', 'expires_at', 'phase', 'reason', 'api_code',
                              'counts', 'attempts', 'last_time', 'active'}
        assert value['schema'] == 1 and type(value['schema']) is int
        assert value['mode'] in ('acceptance', 'manual') and value['expires_at'] == EXPIRY
        assert value['phase'] in ('ready', 'in_flight', 'blocked')
        from tradingagents.utils.tushare_egress import REASONS
        assert value['reason'] in REASONS
        assert value['api_code'] is None or type(value['api_code']) is int
        assert isinstance(value['counts'], dict) and len(value['counts']) <= 32
        for key, count in value['counts'].items():
            assert datetime.strptime(key, '%Y-%m-%d').strftime('%Y-%m-%d') == key
            assert type(count) is int and 0 <= count <= DAILY_LIMIT
        assert isinstance(value['attempts'], list) and len(value['attempts']) <= 320
        assert all(type(t) in (float, int) and math.isfinite(t) and 0 < t <= EXPIRY
                   for t in value['attempts'])
        assert value['attempts'] == sorted(value['attempts'])
        assert sum(value['counts'].values()) == len(value['attempts'])
        assert all(sum(day_at(t).isoformat() == day for t in value['attempts']) == count
                   for day, count in value['counts'].items())
        assert type(value['last_time']) in (float, int) and math.isfinite(value['last_time'])
        assert value['last_time'] >= max(value['attempts'], default=0)
        active = value['active']
        if active is not None:
            assert set(active) == {'id', 'codes', 'start', 'end', 'day', 'sent'}
            assert re.fullmatch('[a-f0-9]{32}', active['id'])
            assert isinstance(active['codes'], list) and 1 <= len(active['codes']) <= 5
            assert list(dict.fromkeys(stock_code(c) for c in active['codes'])) == active['codes']
            start, end = (datetime.strptime(active[k], '%Y%m%d').date() for k in ('start', 'end'))
            assert 0 <= (end-start).days < 30 and active['day'] == end.isoformat()
            assert isinstance(active['sent'], list) and len(active['sent']) <= len(active['codes'])*2
            assert len(set(active['sent'])) == len(active['sent'])
            assert all(s in {c+':'+api for c in active['codes'] for api in ('daily', 'adj_factor')}
                       for s in active['sent'])
        assert value['phase'] != 'in_flight' or active is not None
        assert value['phase'] != 'ready' or value['reason'] == 'none'
    except (AssertionError, ValueError, TypeError, KeyError, AttributeError):
        raise TushareEgressStopped('state_invalid') from None
    return value


class DailyGate(Permit):
    def __init__(self, directory=DIRECTORY):
        super().__init__(directory)

    def read(self, fd):
        stream_fd = os.open('state.json', os.O_RDONLY | os.O_NOFOLLOW, dir_fd=fd)
        with os.fdopen(stream_fd, 'rb') as stream:
            _private_fd(stream.fileno())
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise TushareEgressStopped('state_invalid')
        try:
            return _state(json.loads(raw, object_pairs_hook=_pairs))
        except (ValueError, TypeError):
            raise TushareEgressStopped('state_invalid') from None

    def write(self, fd, value):
        _state(value)
        out = os.open('state.next', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                      0o600, dir_fd=fd)
        with os.fdopen(out, 'w') as stream:
            json.dump(value, stream, sort_keys=True, allow_nan=False)
            stream.flush(); os.fsync(stream.fileno())
        os.replace('state.next', 'state.json', src_dir_fd=fd, dst_dir_fd=fd)
        os.fsync(fd)

    def check(self, fd, state, job=None):
        if self.stopped(fd):
            raise TushareEgressStopped('disabled')
        if state['phase'] != 'ready':
            reason = 'previous_attempt_incomplete' if state['phase'] == 'in_flight' else state['reason']
            raise TushareEgressStopped(reason, state['api_code'])
        now = time.time()
        if now < state['last_time'] or now >= EXPIRY:
            raise TushareEgressStopped('expired')
        if job is not None:
            active = state['active']
            if (not active or active['id'] != job.identity
                    or active['day'] != day_at(now).isoformat()):
                raise TushareEgressStopped('previous_attempt_incomplete')
        return now

    def begin(self, payload, *, acceptance=False):
        approved = scope(payload)
        with self.locked() as fd:
            state = self.read(fd)
            now = self.check(fd, state)
            if state['mode'] != ('acceptance' if acceptance else 'manual'):
                raise TushareEgressStopped('not_authorized')
            if state['active'] is not None:
                raise TushareEgressStopped('busy')
            day = day_at(now).isoformat()
            if state['counts'].get(day, 0) + 2*len(approved['codes']) > DAILY_LIMIT:
                raise TushareEgressStopped('budget_exhausted')
            identity = uuid4().hex
            state.update(active=dict(id=identity, day=day, sent=[], **approved), last_time=now)
            self.write(fd, state)
        return ManualJob(self, identity, approved)

    def status(self):
        with self.locked() as fd:
            state = self.read(fd)
            reason = 'none'
            try:
                self.check(fd, state)
            except TushareEgressStopped as error:
                reason = error.reason
            used = state['counts'].get(day_at(time.time()).isoformat(), 0)
            return dict(mode=state['mode'], reason=reason, phase=state['phase'],
                        daily_limit=DAILY_LIMIT, used_today=used, remaining_today=DAILY_LIMIT-used,
                        per_rolling_minute=MINUTE_LIMIT, busy=state['active'] is not None,
                        expires_at=EXPIRY, request_attempts_total=len(state['attempts']))


class ManualJob:
    def __init__(self, gate, identity, approved):
        self.gate, self.identity, self.approved = gate, identity, approved
        self.daily_dates = {}

    @contextmanager
    def context(self):
        token = CURRENT.set(self)
        try:
            yield self
        finally:
            CURRENT.reset(token)

    def assert_ready(self):
        with self.gate.locked() as fd:
            self.gate.check(fd, self.gate.read(fd), self)

    def fail(self, reason='interrupted'):
        # STOP is independent of the in-flight lock, including cancellation.
        self.gate.stop()
        try:
            with self.gate.locked() as fd:
                state = self.gate.read(fd)
                if state['phase'] == 'ready':
                    state.update(phase='blocked', reason=reason)
                    self.gate.write(fd, state)
        except TushareEgressStopped:
            pass

    def finish(self):
        with self.gate.locked() as fd:
            state = self.gate.read(fd)
            self.gate.check(fd, state, self)
            if len(state['active']['sent']) != len(self.approved['codes'])*2:
                state.update(phase='blocked', reason='previous_attempt_incomplete')
                self.gate.write(fd, state)
                raise TushareEgressStopped('previous_attempt_incomplete')
            state['active'] = None
            self.gate.write(fd, state)

    def perform(self, api, fields, params, operation):
        if (api not in ('daily', 'adj_factor') or fields != ''
                or set(params) - {'ts_code', 'start_date', 'end_date', 'offset', 'limit', 'ts_type_name'}
                or params.get('ts_code') not in self.approved['codes']
                or any(params.get(k) != self.approved[k.replace('_date', '')]
                       for k in ('start_date', 'end_date'))
                or params.get('offset') is not None or params.get('limit') is not None
                or params.get('ts_type_name', 'http://api.waditu.com/dataapi') != 'http://api.waditu.com/dataapi'):
            self.fail('invalid_request')
            raise TushareEgressStopped('invalid_request')
        key = params['ts_code'] + ':' + api
        while True:  # Pacing, not a retry: operation runs exactly once, below.
            with self.gate.locked() as fd:
                state = self.gate.read(fd)
                now = self.gate.check(fd, state, self)
                active = state['active']
                if key in active['sent'] or (api == 'adj_factor' and params['ts_code']+':daily' not in active['sent']):
                    state.update(phase='blocked', reason='invalid_request')
                    self.gate.write(fd, state)
                    raise TushareEgressStopped('invalid_request')
                day = active['day']
                if state['counts'].get(day, 0) >= DAILY_LIMIT:
                    raise TushareEgressStopped('budget_exhausted')
                recent = [t for t in state['attempts'] if now-t < 60]
                if len(recent) < MINUTE_LIMIT:
                    state['counts'][day] = state['counts'].get(day, 0)+1
                    state['attempts'].append(now)
                    state['active']['sent'].append(key)
                    state.update(last_time=now, phase='in_flight')
                    self.gate.write(fd, state)
                    try:
                        if self.gate.stopped(fd):
                            raise TushareEgressStopped('disabled')
                        result = operation()
                        self.validate_table(api, params, result)
                    except BaseException as error:
                        stop = error if isinstance(error, TushareEgressStopped) else TushareEgressStopped('interrupted')
                        state.update(phase='blocked', reason=stop.reason, api_code=stop.api_code)
                        self.gate.write(fd, state)
                        if not isinstance(error, Exception):
                            raise
                        raise stop from None
                    if self.gate.stopped(fd):
                        state.update(phase='blocked', reason='disabled')
                        self.gate.write(fd, state)
                        raise TushareEgressStopped('disabled')
                    state.update(phase='ready', reason='none')
                    self.gate.write(fd, state)
                    return result
                delay = max(0.02, min(0.5, 60-(now-recent[0])+0.01))
            time.sleep(delay)

    def validate_table(self, api, params, result):
        try:
            fields, rows = result['fields'], result['items']
            assert len(set(fields)) == len(fields) and 1 <= len(rows) <= 30
            required = {'ts_code', 'trade_date'} | (
                {'open', 'high', 'low', 'close', 'vol'} if api == 'daily' else {'adj_factor'})
            assert required <= set(fields)
            records = [dict(zip(fields, row)) for row in rows]
            dates = [r['trade_date'] for r in records]
            assert len(set(dates)) == len(dates)
            for row in records:
                assert row['ts_code'] == params['ts_code']
                day = row['trade_date']
                assert datetime.strptime(day, '%Y%m%d').strftime('%Y%m%d') == day
                assert self.approved['start'] <= day <= self.approved['end']
                for key in required - {'ts_code', 'trade_date'}:
                    number = float(row[key])
                    assert math.isfinite(number) and (number >= 0 if key == 'vol' else number > 0)
            if api == 'daily':
                self.daily_dates[params['ts_code']] = set(dates)
            else:
                assert set(dates) == self.daily_dates.get(params['ts_code'])
        except (AssertionError, KeyError, TypeError, ValueError):
            raise TushareEgressStopped('response_invalid') from None


def validate_frame(frame):
    job = CURRENT.get()
    if job is None:
        return
    job.assert_ready()
    if frame is None or frame.empty:
        job.fail('response_invalid')
        raise TushareEgressStopped('response_invalid')


async def enforce_manual_jobs(scheduler, db):
    """Called before restore_preferences dispatch; failures propagate fail-closed."""
    for job_id in JOB_IDS:
        if scheduler.get_job(job_id) is not None:
            scheduler.pause_job(job_id)
        await db.scheduler_preferences.update_one(
            {'_id': job_id}, {'$set': {'enabled': False, 'updated_at': datetime.now(BJ).replace(tzinfo=None)}},
            upsert=True)
    count = await db.scheduler_preferences.count_documents({'_id': {'$in': list(JOB_IDS)}, 'enabled': False})
    if count != len(JOB_IDS):
        raise RuntimeError('TushareManualSchedulerPauseUnverified')


async def restore_optional_mcp(db, logger):
    """Only absence of the optional module is tolerated, not damaged dependencies."""
    import importlib.util
    module = 'app.pro.services.mcp_service'
    try:
        found = importlib.util.find_spec(module) is not None
    except ModuleNotFoundError as error:
        if error.name not in ('app.pro', 'app.pro.services', module):
            raise
        found = False
    if not found:
        count = await db.mcp_server_configs.count_documents({})
        if count:
            logger.error('MCP optional module unavailable; stored configurations retained, no connection attempted')
        else:
            logger.info('MCP optional module not installed; no configured servers; skipped')
        return
    from app.pro.services.mcp_service import MCPService
    await MCPService(db).restore_enabled_servers()


def provision(directory=DIRECTORY):
    """Explicit deployment operation only. Refuses any existing directory."""
    gate = DailyGate(directory)
    now = time.time()
    if now >= EXPIRY:
        raise TushareEgressStopped('expired')
    gate.directory.mkdir(mode=0o700)
    fd = os.open(gate.directory/'gate.lock', os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    os.fsync(fd); os.close(fd)
    with gate.locked() as directory_fd:
        gate.write(directory_fd, dict(schema=1, mode='acceptance', expires_at=EXPIRY,
            phase='ready', reason='none', api_code=None, counts={}, attempts=[], last_time=now, active=None))
    return gate


def open_manual(gate):
    """One-way, after acceptance. No budget reset or STOP removal."""
    with gate.locked() as fd:
        state = gate.read(fd)
        gate.check(fd, state)
        if state['mode'] != 'acceptance' or state['active'] is not None or len(state['attempts']) != 2:
            raise TushareEgressStopped('previous_attempt_incomplete')
        state['mode'] = 'manual'
        gate.write(fd, state)
