"""One-off, two-request history acceptance. No application startup or business writes.

Run in a separate backend Python process with an explicitly approved request.
The original global/manual STOP files and journals stay byte-for-byte unchanged.
A fresh private permit is bound to one symbol/window and cannot be reused/reset.
"""
import ast
import asyncio
import contextlib
from datetime import date, datetime, timedelta, timezone
import hashlib
import importlib.util
import io
import json
import logging
import math
import os
from pathlib import Path
import sys
import time
import types
from typing import Optional, Union

ENTRY_HASH = 'd75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b'
GLOBAL_GATES = ['/app/runtime/tushare-egress', '/app/runtime/tushare-manual-daily-v1']
BJ = timezone(timedelta(hours=8))


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def require(value, reason):
    if not value:
        raise ValueError(reason)


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def import_transport(root):
    # No tradingagents package/application initialization, token cache or provider startup.
    for name in ('tradingagents', 'tradingagents.utils'):
        package = types.ModuleType(name)
        package.__path__ = [str(root / name.replace('.', '/'))]
        sys.modules[name] = package
    egress = load('tradingagents.utils.tushare_egress', root / 'tradingagents/utils/tushare_egress.py')
    manual = load('tradingagents.utils.tushare_manual', root / 'tradingagents/utils/tushare_manual.py')
    https = load('tradingagents.utils.tushare_https', root / 'tradingagents/utils/tushare_https.py')
    return egress, manual, https


def original_gate_hashes(extra=()):
    result = {}
    for path in [*GLOBAL_GATES, *extra]:
        root = Path(path)
        require((root / 'STOP').is_file(), 'original_stop_missing')
        for name in ('STOP', 'permit.json', 'state.json', 'gate.lock'):
            item = root / name
            if item.exists():
                result[str(item)] = digest(item.read_bytes())
    return result


class SampleAuthority:
    """Use the production finite permit, plus exact parameters and response checks."""
    def __init__(self, gate, egress, scope, expected_dates):
        self.gate, self.egress, self.approved = gate, egress, scope
        self.expected_dates = expected_dates
        self.tables = {}
        self.sent = []

    def fail(self, reason='interrupted'):
        self.gate.stop()

    def reject(self, reason):
        self.fail(reason)
        raise self.egress.TushareEgressStopped(reason)

    def assert_ready(self):
        with self.gate.locked() as fd:
            state = self.gate.read(fd)
            if self.gate.stopped(fd):
                raise self.egress.TushareEgressStopped('disabled')
            if time.time() >= state['expires_at']:
                raise self.egress.TushareEgressStopped('expired')
            if state['phase'] == 'exhausted' and state['used'] == 2 and len(self.tables) == 2:
                return  # completed frame checks may run; no further send is permitted
            if state['phase'] == 'in_flight':
                raise self.egress.TushareEgressStopped('previous_attempt_incomplete')
            if state['phase'] != 'ready':
                raise self.egress.TushareEgressStopped(state['reason'], state['api_code'])

    def validate(self, api, result):
        try:
            fields, rows = result['fields'], result['items']
            required = ['ts_code', 'trade_date'] + (['adj_factor'] if api == 'adj_factor' else
                ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount'])
            require(len(fields) == len(set(fields)) and set(required) <= set(fields), 'fields')
            require(len(rows) == len(self.expected_dates), 'row_count')
            records = [dict(zip(fields, row)) for row in rows]
            require(all(len(row) == len(fields) for row in rows), 'row_width')
            require([r['trade_date'] for r in records] == list(reversed(self.expected_dates)), 'date_order')
            for row in records:
                require(row['ts_code'] == self.approved['codes'][0], 'symbol')
                for key in required[2:]:
                    value = row[key]
                    require(not isinstance(value, (bool, str)) and value is not None
                            and math.isfinite(float(value)), 'number')
                    if key not in ('change', 'pct_chg'):
                        require(float(value) >= 0 if key in ('vol', 'amount') else float(value) > 0, 'positive')
                if api == 'daily':
                    require(row['low'] <= min(row['open'], row['close'])
                            <= max(row['open'], row['close']) <= row['high'], 'ohlc')
            # Only validated market fields are persisted, never arbitrary upstream strings.
            return {'fields': required, 'items': [[row[k] for k in required] for row in records]}
        except (KeyError, TypeError, ValueError, OverflowError):
            raise self.egress.TushareEgressStopped('response_invalid') from None

    def perform(self, api, fields, params, operation):
        expected_api = ('daily', 'adj_factor')[len(self.sent)] if len(self.sent) < 2 else None
        if (api != expected_api or fields != ''
                or set(params) - {'ts_code', 'start_date', 'end_date', 'offset', 'limit', 'ts_type_name'}
                or params.get('ts_code') != self.approved['codes'][0]
                or params.get('start_date') != self.approved['start']
                or params.get('end_date') != self.approved['end']
                or params.get('offset') is not None or params.get('limit') is not None
                or params.get('ts_type_name', 'http://api.waditu.com/dataapi') != 'http://api.waditu.com/dataapi'):
            self.reject('invalid_request')
        self.assert_ready()
        self.sent.append(api)  # an uncertain/failed attempt is never retried
        def checked_send():
            table = self.validate(api, operation())
            self.tables[api] = table
            return table
        return self.gate.perform(api, checked_send)


def provision(directory, egress, scope):
    """Explicit one-time authorization only; refuses an existing run directory."""
    directory = Path(directory)
    require(directory.is_absolute() and directory.parent.resolve() == directory.parent, 'permit_parent')
    directory.mkdir(mode=0o700)  # no exist_ok: a failed attempt must never be reset
    for name, raw in [('gate.lock', b''), ('scope.json', json.dumps(scope, sort_keys=True).encode())]:
        fd = os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    gate = egress.Permit(directory)
    with gate.locked() as fd:
        gate.write(fd, dict(schema=1, phase='ready', budget=2, used=0, expires_at=time.time() + 300,
                           allowed_apis=['daily', 'adj_factor'], reason='none', api_code=None))
    return gate


def provider(root, api, egress, manual):
    """Execute unchanged deployed methods; intentionally omit app/provider bootstrap."""
    import pandas as pd
    import tushare as ts
    names = {'get_historical_data', '_normalize_ts_code', '_format_date', '_is_etf', '_standardize_historical_data'}
    source = root / 'tradingagents/dataflows/providers/china/tushare.py'
    tree = ast.parse(source.read_text())
    original = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'TushareProvider')
    methods = [n for n in original.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
    require(len(methods) == len(names), 'provider_methods_missing')
    cls = ast.ClassDef(name='SelectedProvider', bases=[], keywords=[], body=methods, decorator_list=[])
    namespace = dict(asyncio=asyncio, pd=pd, ts=ts, datetime=datetime, date=date, timezone=timezone,
                     timedelta=timedelta, Optional=Optional, Union=Union, TUSHARE_AVAILABLE=True,
                     TushareEgressStopped=egress.TushareEgressStopped,
                     raise_if_egress_blocked=egress.raise_if_egress_blocked,
                     _manual_current=manual.CURRENT, _manual_frame=manual.validate_frame)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[])), str(source), 'exec'), namespace)
    obj = namespace['SelectedProvider']()
    obj.api, obj.is_available = api, lambda: True
    obj.logger = logging.getLogger('isolated_history_acceptance')
    obj.logger.disabled = True
    return obj


def compare_rows(docs, tables, reference):
    daily = {r['trade_date']: r for r in [dict(zip(tables['daily']['fields'], row)) for row in tables['daily']['items']]}
    factors = {r['trade_date']: float(r['adj_factor']) for r in [dict(zip(tables['adj_factor']['fields'], row)) for row in tables['adj_factor']['items']]}
    base = factors[max(factors)]
    ref = {r['trade_date']: r for r in reference}
    differences = []
    for doc in docs:
        day = doc['trade_date'].replace('-', '')
        raw = daily[day]
        require(doc['volume'] == float(raw['vol']), 'volume_conversion_wrong')
        require(math.isclose(doc['amount'], float(raw['amount']) * 1000, abs_tol=1e-5, rel_tol=1e-12), 'amount_conversion_wrong')
        for key in ('open', 'high', 'low', 'close'):
            expected = float('%.4f' % (float(raw[key]) * factors[day] / base))
            require(math.isclose(doc[key], expected, rel_tol=0, abs_tol=1e-8), 'qfq_conversion_wrong')
        for key in ('open', 'high', 'low', 'close', 'volume', 'amount'):
            other = float(ref[doc['trade_date']][key])
            tolerance = max(1., abs(other) * 0.00001) if key == 'amount' else 1. if key == 'volume' else 0.011
            delta = abs(doc[key] - other)
            if delta > tolerance:
                differences.append({'trade_date': doc['trade_date'], 'field': key,
                                    'tushare': doc[key], 'akshare': other, 'difference': delta, 'tolerance': tolerance})
    return differences


def restrict_network(event, args):
    if event in ('socket.getaddrinfo', 'socket.bind', 'socket.sendto', 'subprocess.Popen'):
        raise RuntimeError('acceptance_network_scope_violation')
    if event == 'socket.connect' and args[1] != '/run/tushare-egress/upstream.sock':
        raise RuntimeError('acceptance_network_scope_violation')


def permit_directory(request):
    run = request.get('run_id')
    require(run in ('v1', 'v2'), 'unapproved_run')
    return '/app/runtime/history-acceptance-20261028-600000-' + run


def main(request):
    os.umask(0o077)
    root = Path('/app')
    report = {'status': 'preflight', 'business_database_writes': False, 'real_attempts': 0}
    gate = None
    token = None
    prior = None
    protected = []
    try:
        require(request['execute'] is True, 'execute_not_authorized')
        directory = permit_directory(request)
        if request['run_id'] == 'v2':
            protected = ['/app/runtime/history-acceptance-20261028-600000-v1']
        require(request['symbol'] == '600000' and request['start'] == '20261021'
                and request['end'] == '20261027' and request['budget'] == 2, 'scope_not_approved')
        require(datetime.now(BJ).date().isoformat() == '2026-10-28', 'approval_day_expired')
        for name, sha in request['source_hashes'].items():
            require(digest((root / name).read_bytes()) == sha, 'installed_source_changed')
        runtime = types.ModuleType('acceptance_runtime')
        exec(request['runtime_tool'], runtime.__dict__)
        env, effective = runtime.reconstruct('/app/deploy/migration_production_entrypoint.py', ENTRY_HASH, '/run/secrets')
        require(env.get('TA_TUSHARE_ROUTE') == 'mainland_unix', 'route_changed')
        require(str(env.get('TUSHARE_UNIFIED_ENABLED')).lower() == 'false', 'tushare_unified_enabled')
        prior = original_gate_hashes(protected)
        require(prior == request['original_gate_hashes'], 'original_gate_changed')
        # Read-only DB preflight and credential selection; closed before installing the network restriction.
        from pymongo import MongoClient
        report['step'] = 'database_read_only_preflight'
        client = MongoClient(host=env['MONGODB_HOST'], port=int(env['MONGODB_PORT']),
            username=env['MONGODB_USERNAME'], password=env['MONGODB_PASSWORD'],
            authSource=env.get('MONGODB_AUTH_SOURCE', 'admin'), serverSelectionTimeoutMS=10000)
        try:
            db = client[env['MONGODB_DATABASE']]
            require(db.stock_daily_quotes.count_documents({'symbol': '600000', 'period': 'daily', 'data_source': 'tushare'}) == 0,
                    'legacy_series_now_exists')
            active = db.scheduler_executions.count_documents({'job_id': {'$regex': '^tushare_'}, 'status': {'$in': ['running', 'cancelling']}})
            require(active == 0, 'tushare_task_active')
            for job in ['tushare_historical_sync', 'tushare_realtime_quotes_hourly']:
                pref = db.scheduler_preferences.find_one({'_id': job}, {'enabled': 1}) or {}
                require(pref.get('enabled') is False, 'tushare_preference_enabled')
            selected = db.system_configs.find_one({'is_active': True}, {'data_source_configs': 1}, sort=[('version', -1)]) or {}
            candidates = [c.get('api_key') for c in selected.get('data_source_configs', []) if c.get('type') == 'tushare'
                          and isinstance(c.get('api_key'), str) and c['api_key'] and not c['api_key'].startswith('your_')]
            token = candidates[0].strip() if candidates else env.get('TUSHARE_TOKEN', '').strip()
            require(bool(token), 'token_missing')
            report['token_source'] = 'database' if candidates else 'environment'
            reference = list(db.stock_daily_quotes.find({'symbol': '600000', 'data_source': 'akshare', 'period': 'daily',
                'trade_date': {'$gte': '2026-10-21', '$lte': '2026-10-27'}},
                {'_id': 0, 'trade_date': 1, 'open': 1, 'high': 1, 'low': 1, 'close': 1, 'volume': 1, 'amount': 1}).sort('trade_date', 1))
        finally:
            client.close()
        expected_dates = ['20261021', '20261022', '20261023', '20261026', '20261027']
        require([r['trade_date'].replace('-', '') for r in reference] == expected_dates, 'reference_dates_changed')
        egress, manual, https = import_transport(root)
        report['step'] = 'loading_installed_provider_methods'
        contract = load('acceptance_history_contract', root / 'app/services/historical_contract.py')
        obj = provider(root, https.create_tushare_client(token, timeout=20), egress, manual)
        token = None
        os.environ['TA_TUSHARE_ROUTE'] = 'mainland_unix'  # this isolated process only, verified above
        scope = {'codes': ['600000.SH'], 'start': request['start'], 'end': request['end']}
        gate = provision(directory, egress, scope)
        report['step'] = 'two_request_sample'
        job = SampleAuthority(gate, egress, scope, expected_dates)
        report.update(scope=scope, permit_directory=str(gate.directory), runtime=effective)
        sys.addaudithook(restrict_network)
        context_token = manual.CURRENT.set(job)
        try:
            # SDK may print errors it catches. Never expose arbitrary upstream diagnostics.
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                frame = asyncio.run(obj.get_historical_data('600000', '20261021', '20261027', 'daily'))
                job.assert_ready()
                require(frame is not None and len(frame) == 5, 'provider_frame_invalid')
                docs = contract.tushare_documents('600000', frame, 'CN', 'daily')
                require(len(job.tables) == 2 and len(job.sent) == 2, 'request_count_invalid')
                differences = compare_rows(docs, job.tables, reference)
                report['step'] = 'captured_data_replay'
                # Exact same deployed provider/SDK methods, replaying captured tables only.
                import pandas as pd
                class ReplayClient:
                    def daily(self, **kwargs):
                        return pd.DataFrame(job.tables['daily']['items'], columns=job.tables['daily']['fields'])
                    def adj_factor(self, **kwargs):
                        return pd.DataFrame(job.tables['adj_factor']['items'], columns=job.tables['adj_factor']['fields'])
                obj.api = ReplayClient()
                replay = asyncio.run(obj.get_historical_data('600000', '20261021', '20261027', 'daily'))
                pd.testing.assert_frame_equal(frame, replay)
                require(contract.tushare_documents('600000', replay, 'CN', 'daily') == docs, 'replay_documents_differ')
            report.update(status='passed' if not differences else 'cross_source_comparison_failed',
                          tables=job.tables, documents=docs, reference=reference, differences=differences,
                          replay_identical=True, source_hashes=request['source_hashes'],
                          limitations=['provider bootstrap omitted; unchanged deployed method bodies executed',
                                       'no production business writes or full-market task acceptance'])
        finally:
            manual.CURRENT.reset(context_token)
    except BaseException as error:
        report['status'] = 'failed'
        report['error_type'] = type(error).__name__
        # Only transport allowlisted reason/code. No traceback, token or upstream text.
        report['reason'] = getattr(error, 'reason', 'local_validation_or_execution_failed')
        report['api_code'] = getattr(error, 'api_code', None)
    finally:
        token = None
        if gate is not None:
            gate.stop()
            with gate.locked() as fd:
                state = gate.read(fd)
                report.update(real_attempts=state['used'], final_permit=state, sample_stop_exists=gate.stopped(fd))
        if prior is not None:
            report['original_gates_unchanged'] = original_gate_hashes(protected) == prior
            if not report['original_gates_unchanged']:
                report['status'] = 'failed_original_gate_drift'
        print(json.dumps(report, allow_nan=False, default=str))
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main(REQUEST))
