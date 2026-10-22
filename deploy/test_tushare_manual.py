"""Synthetic-only tests, locally or on the actual native image. No real tokens."""
import ast
import asyncio
import contextlib
from datetime import datetime
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import types
import sys
import unittest
from unittest.mock import Mock, AsyncMock, patch

from offline_tushare_support import SOURCE, load, egress as eg, policy

NATIVE = importlib.util.find_spec('tushare') is not None and importlib.util.find_spec('pandas') is not None
os.umask(0o077)
os.environ['TA_TUSHARE_ROUTE'] = 'mainland_unix'
os.environ['OPENBLAS_NUM_THREADS'] = '1'


def deny_network(event, args):
    if event in ('socket.getaddrinfo', 'socket.connect', 'socket.bind', 'socket.sendto', 'subprocess.Popen'):
        raise RuntimeError('OfflineNetworkDenied')


sys.addaudithook(deny_network)
FIXTURES = Path(tempfile.mkdtemp(prefix='ta-manual-offline-'))
NOW = datetime(2026, 10, 19, 21, tzinfo=policy.BJ).timestamp()


def payload(symbol='000001', days=30):
    return dict(symbol=symbol, days=days, sync_historical=True, sync_realtime=False,
                sync_financial=False, sync_basic=False, data_source='tushare')


def table(api, params):
    code = params['ts_code']
    fields = ['ts_code', 'trade_date', 'adj_factor'] if api == 'adj_factor' else [
        'ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
    rows = [[code, day, 2.] if api == 'adj_factor' else
            [code, day, 10., 12., 9., 11., 10., 1., 10., 100., 1000.]
            for day in ('20261014', '20261013')]
    return dict(fields=fields, items=rows)


class GateTests(unittest.TestCase):
    def setUp(self):
        self.clock = NOW
        self.clock_patch = patch.object(policy.time, 'time', side_effect=lambda: self.clock)
        self.clock_patch.start(); self.addCleanup(self.clock_patch.stop)
        self.directory = FIXTURES/self.id().split('.')[-1]
        self.gate = policy.provision(self.directory)
        self.sent = []

    def state(self):
        with self.gate.locked() as fd:
            return self.gate.read(fd)

    def write(self, value):
        with self.gate.locked() as fd:
            self.gate.write(fd, value)

    def begin(self, data=None):
        return self.gate.begin(data or payload(), acceptance=True)

    def send(self, job, api, **overrides):
        params = dict(ts_code=job.approved['codes'][0], start_date=job.approved['start'],
                      end_date=job.approved['end'], offset=None, limit=None)
        params.update(overrides)
        def op():
            self.sent.append((api, self.clock))
            return table(api, params)
        return job.perform(api, '', params, op)


    def test_invalid_scope_rejected_before_active_record(self):
        cases = [payload('159941'), payload('000001.SH'), payload('900001'), payload('000001', 31),
                 {**payload(), 'sync_financial': True}, {**payload(), 'sync_realtime': True},
                 {**payload(), 'sync_basic': True}, {**payload(), 'symbols': []},
                 {**payload(), 'symbols': ['000001']*6}, {**payload(), 'sync_historical': False}]
        for item in cases:
            with self.assertRaises(eg.TushareEgressStopped): self.begin(item)
        self.assertIsNone(self.state()['active'])
        self.assertEqual(self.sent, [])

    def test_window_inclusive_and_dedup(self):
        value = policy.scope({**payload(), 'symbols': ['000001', '000001.SZ', '600000']})
        self.assertEqual(value['codes'], ['000001.SZ', '600000.SH'])
        self.assertEqual(value['start'], '20260920')
        self.assertEqual(value['end'], '20261019')

    def test_two_calls_counted_and_finish(self):
        job = self.begin()
        for api in ('daily', 'adj_factor'): self.send(job, api)
        job.finish()
        self.assertEqual(self.gate.status()['used_today'], 2)
        policy.open_manual(self.gate)
        self.assertEqual(self.gate.status()['mode'], 'manual')
        with self.assertRaises(eg.TushareEgressStopped): policy.open_manual(self.gate)

    def test_cross_instance_single_task(self):
        self.begin()
        other = policy.DailyGate(self.directory)
        with self.assertRaisesRegex(eg.TushareEgressStopped, 'busy'):
            other.begin(payload(), acceptance=True)

    def test_sdk_cannot_repeat_or_change_symbol(self):
        job = self.begin()
        self.send(job, 'daily')
        with self.assertRaisesRegex(eg.TushareEgressStopped, 'invalid_request'):
            self.send(job, 'daily')
        with self.assertRaises(eg.TushareEgressStopped): self.send(job, 'adj_factor')
        self.assertEqual(len(self.sent), 1)

    def test_forbidden_api_no_attempt(self):
        job = self.begin()
        with self.assertRaises(eg.TushareEgressStopped): self.send(job, 'stock_basic')
        self.assertEqual(len(self.sent), 0)
        self.assertTrue((self.directory/'STOP').exists())

    def test_bad_pagination_no_attempt(self):
        job = self.begin()
        with self.assertRaises(eg.TushareEgressStopped): self.send(job, 'daily', limit=5000)
        self.assertEqual(self.gate.status()['used_today'], 0)

    def test_exception_attempt_not_refunded(self):
        job = self.begin()
        params = dict(ts_code='000001.SZ', start_date=job.approved['start'], end_date=job.approved['end'])
        def failure(): raise eg.TushareEgressStopped('timeout')
        with self.assertRaisesRegex(eg.TushareEgressStopped, 'timeout'):
            job.perform('daily', '', params, failure)
        self.assertEqual(self.gate.status()['used_today'], 1)
        with self.assertRaises(eg.TushareEgressStopped): self.send(job, 'adj_factor')

    def test_inflight_survives_restart(self):
        job = self.begin()
        state = self.state(); state['phase'] = 'in_flight'; self.write(state)
        with self.assertRaisesRegex(eg.TushareEgressStopped, 'previous_attempt_incomplete'):
            policy.DailyGate(self.directory).begin(payload(), acceptance=True)

    def test_rolling_minute_and_daily_cap(self):
        def sleep(seconds): self.clock += seconds
        with patch.object(policy.time, 'sleep', side_effect=sleep):
            for _ in range(5):
                job = self.begin()
                self.send(job, 'daily'); self.send(job, 'adj_factor'); job.finish()
        self.assertGreaterEqual(self.sent[4][1]-self.sent[0][1], 60)
        self.assertGreaterEqual(self.sent[8][1]-self.sent[4][1], 60)
        self.assertEqual(self.gate.status()['used_today'], 10)
        with self.assertRaisesRegex(eg.TushareEgressStopped, 'budget_exhausted'): self.begin()
        self.assertEqual(policy.DailyGate(self.directory).status()['used_today'], 10)

    def test_midnight_not_utc_reset_and_failure_persists(self):
        job = self.begin(); self.send(job, 'daily'); self.send(job, 'adj_factor'); job.finish()
        self.clock += 3600
        self.assertEqual(self.gate.status()['used_today'], 2)
        self.clock += 7200
        self.assertEqual(self.gate.status()['used_today'], 0)
        self.assertEqual(self.gate.status()['request_attempts_total'], 2)
        new = self.begin(); new.fail('timeout')
        self.clock += 86400
        with self.assertRaises(eg.TushareEgressStopped): self.begin()

    def test_expiry_and_backward_clock(self):
        self.clock = NOW-1
        with self.assertRaises(eg.TushareEgressStopped): self.begin()
        self.clock = policy.EXPIRY
        with self.assertRaisesRegex(eg.TushareEgressStopped, 'expired'): self.begin()

    def test_stop_during_call_refuses_next(self):
        job = self.begin()
        params = dict(ts_code='000001.SZ', start_date=job.approved['start'], end_date=job.approved['end'])
        def operation():
            self.gate.stop()
            return table('daily', params)
        with self.assertRaises(eg.TushareEgressStopped): job.perform('daily', '', params, operation)
        self.assertEqual(self.gate.status()['used_today'], 1)

    def test_partial_factors_never_accepted(self):
        job = self.begin(); self.send(job, 'daily')
        params = dict(ts_code='000001.SZ', start_date=job.approved['start'], end_date=job.approved['end'])
        response = table('adj_factor', params); response['items'].pop()
        with self.assertRaisesRegex(eg.TushareEgressStopped, 'response_invalid'):
            job.perform('adj_factor', '', params, lambda: response)
        self.assertEqual(self.state()['phase'], 'blocked')

    def test_context_thread_propagates_but_unrelated_task_does_not(self):
        job = self.begin()
        async def run():
            self.assertIsNone(policy.CURRENT.get())
            with job.context():
                self.assertIs(await asyncio.to_thread(policy.CURRENT.get), job)
                eg.raise_if_egress_blocked()
            self.assertIsNone(await asyncio.to_thread(policy.CURRENT.get))
        asyncio.run(run())

    def test_active_scope_does_not_authorize_unrelated_transport(self):
        self.begin()
        client = eg.MainlandTransport('SYNTHETIC', lambda *_: '')
        with patch.object(client, '_send') as send:
            with self.assertRaises(eg.TushareEgressStopped): client.query('daily')
            send.assert_not_called()

    def test_incomplete_task_requires_review(self):
        job = self.begin(); self.send(job, 'daily')
        with self.assertRaises(eg.TushareEgressStopped): job.finish()
        self.assertIsNotNone(self.state()['active'])
        self.assertEqual(self.state()['phase'], 'blocked')

    def test_scheduler_pause_and_forbid_resume(self):
        col = Mock(update_one=AsyncMock(), count_documents=AsyncMock(return_value=5))
        scheduler = Mock()
        asyncio.run(policy.enforce_manual_jobs(scheduler, types.SimpleNamespace(scheduler_preferences=col)))
        self.assertEqual(scheduler.pause_job.call_count, 5)
        self.assertTrue(all(c.args[1]['$set']['enabled'] is False for c in col.update_one.call_args_list))
        source = (SOURCE/'app/services/scheduler_runtime.py').read_text()
        fn = next(n for n in ast.parse(source).body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'save_enabled')
        ns = {}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), '<save-enabled>', 'exec'), ns)
        with self.assertRaises(ValueError): asyncio.run(ns['save_enabled'](scheduler, None, policy.JOB_IDS[0], True))

    def test_optional_mcp_absence_not_installed(self):
        db = types.SimpleNamespace(mcp_server_configs=Mock(count_documents=AsyncMock(return_value=0)))
        log = Mock()
        with patch('importlib.util.find_spec', return_value=None):
            asyncio.run(policy.restore_optional_mcp(db, log))
        log.info.assert_called_once(); log.error.assert_not_called()
        db.mcp_server_configs.count_documents.return_value = 1
        with patch('importlib.util.find_spec', return_value=None):
            asyncio.run(policy.restore_optional_mcp(db, log))
        log.error.assert_called_once()

    def test_persisted_state_permissions_and_partial_write(self):
        self.assertEqual(self.directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual((self.directory/'state.json').stat().st_mode & 0o777, 0o600)
        (self.directory/'state.next').touch(mode=0o600)
        with self.assertRaises(eg.TushareEgressStopped): self.begin()
        self.assertEqual(len(self.state()['attempts']), 0)

    def test_manual_entry_auth_and_dispatch_before_original_catch(self):
        path = 'app/routers/stock_sync.py'
        source = (SOURCE/path).read_text()
        tree = ast.parse(source)
        for name in ('sync_single_stock', 'sync_batch_stocks'):
            fn = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
            self.assertEqual(ast.unparse(fn.args.defaults[-1]), 'Depends(get_current_user)')
            self.assertIsInstance(fn.body[1], ast.If)
            self.assertIn('submit(request,', ast.unparse(fn.body[1]))
            self.assertIsInstance(fn.body[2], ast.Try)

    def test_partial_batch_failure_never_continues(self):
        job = self.begin({**payload(), 'symbols': ['000001', '600000']})
        self.send(job, 'daily'); self.send(job, 'adj_factor')
        job.fail('interrupted')
        with self.assertRaises(eg.TushareEgressStopped):
            self.send(job, 'daily', ts_code='600000.SH')
        self.assertEqual(len(self.sent), 2)

    @unittest.skipUnless(NATIVE, 'Actual SDK only in isolated native image')
    def test_actual_sdk_and_transport(self):
        import tushare as sdk
        adapter = load('tradingagents.utils.tushare_https', (SOURCE/'tradingagents/utils/tushare_https.py'))
        client = adapter.create_tushare_client('SYNTHETIC')
        job = self.begin()
        sent = []
        def fake_send(_, api, body):
            params = json.loads(body)['params']; sent.append(api)
            return table(api, params)
        with job.context(), patch.object(eg.MainlandTransport, '_send', fake_send):
            with contextlib.redirect_stdout(io.StringIO()):
                frame = sdk.pro_bar(api=client, ts_code='000001.SZ', start_date=job.approved['start'],
                    end_date=job.approved['end'], freq='D', adj='qfq', retry_count=1)
            eg.raise_if_egress_blocked(); policy.validate_frame(frame)
        self.assertEqual(sent, ['daily', 'adj_factor'])
        self.assertEqual(len(frame), 2); job.finish()

    @unittest.skipUnless(NATIVE, 'Actual provider/SDK only in isolated native image')
    def test_actual_provider_one_attempt_and_context(self):
        import tushare as sdk
        import pandas as pd
        adapter = load('tradingagents.utils.tushare_https', (SOURCE/'tradingagents/utils/tushare_https.py'))
        source = (SOURCE/'tradingagents/dataflows/providers/china/tushare.py').read_text()
        cls = next(n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == 'TushareProvider')
        ns = dict(BaseStockDataProvider=object, os=os, asyncio=asyncio, datetime=datetime, pd=pd, ts=sdk,
                  TUSHARE_AVAILABLE=True, TushareEgressStopped=eg.TushareEgressStopped,
                  raise_if_egress_blocked=eg.raise_if_egress_blocked,
                  _manual_current=policy.CURRENT, _manual_frame=policy.validate_frame)
        tree = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), cls], type_ignores=[])
        exec(compile(ast.fix_missing_locations(tree), '<actual-provider>', 'exec'), ns)
        provider = ns['TushareProvider'].__new__(ns['TushareProvider'])
        provider.api = adapter.create_tushare_client('SYNTHETIC')
        provider._get_token_from_database = lambda: 'SYNTHETIC'
        provider._current_token = 'SYNTHETIC'
        provider.connected, provider.config, provider.logger = True, {}, Mock()
        provider._normalize_ts_code = lambda value: value
        provider._format_date = lambda value: value
        provider._is_etf = lambda value: False
        provider._standardize_historical_data = lambda frame: frame
        job = self.begin()
        sent = []
        def rejected(*args):
            sent.append(True)
            raise eg.TushareEgressStopped('usage_limit', 40203)
        async def request():
            with job.context():
                return await provider.get_historical_data('000001.SZ', job.approved['start'], job.approved['end'])
        with patch.object(eg.MainlandTransport, '_send', rejected), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(eg.TushareEgressStopped, 'usage_limit'):
                asyncio.run(request())
        self.assertEqual(len(sent), 1)
        self.assertEqual(self.gate.status()['used_today'], 1)


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(GateTests)
    result = unittest.TextTestRunner(stream=sys.stderr, verbosity=1).run(suite)
    print(json.dumps(dict(tests=result.testsRun, failures=len(result.failures), errors=len(result.errors),
                         skipped=len(result.skipped), external_requests=0, native=NATIVE)))
    sys.exit(not result.wasSuccessful())
