"""Actual patched methods + existing permit/transport; synthetic responses only.

No app startup, MongoDB, Redis, real Token, SDK installation or external sockets.
The unchanged SDK is represented by an explicit two-call qfq contract stub.
Thread dispatch is awaited inline so sandbox thread wakeups cannot hang the
harness; this does not test native executor scheduling or modify production.
"""
import ast
import asyncio
import difflib
import faulthandler
from datetime import datetime, date, timedelta
import hashlib
import importlib.util
import json
import os
import signal
from pathlib import Path
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

from offline_tushare_support import SOURCE, egress, policy, authorize_new, deny_external

fix = types.SimpleNamespace(PROVIDER='tradingagents/dataflows/providers/china/tushare.py',
    SERVICE='app/worker/tushare_sync_service.py', ROUTER='app/routers/multi_source_sync.py')

sys.addaudithook(deny_external)
TEST_ROOT = Path(tempfile.mkdtemp(prefix='ta-history-readiness-offline-'))
AFTER = {p: (SOURCE/p).read_text() for p in (fix.PROVIDER, fix.SERVICE, fix.ROUTER)}
FAKE = 'SYNTHETIC_DB_TOKEN_NOT_A_SECRET'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


admin = types.SimpleNamespace(authorize_new=authorize_new)


class Frame:
    empty = False
    def __len__(self): return 1


def instance(path, classname, **extra):
    tree = ast.parse(AFTER[path])
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == classname)
    module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), cls], type_ignores=[])
    ns = dict(BaseStockDataProvider=object, os=os, asyncio=asyncio, datetime=datetime,
              date=date, timedelta=timedelta, logger=Mock(), TUSHARE_AVAILABLE=True,
              TushareEgressStopped=egress.TushareEgressStopped,
              raise_if_egress_blocked=egress.raise_if_egress_blocked,
              _manual_current=policy.CURRENT, _manual_frame=policy.validate_frame, **extra)
    exec(compile(ast.fix_missing_locations(module), '<reviewed-patched-class>', 'exec'), ns)
    return ns[classname].__new__(ns[classname]), ns


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.directory = TEST_ROOT / self.id().split('.')[-1]
        admin.authorize_new(self.directory, 2, 300, ['daily', 'adj_factor', 'stock_basic'])
        self.permit = egress.Permit(self.directory)
        self.sent, self.sdk_options = [], []
        self.reject = None
        outer = self

        class FakeHTTP:
            def __init__(self, timeout): self._expired = threading.Event()
            def connect(self): pass
            def request(self, method, path, **kwargs):
                payload = json.loads(kwargs['body'])
                self.api = payload['api_name']
                outer.sent.append(self.api)
            def getresponse(self):
                body = ({'code': 40203, 'msg': 'synthetic_limit'} if outer.reject == self.api else
                        {'code': 0, 'data': {'fields': ['synthetic'], 'items': [[1]]}})
                raw = json.dumps(body).encode()
                return types.SimpleNamespace(status=200, read=lambda n: raw[:n])
            def close(self): pass

        class FakeClient:
            def __init__(self, token):
                self.transport = egress.MainlandTransport(token, lambda *_:
                    'tushare_api_rejected code=40203 reason=usage_limit window=unknown',
                    permit=outer.permit)
            def query(self, api, **kwargs):
                self.transport.query(api, **kwargs)
                return Frame()
            def stock_basic(self, **kwargs): return self.query('stock_basic', **kwargs)

        def pro_bar(**kwargs):
            self.sdk_options.append(kwargs)
            self.assertEqual(kwargs['adj'], 'qfq')
            api = kwargs['api']
            params = {k: kwargs[k] for k in ('ts_code', 'start_date', 'end_date')}
            api.query('daily', **params)
            api.query('adj_factor', **params)
            return Frame()

        self.factory = Mock(side_effect=FakeClient)
        self.sdk = types.SimpleNamespace(pro_bar=Mock(side_effect=pro_bar))
        self.provider, self.ns = instance(fix.PROVIDER, 'TushareProvider',
            create_tushare_client=self.factory, ts=self.sdk)
        p = self.provider
        p.api, p.config, p.connected, p.logger = None, {}, False, Mock()
        p._get_token_from_database = Mock(return_value=FAKE)
        p._normalize_ts_code = lambda s: s
        p._format_date = lambda s: s
        p._is_etf = lambda s: False
        p._standardize_historical_data = lambda frame: frame
        async def inline_to_thread(function, *args, **kwargs):
            return function(*args, **kwargs)
        for context in (patch.dict(os.environ, {'TA_TUSHARE_ROUTE': 'mainland_unix'}, clear=True),
                        patch.object(egress, 'Permit', return_value=self.permit),
                        patch.object(egress, 'UnixTLSConnection', FakeHTTP),
                        patch.object(asyncio, 'to_thread', inline_to_thread)):
            context.start(); self.addCleanup(context.stop)

    def state(self):
        with self.permit.locked() as fd: return self.permit.read(fd)

    def history(self):
        return asyncio.run(self.provider.get_historical_data('SYNTHETIC.SZ', '20260101', '20260102'))

    def test_connect_does_not_probe_or_reserve(self):
        self.reject = 'stock_basic'
        self.assertTrue(self.provider.connect_sync())
        self.assertTrue(self.provider.connected)
        self.assertEqual(self.sent, [])
        self.assertEqual(self.state()['used'], 0)

    def test_repeated_sync_async_connect_reuses_client_without_calls(self):
        self.assertTrue(self.provider.connect_sync())
        first = self.provider.api
        self.assertTrue(asyncio.run(self.provider.connect()))
        self.assertTrue(self.provider.is_available())
        self.assertIs(self.provider.api, first)
        self.assertEqual(self.factory.call_count, 1)
        self.assertEqual(self.sent, [])

    def test_database_token_precedence_and_refresh_stay_intact(self):
        self.provider.config = {'token': 'SYNTHETIC_ENV_TOKEN'}
        self.assertTrue(self.provider.connect_sync())
        self.factory.assert_called_once_with(FAKE)
        first = self.provider.api
        self.provider._get_token_from_database.return_value = 'SYNTHETIC_CHANGED_TOKEN'
        self.assertTrue(self.provider.connect_sync())
        self.assertIsNot(self.provider.api, first)
        self.assertEqual(self.factory.call_count, 2)
        self.assertEqual(self.sent, [])

    def test_missing_token_still_fails_locally(self):
        self.provider._get_token_from_database.return_value = None
        self.assertFalse(self.provider.connect_sync())
        self.assertFalse(self.provider.connected)
        self.factory.assert_not_called()
        self.assertEqual(self.sent, [])

    def test_missing_sdk_still_fails_locally(self):
        self.ns['TUSHARE_AVAILABLE'] = False
        self.assertFalse(self.provider.connect_sync())
        self.factory.assert_not_called()

    def test_explicit_validation_still_calls_stock_basic(self):
        self.assertTrue(self.provider.connect_sync(validate=True))
        self.assertEqual(self.sent, ['stock_basic'])

    def test_basic_sync_keeps_real_request_and_propagates_rejection(self):
        self.reject = 'stock_basic'
        self.assertTrue(self.provider.connect_sync())
        self.provider.config = {'token': 'SYNTHETIC_OTHER_TOKEN'}
        with self.assertRaises(egress.TushareEgressStopped) as caught:
            self.provider.get_stock_list_sync(market='CN')
        self.assertEqual((caught.exception.reason, caught.exception.api_code), ('usage_limit', 40203))
        self.assertEqual(self.sent, ['stock_basic'])
        self.assertEqual(self.factory.call_count, 1)

    def test_history_succeeds_without_stock_basic_and_keeps_qfq(self):
        self.reject = 'stock_basic'
        self.assertTrue(asyncio.run(self.provider.connect()))
        self.assertIsInstance(self.history(), Frame)
        self.assertEqual(self.sent, ['daily', 'adj_factor'])
        self.assertEqual(self.state()['used'], 2)
        self.assertEqual(self.sdk_options[0]['adj'], 'qfq')

    def test_daily_rejection_stops_before_factor_and_no_request_retry(self):
        self.reject = 'daily'
        self.provider.config = {'token': 'SYNTHETIC_OTHER_TOKEN'}
        for _ in range(2):
            with self.assertRaises(egress.TushareEgressStopped): self.history()
        self.assertEqual(self.sent, ['daily'])
        self.assertEqual(self.state()['used'], 1)
        self.assertEqual(self.factory.call_count, 1)

    def test_factor_rejection_never_returns_unadjusted_success(self):
        self.reject = 'adj_factor'
        with self.assertRaises(egress.TushareEgressStopped): self.history()
        self.assertEqual(self.sent, ['daily', 'adj_factor'])
        self.assertEqual(self.state()['phase'], 'blocked')

    def test_sdk_suppressed_error_is_not_success(self):
        self.reject = 'daily'
        original = self.sdk.pro_bar.side_effect
        def swallowed(**kwargs):
            try: return original(**kwargs)
            except egress.TushareEgressStopped: return None
        self.sdk.pro_bar.side_effect = swallowed
        with self.assertRaises(egress.TushareEgressStopped): self.history()
        self.assertEqual(self.sent, ['daily'])

    def test_sdk_replaced_error_retains_original_guard_cause(self):
        self.reject = 'daily'
        original = self.sdk.pro_bar.side_effect
        def replaced(**kwargs):
            try: return original(**kwargs)
            except egress.TushareEgressStopped: raise OSError('SYNTHETIC_SDK_ERROR')
        self.sdk.pro_bar.side_effect = replaced
        with self.assertRaises(egress.TushareEgressStopped) as caught: self.history()
        self.assertEqual(caught.exception.reason, 'usage_limit')
        self.assertEqual(self.sent, ['daily'])

    def test_stop_is_not_bypassed_by_readiness(self):
        self.permit.stop()
        self.assertTrue(self.provider.connect_sync())
        with self.assertRaises(egress.TushareEgressStopped) as caught: self.history()
        self.assertEqual(caught.exception.reason, 'disabled')
        self.assertEqual(self.sent, [])
        self.assertEqual(self.state()['used'], 0)

    def test_exhausted_budget_cannot_be_reopened_by_connect(self):
        self.assertIsInstance(self.history(), Frame)
        self.assertTrue(self.provider.connect_sync())
        with self.assertRaises(egress.TushareEgressStopped): self.history()
        self.assertEqual(self.sent, ['daily', 'adj_factor'])
        self.assertEqual(self.state()['used'], 2)

    def test_expired_permit_still_blocks(self):
        with patch.object(egress.time, 'time', return_value=self.state()['expires_at'] + 1):
            with self.assertRaises(egress.TushareEgressStopped) as caught: self.history()
        self.assertEqual(caught.exception.reason, 'expired')
        self.assertEqual(self.sent, [])

    def test_initialize_means_readiness_not_network_success(self):
        service, ns = instance(fix.SERVICE, 'TushareSyncService',
            get_historical_data_service=AsyncMock(return_value=object()),
            get_news_data_service=AsyncMock(return_value=object()))
        service.provider = self.provider
        self.reject = 'stock_basic'
        asyncio.run(service.initialize())
        self.assertEqual(self.sent, [])
        ns['get_historical_data_service'].assert_awaited_once()
        self.assertIn('尚未验证上游接口权限', ns['logger'].info.call_args.args[0])

    def test_initialize_missing_token_is_not_reported_as_network_failure(self):
        service, ns = instance(fix.SERVICE, 'TushareSyncService',
            get_historical_data_service=AsyncMock(), get_news_data_service=AsyncMock())
        service.provider = self.provider
        self.provider._get_token_from_database.return_value = None
        with self.assertRaisesRegex(RuntimeError, '客户端未就绪'):
            asyncio.run(service.initialize())
        ns['get_historical_data_service'].assert_not_called()
        self.assertEqual(self.sent, [])




    def router_check(self, adapter=None):
        if adapter is None:
            adapter = types.SimpleNamespace(name='tushare', priority=3, _provider=self.provider,
                is_available=Mock(side_effect=lambda: self.provider.connected and self.provider.api is not None),
                get_token_source=lambda: getattr(self.provider, 'token_source', None))
        tree = ast.parse(AFTER[fix.ROUTER])
        fn = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == '_test_single_adapter')
        ns = dict(asyncio=asyncio, logger=Mock())
        exec(compile(ast.Module(body=[fn], type_ignores=[]), '<patched-explicit-test>', 'exec'), ns)
        return asyncio.run(ns['_test_single_adapter'](adapter)), adapter

    def test_explicit_ui_test_really_checks_stock_basic(self):
        result, _ = self.router_check()
        self.assertTrue(result['available'])
        self.assertEqual(result['verification_api'], 'stock_basic')
        self.assertIn('不代表其他接口权限', result['message'])
        self.assertEqual(self.sent, ['stock_basic'])

    def test_explicit_ui_rejection_is_not_reported_as_connected(self):
        self.reject = 'stock_basic'
        result, _ = self.router_check()
        self.assertFalse(result['available'])
        self.assertIn('stock_basic 检查失败', result['message'])
        self.assertIn('usage_limit', result['message'])
        self.assertEqual(self.sent, ['stock_basic'])

    def test_explicit_ui_false_result_cannot_be_overridden_by_readiness(self):
        self.provider._get_token_from_database.return_value = None
        result, adapter = self.router_check()
        self.assertFalse(result['available'])
        self.assertIn('stock_basic 检查未通过', result['message'])
        adapter.is_available.assert_not_called()
        self.assertEqual(self.sent, [])

    def test_explicit_ui_test_respects_production_style_stop(self):
        self.permit.stop()
        result, _ = self.router_check()
        self.assertFalse(result['available'])
        self.assertIn('disabled', result['message'])
        self.assertEqual(self.sent, [])

    def test_other_data_source_ui_behavior_unchanged(self):
        result, _ = self.router_check(types.SimpleNamespace(name='other_source', priority=1,
                                                            is_available=lambda: True))
        self.assertTrue(result['available'])
        self.assertEqual(result['message'], '✅ 连接成功')
        self.assertNotIn('verification_api', result)
        self.assertEqual(self.sent, [])


if __name__ == '__main__':
    # Bound the local harness too; a stuck test must not become an endless job.
    faulthandler.dump_traceback_later(15)
    signal.alarm(25)
    print(json.dumps(dict(synthetic_artifacts_retained=str(TEST_ROOT), network='denied',
                          real_credentials=False, business_writes=False, deployment=False)), flush=True)
    try:
        unittest.main(verbosity=2)
    finally:
        faulthandler.cancel_dump_traceback_later()
        signal.alarm(0)
