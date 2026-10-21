"""Offline transport tests: fake Token only, no app initialization or sockets."""
import ast
import contextlib
import datetime
import importlib.metadata
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import traceback
import unittest
from unittest.mock import patch


def deny_network(event, args):
    if event in {'socket.connect', 'socket.getaddrinfo', 'socket.sendto', 'subprocess.Popen'}:
        raise RuntimeError('OfflineNetworkBlocked')


sys.addaudithook(deny_network)
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent
FAKE = 'SYNTHETIC_TOKEN_NOT_A_CREDENTIAL_20261017'
SPEC = importlib.util.spec_from_file_location('https_transport', ROOT / 'tradingagents/utils/tushare_https.py')
transport = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(transport)


def response(data=None, status=200):
    res = requests.Response()
    res.status_code = status
    res._content = json.dumps(data if data is not None else {
        'code': 0, 'data': {'fields': ['ts_code', 'close'], 'items': [['FAKE.SZ', 1.25]]}
    }).encode()
    # Match a fully buffered real requests response so close() does not attempt
    # to close a nonexistent raw socket in this socket-free fixture.
    res._content_consumed = True
    return res


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.sent = []
        self.reply = response()
        self.error = None
        def send(session, request, **kwargs):
            self.sent.append((session, request, kwargs))
            if self.error is not None:
                raise self.error
            return self.reply
        self.sender = patch.object(requests.Session, 'send', send)
        self.sender.start()
        self.addCleanup(self.sender.stop)
        self.api = transport.create_tushare_client(FAKE)

    def test_01_https_verified_no_redirect_proxy_netrc_or_env_changes(self):
        fake_env = {'HTTP_PROXY': 'http://proxy.invalid:9999', 'HTTPS_PROXY': 'http://proxy.invalid:9999',
                    'ALL_PROXY': 'http://proxy.invalid:9999', 'REQUESTS_CA_BUNDLE': '/no/such/ca',
                    'NETRC': '/no/such/netrc'}
        with patch.dict(os.environ, fake_env), patch('requests.sessions.get_netrc_auth', side_effect=AssertionError('netrc read')):
            before = dict(os.environ)
            df = self.api.stock_basic(fields='ts_code,close', limit=1)
            self.assertEqual(dict(os.environ), before)
        self.assertEqual(df.to_dict('records'), [{'ts_code': 'FAKE.SZ', 'close': 1.25}])
        self.assertEqual(len(self.sent), 1)
        session, req, opts = self.sent[0]
        self.assertFalse(session.trust_env)
        self.assertEqual(req.url, 'https://api.waditu.com/dataapi/stock_basic')
        self.assertEqual(req.method, 'POST')
        self.assertIs(opts['verify'], True)
        self.assertIs(opts['allow_redirects'], False)
        self.assertEqual(opts['proxies'], {})
        self.assertEqual(opts['timeout'], 30)
        self.assertNotIn('Authorization', req.headers)
        self.assertNotIn('Cookie', req.headers)
        self.assertEqual(session.get_adapter(req.url).max_retries.total, 0)

    def test_02_payload_and_dataframe_match_installed_sdk(self):
        dist = importlib.metadata.distribution('tushare')
        self.assertEqual(dist.version, '1.4.29')
        spec = importlib.util.spec_from_file_location('pinned_sdk_client', dist.locate_file('tushare/pro/client.py'))
        sdk = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(sdk)
        captured = []
        def old_post(url, **kwargs):
            captured.append((url, kwargs))
            return response()
        with patch.object(sdk.requests, 'post', old_post):
            old = sdk.DataApi(FAKE).query('trade_cal', fields='ts_code,close', exchange='SSE')
        new = self.api.query('trade_cal', fields='ts_code,close', exchange='SSE')
        pd.testing.assert_frame_equal(old, new)
        _, request, opts = self.sent[0]
        self.assertEqual(request.url, captured[0][0].replace('http://', 'https://', 1))
        self.assertEqual(json.loads(request.body), captured[0][1]['json'])
        self.assertEqual(opts['timeout'], captured[0][1]['timeout'])

    def test_03_explicit_discriminator_and_empty_data(self):
        self.reply = response({'code': 0, 'data': {'fields': ['ts_code'], 'items': []}})
        df = self.api.daily(ts_type_name='SYNTHETIC_DISCRIMINATOR')
        self.assertTrue(df.empty)
        self.assertEqual(list(df.columns), ['ts_code'])
        self.assertEqual(json.loads(self.sent[0][1].body)['params']['ts_type_name'], 'SYNTHETIC_DISCRIMINATOR')

    def test_04_redirects_do_not_forward_token_or_retry(self):
        for status in (301, 302, 303, 307, 308):
            with self.subTest(status=status):
                self.sent.clear()
                self.reply = response({'msg': FAKE}, status=status)
                self.reply.headers['Location'] = 'http://untrusted.invalid/' + FAKE
                with self.assertRaisesRegex(transport.TushareTransportError, '^tushare_redirect_blocked$'):
                    self.api.stock_basic()
                self.assertEqual(len(self.sent), 1)

    def test_05_http_and_api_errors_never_echo_server_body(self):
        for reply, category in ((response({'msg': FAKE}, 401), 'tushare_http_error'),
                                (response({'code': -1, 'msg': FAKE}), 'tushare_api_rejected code=-1 reason=unclassified window=unknown'),
                                (response({'code': FAKE}), 'tushare_response_invalid'),
                                (response({'code': 0, 'data': FAKE}), 'tushare_response_invalid')):
            self.reply = reply
            with self.assertRaisesRegex(transport.TushareTransportError, '^' + category + '$'):
                self.api.trade_cal()

    def test_06_tls_timeouts_network_errors_sanitized_and_no_fallback(self):
        for error, category in ((requests.exceptions.SSLError(FAKE), 'tushare_tls_failed'),
                                (requests.exceptions.Timeout(FAKE), 'tushare_timeout'),
                                (requests.exceptions.ConnectionError(FAKE), 'tushare_network_failed')):
            self.sent.clear()
            self.error = error
            try:
                self.api.daily()
            except transport.TushareTransportError as caught:
                self.assertEqual(str(caught), category)
                self.assertNotIn(FAKE, ''.join(traceback.format_exception(caught)))
            else:
                self.fail('Expected safe failure')
            self.assertEqual(len(self.sent), 1)
            self.assertTrue(self.sent[0][1].url.startswith('https://'))

    def test_07_bad_json_sanitized(self):
        self.reply._content = FAKE.encode()
        with self.assertRaisesRegex(transport.TushareTransportError, '^tushare_response_invalid$'):
            self.api.daily()

    def test_08_no_implicit_token_persistence_and_invalid_api_name(self):
        for bad in ('', None, 123, '   '):
            with self.assertRaisesRegex(transport.TushareTransportError, '^tushare_token_missing$'):
                transport.create_tushare_client(bad)
        for name in ('../token', '//evil.invalid', 'daily?x=1', 'https://evil.invalid', ''):
            with self.assertRaisesRegex(transport.TushareTransportError, '^tushare_api_name_invalid$'):
                self.api.query(name)
        self.assertEqual(self.sent, [])
        self.assertNotIn(FAKE, repr(self.api))

    def test_09_sessions_are_not_shared(self):
        self.api.daily()
        self.api.daily()
        self.assertIsNot(self.sent[0][0], self.sent[1][0])

    def test_10_both_callsites_use_shared_factory(self):
        paths = ('app/services/config_service.py', 'tradingagents/dataflows/providers/china/tushare.py')
        for path in paths:
            tree = ast.parse((ROOT / path).read_text())
            self.assertEqual(sum(isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                                 and n.func.id == 'create_tushare_client' for n in ast.walk(tree)), 1)
            self.assertFalse(any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                                 and n.func.attr == 'pro_api' for n in ast.walk(tree)))

    def test_11_real_sdk_pro_bar_consumes_adapted_api_without_own_client(self):
        dist = importlib.metadata.distribution('tushare')
        tree = ast.parse(Path(dist.locate_file('tushare/pro/data_pro.py')).read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'pro_bar')
        namespace = {'datetime': datetime, 'FORMAT': lambda x: '%.2f' % x}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), '<pinned-pro-bar>', 'exec'), namespace)
        self.reply = response({'code': 0, 'data': {'fields': ['ts_code', 'trade_date', 'close'],
                               'items': [['FAKE.SZ', '20260102', 2.0], ['FAKE.SZ', '20260101', 1.0]]}})
        with contextlib.redirect_stdout(io.StringIO()) as capture:
            df = namespace['pro_bar'](ts_code='FAKE.SZ', api=self.api, start_date='20260101', end_date='20260102',
                                      freq='D', adj=None, fields='ts_code,trade_date,close')
        self.assertEqual(len(df), 2)
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(json.loads(self.sent[0][1].body)['api_name'], 'daily')
        self.assertEqual(capture.getvalue(), '')

    def assert_rejection(self, payload, expected, api=None):
        self.reply = response(payload)
        start_count = len(self.sent)
        try:
            (api or self.api).stock_basic(list_status='L', limit=1)
        except transport.TushareTransportError as caught:
            self.assertEqual(str(caught), expected)
            self.assertEqual(caught.args, (expected,))
            self.assertFalse(vars(caught))
            # Standard traceback rendering must not expose response or Token.
            rendered = ''.join(traceback.format_exception(caught))
            self.assertNotIn(FAKE, rendered)
            self.assertNotIn('PRIVATE_REPLY_SENTINEL', rendered)
        else:
            self.fail('Expected safe API rejection')
        self.assertEqual(len(self.sent) - start_count, 1)

    def test_12_usage_ceiling_without_invented_window_or_code_mapping(self):
        self.assert_rejection(
            {'code': 40203, 'msg': '该接口调用次数已达到上限 PRIVATE_REPLY_SENTINEL ' + FAKE},
            'tushare_api_rejected code=40203 reason=usage_limit window=unknown')
        self.assert_rejection(
            {'code': 40203, 'msg': 'PRIVATE_REPLY_SENTINEL'},
            'tushare_api_rejected code=40203 reason=unclassified window=unknown')

    def test_13_frequency_windows_and_permission_document_not_denial(self):
        for text, window in (
            ('每分钟最多访问该接口50次，权限的具体详情访问官方文档', 'minute'),
            ('每小时只能调用一次', 'hour'),
            ('每天最多调用一次', 'day'),
            ('每秒最多调用一次', 'second'),
            ('每分钟最多5次，每天最多100次', 'unknown'),
            ('每60分钟最多调用一次', 'minute'),
        ):
            with self.subTest(window=window):
                self.assert_rejection({'code': 40203, 'msg': text},
                    'tushare_api_rejected code=40203 reason=usage_limit window=' + window)

    def test_14_permissions_points_and_other_fixed_categories(self):
        for message, reason in (
            ('您没有访问该接口的权限', 'permission_denied'),
            ('积分不足2000', 'insufficient_points'),
            ('积分不足，您没有访问该接口的权限', 'insufficient_points,permission_denied'),
            ('token已经过期', 'token_invalid'),
            ('IP地址被限制', 'ip_restriction'),
            ('参数错误', 'invalid_parameters'),
            ('系统异常', 'service_unavailable'),
        ):
            with self.subTest(reason=reason):
                self.assert_rejection({'code': 2002, 'msg': message},
                    'tushare_api_rejected code=2002 reason=' + reason + ' window=unknown')

    def test_15_missing_malformed_oversized_and_escaped_messages(self):
        for msg in (None, 42, False, {'message': FAKE}, [FAKE], '次数已达上限' * 1000,
                    r'\u79ef\u5206\u4e0d\u8db3', 'PRIVATE_REPLY_SENTINEL\n' + FAKE):
            self.assert_rejection({'code': 40203, 'msg': msg},
                'tushare_api_rejected code=40203 reason=unclassified window=unknown')
        self.assert_rejection({'code': 40203},
            'tushare_api_rejected code=40203 reason=unclassified window=unknown')

    def test_16_only_small_integer_code_and_no_echoed_secret_attributes(self):
        self.assert_rejection({'code': 10**40, 'msg': FAKE, 'token': FAKE, 'data': {'private': FAKE}},
            'tushare_api_rejected code=unknown reason=unclassified window=unknown')
        for code in (True, '40203', None, 40203.0):
            self.reply = response({'code': code, 'msg': '调用次数已达上限 ' + FAKE})
            with self.assertRaisesRegex(transport.TushareTransportError, '^tushare_response_invalid$'):
                self.api.stock_basic()

    def test_17_echoed_token_cannot_invent_a_reason(self):
        fake_token = 'synthetic-调用次数已达上限-token'
        api = transport.create_tushare_client(fake_token)
        self.assert_rejection({'code': 40203, 'msg': fake_token},
            'tushare_api_rejected code=40203 reason=unclassified window=unknown', api=api)
        unicode_token = 'ＳＹＮＴＨＥＴＩＣ-调用次数已达上限-token'
        api = transport.create_tushare_client(unicode_token)
        self.assert_rejection({'code': 40203, 'msg': 'SYNTHETIC-调用次数已达上限-token'},
            'tushare_api_rejected code=40203 reason=unclassified window=unknown', api=api)

    def test_18_reason_fields_and_full_width_numeric_normalization(self):
        self.assert_rejection({'code': 40203, 'message': '每１小时最多访问一次'},
            'tushare_api_rejected code=40203 reason=usage_limit window=hour')
        self.assert_rejection({'code': 40203, 'detail': 'Quota exhausted'},
            'tushare_api_rejected code=40203 reason=usage_limit window=unknown')

    def test_19_success_messages_do_not_change_success_path(self):
        self.reply = response({'code': 0, 'msg': '调用次数已达上限 ' + FAKE,
            'data': {'fields': ['value'], 'items': [[1]]}})
        self.assertEqual(self.api.stock_basic().to_dict('records'), [{'value': 1}])
        self.assertEqual(len(self.sent), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
