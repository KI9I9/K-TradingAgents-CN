"""Offline SDK adapter/classifier regressions for the guarded mainland transport.

Run as a separate process; uses synthetic values and never starts the app.
TLS, request budgets, persistence and stopping are covered by test_tushare_egress.
"""
import ast
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

from offline_tushare_support import SOURCE, load, egress, deny_external

sys.addaudithook(deny_external)
transport = load('tradingagents.utils.tushare_https', SOURCE/'tradingagents/utils/tushare_https.py')
FAKE = 'SYNTHETIC_TOKEN_NOT_A_CREDENTIAL_20261017'
ROOT = SOURCE


class AdapterTests(unittest.TestCase):
    def test_guarded_transport_keeps_arguments_and_dataframe(self):
        fake = Mock()
        fake.query.return_value = {'fields': ['ts_code', 'close'], 'items': [['FAKE.SZ', 1.25]]}
        with patch.object(transport, 'MainlandTransport', return_value=fake) as factory:
            frame = transport.create_tushare_client(FAKE).daily(fields='ts_code,close', limit=1)
        factory.assert_called_once_with(FAKE, transport._rejection_diagnostic, 30)
        fake.query.assert_called_once_with('daily', fields='ts_code,close', limit=1)
        self.assertEqual(frame.to_dict('records'), [{'ts_code': 'FAKE.SZ', 'close': 1.25}])

    def test_stopped_transport_is_propagated_without_fallback(self):
        fake = Mock()
        error = egress.TushareEgressStopped('usage_limit', 40203)
        fake.query.side_effect = error
        with patch.object(transport, 'MainlandTransport', return_value=fake) as factory:
            with self.assertRaises(egress.TushareEgressStopped) as caught:
                transport.create_tushare_client(FAKE).daily()
        self.assertIs(caught.exception, error)
        self.assertEqual(factory.call_count, 1)
        self.assertEqual(fake.query.call_count, 1)
        self.assertNotIn(FAKE, str(caught.exception))

    def test_invalid_token_and_private_attributes_fail_locally(self):
        for token in ('', None, 123, '   '):
            with self.assertRaisesRegex(egress.TushareEgressStopped, 'token_invalid'):
                transport.create_tushare_client(token)
        api = transport.create_tushare_client(FAKE)
        with self.assertRaises(AttributeError):
            api._unexpected_private_attribute
        self.assertNotIn(FAKE, repr(api))

    def test_10_both_callsites_use_shared_factory(self):
        paths = ('app/services/config_service.py', 'tradingagents/dataflows/providers/china/tushare.py')
        for path in paths:
            tree = ast.parse((ROOT / path).read_text())
            self.assertEqual(sum(isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                                 and n.func.id == 'create_tushare_client' for n in ast.walk(tree)), 1)
            self.assertFalse(any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                                 and n.func.attr == 'pro_api' for n in ast.walk(tree)))



class ClassifierTests(unittest.TestCase):
    def setUp(self):
        self.api = transport.create_tushare_client(FAKE)

    def assert_rejection(self, payload, expected, api=None):
        token = (api or self.api)._HttpsTushareClient__token
        diagnostic = transport._rejection_diagnostic(payload, token)
        self.assertEqual(diagnostic, expected)
        self.assertNotIn(token, diagnostic)
        self.assertNotIn('PRIVATE_REPLY_SENTINEL', diagnostic)

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


    def test_16_only_small_integer_code_is_reported(self):
        for code in (10**40, True, '40203', None, 40203.0):
            self.assert_rejection({'code': code, 'msg': FAKE, 'token': FAKE},
                'tushare_api_rejected code=unknown reason=unclassified window=unknown')

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



if __name__ == '__main__':
    unittest.main(verbosity=2)
