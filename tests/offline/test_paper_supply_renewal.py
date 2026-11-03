"""Exercise real permit replacement, expiry checks and rollback ownership."""
from datetime import datetime, timedelta
import fcntl
import inspect
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT/'deploy'))
import renew_paper_quote_supply as renewal
from paper_supply_opening import validate_quote
from app.services import paper_quote_supply as supply
from app.services.quote_contract import CN_TZ

NOW = datetime(2026, 11, 3, 14, 0, tzinfo=CN_TZ)


class Renewal(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name); self.path = self.root/'permit.json'
        self.old = renewal.encoded(dict(version=1, enabled=True, source='sina_l1',
            scope='paper_execution_sh_sz', not_before=(NOW-timedelta(hours=1)).isoformat(),
            expires_at=(NOW+timedelta(hours=23)).isoformat()))
        self.path.write_bytes(self.old); self.path.chmod(0o600)
        self.payload = dict(previous_sha256=renewal.digest(self.old),
            expires_at='2026-11-08T14:00:00+08:00', backup=str(self.root/'backup'),
            auth={'password': 'SECRET_CANARY'})
        for patcher in [patch.object(supply, 'cn_now', return_value=NOW),
                        patch.object(renewal.os, 'geteuid', return_value=1000),
                        patch.object(renewal, 'snapshot', side_effect=self.snapshot),
                        patch.object(renewal, 'accept_quotes', return_value=[{'verified': True}])]:
            patcher.start(); self.addCleanup(patcher.stop)

    def snapshot(self, payload, backup=None):
        return dict(modules={'module': 'hash'}, runtime={
            str(renewal.PERMIT): renewal.digest(self.path.read_bytes()), 'history': 'unchanged'},
            collections={'paper_orders': {'count': 0, 'sha256': 'same', 'raw_sha256': 'same'}}, used=11178)

    def run_renewal(self):
        report = renewal.renew(self.payload, self.path)
        self.assertNotIn('SECRET_CANARY', json.dumps(report))
        return report

    def test_explicit_finite_renewal_backups_and_reads_actual_new_permit(self):
        report = self.run_renewal()
        self.assertEqual(report['status'], 'renewed', report)
        doc = supply.read_permit(NOW, self.path)
        self.assertEqual(doc['renewal_of_sha256'], renewal.digest(self.old))
        self.assertEqual(doc['expires_at'], self.payload['expires_at'])
        self.assertEqual(doc['scope'], 'paper_execution_sh_sz')
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual((self.root/'backup/previous-permit.json').read_bytes(), self.old)
        self.assertIsNone(supply.read_permit(NOW+timedelta(days=5), self.path))
        self.assertFalse(self.run_renewal()['ok'])  # Exact previous hash prevents replay.

    def test_precheck_does_not_replace_or_fetch_quotes(self):
        self.payload['check_only'] = True
        self.assertEqual(self.run_renewal()['status'], 'ready_no_renewal')
        self.assertEqual(self.path.read_bytes(), self.old)
        self.assertFalse((self.root/'backup').exists())
        renewal.accept_quotes.assert_not_called()

    def test_large_protected_manifest_is_backed_up_without_permit_size_limit(self):
        def large(payload, backup=None):
            value = self.snapshot(payload, backup)
            value['runtime'].update({f'history-{i}': 'a'*64 for i in range(6000)})
            return value
        renewal.snapshot.side_effect = large
        self.assertEqual(self.run_renewal()['status'], 'renewed')
        self.assertGreater((self.root/'backup/before.json').stat().st_size, 4096)

    def test_previous_hash_drift_refuses_before_snapshot(self):
        self.payload['previous_sha256'] = 'f'*64
        self.assertEqual(self.run_renewal()['reason'], 'previous_permit_changed')
        self.assertEqual(self.path.read_bytes(), self.old)
        renewal.snapshot.assert_not_called()

    def test_expiry_never_implicit_or_unbounded(self):
        for expiry in ('bad', NOW.isoformat(), (NOW+timedelta(days=8)).isoformat(),
                       (NOW+timedelta(hours=1)).isoformat()):
            self.payload['expires_at'] = expiry
            self.assertFalse(self.run_renewal()['ok'])
            self.assertEqual(self.path.read_bytes(), self.old)
        renewal.accept_quotes.assert_not_called()

    def test_quote_acceptance_failure_restores_exact_original(self):
        renewal.accept_quotes.side_effect = ValueError('SECRET_CANARY')
        report = self.run_renewal()
        self.assertEqual(report['status'], 'previous_permit_restored', report)
        self.assertEqual(self.path.read_bytes(), self.old)
        self.assertFalse((self.root/'backup/completion.json').exists())

    def test_snapshot_drift_restores_previous_permit(self):
        def changed(payload, backup=None):
            value = self.snapshot(payload, backup)
            if self.path.read_bytes() != self.old:
                value['used'] += 1
            return value
        renewal.snapshot.side_effect = changed
        self.assertEqual(self.run_renewal()['status'], 'previous_permit_restored')
        self.assertEqual(self.path.read_bytes(), self.old)

    def test_error_after_replace_is_also_rolled_back(self):
        actual = renewal.replace_owned; calls = []
        def replace(path, before, after):
            actual(path, before, after); calls.append(1)
            if len(calls) == 1:
                raise OSError('fsync failed after publication')
        with patch.object(renewal, 'replace_owned', side_effect=replace):
            self.assertEqual(self.run_renewal()['status'], 'previous_permit_restored')
        self.assertEqual(self.path.read_bytes(), self.old)

    def test_unknown_concurrent_replacement_is_preserved(self):
        def acceptance(*_):
            self.path.write_bytes(b'unexpected operator replacement')
            raise ValueError('failed')
        renewal.accept_quotes.side_effect = acceptance
        self.assertEqual(self.run_renewal()['status'], 'rollback_requires_attention')
        self.assertEqual(self.path.read_bytes(), b'unexpected operator replacement')

    def test_busy_lock_and_symlink_refused(self):
        with self.path.with_suffix('.renewal.lock').open('w') as stream:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertFalse(self.run_renewal()['ok'])
            renewal.snapshot.assert_not_called()
        target = self.root/'target'; self.path.rename(target); self.path.symlink_to(target)
        self.assertFalse(self.run_renewal()['ok'])
        self.assertEqual(target.read_bytes(), self.old)

    def test_expired_previous_permit_is_not_implicitly_reopened(self):
        with patch.object(supply, 'cn_now', return_value=NOW+timedelta(days=1)):
            self.assertEqual(self.run_renewal()['reason'], 'previous_permit_not_active')
        self.assertEqual(self.path.read_bytes(), self.old)

    def test_actual_api_acceptance_only_logs_in_and_reads_two_quotes(self):
        # Execute the actual adapter and shared validator, with a local fake HTTP function.
        query_code = '''
def request(path, token=None, body=None):
    assert '/orders' not in path
    if path == '/api/auth/login':
        return {'success': True, 'data': {'access_token': 'TOKEN_CANARY'}}
    assert path in ('/api/paper/quote/600000?market=CN', '/api/paper/quote/159865?market=ETF')
    assert token == 'TOKEN_CANARY'
    code = path.split('/quote/')[1].split('?')[0]
    return {'success': True, 'data': {'code': code, 'low': 9, 'high': 11, 'current_price': 10,
        'pricing_basis': {'source': 'sina_l1_on_demand', 'channel': 'upstream',
        'as_of': '2026-11-03T14:00:00+08:00', 'trading_status': 'normal',
        'trading_status_raw': '00', 'trading_status_source': 'sina_l1_field_32',
        'max_delay_seconds': 90, 'payload_sha256': 'a'*64}}}
'''
        # Restore the real function only for this test.
        # The real adapter is retained below before setUp patches it.
        result = REAL_ACCEPT(dict(self.payload, query_code=query_code,
                                 validate_code=inspect.getsource(validate_quote)), lambda: NOW)
        self.assertEqual([row['symbol'] for row in result], ['600000', '159865'])
        self.assertNotIn('TOKEN_CANARY', json.dumps(result))


REAL_ACCEPT = renewal.accept_quotes
if __name__ == '__main__':
    unittest.main()
