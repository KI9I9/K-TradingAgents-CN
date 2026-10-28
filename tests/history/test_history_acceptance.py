"""Actual finite permit + SDK + deployed method bodies; synthetic responses only."""
import asyncio
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('sample', ROOT / 'deploy/history_sample_acceptance.py')
sample = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sample)
egress, manual, https = sample.import_transport(ROOT)


def deny_external(event, args):
    if event in ('socket.connect', 'socket.getaddrinfo', 'socket.bind', 'socket.sendto', 'subprocess.Popen'):
        raise RuntimeError('offline_network_denied')


sys.addaudithook(deny_external)
DATES = ['20261021', '20261022', '20261023', '20261026', '20261027']
SCOPE = {'codes': ['600000.SH'], 'start': DATES[0], 'end': DATES[-1]}
PARAMS = dict(ts_code='600000.SH', start_date=DATES[0], end_date=DATES[-1])


def table(api):
    fields = ['ts_code', 'trade_date', 'adj_factor'] if api == 'adj_factor' else [
        'ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
    return {'fields': fields, 'items': [['600000.SH', day, 2.] if api == 'adj_factor' else
            ['600000.SH', day, 10., 12., 9., 11., 10., 1., 10., 123.45, 678.9] for day in reversed(DATES)]}


class SampleTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.directory = Path(temp.name) / 'one-off'
        self.gate = sample.provision(self.directory, egress, SCOPE)
        self.job = sample.SampleAuthority(self.gate, egress, SCOPE, DATES)
        self.sends = []

    def state(self):
        with self.gate.locked() as fd:
            return self.gate.read(fd)

    def send(self, api, params=None, response=None):
        def operation():
            self.sends.append(api)
            return response if response is not None else table(api)
        return self.job.perform(api, '', params or PARAMS, operation)

    def test_run_paths_are_independent_and_unknown_run_rejected(self):
        spec = importlib.util.spec_from_file_location('runner', ROOT / 'deploy/run_history_sample.py')
        runner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(runner)
        first, second = runner.run_configuration('v1'), runner.run_configuration('v2')
        self.assertNotEqual(first['artifact'], second['artifact'])
        self.assertNotEqual(first['result'], second['result'])
        for key in ('driver', 'stage', 'execute', 'status'):
            self.assertIn('20261028-600000-v2', second[key])
            self.assertNotIn('20261028-600000-v1', second[key])
            compile(second[key], '<generated>', 'exec')
        self.assertIn(sample.permit_directory({'run_id': 'v2'}), second['driver'])
        for invalid in ('v3', '../v1', None):
            with self.assertRaises(ValueError):
                runner.run_configuration(invalid)
            with self.assertRaises(ValueError):
                sample.permit_directory({'run_id': invalid})

    def test_new_run_does_not_reset_failed_run(self):
        self.send('daily')
        self.gate.stop()
        before = {p.name: p.read_bytes() for p in self.directory.iterdir()}
        second = sample.provision(self.directory.parent / 'second-run', egress, SCOPE)
        second_job = sample.SampleAuthority(second, egress, SCOPE, DATES)
        second_job.perform('daily', '', PARAMS, lambda: table('daily'))
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.directory.iterdir()})
        with self.assertRaises(egress.TushareEgressStopped):
            self.send('adj_factor')
        self.assertEqual(self.state()['used'], 1)

    def test_exact_two_then_no_third_request(self):
        self.send('daily'); self.send('adj_factor')
        self.job.assert_ready()
        with self.assertRaises(egress.TushareEgressStopped):
            self.send('daily')
        self.assertEqual(self.sends, ['daily', 'adj_factor'])
        self.assertEqual(self.state()['used'], 2)

    def test_wrong_scope_stops_before_sending(self):
        with self.assertRaises(egress.TushareEgressStopped):
            self.send('daily', {**PARAMS, 'ts_code': '000001.SZ'})
        self.assertEqual(self.sends, [])
        self.assertEqual(self.state()['used'], 0)
        self.assertTrue((self.directory / 'STOP').exists())

    def test_expanding_date_window_stops_before_sending(self):
        with self.assertRaises(egress.TushareEgressStopped):
            self.send('daily', {**PARAMS, 'start_date': '19900101'})
        self.assertEqual(self.sends, [])

    def test_pagination_rejected_before_sending(self):
        with self.assertRaises(egress.TushareEgressStopped):
            self.send('daily', {**PARAMS, 'limit': 5000})
        self.assertEqual(self.sends, [])

    def test_factor_cannot_be_first(self):
        with self.assertRaises(egress.TushareEgressStopped):
            self.send('adj_factor')
        self.assertEqual(self.state()['used'], 0)

    def test_missing_date_is_failure_and_consumes_attempt(self):
        response = table('daily'); response['items'].pop()
        with self.assertRaisesRegex(egress.TushareEgressStopped, 'response_invalid'):
            self.send('daily', response=response)
        self.assertEqual(self.state()['used'], 1)
        self.assertEqual(self.state()['phase'], 'blocked')
        with self.assertRaises(egress.TushareEgressStopped):
            self.send('adj_factor')
        self.assertEqual(self.sends, ['daily'])

    def test_factor_order_cannot_change_qfq_base(self):
        self.send('daily')
        response = table('adj_factor'); response['items'].reverse()
        with self.assertRaisesRegex(egress.TushareEgressStopped, 'response_invalid'):
            self.send('adj_factor', response=response)
        self.assertEqual(self.state()['used'], 2)

    def test_extra_upstream_strings_not_persisted(self):
        response = table('daily'); response['fields'].append('untrusted')
        for row in response['items']:row.append('arbitrary-upstream-string')
        saved = self.send('daily', response=response)
        self.assertNotIn('arbitrary-upstream-string', json.dumps(saved))

    def test_original_run_cannot_be_reset(self):
        self.send('daily')
        with self.assertRaises(FileExistsError):
            sample.provision(self.directory, egress, SCOPE)
        self.assertEqual(self.state()['used'], 1)

    def test_expired_permit_never_sends(self):
        expires = self.state()['expires_at']
        with patch.object(egress.time, 'time', return_value=expires + 1):
            with self.assertRaisesRegex(egress.TushareEgressStopped, 'expired'):
                self.send('daily')
        self.assertEqual(self.sends, [])

    def test_interrupted_request_never_assumed_unsent(self):
        with self.gate.locked() as fd:
            state = self.gate.read(fd); state.update(used=1, phase='in_flight')
            self.gate.write(fd, state)
        with self.assertRaisesRegex(egress.TushareEgressStopped, 'previous_attempt_incomplete'):
            self.send('daily')
        self.assertEqual(self.sends, [])

    def run_provider(self, send):
        token = manual.CURRENT.set(self.job)
        try:
            obj = sample.provider(ROOT, https.create_tushare_client('SYNTHETIC_ONLY'), egress, manual)
            with patch.dict(os.environ, {'TA_TUSHARE_ROUTE': 'mainland_unix'}), \
                    patch.object(egress.MainlandTransport, '_send', side_effect=send), contextlib.redirect_stdout(io.StringIO()):
                return asyncio.run(obj.get_historical_data('600000', DATES[0], DATES[-1], 'daily'))
        finally:
            manual.CURRENT.reset(token)

    def test_real_sdk_and_provider_contract_conversion(self):
        def send(api, body):
            self.sends.append(api)
            return table(api)
        frame = self.run_provider(send)
        contract = sample.load('test_sample_contract', ROOT / 'app/services/historical_contract.py')
        docs = contract.tushare_documents('600000', frame, 'CN', 'daily')
        reference = [{k: doc[k] for k in ['trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount']} for doc in docs]
        self.assertEqual(sample.compare_rows(docs, self.job.tables, reference), [])
        self.assertEqual(self.sends, ['daily', 'adj_factor'])
        self.assertEqual(self.state()['used'], 2)
        self.assertEqual(docs[0]['volume'], 123.45)
        self.assertEqual(docs[0]['amount'], 678900.)
        self.assertNotIn('is_final', docs[0])
        self.assertNotIn('trading_status', docs[0])

    def test_real_sdk_upstream_limit_does_not_retry(self):
        def send(api, body):
            self.sends.append(api)
            raise egress.TushareEgressStopped('usage_limit', 40203)
        with self.assertRaisesRegex(egress.TushareEgressStopped, 'usage_limit'):
            self.run_provider(send)
        self.assertEqual(self.sends, ['daily'])
        self.assertEqual(self.state()['used'], 1)
        self.assertEqual(self.state()['phase'], 'blocked')

    def test_hundred_fold_units_fail_independent_comparison(self):
        self.send('daily'); self.send('adj_factor')
        docs = [dict(trade_date='2026-10-21', open=10., high=12., low=9., close=11., volume=12345., amount=678900.)]
        with self.assertRaisesRegex(ValueError, 'volume_conversion_wrong'):
            sample.compare_rows(docs, self.job.tables, [])


if __name__ == '__main__':
    unittest.main()
