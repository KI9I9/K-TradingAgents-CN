"""Real fsync/replace/flock journal tests, synthetic adapters only."""
from copy import deepcopy
from datetime import datetime, timezone
import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT/'deploy'/file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


j = load('journal', 'history_batch_journal.py')
p = load('planner', 'history_sync_plan.py')


def plan(codes=('600000.SH', '600001.SH')):
    return p.build_plan(list(codes), {code: dict(data_source='tushare', market='CN', period='daily',
        count=5, latest='2026-10-27', contract_verified=True, content_sha256='a'*64) for code in codes},
        end='2026-10-28', now=datetime(2026,10,29,tzinfo=timezone.utc),
        quota=dict(daily_requests=10, rolling_minute_requests=4, evidence_ref='synthetic'))


class Adapter:
    def __init__(self):
        self.calls = []
        self.database = {}
        self.denied = set()
        self.fetch_error = None
        self.write_error = None
        self.after_write_error = None
        self.valid = True
        self.drift = False

    def authorize(self, identity, item, action):
        self.calls.append(('authorize', item['code'], action))
        return action not in self.denied

    def preflight(self, item):
        self.calls.append(('preflight', item['code']))
        return 'b'*64 if self.drift else item['observation_sha256']

    def fetch(self, item):
        self.calls.append(('fetch', item['code']))
        if self.fetch_error: raise self.fetch_error
        return {'symbol': item['code'], 'rows': [{'trade_date': item['end'], 'close': 10}]}

    def validate(self, item, payload):
        self.calls.append(('validate', item['code']))
        return self.valid and payload['symbol'] == item['code']

    def write(self, item, payload):
        self.calls.append(('write', item['code']))
        if self.write_error: raise self.write_error
        self.database[item['code']] = deepcopy(payload)
        if self.after_write_error: raise self.after_write_error

    def matches(self, item, payload):
        self.calls.append(('matches', item['code']))
        return self.database.get(item['code']) == payload


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)/'runs'
        self.ws = j.Workspace(self.path)
        self.ws.provision()
        self.plan = plan()
        self.identity = self.ws.create(self.plan)
        self.a = Adapter()

    def advance(self, count=2):
        return self.ws.advance(self.identity, self.a, max_items=count)

    def calls(self, name):
        return [c for c in self.a.calls if c[0] == name]

    def test_bounded_slice_reopens_without_refetch(self):
        self.assertEqual(self.advance(1)['committed'], 1)
        self.ws = j.Workspace(self.path)
        self.assertTrue(self.advance(1)['complete'])
        self.assertEqual(len(self.calls('fetch')), 2)
        self.assertEqual(self.advance()['processed'], 0)
        self.assertEqual(self.ws.status(self.identity)['counts']['committed'], 2)

    def test_exclusive_creation_never_resets(self):
        self.advance(1)
        with self.assertRaises(FileExistsError): self.ws.create(self.plan)
        with self.assertRaises(FileExistsError): self.ws.provision()
        self.assertEqual(self.ws.status(self.identity)['counts']['committed'], 1)

    def test_unknown_fetch_blocks_every_later_request(self):
        self.a.fetch_error = TimeoutError('synthetic')
        with self.assertRaises(TimeoutError): self.advance()
        self.a.fetch_error = None
        with self.assertRaisesRegex(j.BatchStopped, 'request_outcome_unknown'): self.advance()
        self.assertEqual(len(self.calls('fetch')), 1)
        self.assertEqual(len(self.calls('write')), 0)
        self.assertEqual(self.ws.status(self.identity)['counts']['fetching'], 1)

    def test_real_process_death_after_fetch_marker(self):
        def child():
            adapter = Adapter()
            adapter.fetch = lambda item: os._exit(93)
            j.Workspace(self.path).advance(self.identity, adapter, max_items=1)
        process = multiprocessing.get_context('fork').Process(target=child)
        process.start(); process.join(5)
        if process.is_alive():
            process.kill(); process.join(); self.fail('child did not exit')
        self.assertEqual(process.exitcode, 93)
        with self.assertRaisesRegex(j.BatchStopped, 'request_outcome_unknown'): self.advance()
        self.assertEqual(self.calls('fetch'), [])

    def test_response_saved_before_write_denial_can_resume(self):
        self.a.denied.add('write')
        with self.assertRaisesRegex(j.BatchStopped, 'not_authorized'): self.advance()
        self.assertEqual(self.ws.status(self.identity)['counts']['fetched'], 1)
        self.a.denied.clear()
        self.assertTrue(self.advance()['complete'])
        self.assertEqual(len(self.calls('fetch')), 2)

    def test_crash_after_write_reconciles_without_rewrite(self):
        self.a.after_write_error = SystemExit('synthetic crash')
        with self.assertRaises(SystemExit): self.advance()
        self.a.after_write_error = None
        result = self.advance(1)
        self.assertEqual(result['committed'], 1)
        self.assertEqual(len(self.calls('fetch')), 1)
        self.assertEqual(len(self.calls('write')), 1)
        self.assertEqual(len(self.calls('matches')), 1)

    def test_failed_write_not_assumed_absent_or_retried(self):
        self.a.write_error = TimeoutError('synthetic')
        with self.assertRaises(TimeoutError): self.advance()
        self.a.write_error = None
        with self.assertRaisesRegex(j.BatchStopped, 'write_outcome_unresolved'): self.advance()
        self.assertEqual(len(self.calls('fetch')), 1)
        self.assertEqual(len(self.calls('write')), 1)

    def test_same_row_count_but_different_content_does_not_commit(self):
        self.a.after_write_error = RuntimeError('synthetic')
        with self.assertRaises(RuntimeError): self.advance()
        self.a.database['600000.SH']['rows'][0]['close'] = 99
        with self.assertRaisesRegex(j.BatchStopped, 'write_outcome_unresolved'): self.advance()
        self.assertEqual(self.ws.status(self.identity)['counts']['committed'], 0)

    def test_authority_denial_keeps_pending_without_calls(self):
        self.a.denied.add('fetch')
        with self.assertRaisesRegex(j.BatchStopped, 'not_authorized'): self.advance()
        self.assertEqual(self.calls('fetch'), [])
        self.assertEqual(self.ws.status(self.identity)['counts']['pending'], 2)

    def test_inventory_drift_prevents_request(self):
        self.a.drift = True
        with self.assertRaisesRegex(j.BatchStopped, 'inventory_changed'): self.advance()
        self.assertEqual(self.calls('fetch'), [])

    def test_inventory_drift_after_fetch_prevents_write(self):
        original = self.a.fetch
        def fetch(item):
            value = original(item); self.a.drift = True; return value
        self.a.fetch = fetch
        with self.assertRaisesRegex(j.BatchStopped, 'inventory_changed'): self.advance()
        self.assertEqual(self.calls('write'), [])
        self.assertEqual(self.ws.status(self.identity)['counts']['fetched'], 1)

    def test_invalid_payload_is_not_retried(self):
        self.a.valid = False
        with self.assertRaisesRegex(j.BatchStopped, 'payload_rejected'): self.advance()
        self.a.valid = True
        with self.assertRaisesRegex(j.BatchStopped, 'request_outcome_unknown'): self.advance()
        self.assertEqual(len(self.calls('fetch')), 1)

    def test_reconcile_authority_revoked_after_write(self):
        self.a.denied.add('reconcile')
        with self.assertRaisesRegex(j.BatchStopped, 'not_authorized'): self.advance()
        self.assertEqual(self.calls('matches'), [])
        self.a.denied.clear()
        self.assertEqual(self.advance(1)['committed'], 1)
        self.assertEqual(len(self.calls('write')), 1)

    def test_workspace_lock_covers_other_runs_and_instances(self):
        other = self.ws.create(plan(('600002.SH',)))
        with self.ws.locked():
            with self.assertRaisesRegex(j.BatchStopped, 'busy'):
                j.Workspace(self.path).advance(other, self.a, max_items=1)
            with self.assertRaisesRegex(j.BatchStopped, 'busy'): self.advance()
        self.assertEqual(self.calls('fetch'), [])

    def test_tampered_plan_refused_before_actions(self):
        file = self.path/self.identity/'plan.json'
        value = json.loads(file.read_text()); value['items'][0]['end'] = '2026-10-29'
        file.write_text(json.dumps(value))
        with self.assertRaisesRegex(j.BatchStopped, 'plan_hash_mismatch'): self.advance()
        self.assertEqual(self.a.calls, [])

    def test_payload_corruption_refused(self):
        self.a.denied.add('write')
        with self.assertRaises(j.BatchStopped): self.advance()
        file = self.path/self.identity/'600000.SH.json'
        value = json.loads(file.read_text()); value['payload']['rows'][0]['close'] = 99
        file.write_text(json.dumps(value))
        with self.assertRaisesRegex(j.BatchStopped, 'payload_corrupt'): self.advance()

    def test_missing_state_not_recreated(self):
        (self.path/self.identity/'600001.SH.json').unlink()
        with self.assertRaises(FileNotFoundError): self.advance()
        self.assertEqual(self.calls('fetch'), [])

    def test_symlink_state_refused(self):
        file = self.path/self.identity/'600000.SH.json'
        moved = file.with_suffix('.original'); file.rename(moved); file.symlink_to(moved)
        with self.assertRaisesRegex(j.BatchStopped, 'unsafe_journal_path'): self.advance()

    def test_state_permissions_refused(self):
        (self.path/self.identity/'600000.SH.json').chmod(0o644)
        with self.assertRaisesRegex(j.BatchStopped, 'unsafe_journal_path'): self.advance()

    def test_atomic_failure_before_marker_means_no_request(self):
        with patch.object(j.os, 'replace', side_effect=OSError('synthetic disk failure')):
            with self.assertRaises(OSError): self.advance()
        self.assertEqual(self.calls('fetch'), [])
        self.assertEqual(self.ws.status(self.identity)['counts']['pending'], 2)

    def test_atomic_failure_saving_response_does_not_refetch(self):
        original = j._atomic
        def save(path, value):
            if value.get('phase') == 'fetched': raise OSError('synthetic disk failure')
            return original(path, value)
        with patch.object(j, '_atomic', side_effect=save):
            with self.assertRaises(OSError): self.advance()
        with self.assertRaisesRegex(j.BatchStopped, 'request_outcome_unknown'): self.advance()
        self.assertEqual(len(self.calls('fetch')), 1)

    def test_atomic_failure_saving_commit_reconciles(self):
        original = j._atomic
        def save(path, value):
            if value.get('phase') == 'committed': raise OSError('synthetic disk failure')
            return original(path, value)
        with patch.object(j, '_atomic', side_effect=save):
            with self.assertRaises(OSError): self.advance()
        self.assertEqual(self.advance(1)['committed'], 1)
        self.assertEqual(len(self.calls('write')), 1)

    def test_adapter_mutations_cannot_change_frozen_data(self):
        original = self.a.write
        def write(item, payload):
            original(item, payload)
            item['code'] = '600999.SH'; payload['rows'][0]['close'] = 99
        self.a.write = write
        self.assertTrue(self.advance()['complete'])

    def test_unverified_or_blocked_plan_not_staged(self):
        for field, value in [('quota', None), ('blocked_count', 1)]:
            candidate = deepcopy(self.plan); candidate[field] = value
            del candidate['plan_sha256']; candidate['plan_sha256'] = j.digest(candidate)
            with self.subTest(field=field), self.assertRaises(j.BatchStopped): self.ws.create(candidate)

    def test_stop_during_fetch_preserves_response_without_write(self):
        original = self.a.fetch
        def fetch(item):
            value = original(item)
            j.Workspace(self.path).stop()  # no wait for the runner's flock
            return value
        self.a.fetch = fetch
        with self.assertRaisesRegex(j.BatchStopped, 'workspace_stopped'): self.advance()
        self.assertEqual(self.ws.status(self.identity)['counts']['fetched'], 1)
        self.assertEqual(self.calls('write'), [])
        with self.assertRaisesRegex(j.BatchStopped, 'workspace_stopped'): self.advance()
        self.assertEqual(len(self.calls('fetch')), 1)

    def test_stop_before_fetch_has_no_external_calls(self):
        self.ws.stop(); self.ws.stop()
        with self.assertRaisesRegex(j.BatchStopped, 'workspace_stopped'): self.advance()
        self.assertEqual(self.a.calls, [])

    def test_stop_during_preflight_is_rechecked(self):
        original = self.a.preflight
        def preflight(item):
            value = original(item); self.ws.stop(); return value
        self.a.preflight = preflight
        with self.assertRaisesRegex(j.BatchStopped, 'workspace_stopped'): self.advance()
        self.assertEqual(self.calls('fetch'), [])

    def test_invalid_slice_and_run_path(self):
        for value in (True, 0, -1, 1.5):
            with self.subTest(value=value), self.assertRaises(j.BatchStopped): self.advance(value)
        with self.assertRaises(j.BatchStopped): self.ws.status('../escape')
        self.assertEqual(self.calls('fetch'), [])


if __name__ == '__main__':
    unittest.main()
