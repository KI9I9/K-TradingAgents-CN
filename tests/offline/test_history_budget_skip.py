"""Exercise real gate/journal files and the actual scheduler day reservation."""
from datetime import datetime, timedelta
import fcntl
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tradingagents.utils.tushare_batch import provision, sha, BJ
from deploy.history_batch_journal import Workspace, digest
from deploy.history_sync_plan import build_plan
from deploy.reserve_history_budget_skip import reserve_skip
from app.services import tushare_history_schedule as schedule

NOW = datetime(2026, 11, 3, 11, 0, tzinfo=BJ)


class BudgetSkip(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        clock = patch('tradingagents.utils.tushare_batch.time.time', return_value=NOW.timestamp())
        clock.start(); self.addCleanup(clock.stop)
        codes = ['600000.SH', '600001.SH', '600002.SH']
        policy = dict(schema=2, codes=codes, min_date='2026-10-01', max_date='2026-11-06',
            expires_at=(NOW + timedelta(days=3)).timestamp(), max_requests=100,
            daily_requests=100, minute_requests=4, write_enabled=True)
        self.gate = provision(self.root / 'gate', policy)
        series = {c: dict(data_source='tushare', market='CN', period='daily', count=1,
            latest='2026-10-30', contract_verified=True, content_sha256='a'*64) for c in codes}
        plan = build_plan(codes, series, end='2026-11-02', now=NOW, price_adjustment='none',
            quota=dict(daily_requests=100, rolling_minute_requests=4, evidence_ref='fixture'))
        self.ws = Workspace(self.gate.directory / 'runs'); self.ws.provision()
        self.identity = self.ws.create(plan)
        for code in codes:
            path = self.ws.path / self.identity / (code + '.json')
            row = json.loads(path.read_bytes())
            row.update(phase='committed', fetch_attempts=1, payload={'saved': True})
            row['payload_sha256'] = digest(row['payload']); path.write_text(json.dumps(row))
        with self.gate.locked() as fd:
            _, state = self.gate.read(fd)
            state.update(plan=plan, used=98, days={'2026-11-03': 98})
            self.gate.write(fd, state)
        self.permit = dict(schema=1, generation=str(self.gate.directory), policy_sha256=sha(policy),
            cron=schedule.CRON, timezone=schedule.TIMEZONE, expires_at=policy['expires_at'])
        self.permit_path = self.root / 'schedule.json'
        self.permit_path.write_text(json.dumps(self.permit)); self.permit_path.chmod(0o600)
        self.paths = [self.gate.directory / 'state.json', self.gate.directory / 'policy.json', self.permit_path]
        self.before = {p: p.read_bytes() for p in self.paths}
        self.expected = dict(day='2026-11-03', generation=str(self.gate.directory),
            plan_sha256=self.identity, used=98, daily_used=98, required_requests=3)
        self.marker = self.gate.directory / 'scheduled-2026-11-03.json'
        self.backup = self.root / 'backup'

    def run_skip(self, **kwargs):
        return reserve_skip(self.gate, self.ws, self.permit, self.expected,
            now=kwargs.pop('now', NOW), backup_root=self.backup, protected_paths=self.paths,
            business_snapshot=kwargs.pop('business_snapshot', lambda: {'prefs': 'same'}), **kwargs)

    def test_real_scheduler_skips_today_and_accepts_tomorrow_without_policy_change(self):
        result = self.run_skip()
        self.assertEqual(result['status'], 'reserved_today_only')
        self.assertEqual(result['remaining_today'], 2)
        self.assertEqual({p: p.read_bytes() for p in self.paths}, self.before)
        self.assertEqual(self.marker.stat().st_mode & 0o777, 0o600)
        with patch.object(schedule, 'PERMIT', self.permit_path):
            self.assertFalse(schedule.reserve_day(self.gate, NOW))
            self.assertTrue(schedule.reserve_day(self.gate, NOW + timedelta(days=1)))
        manifest = json.loads((self.backup / 'before.json').read_bytes())
        for name, record in manifest['files'].items():
            self.assertEqual((self.backup / record['file']).read_bytes(), self.before[Path(name)])

    def test_check_is_read_only(self):
        self.assertEqual(self.run_skip(check_only=True)['status'], 'checked_no_changes')
        self.assertFalse(self.backup.exists()); self.assertFalse(self.marker.exists())

    def test_existing_slot_never_overwritten(self):
        self.marker.write_bytes(b'previous operator evidence')
        with self.assertRaisesRegex(RuntimeError, 'day_slot_already_reserved'): self.run_skip()
        self.assertEqual(self.marker.read_bytes(), b'previous operator evidence')
        self.assertFalse(self.backup.exists())

    def test_counter_drift_or_sufficient_budget_refused(self):
        self.expected['used'] = 97
        with self.assertRaisesRegex(RuntimeError, 'ledger_changed'): self.run_skip()
        self.expected.update(used=97, daily_used=97)
        with self.gate.locked() as fd:
            _, state = self.gate.read(fd); state.update(used=97, days={'2026-11-03': 97})
            self.gate.write(fd, state)
        with self.assertRaisesRegex(RuntimeError, 'budget_skip_not_justified'): self.run_skip()
        self.assertFalse(self.marker.exists())

    def test_other_day_or_late_execution_refused(self):
        for now in [NOW + timedelta(days=1), NOW.replace(hour=18, minute=25)]:
            with self.assertRaisesRegex(RuntimeError, 'outside_approved_day'): self.run_skip(now=now)

    def test_running_admission_refused(self):
        with self.gate.admission_path.open('r+') as stream:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError): self.run_skip()
        self.assertFalse(self.marker.exists())

    def test_failure_after_reservation_keeps_safe_slot_and_backups(self):
        states = iter([{'prefs': 'same'}, {'prefs': 'changed'}])
        with self.assertRaisesRegex(RuntimeError, 'business_state_changed'):
            self.run_skip(business_snapshot=lambda: next(states))
        self.assertTrue(self.marker.is_file()); self.assertTrue((self.backup / 'before.json').is_file())
        with self.assertRaisesRegex(RuntimeError, 'already_reserved'): self.run_skip()

    def test_incomplete_prior_plan_refused(self):
        path = self.ws.path / self.identity / '600000.SH.json'
        row = json.loads(path.read_bytes()); row.update(phase='pending', fetch_attempts=0,
            payload=None, payload_sha256=None); path.write_text(json.dumps(row))
        with self.assertRaisesRegex(RuntimeError, 'previous_plan_incomplete'): self.run_skip()
        self.assertFalse(self.marker.exists())


if __name__ == '__main__': unittest.main()
