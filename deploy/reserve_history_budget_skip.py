"""Reserve one existing scheduler day slot for an explicit budget skip.

This operator tool neither fetches data nor changes a permit, STOP, quota, cron
or preference. The existing reserve_day() sees today's occupied slot and skips;
tomorrow uses a different filename. No automatic removal or retry on failure.
"""
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path

from tradingagents.utils.tushare_egress import _private_fd
from tradingagents.utils.tushare_batch import BJ, sha


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def write_new(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    require(path.read_bytes() == raw, 'write_readback_failed')
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def reserve_skip(gate, workspace, permit, expected, *, now, backup_root,
                 protected_paths, business_snapshot, check_only=False):
    now = now.astimezone(BJ)
    require(now.date().isoformat() == expected['day'] and now.weekday() < 5
            and (now.hour, now.minute) < (18, 25), 'outside_approved_day')
    require(str(gate.directory) == expected['generation'], 'generation_changed')
    require(not backup_root.exists() and not backup_root.is_symlink(), 'already_reserved')
    marker = gate.directory / ('scheduled-' + expected['day'] + '.json')
    admission = os.open(gate.admission_path, os.O_RDWR | os.O_NOFOLLOW)
    try:
        _private_fd(admission)
        fcntl.flock(admission, fcntl.LOCK_EX | fcntl.LOCK_NB)
        gate.assert_selected()
        policy, state = gate.status()
        require(policy['schema'] == 2 and state['active'] is None, 'history_not_idle')
        require(state['used'] == expected['used']
                and state['days'].get(expected['day'], 0) == expected['daily_used']
                and state['plan']['plan_sha256'] == expected['plan_sha256'], 'ledger_changed')
        require(permit['generation'] == str(gate.directory)
                and permit['policy_sha256'] == sha(policy)
                and permit['expires_at'] == policy['expires_at'], 'schedule_changed')
        checkpoint = workspace.status(expected['plan_sha256'])
        require(checkpoint['complete'], 'previous_plan_incomplete')
        needed = len(policy['codes'])  # One daily request per stock; conservative full round.
        remaining = policy['daily_requests'] - expected['daily_used']
        require(needed == expected['required_requests'] and 0 <= remaining < needed,
                'budget_skip_not_justified')
        require(not os.path.lexists(marker), 'day_slot_already_reserved')
        protected = {}
        for path in protected_paths:
            require(not path.is_symlink() and path.is_file(), 'protected_file_invalid')
            protected[str(path)] = path.read_bytes()
        before = business_snapshot()  # Also checks scheduler preference and no running job.
        result = dict(day=expected['day'], reason='operator_skip_daily_budget',
                      used_today=expected['daily_used'], remaining_today=remaining,
                      required_requests=needed, cron_unchanged=True,
                      quota_unchanged=True, requests_sent=0)
        if check_only:
            return dict(result, status='checked_no_changes')
        backup_root.mkdir(mode=0o700)
        hashes = {}
        for index, (name, raw) in enumerate(protected.items()):
            filename = str(index) + '.backup'
            write_new(backup_root / filename, raw)
            hashes[name] = dict(file=filename, sha256=hashlib.sha256(raw).hexdigest())
        write_new(backup_root / 'before.json', encoded(dict(
            expected=expected, files=hashes, business=before,
            checkpoint=checkpoint, marker_previously_absent=True)))
        # Refuse drift after backup. This never removes/replaces an existing slot.
        require(all(Path(name).read_bytes() == raw for name, raw in protected.items()),
                'protected_state_changed_before_skip')
        reservation = dict(at=now.isoformat(), generation=str(gate.directory),
                           policy_sha256=sha(policy), operator_skip=result)
        write_new(marker, encoded(reservation))
        # If any later check fails the day slot remains closed, with backups intact.
        require(all(Path(name).read_bytes() == raw for name, raw in protected.items()),
                'protected_state_changed_after_skip')
        require(business_snapshot() == before, 'business_state_changed')
        require(workspace.status(expected['plan_sha256']) == checkpoint, 'workspace_changed')
        result.update(status='reserved_today_only', marker=str(marker),
                      backup=str(backup_root), protected_state_unchanged=True)
        write_new(backup_root / 'completion.json', encoded(result))
        return result
    finally:
        os.close(admission)


def main(request):
    """Run once in the accepted backend, with the existing runtime reconstructor."""
    import types
    from pymongo import MongoClient
    from bson.codec_options import CodecOptions
    from bson.raw_bson import RawBSONDocument
    from tradingagents.utils.tushare_batch import selected_gate
    from app.services.tushare_history_schedule import load_schedule, PERMIT
    from deploy.history_batch_journal import Workspace

    require(os.geteuid() == 1000, 'backend_uid_changed')
    for name, digest in request['source_hashes'].items():
        require(hashlib.sha256(Path(name).read_bytes()).hexdigest() == digest,
                'production_source_changed')
    runtime = types.ModuleType('runtime')
    exec(request['runtime_tool'], runtime.__dict__)
    env, effective = runtime.reconstruct('/app/deploy/migration_production_entrypoint.py',
        'd75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b', '/run/secrets')
    require(all(not v['enabled'] for v in effective['gates'].values()), 'legacy_quote_gate_open')
    client = MongoClient(host=env['MONGODB_HOST'], port=int(env['MONGODB_PORT']),
        username=env['MONGODB_USERNAME'], password=env['MONGODB_PASSWORD'],
        authSource=env.get('MONGODB_AUTH_SOURCE', 'admin'), retryReads=False,
        retryWrites=False, serverSelectionTimeoutMS=10000, socketTimeoutMS=15000)
    try:
        db = client[env['MONGODB_DATABASE']]
        def business_snapshot():
            require(db.scheduler_executions.count_documents({'job_id': {'$regex': '^tushare_'},
                'status': {'$in': ['running', 'cancelling']}}) == 0, 'execution_active')
            require((db.scheduler_preferences.find_one({'_id': 'tushare_historical_sync'})
                     or {}).get('enabled') is True, 'schedule_preference_changed')
            completed = db.scheduler_executions.find_one({'_id': __import__('bson').ObjectId(
                request['expected']['execution_id'])}) or {}
            require(completed.get('status') == 'success'
                    and completed.get('result', {}).get('complete') is True,
                    'previous_execution_not_complete')
            result = {}
            for name in ['scheduler_preferences', 'paper_accounts', 'paper_positions',
                         'paper_orders', 'paper_trades', 'market_quotes']:
                col = db.get_collection(name, codec_options=CodecOptions(document_class=RawBSONDocument))
                digest = hashlib.sha256(); count = 0
                for row in col.find({}).sort('_id', 1).max_time_ms(15000):
                    digest.update(row.raw); count += 1
                result[name] = dict(count=count, sha256=digest.hexdigest())
            result['history_counts'] = {name: db[name].count_documents({'data_source': 'tushare'})
                for name in ['stock_daily_quotes', 'stock_daily_quotes_unadjusted']}
            return result
        gate = selected_gate()
        permit = load_schedule(gate)
        protected = [gate.directory / name for name in ['state.json', 'policy.json']]
        protected += [gate.directory.parent / 'active.json', PERMIT,
                      Path('/app/runtime/tushare-egress/STOP'),
                      Path('/app/runtime/paper-quote-supply.json')]
        return reserve_skip(gate, Workspace(gate.directory / 'runs'), permit, request['expected'],
            now=datetime.now(BJ), backup_root=Path('/app/runtime/history-budget-skip-20261103-v1'),
            protected_paths=protected, business_snapshot=business_snapshot,
            check_only=request.get('check_only') is True)
    finally:
        client.close()


if __name__ == '__main__':
    import sys
    os.umask(0o077)
    print('BUDGET_SKIP_RESULT:' + json.dumps(main(json.load(sys.stdin))))
