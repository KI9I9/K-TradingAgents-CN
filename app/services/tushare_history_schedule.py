"""Explicit, finite scheduling permission for the same unadjusted batch path.

Reading this module never grants or renews permission. The operator installs a
private file bound to one generation; legacy Tushare jobs remain stopped.
"""
import asyncio
from datetime import datetime
import json
import math
import os
from pathlib import Path

from tradingagents.utils.tushare_batch import selected_gate, sha, BJ
from tradingagents.utils.tushare_egress import TushareEgressStopped, _pairs, _private_fd

JOB_ID = 'tushare_historical_sync'
PERMIT = Path('/app/runtime/tushare-history-schedule.json')
CRON = '30 18 * * mon-fri'
TIMEZONE = 'Asia/Shanghai'


def load_schedule(gate=None, *, path=None):
    """Check the file, selected generation, STOP, expiry and existing budgets."""
    try:
        fd = os.open(path or PERMIT, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as stream:
            _private_fd(stream.fileno())
            raw = stream.read(8193)
        if len(raw) > 8192:
            raise ValueError()
        permit = json.loads(raw, object_pairs_hook=_pairs)
        if (set(permit) != {'schema', 'generation', 'policy_sha256', 'cron', 'timezone', 'expires_at'}
                or type(permit['schema']) is not int or permit['schema'] != 1
                or permit['cron'] != CRON or permit['timezone'] != TIMEZONE
                or type(permit['expires_at']) not in (int, float)
                or not math.isfinite(permit['expires_at'])):
            raise ValueError()
        gate = gate or selected_gate()
        gate.assert_selected()
        policy, _ = gate.status()
        if (policy['schema'] != 2 or permit['generation'] != str(gate.directory)
                or permit['policy_sha256'] != sha(policy)
                or permit['expires_at'] != policy['expires_at']
                or policy['minute_requests'] > 40 or policy['daily_requests'] > 7500):
            raise ValueError()
        return permit
    except (OSError, ValueError, TypeError, KeyError, OverflowError):
        raise TushareEgressStopped('not_authorized') from None


def configure_history_job(scheduler):
    """Restore only the approved trigger, paused until the stored user choice."""
    load_schedule()
    from apscheduler.triggers.cron import CronTrigger
    if scheduler.get_job(JOB_ID) is None:
        raise TushareEgressStopped('not_authorized')
    scheduler.reschedule_job(JOB_ID, trigger=CronTrigger.from_crontab(CRON, timezone=TIMEZONE))
    scheduler.modify_job(JOB_ID, max_instances=1, coalesce=True, misfire_grace_time=300)
    scheduler.pause_job(JOB_ID)


async def trade_day_reason(now):
    """No Tushare calendar calls and no weekday/fallback treated as evidence."""
    if now.weekday() >= 5:
        return 'non_trading_day'
    if (now.hour, now.minute) < (18, 30):
        return 'before_daily_sync_time'
    from app.services.trading_calendar_service import get_trading_calendar_service
    calendar = get_trading_calendar_service()
    today = now.date().isoformat()
    cache = await calendar._read_cache()
    last = None
    if (isinstance(cache, dict) and cache.get('fetched_at') == today
            and cache.get('source') == 'akshare'):
        last = cache.get('last_trade_date')
    if last is None:
        try:
            # This source does not consume this account's limited daily budget.
            last = await asyncio.wait_for(calendar._fetch_from_akshare(), timeout=45)
        except Exception:
            return 'calendar_unverified'
    try:
        day = datetime.strptime(last, '%Y-%m-%d').date()
    except (TypeError, ValueError):
        return 'calendar_unverified'
    if day > now.date():
        return 'calendar_unverified'
    return None if day == now.date() else 'non_trading_day'


def reserve_day(gate, now):
    """Admission lock is held by the caller; a day is never retried silently."""
    permit = load_schedule(gate)
    path = gate.directory / ('scheduled-' + now.date().isoformat() + '.json')
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        return False
    with os.fdopen(fd, 'w') as stream:
        json.dump(dict(at=now.isoformat(), generation=permit['generation'],
                       policy_sha256=permit['policy_sha256']), stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    directory_fd = os.open(gate.directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return True
