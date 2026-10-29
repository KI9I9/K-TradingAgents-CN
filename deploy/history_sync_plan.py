"""Offline incremental planning only: no provider, database, gate or task imports.

Input observations must be produced by a separate read-only inventory. Their hash
binds a plan to that input; it does not authenticate it or authorize execution.
Default manual planning remains capped at 30 days. An explicit batch policy
may select up to 366 days; windows are never silently truncated or split across
different qfq anchors.
"""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import re

CN = timezone(timedelta(hours=8))


class PlanError(ValueError):
    pass


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     allow_nan=False).encode()).hexdigest()


def _day(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise PlanError('invalid_date')
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise PlanError('invalid_date') from None


def _code(value):
    if not isinstance(value, str):
        raise PlanError('invalid_stock')
    match = re.fullmatch(r'(\d{6})(?:\.(SH|SZ|BJ))?', value)
    if not match:
        raise PlanError('invalid_stock')
    code, suffix = match.groups()
    exchange = ('SH' if code.startswith(('600', '601', '603', '605', '688', '689'))
                else 'SZ' if code.startswith(('000', '001', '002', '003', '300', '301'))
                else 'BJ' if code.startswith(('430', '83', '87', '88', '920')) else None)
    if exchange is None or suffix not in (None, exchange):
        raise PlanError('invalid_stock')
    return code + '.' + exchange


def _observation(value):
    required = {'data_source', 'market', 'period', 'count', 'latest',
                'contract_verified', 'content_sha256'}
    if (not isinstance(value, dict) or set(value) != required
            or value['data_source'] != 'tushare' or value['market'] != 'CN'
            or value['period'] != 'daily' or type(value['count']) is not int
            or value['count'] < 0 or type(value['contract_verified']) is not bool
            or not isinstance(value['content_sha256'], str)
            or not re.fullmatch('[a-f0-9]{64}', value['content_sha256'])):
        raise PlanError('invalid_series_observation')
    if value['count'] == 0:
        if value['latest'] is not None:
            raise PlanError('empty_series_has_cursor')
    else:
        _day(value['latest'])
    return value


def build_plan(symbols, series, *, end, now, bootstrap_start=None, quota=None, max_window_days=30, incremental=True):
    """Freeze a candidate pool and same-source cursors; never grant permission.

    quota describes an aggregate shared budget for daily + adj_factor, not a
    per-endpoint allowance. Missing evidence yields no numeric throughput claim.
    Bootstrap defaults to the existing worker's now-365-day behavior.
    """
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise PlanError('aware_clock_required')
    if type(max_window_days) is not int or not 1 <= max_window_days <= 366 or type(incremental) is not bool:
        raise PlanError('invalid_window_policy')
    today = now.astimezone(CN)
    end_day = _day(end)
    if end_day > today.date() or (end_day == today.date() and today.hour < 18):
        raise PlanError('daily_window_not_closed')
    start = _day(bootstrap_start) if bootstrap_start is not None else today.date()-timedelta(days=365)
    if start > end_day:
        raise PlanError('bootstrap_after_end')
    if not isinstance(symbols, list) or not symbols:
        raise PlanError('empty_or_invalid_pool')
    codes = sorted(set(_code(code) for code in symbols))
    if not isinstance(series, dict) or set(series) != set(codes):
        raise PlanError('inventory_must_exactly_cover_pool')
    observations = {code: _observation(series[code]) for code in codes}
    items = []
    for code in codes:
        row = observations[code]
        reason = None
        first = _day(row['latest']) if row['count'] and incremental else start
        if row['count'] and not row['contract_verified']:
            reason = 'legacy_contract_unproven'
        elif first > end_day:
            reason = 'cursor_after_requested_end'
        elif (end_day-first).days >= max_window_days:
            reason = 'window_exceeds_current_transport'
        items.append(dict(code=code, start=first.isoformat(), end=end,
                          observation_sha256=fingerprint(row),
                          status='blocked' if reason else 'candidate', reason=reason,
                          request_cost=2 if reason is None else None))
    candidate_count = sum(item['status'] == 'candidate' for item in items)
    blocked = len(items)-candidate_count
    budget = None
    if quota is not None:
        if (not isinstance(quota, dict)
                or set(quota) != {'daily_requests', 'rolling_minute_requests', 'evidence_ref'}
                or any(type(quota[k]) is not int or quota[k] < 2
                       for k in ('daily_requests', 'rolling_minute_requests'))
                or not isinstance(quota['evidence_ref'], str) or not quota['evidence_ref'].strip()):
            raise PlanError('invalid_quota_evidence')
        capacity = quota['daily_requests']//2
        budget = dict(**quota, candidate_stocks_per_day=capacity,
                      candidate_budget_days_lower_bound=math.ceil(candidate_count/capacity))
    plan = dict(schema=1, mode='incremental_daily_qfq' if incremental else 'year_daily_qfq',
                max_window_days=max_window_days, execution_authorized=False,
                end=end, bootstrap_start=start.isoformat(),
                inventory_sha256=fingerprint(observations), pool=codes, items=items,
                candidate_count=candidate_count, blocked_count=blocked,
                candidate_requests=candidate_count*2,
                total_requests=None if blocked else candidate_count*2,
                quota=budget, full_market_ready=False,
                unresolved=['execution_not_implemented', 'permit_not_granted']
                + (['series_blockers'] if blocked else [])
                + (['quota_unverified'] if quota is None else []))
    plan['plan_sha256'] = fingerprint(plan)
    return plan
