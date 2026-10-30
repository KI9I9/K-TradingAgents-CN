"""Tushare historical bars: explicit provenance, lots/CNY, no inferred finality.

This contract does not authorize network access or migrate existing records.
Legacy or differently adjusted series must be reviewed before they can be extended.
"""
from datetime import date, datetime, timezone, timedelta
import math
import re

import pandas as pd

PERIODS = frozenset(('daily', 'weekly', 'monthly'))
CN_TZ = timezone(timedelta(hours=8))
VERSION = 1


class HistoricalContractError(ValueError):
    pass


def trade_date(value):
    if isinstance(value, str) and re.fullmatch(r'\d{8}', value):
        value = f'{value[:4]}-{value[4:6]}-{value[6:]}'
    if isinstance(value, (datetime, date)) and not pd.isna(value):
        value = value.strftime('%Y-%m-%d')
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise HistoricalContractError('history_date_invalid')
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        raise HistoricalContractError('history_date_invalid') from None


def _number(value, field, required=False, nonnegative=False):
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)) or value == '':
        if required:
            raise HistoricalContractError('history_missing_' + field)
        return None
    if isinstance(value, bool):
        raise HistoricalContractError('history_invalid_' + field)
    try:
        result = float(value)
    except (ValueError, TypeError):
        raise HistoricalContractError('history_invalid_' + field) from None
    if not math.isfinite(result) or (nonnegative and result < 0):
        raise HistoricalContractError('history_invalid_' + field)
    return result


def _value(row, primary, alias=None, **kwargs):
    value = _number(row.get(primary), primary, **kwargs) if primary in row else None
    other = _number(row.get(alias), primary, **kwargs) if alias and alias in row else None
    if value is not None and other is not None and value != other:
        raise HistoricalContractError('history_conflicting_' + primary)
    result = value if value is not None else other
    if result is None and kwargs.get('required'):
        raise HistoricalContractError('history_missing_' + primary)
    return result


def tushare_documents(symbol, frame, market, period, now=None):
    """Validate the whole frame before any write. Never mutate the provider frame."""
    if market != 'CN' or period not in PERIODS or not re.fullmatch(r'\d{6}', str(symbol)):
        raise HistoricalContractError('history_identity_invalid')
    if not isinstance(frame, pd.DataFrame) or not frame.columns.is_unique or len(frame) > 20000:
        raise HistoricalContractError('history_frame_invalid')
    meta = frame.attrs.get('historical_contract', {})
    if (meta.get('source') != 'tushare' or meta.get('symbol') != symbol
            or meta.get('period') != period or meta.get('volume_unit') != 'lot'
            or meta.get('amount_unit') != 'thousand_CNY'
            or (meta.get('endpoint'), meta.get('price_adjustment')) not in
               (('pro_bar', 'qfq'), ('fund_daily', 'none'), ('daily', 'none'))):
        raise HistoricalContractError('history_provenance_unproven')
    if meta['endpoint'] in ('fund_daily', 'daily') and period != 'daily':
        raise HistoricalContractError('history_fund_period_unsupported')
    start, end = trade_date(meta.get('start_date')), trade_date(meta.get('end_date'))
    if start > end:
        raise HistoricalContractError('history_range_invalid')
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise HistoricalContractError('history_clock_naive')
    today = now.astimezone(CN_TZ).date().isoformat()
    full_symbol = symbol + ('.BJ' if symbol.startswith(('4', '8', '920')) else
                            '.SH' if symbol.startswith(('5', '6', '9')) else '.SZ')
    docs, seen = [], set()
    for index, row in frame.iterrows():
        dates = [row.get(k) for k in ('date', 'trade_date') if k in row and pd.notna(row.get(k))]
        day = trade_date(dates[0] if dates else index)
        if any(trade_date(value) != day for value in dates[1:]):
            raise HistoricalContractError('history_conflicting_date')
        if day in seen or not start <= day <= end or day > today:
            raise HistoricalContractError('history_duplicate_or_out_of_range_date')
        seen.add(day)
        for name in ('symbol', 'code', 'ts_code'):
            if name in row and pd.notna(row[name]) and str(row[name]) not in (symbol, full_symbol):
                raise HistoricalContractError('history_symbol_mismatch')
        prices = {key: _value(row, key, required=True, nonnegative=True)
                  for key in ('open', 'high', 'low', 'close')}
        if (min(prices.values()) <= 0 or prices['low'] > min(prices['open'], prices['close'])
                or prices['high'] < max(prices['open'], prices['close'])):
            raise HistoricalContractError('history_ohlc_invalid')
        volume = _value(row, 'volume', 'vol', required=True, nonnegative=True)
        amount = _value(row, 'amount', 'turnover', nonnegative=True)
        amount = None if amount is None else amount * 1000.0
        if amount is not None and not math.isfinite(amount):
            raise HistoricalContractError('history_invalid_amount')
        doc = dict(symbol=symbol, code=symbol, full_symbol=full_symbol, market=market,
                   trade_date=day, period=period, data_source='tushare', **prices,
                   volume=volume, amount=amount, volume_unit='lot', amount_unit='CNY',
                   price_adjustment=meta['price_adjustment'], source_endpoint=meta['endpoint'],
                   history_contract_version=VERSION, version=1)
        # Preserve upstream changes; do not recompute qfq close against a possibly raw pre_close.
        for key in ('pre_close', 'change', 'pct_chg'):
            doc[key] = _value(row, key, nonnegative=(key == 'pre_close'))
        docs.append(doc)
    return sorted(docs, key=lambda doc: doc['trade_date'])


def validate_existing_contract(existing, adjustment):
    for old in existing:
        if (old.get('history_contract_version') != VERSION or old.get('volume_unit') != 'lot'
                or old.get('amount_unit') != 'CNY'
                or old.get('price_adjustment') != adjustment):
            raise HistoricalContractError('history_existing_contract_unproven')


def validate_existing_series(existing, documents):
    """Refuse silent unit migrations or concatenation across qfq adjustment changes."""
    if not documents:
        return
    first = documents[0]
    validate_existing_contract(existing, first['price_adjustment'])
    if not existing or first['price_adjustment'] != 'qfq':
        return
    old_by_day = {doc['trade_date']: doc for doc in existing}
    new_by_day = {doc['trade_date']: doc for doc in documents}
    latest = max(old_by_day)
    if max(new_by_day) > latest and latest not in new_by_day:
        raise HistoricalContractError('history_adjustment_overlap_missing')
    for day in old_by_day.keys() & new_by_day.keys():
        for field in ('open', 'high', 'low', 'close'):
            old = _number(old_by_day[day].get(field), field, required=True)
            if not math.isclose(old, new_by_day[day][field], rel_tol=1e-8, abs_tol=1e-6):
                raise HistoricalContractError('history_adjustment_or_price_revision')
