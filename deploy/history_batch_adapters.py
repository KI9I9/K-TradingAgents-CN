"""Async inventory and existing-service adapters. No clients or permits created.

Callers inject collections, the existing historical service, a provider factory,
and separate operation authorization. No application startup or execution CLI.
"""
from copy import deepcopy
import hashlib
import json
import math

from bson import json_util
import pandas as pd

from app.services.historical_contract import (
    HistoricalContractError, trade_date, tushare_documents,
    validate_existing_contract, validate_existing_series,
)
from deploy.history_sync_plan import _code, fingerprint
from deploy.history_batch_journal import BatchStopped

UNIQUE_KEY = [('symbol', 1), ('trade_date', 1), ('data_source', 1), ('period', 1)]


def raw_json(rows):
    # Exact BSON types (including IDs/timestamps), deterministic row/key order.
    return json_util.dumps(sorted(rows, key=lambda row: row['trade_date']),
        json_options=json_util.CANONICAL_JSON_OPTIONS, sort_keys=True, separators=(',', ':'))


def raw_hash(rows):
    return hashlib.sha256(raw_json(rows).encode()).hexdigest()


class Inventory:
    def __init__(self, collection):
        self.collection = collection

    async def snapshot(self, code):
        code = _code(code)
        indexes = await self.collection.index_information()
        if not any(i.get('unique') is True and i.get('key') == UNIQUE_KEY
                   and not i.get('sparse') and not i.get('partialFilterExpression')
                   for i in indexes.values()):
            raise BatchStopped('history_unique_index_missing')
        query = dict(symbol=code[:6], data_source='tushare', period='daily')
        rows = await self.collection.find(query).max_time_ms(10000).to_list(length=20001)
        if len(rows) > 20000:
            raise BatchStopped('history_series_too_large')
        # Reject broken identity/dates rather than invent a cursor.
        for row in rows:
            if any(row.get(k) != v for k, v in query.items()) or trade_date(row.get('trade_date')) != row['trade_date']:
                raise BatchStopped('history_inventory_identity_invalid')
        if len({r['trade_date'] for r in rows}) != len(rows):
            raise BatchStopped('history_duplicate_dates')
        verified = all(r.get('market') == 'CN' for r in rows)
        try:
            validate_existing_contract(rows, 'qfq')
        except HistoricalContractError:
            verified = False
        observation = dict(data_source='tushare', market='CN', period='daily', count=len(rows),
            latest=max((r['trade_date'] for r in rows), default=None),
            contract_verified=verified, content_sha256=raw_hash(rows))
        return observation, deepcopy(rows)

    async def collect(self, symbols):
        if not isinstance(symbols, list) or not symbols:
            raise BatchStopped('explicit_nonempty_pool_required')
        codes = sorted(set(_code(code) for code in symbols))
        if len(codes) > 10000:
            raise BatchStopped('pool_too_large')
        series = {}
        for code in codes:
            series[code], _ = await self.snapshot(code)
        # No quotas inferred, and no index creation / provider initialization.
        return dict(symbols=codes, series=series)


def freeze_frame(frame):
    if not isinstance(frame, pd.DataFrame) or frame.empty or len(frame) > 366 or not frame.columns.is_unique:
        raise BatchStopped('invalid_frame')
    columns = list(frame.columns)
    if any(not isinstance(c, str) for c in columns):
        raise BatchStopped('invalid_frame_columns')
    def plain(value):
        if pd.isna(value):
            return None
        if hasattr(value, 'item'):
            value = value.item()
        if value is not None and type(value) not in (str, int, float, bool):
            raise BatchStopped('unsupported_frame_value')
        if isinstance(value, float) and not math.isfinite(value):
            raise BatchStopped('nonfinite_frame_value')
        return value
    index, data = [], []
    for idx, row in frame.iterrows():
        # Validate conflicting date columns before this function via tushare_documents.
        day = next((row[c] for c in ('trade_date', 'date') if c in row and pd.notna(row[c])), idx)
        index.append(trade_date(day))
        data.append([trade_date(row[c]) if c in ('trade_date', 'date') and pd.notna(row[c])
                     else plain(row[c]) for c in columns])
    result = dict(columns=columns, index=index, data=data,
                  historical_contract=deepcopy(frame.attrs.get('historical_contract')))
    # Round-trip via strict JSON; no pandas pickle or executable content on disk.
    return json.loads(json.dumps(result, allow_nan=False))


def thaw_frame(value):
    if not isinstance(value, dict) or set(value) != {'columns', 'index', 'data', 'historical_contract'}:
        raise BatchStopped('invalid_frozen_frame')
    frame = pd.DataFrame(value['data'], index=value['index'], columns=value['columns'])
    frame.attrs['historical_contract'] = deepcopy(value['historical_contract'])
    return frame


def same_collection(left, right):
    """Compare resolved Mongo targets, not ephemeral loop-safe proxy identity.

    Motor and the application's collection proxy expose a PyMongo delegate.
    Require the very same client as well as PyMongo collection equality; client
    equality alone may accept distinct clients using the same server address.
    """
    if left is right:
        return True
    from pymongo.collection import Collection
    a, b = getattr(left, 'delegate', None), getattr(right, 'delegate', None)
    return (isinstance(a, Collection) and isinstance(b, Collection)
            and a.database.client is b.database.client and a == b)


class HistoricalAdapter:
    """Connects the durable core to real async provider and historical methods.

    authorize is an async, read-only policy check for fetch/write/reconcile.
    job_factory is async: returns an already explicitly authorized ManualJob-like
    object with exact one-stock scope. Its transport MUST debit each HTTP call;
    this class never provisions, resets, renews or removes STOP from that gate.
    provider_factory is async and runs ONLY inside job.context(), after checks.
    """
    def __init__(self, collection, historical_service, provider_factory, job_factory, authorize):
        self.inventory = Inventory(collection)
        self.service = historical_service
        self.provider_factory = provider_factory
        self.job_factory = job_factory
        self.authorize = authorize

    async def preflight(self, item):
        observation, _ = await self.inventory.snapshot(item['code'])
        return fingerprint(observation)

    async def fetch(self, item):
        observation, baseline = await self.inventory.snapshot(item['code'])
        if fingerprint(observation) != item['observation_sha256'] or not observation['contract_verified']:
            raise BatchStopped('inventory_changed_or_legacy')
        job = await self.job_factory(deepcopy(item))
        expected = dict(codes=[item['code']], start=item['start'].replace('-', ''), end=item['end'].replace('-', ''))
        if job.approved != expected:
            job.fail('invalid_request')
            raise BatchStopped('transport_scope_mismatch')
        try:
            with job.context():
                job.assert_ready()
                provider = await self.provider_factory()
                frame = await provider.get_historical_data(item['code'][:6], item['start'], item['end'], period='daily')
                job.assert_ready()
                documents = tushare_documents(item['code'][:6], frame, 'CN', 'daily')
                frozen = freeze_frame(frame)
                rebuilt = tushare_documents(item['code'][:6], thaw_frame(frozen), 'CN', 'daily')
                if documents != rebuilt:
                    raise BatchStopped('frame_roundtrip_changed')
                payload = dict(schema=1, code=item['code'], start=item['start'], end=item['end'],
                    frame=frozen, documents=documents, baseline=raw_json(baseline))
                if not await self.validate(item, payload):
                    raise BatchStopped('payload_contract_invalid')
                # finish verifies daily + adj_factor were accounted for by transport.
                job.finish()
                return payload
        except BaseException:
            job.fail('interrupted')
            raise

    async def validate(self, item, payload):
        try:
            if (set(payload) != {'schema', 'code', 'start', 'end', 'frame', 'documents', 'baseline'}
                    or type(payload['schema']) is not int or payload['schema'] != 1
                    or any(payload[k] != item[k] for k in ('code', 'start', 'end'))):
                return False
            frame = thaw_frame(payload['frame'])
            meta = frame.attrs['historical_contract']
            if (trade_date(meta['start_date']) != item['start'] or trade_date(meta['end_date']) != item['end']
                    or meta['price_adjustment'] != 'qfq' or meta['endpoint'] != 'pro_bar'):
                return False
            documents = tushare_documents(item['code'][:6], frame, 'CN', 'daily')
            if not 1 <= len(documents) <= 366 or documents != payload['documents']:
                return False
            baseline = json_util.loads(payload['baseline'])
            if not isinstance(baseline, list):
                return False
            for row in baseline:
                if (row.get('symbol') != item['code'][:6] or row.get('data_source') != 'tushare'
                        or row.get('market') != 'CN' or row.get('period') != 'daily'):
                    return False
            if len({r['trade_date'] for r in baseline}) != len(baseline):
                return False
            observation = dict(data_source='tushare', market='CN', period='daily', count=len(baseline),
                latest=max((r['trade_date'] for r in baseline), default=None),
                contract_verified=True, content_sha256=raw_hash(baseline))
            if fingerprint(observation) != item['observation_sha256']:
                return False
            validate_existing_series(baseline, documents)
            return True
        except (ValueError, KeyError, TypeError, AttributeError, BatchStopped):
            return False

    async def write(self, item, payload):
        if not await self.validate(item, payload) or await self.preflight(item) != item['observation_sha256']:
            raise BatchStopped('write_preflight_failed')
        # The legacy service resolves its DB through the application getter. Verify
        # that it targets the same collection as inventory BEFORE permitting writes.
        target_rows = await self.service.validate_tushare_series(item['code'][:6], 'daily', 'CN', 'qfq')
        if not same_collection(self.service.db.stock_daily_quotes, self.inventory.collection):
            raise BatchStopped('service_collection_mismatch')
        if raw_hash(target_rows) != raw_hash(json_util.loads(payload['baseline'])):
            raise BatchStopped('write_inventory_changed')
        count = await self.service.save_historical_data(item['code'][:6], thaw_frame(payload['frame']),
            'tushare', market='CN', period='daily', overwrite=True)
        if type(count) is not int or count != len(payload['documents']):
            raise BatchStopped('write_count_unconfirmed')

    async def matches(self, item, payload):
        if not await self.validate(item, payload):
            return False
        _, actual = await self.inventory.snapshot(item['code'])
        baseline = {r['trade_date']: r for r in json_util.loads(payload['baseline'])}
        incoming = {r['trade_date']: r for r in payload['documents']}
        found = {r['trade_date']: r for r in actual}
        if set(found) != set(baseline) | set(incoming):
            return False
        for day, row in found.items():
            if day not in incoming:
                if raw_json([row]) != raw_json([baseline[day]]):
                    return False
                continue
            business = {k: v for k, v in row.items() if k not in ('_id', 'created_at', 'updated_at')}
            if business != incoming[day]:
                return False
            old = baseline.get(day)
            if old is not None:
                old_business = {k: v for k, v in old.items() if k not in ('_id', 'created_at', 'updated_at')}
                if old_business == incoming[day]:
                    if raw_json([old]) != raw_json([row]):
                        return False  # replay must preserve original BSON identity/timestamps
                elif any(row.get(k) != old.get(k) for k in ('_id', 'created_at')):
                    return False
        return True
