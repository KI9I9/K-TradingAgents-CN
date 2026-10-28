"""Bounded captured-history save/replay. No upstream/provider startup or automatic rollback."""
import ast
import asyncio
from datetime import datetime
import hashlib
import importlib.util
import json
import logging
import os
from pathlib import Path
import sys
import types
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from bson import BSON, ObjectId
from pymongo import MongoClient, UpdateOne
from pymongo.write_concern import WriteConcern

SERIES = dict(symbol='600000', data_source='tushare', period='daily')
DAYS = ['2026-10-21', '2026-10-22', '2026-10-23', '2026-10-26', '2026-10-27']
RUN = Path('/app/runtime/history-write-20261028-600000-v1')
ENTRY = 'd75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b'


def require(value, reason):
    if not value:
        raise ValueError(reason)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def bson_rows(rows):
    return b''.join(BSON.encode(row) for row in sorted(rows, key=lambda r: str(r['_id'])))


def save_exclusive(path, raw):
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(parent)
    finally: os.close(parent)


def captured_frame(captured, contract):
    require(captured['status'] == 'passed' and captured['real_attempts'] == 2
            and captured['replay_identical'] is True and captured['differences'] == [], 'sample_not_passed')
    docs = captured['documents']
    require(len(docs) == 5 and [d['trade_date'] for d in docs] == DAYS, 'scope_dates')
    for d in docs:
        require(all(d.get(k) == v for k, v in SERIES.items()), 'scope_series')
        require(d.get('source_endpoint') == 'pro_bar' and d.get('price_adjustment') == 'qfq', 'scope_provenance')
    frame = pd.DataFrame(docs).copy(deep=True)
    frame['amount'] = frame['amount'] / 1000.0
    frame.attrs['historical_contract'] = dict(source='tushare', symbol='600000', period='daily',
        volume_unit='lot', amount_unit='thousand_CNY', start_date=DAYS[0], end_date=DAYS[-1],
        endpoint='pro_bar', price_adjustment='qfq')
    require(contract.tushare_documents('600000', frame, 'CN', 'daily') == docs, 'capture_roundtrip')
    return frame


class Cursor:
    def __init__(self, cursor): self.cursor = cursor
    async def to_list(self, length): return list(self.cursor.limit(length))


class BoundedCollection:
    """Only exact five insert-if-absent operations may reach MongoDB once."""
    def __init__(self, collection, documents, ids, write_enabled=False):
        self.collection, self.expected, self.ids = collection, {d['trade_date']: d for d in documents}, ids
        self.write_enabled, self.batches = write_enabled, 0

    async def index_information(self): return self.collection.index_information()

    def find(self, query):
        require(query == SERIES, 'read_scope')
        return Cursor(self.collection.find(query))

    async def bulk_write(self, operations, ordered):
        require(self.write_enabled and self.batches == 0 and ordered is True, 'write_budget_or_mode')
        require(len(operations) == 5, 'write_count')
        validated, seen = [], set()
        for op in operations:
            require(isinstance(op, UpdateOne) and op._upsert is True, 'insert_only')
            require(set(op._doc) == {'$setOnInsert'}, 'update_forbidden')
            doc = dict(op._doc['$setOnInsert']); day = doc.get('trade_date')
            require(day in self.expected and day not in seen, 'write_dates')
            require(op._filter == {**SERIES, 'trade_date': day}, 'write_scope')
            require(isinstance(doc.get('created_at'), datetime) and doc.get('created_at') == doc.get('updated_at'), 'timestamps')
            require({k: v for k, v in doc.items() if k not in ('created_at', 'updated_at')} == self.expected[day], 'write_payload')
            doc['_id'] = self.ids[day]
            validated.append(UpdateOne(op._filter, {'$setOnInsert': doc}, upsert=True)); seen.add(day)
        self.batches += 1  # an uncertain result must never be retried
        return self.collection.bulk_write(validated, ordered=True)


def load_service(root, bounded):
    """Run installed save/validation method bodies without app initialization or DDL."""
    for name in ('app', 'app.core', 'app.services'):
        mod = types.ModuleType(name); mod.__path__ = []; sys.modules[name] = mod
    name = 'app.services.historical_contract'
    spec = importlib.util.spec_from_file_location(name, root / 'app/services/historical_contract.py')
    contract = importlib.util.module_from_spec(spec); sys.modules[name] = contract; spec.loader.exec_module(contract)
    db = types.ModuleType('app.core.database')
    db.get_mongo_db = lambda: types.SimpleNamespace(stock_daily_quotes=bounded)
    sys.modules[db.__name__] = db
    tree = ast.parse((root / 'app/services/historical_data_service.py').read_text())
    original = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'HistoricalDataService')
    names = {'save_historical_data', '_save_tushare_history', 'validate_tushare_series'}
    methods = [n for n in original.body if isinstance(n, ast.AsyncFunctionDef) and n.name in names]
    require(len(methods) == 3, 'service_methods')
    cls = ast.ClassDef(name='InstalledService', bases=[], keywords=[], body=methods, decorator_list=[])
    ns = dict(pd=pd, datetime=datetime, Dict=Dict, Any=Any, logger=logging.getLogger('bounded_save'))
    exec(compile(ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[])), '<installed-service>', 'exec'), ns)
    return ns['InstalledService'](), contract


async def save_and_replay(service, frame, collection, documents, ids):
    expected = {d['trade_date']: d for d in documents}
    count = await service.save_historical_data('600000', frame, 'tushare', 'CN', 'daily', overwrite=False)
    require(count == 5, 'save_count')
    first = list(collection.find(SERIES))
    require(len(first) == 5, 'persisted_count')
    for row in first:
        require(row['_id'] == ids[row['trade_date']], 'concurrent_writer_detected')
        require({k: v for k, v in row.items() if k not in ('_id', 'created_at', 'updated_at')} == expected[row['trade_date']], 'persisted_payload')
    count = await service.save_historical_data('600000', frame, 'tushare', 'CN', 'daily', overwrite=False)
    second = list(collection.find(SERIES))
    require(count == 5 and bson_rows(first) == bson_rows(second), 'replay_changed_bson')
    return first


def main(request):
    os.umask(0o077)
    report = {'status': 'failed', 'upstream_requests': 0, 'write_attempted': False}
    client = None
    reserved = False
    root = Path('/app')
    try:
        require(request['mode'] in ('preflight', 'execute'), 'mode')
        for path, digest in request['source_hashes'].items():
            require(sha((root / path).read_bytes()) == digest, 'installed_source_changed')
        raw = request['captured_json'].encode()
        require(sha(raw) == request['captured_sha256'], 'capture_changed')
        captured = json.loads(raw)['result.json']
        def protected_hashes():
            return {p: sha(Path(p).read_bytes()) for p in request['protected_hashes']}
        require(protected_hashes() == request['protected_hashes'], 'gate_changed')
        rt = types.ModuleType('runtime'); exec(request['runtime_tool'], rt.__dict__)
        env, effective = rt.reconstruct('/app/deploy/migration_production_entrypoint.py', ENTRY, '/run/secrets')
        require(all(not v['enabled'] for v in effective['gates'].values()), 'quote_gate_enabled')
        require(str(env.get('TUSHARE_UNIFIED_ENABLED')).lower() == 'false', 'tushare_enabled')
        client = MongoClient(host=env['MONGODB_HOST'], port=int(env['MONGODB_PORT']),
            username=env['MONGODB_USERNAME'], password=env['MONGODB_PASSWORD'],
            authSource=env.get('MONGODB_AUTH_SOURCE', 'admin'), serverSelectionTimeoutMS=10000,
            retryWrites=False, retryReads=False, socketTimeoutMS=10000, connectTimeoutMS=10000)
        db = client[env['MONGODB_DATABASE']]
        collection = db.get_collection('stock_daily_quotes', write_concern=WriteConcern(w='majority', j=True, wtimeout=10000))
        def check_jobs():
            for job in ('tushare_historical_sync', 'tushare_realtime_quotes_hourly'):
                require((db.scheduler_preferences.find_one({'_id': job}, {'enabled': 1}) or {}).get('enabled') is False, 'task_enabled')
            require(db.scheduler_executions.count_documents({'job_id': {'$regex': '^tushare_'},
                'status': {'$in': ['running', 'cancelling']}}) == 0, 'task_inflight')
        check_jobs()
        require(not RUN.exists(), 'run_already_reserved')
        before = list(collection.find({'data_source': 'tushare'})); backup = bson_rows(before)
        require(len(before) == 2089, 'tushare_count_drift')
        require(not list(collection.find(SERIES)), 'series_not_empty')
        ids = {day: ObjectId() for day in DAYS}
        bounded = BoundedCollection(collection, captured['documents'], ids)
        service, contract = load_service(root, bounded)
        frame = captured_frame(captured, contract)
        require(asyncio.run(service.validate_tushare_series('600000', 'daily')) == [], 'series_changed')
        index_hash = sha(json.dumps(collection.index_information(), sort_keys=True, default=str).encode())
        baseline = dict(tushare_count=len(before), tushare_bson_sha256=sha(backup), index_sha256=index_hash)
        report.update(baseline=baseline, target_count=5, protected_hashes_unchanged=True)
        if request['mode'] == 'preflight':
            report.update(status='preflight_passed', business_database_writes=False)
        else:
            require(request.get('approved') is True, 'write_not_authorized')
            require(baseline == request['baseline'], 'baseline_drift')
            RUN.mkdir(mode=0o700)  # immutable one-run reservation; never reset/retry
            reserved = True
            save_exclusive(RUN / 'before.bson', backup)
            save_exclusive(RUN / 'manifest.json', json.dumps(dict(baseline=baseline,
                captured_sha256=request['captured_sha256'], ids={k: str(v) for k, v in ids.items()},
                source_hashes=request['source_hashes']), sort_keys=True).encode())
            check_jobs(); require(protected_hashes() == request['protected_hashes'], 'gate_changed_before_write')
            require(not list(collection.find(SERIES)), 'series_changed_before_write')
            bounded.write_enabled = True
            report['write_attempted'] = True
            rows = asyncio.run(save_and_replay(service, frame, collection, captured['documents'], ids))
            after = list(collection.find({'data_source': 'tushare', '_id': {'$nin': list(ids.values())}}))
            require(sha(bson_rows(after)) == sha(backup), 'other_tushare_rows_changed')
            require(protected_hashes() == request['protected_hashes'], 'gate_changed_after_write')
            save_exclusive(RUN / 'after.bson', bson_rows(rows))
            report.update(status='passed', saved_count=5, replay_identical=True,
                write_batches=bounded.batches, other_tushare_unchanged=True, protected_hashes_unchanged=True)
    except BaseException as error:
        report.update(status='failed', error_type=type(error).__name__, automatic_retry=False,
                      automatic_rollback=False)
    finally:
        if client is not None: client.close()
        if reserved:
            # Preserve an existing outcome rather than overwriting it on accidental re-execution.
            try: save_exclusive(RUN / 'result.json', json.dumps(report, default=str).encode())
            except FileExistsError: pass
        print(json.dumps(report, default=str))
    return 0 if report['status'] in ('preflight_passed', 'passed') else 1


if __name__ == '__main__':
    raise SystemExit(main(REQUEST))
