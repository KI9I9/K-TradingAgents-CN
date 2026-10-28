#!/usr/bin/env python3
"""Read-only, bounded production inventory. Does not import providers or create indexes.

Run inside backend with quote_guard_runtime available (or injected in sys.modules).
Only aggregate market-data metadata, hashes and gate/task state are emitted.
"""
import argparse
import hashlib
import json
from pathlib import Path

from pymongo import MongoClient
from quote_guard_runtime import reconstruct


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-entrypoint-sha256', required=True)
    args = parser.parse_args()
    env, runtime = reconstruct('/app/deploy/migration_production_entrypoint.py',
                               args.expected_entrypoint_sha256, '/run/secrets')
    client = MongoClient(host=env['MONGODB_HOST'], port=int(env['MONGODB_PORT']),
                         username=env['MONGODB_USERNAME'], password=env['MONGODB_PASSWORD'],
                         authSource=env.get('MONGODB_AUTH_SOURCE', 'admin'), serverSelectionTimeoutMS=10000)
    try:
        db = client[env['MONGODB_DATABASE']]
        collection = db.stock_daily_quotes
        groups = list(collection.aggregate([{'$group': {
            '_id': {'source': '$data_source', 'period': '$period'},
            'count': {'$sum': 1}, 'first': {'$min': '$trade_date'}, 'last': {'$max': '$trade_date'},
            'without_contract_v1': {'$sum': {'$cond': [{'$eq': ['$history_contract_version', 1]}, 0, 1]}},
        }}], maxTimeMS=15000))
        indexes = [{k: v for k, v in index.items() if k in ('name', 'key', 'unique', 'sparse', 'partialFilterExpression')}
                   for index in collection.list_indexes()]
        jobs = ['tushare_historical_sync', 'tushare_realtime_quotes_hourly', 'akshare_historical_sync']
        prefs = [{'job_id': doc['_id'], 'enabled': doc.get('enabled')}
                 for doc in db.scheduler_preferences.find({'_id': {'$in': jobs}}, {'enabled': 1})]
        gate_root = Path('/app/runtime/tushare-egress')
        gate = {'stop_exists': (gate_root/'STOP').exists(), 'permit_exists': (gate_root/'permit.json').exists()}
        if gate['permit_exists']:
            value = json.loads((gate_root/'permit.json').read_text())
            gate['permit'] = {k: value.get(k) for k in ('phase', 'reason', 'budget', 'used', 'allowed_apis')}
        paths = ['app/services/historical_data_service.py', 'app/worker/tushare_sync_service.py',
                 'tradingagents/dataflows/providers/china/tushare.py', 'app/services/quote_contract.py']
        report = {'runtime': runtime, 'groups': groups, 'indexes': indexes, 'preferences': prefs,
                  'gate': gate, 'historical_flags': {k: str(env.get(k, '<absent>')).lower()
                      for k in ('TUSHARE_UNIFIED_ENABLED', 'TUSHARE_HISTORICAL_SYNC_ENABLED')},
                  'module_hashes': {p: hashlib.sha256((Path('/app')/p).read_bytes()).hexdigest() for p in paths},
                  'active_historical_executions': db.scheduler_executions.count_documents(
                      {'job_id': 'tushare_historical_sync', 'status': {'$in': ['running', 'cancelling']}})}
        print(json.dumps(report, default=str))
    finally:
        client.close()


if __name__ == '__main__':
    try:
        main()
    except Exception:
        raise SystemExit('HISTORY_AUDIT_FAILED: raw exception withheld')
