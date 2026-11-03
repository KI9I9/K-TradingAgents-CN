"""Authenticated read-only acceptance; never create an account or submit an order.

Run after the continuous session so quote-preview acceptance makes no upstream
request. Credentials arrive on stdin and are never included in the receipt.
"""
from datetime import datetime
import json
import sys
import types
import urllib.request
from zoneinfo import ZoneInfo


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('redirect_refused')


def request(path, token=None, body=None):
    headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = 'Bearer '+token
    req = urllib.request.Request('http://127.0.0.1:8000'+path, headers=headers,
        data=json.dumps(body).encode() if body is not None else None)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    with opener.open(req, timeout=20) as response:
        value = json.load(response)
    assert value.get('success') is True, 'api_unsuccessful'
    return value['data']


def main():
    payload = json.load(sys.stdin)
    now = datetime.now(ZoneInfo('Asia/Shanghai')); minute = now.hour*60+now.minute
    assert now.weekday() < 5 and minute >= 900, 'acceptance_requires_after_close'
    login = request('/api/auth/login', body=payload['auth'])
    token, user = login['access_token'], login['user']['id']
    runtime = types.ModuleType('runtime'); exec(payload['runtime'], runtime.__dict__)
    env, _ = runtime.reconstruct('/app/deploy/migration_production_entrypoint.py',
        'd75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b', '/run/secrets')
    from pymongo import MongoClient
    client = MongoClient(host=env['MONGODB_HOST'], port=int(env['MONGODB_PORT']),
        username=env['MONGODB_USERNAME'], password=env['MONGODB_PASSWORD'],
        authSource=env.get('MONGODB_AUTH_SOURCE', 'admin'), retryWrites=False,
        retryReads=False, serverSelectionTimeoutMS=5000, socketTimeoutMS=10000)
    try:
        db = client[env['MONGODB_DATABASE']]
        existing = db.paper_accounts.find_one({'user_id': user}, {'cash': 1, 'realized_pnl': 1})
        account_safe = bool(existing and isinstance(existing.get('cash'), dict)
                            and isinstance(existing.get('realized_pnl'), dict))
        # /account may create or migrate an account; call only a current-schema account.
        account = request('/api/paper/account', token) if account_safe else None
        positions = request('/api/paper/positions', token)['items']
        for row in positions:
            assert row['valuation_status'] in ('empty', 'missing', 'reference')
            if row['quantity'] and row['last_price'] is None:
                assert row['market_value'] is None and row['unrealized_pnl'] is None
            if row['last_price'] is not None:
                assert row['valuation_status'] == 'reference'
        if account:
            summary = account['account']
            for currency, state in summary['valuation'].items():
                missing = sum(1 for row in positions if row['currency'] == currency
                              and row['quantity'] and row['last_price'] is None)
                assert state['missing_count'] == missing and state['complete'] == (missing == 0)
                if missing:
                    assert summary['equity'][currency] is None and summary['positions_value'][currency] is None
                else:
                    assert summary['equity'][currency] is not None
        quotes = []
        for code in ('600000', '159865'):
            quote = request('/api/paper/quote/'+code+'?market=CN', token)
            assert quote['available'] is False and quote['current_price'] is None
            assert quote['high'] is None and quote['low'] is None
            assert quote['unavailable_reason']['code'] == 'outside_continuous_session'
            assert '连续交易时段' in quote['unavailable_reason']['message']
            quotes.append(dict(code=code, reason=quote['unavailable_reason']))
    finally:
        client.close()
    print('PAPER_DISPLAY_ACCEPT:'+json.dumps(dict(ok=True, positions=len(positions),
        missing_prices=sum(p['valuation_status'] == 'missing' for p in positions),
        account_checked=account_safe, quotes=quotes, orders_created=0), ensure_ascii=False))


if __name__ == '__main__':
    main()
