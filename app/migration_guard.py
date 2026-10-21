"""Temporary migration review guard. Normal deployments are unchanged by default.

This is a limited login/identity check, not the production app or a full read-only
UI. Login/logout can write session/audit/last-login state. All other application
routes are blocked, including GET routes that could initiate external work.
Docker INTERNAL networking is required in addition to this application guard.
"""
import json
import os

ALLOW = frozenset({('POST', '/api/auth/login'), ('POST', '/api/auth/logout'),
                   ('GET', '/api/auth/me')})


def enabled():
    value = os.environ.get('TA_MIGRATION_REVIEW', '0')
    if value not in {'0', '1'}:
        raise ValueError('Invalid migration review flag')
    return value == '1'


class ReviewGuard:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if not enabled() or scope['type'] == 'lifespan':
            return await self.app(scope, receive, send)
        if scope['type'] == 'websocket':
            return await send({'type': 'websocket.close', 'code': 1008})
        if scope['type'] != 'http':
            return
        route = (scope.get('method'), scope.get('path'))
        if route in ALLOW:
            return await self.app(scope, receive, send)
        healthy = route == ('GET', '/api/health')
        payload = {'mode': 'migration_review', 'production_ready': False,
                   'status': 'healthy' if healthy else 'blocked_during_migration_review'}
        body = json.dumps(payload).encode()
        await send({'type': 'http.response.start', 'status': 200 if healthy else 423,
                    'headers': [(b'content-type', b'application/json'), (b'cache-control', b'no-store')]})
        await send({'type': 'http.response.body', 'body': body})
