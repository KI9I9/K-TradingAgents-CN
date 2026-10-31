"""Real HTTP framing over private Unix sockets; no external network or credentials.

TLS is replaced by a pass-through socket here. The existing egress regressions
separately verify CA/hostname validation and handshake deadlines.
"""
import asyncio
from contextvars import copy_context
import json
import os
from pathlib import Path
import socket
import socketserver
import ssl
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import patch

from offline_tushare_support import egress, authorize_new

ROOT = Path(tempfile.mkdtemp(prefix='ta-keepalive-offline-'))
FAKE = 'SYNTHETIC_ONLY_NO_REAL_TOKEN'
GOOD = {'code': 0, 'data': {'fields': ['ts_code'], 'items': [['SYNTHETIC.SZ']]}}


def offline_only(event, args):
    if event in ('socket.getaddrinfo', 'socket.sendto', 'subprocess.Popen'):
        raise RuntimeError('OfflineOnly')
    if event in ('socket.connect', 'socket.bind'):
        if not isinstance(args[1], str) or not args[1].startswith(str(ROOT) + '/'):
            raise RuntimeError('OfflineUnixOnly')


sys.addaudithook(offline_only)


class PlainTLS:
    def __init__(self, raw):
        self.raw = raw

    def __getattr__(self, name):
        return getattr(self.raw, name)

    def do_handshake(self):
        pass

    def pending(self):
        return 0


class Server(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True

    def __init__(self, path):
        self.lock = threading.Lock()
        self.connections = 0
        self.requests = []
        self.actions = []
        self.received = threading.Event()
        self.closed_idle = threading.Event()
        self.stopping = threading.Event()
        super().__init__(path, Handler)


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        with self.server.lock:
            self.server.connections += 1
            identity = self.server.connections
        try:
            while not self.server.stopping.is_set():
                line = self.rfile.readline(8192)
                if not line:
                    return
                headers = {}
                while True:
                    line = self.rfile.readline(8192)
                    if line in (b'\r\n', b''):
                        break
                    name, value = line.decode().split(':', 1)
                    headers[name.lower()] = value.strip()
                body = json.loads(self.rfile.read(int(headers['content-length'])))
                with self.server.lock:
                    self.server.requests.append((identity, body, headers))
                    action = self.server.actions.pop(0) if self.server.actions else 'success'
                self.server.received.set()
                if action == 'stall':
                    self.server.stopping.wait(2)
                    return
                payload = b'bad json' if action == 'malformed' else json.dumps(GOOD).encode()
                close = action == 'close' or headers.get('connection') == 'close'
                self.wfile.write(('HTTP/1.1 200 OK\r\nContent-Length: ' + str(len(payload))
                    + '\r\nConnection: ' + ('close' if close else 'keep-alive') + '\r\n\r\n').encode())
                if action == 'trickle':
                    for byte in payload:
                        self.wfile.write(bytes([byte]))
                        if self.server.stopping.wait(.02):
                            return
                else:
                    self.wfile.write(payload)
                if action == 'idle_close':
                    self.request.shutdown(socket.SHUT_RDWR)
                    self.server.closed_idle.set()
                    return
                if close:
                    return
        except (OSError, ValueError):
            return


class KeepaliveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        self.path = str(self.directory / 'upstream.sock')
        self.server = Server(self.path)
        thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        thread.start()
        self.addCleanup(self.stop_server)
        authorize_new(self.directory / 'permit', 5, 300, ['stock_basic'])
        self.permit = egress.Permit(self.directory / 'permit')
        context = types.SimpleNamespace(check_hostname=True, verify_mode=ssl.CERT_REQUIRED,
            wrap_socket=lambda raw, **kw: PlainTLS(raw))
        for setting in (patch.dict(os.environ, {'TA_TUSHARE_ROUTE': 'mainland_unix'}),
                        patch.object(egress, 'SOCKET_PATH', self.path),
                        patch.object(egress.ssl, 'create_default_context', return_value=context)):
            setting.start()
            self.addCleanup(setting.stop)

    def stop_server(self):
        self.server.stopping.set()
        self.server.shutdown()
        self.server.server_close()

    def query(self, timeout=1, token=FAKE):
        # Recreated clients match the real per-stock provider lifecycle.
        return egress.MainlandTransport(token, lambda *_: '', timeout, self.permit).query('stock_basic')

    def state(self):
        with self.permit.locked() as fd:
            return self.permit.read(fd)

    def test_three_clients_reuse_one_connection_and_charge_every_query(self):
        with egress.history_transport_session() as session:
            for _ in range(3):
                self.assertEqual(self.query(), GOOD['data'])
            cached = session._cached
            self.assertEqual(self.server.connections, 1)
            self.assertEqual(self.state()['used'], 3)
        self.assertTrue(cached._closed)
        self.assertIsNone(cached.sock)
        self.assertEqual(len(self.server.requests), 3)

    def test_default_stays_one_connection_per_request(self):
        self.query()
        self.query()
        self.assertEqual(self.server.connections, 2)
        self.assertTrue(all(r[2]['connection'] == 'close' for r in self.server.requests))

    def test_stopped_gate_cannot_use_cached_socket(self):
        with egress.history_transport_session():
            self.query()
            self.permit.stop()
            with self.assertRaisesRegex(egress.TushareEgressStopped, 'disabled'):
                self.query()
        self.assertEqual(len(self.server.requests), 1)
        self.assertEqual(self.state()['used'], 1)

    def test_exhausted_budget_cannot_use_cached_socket(self):
        with egress.history_transport_session():
            for _ in range(5):
                self.query()
            with self.assertRaisesRegex(egress.TushareEgressStopped, 'budget_exhausted'):
                self.query()
        self.assertEqual(len(self.server.requests), 5)
        self.assertEqual(self.server.connections, 1)

    def test_server_close_header_is_respected(self):
        self.server.actions = ['close', 'success']
        with egress.history_transport_session():
            self.query()
            self.query()
        self.assertEqual(self.server.connections, 2)

    def test_idle_eof_is_discarded_before_next_request(self):
        self.server.actions = ['idle_close', 'success']
        with egress.history_transport_session():
            self.query()
            self.assertTrue(self.server.closed_idle.wait(1))
            self.query()
        self.assertEqual(self.server.connections, 2)
        self.assertEqual(self.state()['used'], 2)

    def test_token_change_and_scope_change_do_not_share_connection(self):
        with egress.history_transport_session():
            self.query()
            self.query(token='SYNTHETIC_SECOND_TOKEN')
        with egress.history_transport_session():
            self.query()
        self.assertEqual(self.server.connections, 3)

    def test_idle_bound_discards_connection_before_another_request(self):
        with egress.history_transport_session() as session:
            self.query()
            session._last_used -= 6
            self.query()
        self.assertEqual(self.server.connections, 2)

    def test_age_bound_discards_connection_before_another_request(self):
        with egress.history_transport_session() as session:
            self.query()
            session._cached._created_at -= 121
            self.query()
        self.assertEqual(self.server.connections, 2)

    def test_idle_connection_has_no_previous_request_watchdog(self):
        with egress.history_transport_session():
            self.query(timeout=.05)
            time.sleep(.1)
            self.query(timeout=.05)
        self.assertEqual(self.server.connections, 1)

    def test_timeout_on_reuse_is_not_transparently_retried(self):
        self.server.actions = ['success', 'stall']
        with egress.history_transport_session() as session:
            self.query(timeout=.08)
            time.sleep(.05)
            started = time.monotonic()
            with self.assertRaisesRegex(egress.TushareEgressStopped, 'timeout'):
                self.query(timeout=.08)
            self.assertGreater(time.monotonic() - started, .06)
            self.assertLess(time.monotonic() - started, .6)
            self.assertIsNone(session._cached)
            with self.assertRaisesRegex(egress.TushareEgressStopped, 'timeout'):
                self.query(timeout=.08)
        self.assertEqual(len(self.server.requests), 2)
        self.assertEqual(self.state()['used'], 2)
        self.assertEqual(self.server.connections, 1)

    def test_trickling_body_cannot_extend_total_deadline(self):
        self.server.actions = ['success', 'trickle']
        with egress.history_transport_session():
            self.query(timeout=.12)
            started = time.monotonic()
            with self.assertRaisesRegex(egress.TushareEgressStopped, 'timeout'):
                self.query(timeout=.12)
            self.assertLess(time.monotonic() - started, .6)

    def test_close_response_body_also_keeps_total_deadline(self):
        self.server.actions = ['trickle']
        started = time.monotonic()
        with self.assertRaisesRegex(egress.TushareEgressStopped, 'timeout'):
            self.query(timeout=.12)
        self.assertLess(time.monotonic() - started, .6)

    def test_old_timer_callback_cannot_close_new_attempt(self):
        conn = egress.UnixTLSConnection(1)
        try:
            conn.connect()
            old_id = conn._watchdog_id
            conn.finish_request()
            conn.begin_request()
            conn._expire(old_id)
            self.assertFalse(conn._expired.is_set())
            self.assertIsNotNone(conn.sock)
        finally:
            conn.close()

    def test_bad_json_is_not_cached_or_resent(self):
        self.server.actions = ['malformed']
        with egress.history_transport_session() as session:
            with self.assertRaisesRegex(egress.TushareEgressStopped, 'response_invalid'):
                self.query()
            self.assertIsNone(session._cached)
        self.assertEqual(len(self.server.requests), 1)

    def test_context_is_shared_across_to_thread_calls(self):
        async def work():
            with egress.history_transport_session():
                await asyncio.to_thread(self.query)
                await asyncio.to_thread(self.query)
        asyncio.run(work())
        self.assertEqual(self.server.connections, 1)

    def test_scope_close_interrupts_inflight_read(self):
        self.server.actions = ['stall']
        errors = []
        with egress.history_transport_session() as session:
            context = copy_context()
            def request():
                try:
                    self.query()
                except egress.TushareEgressStopped as error:
                    errors.append(error.reason)
            thread = threading.Thread(target=lambda: context.run(request))
            thread.start()
            self.assertTrue(self.server.received.wait(1))
            started = time.monotonic()
            session.close()
            thread.join(1)
            self.assertFalse(thread.is_alive())
            self.assertLess(time.monotonic() - started, .6)
        self.assertEqual(len(errors), 1)
        self.assertIsNone(session._cached)


if __name__ == '__main__':
    unittest.main()
