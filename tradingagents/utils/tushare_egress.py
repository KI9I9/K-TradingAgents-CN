"""Fail-closed, finite-permit Tushare transport. No application imports or secrets on disk.

This is a bounded acceptance candidate, not an unlimited production grant.
Only an explicitly provisioned permit can authorize a request; failures latch.
"""
from contextlib import contextmanager
from contextvars import ContextVar
import fcntl
import http.client
import json
import math
import os
from pathlib import Path
import re
import select
import socket
import ssl
import stat
import threading
import time

HOST = 'api.waditu.com'
SOCKET_PATH = '/run/tushare-egress/upstream.sock'
STATE_DIR = '/app/runtime/tushare-egress'
CA_FILE = '/etc/ssl/certs/ca-certificates.crt'
MAX_BODY = 4 * 1024 * 1024
MAX_PERMIT = 5
MAX_TTL = 600
REASONS = frozenset(('none', 'not_authorized', 'disabled', 'invalid_mode', 'busy',
    'state_invalid', 'state_io_failed', 'expired', 'budget_exhausted',
    'previous_attempt_incomplete', 'api_not_approved', 'invalid_request',
    'network_failed', 'tls_failed', 'timeout', 'redirect_blocked', 'http_error',
    'response_invalid', 'response_too_large', 'api_rejected', 'usage_limit',
    'insufficient_points', 'permission_denied', 'token_invalid', 'ip_restriction',
    'invalid_parameters', 'service_unavailable', 'unclassified', 'interrupted'))


class TushareEgressStopped(RuntimeError):
    def __init__(self, reason, api_code=None, http_status=None):
        self.reason = reason if reason in REASONS else 'state_invalid'
        self.api_code = api_code if type(api_code) is int and -1000000 <= api_code <= 1000000 else None
        # HTTP status is diagnostic metadata only.  It is deliberately not
        # persisted in permit.json (api_code remains reserved for Tushare's
        # JSON API code), and no response body or headers are retained.
        self.http_status = (http_status if type(http_status) is int and 100 <= http_status <= 599
                            else None)
        super().__init__('tushare_egress_stopped reason=' + self.reason
                         + ' code=' + (str(self.api_code) if self.api_code is not None else 'unknown')
                         + (' http_status=' + str(self.http_status) if self.http_status is not None else ''))


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError()
        result[key] = value
    return result


def _state(data):
    keys = {'schema', 'phase', 'budget', 'used', 'expires_at', 'allowed_apis', 'reason', 'api_code'}
    if (not isinstance(data, dict) or set(data) != keys or type(data['schema']) is not int
            or data['schema'] != 1 or data['phase'] not in ('ready', 'in_flight', 'blocked', 'exhausted', 'disabled')
            or type(data['budget']) is not int or not 1 <= data['budget'] <= MAX_PERMIT
            or type(data['used']) is not int or not 0 <= data['used'] <= data['budget']
            or type(data['expires_at']) not in (int, float) or not math.isfinite(data['expires_at'])
            or not isinstance(data['allowed_apis'], list) or not 1 <= len(data['allowed_apis']) <= 8
            or not all(isinstance(api, str) and re.fullmatch('[a-z][a-z0-9_]{0,63}', api)
                       for api in data['allowed_apis'])
            or len(set(data['allowed_apis'])) != len(data['allowed_apis']) or data['reason'] not in REASONS
            or (data['api_code'] is not None and (type(data['api_code']) is not int
                                                 or not -1000000 <= data['api_code'] <= 1000000))):
        raise TushareEgressStopped('state_invalid')
    if (data['phase'] == 'ready' and (data['reason'] != 'none' or data['used'] >= data['budget'])):
        raise TushareEgressStopped('state_invalid')
    return data


def _private_fd(fd):
    info = os.fstat(fd)
    if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.geteuid() or info.st_nlink != 1):
        raise TushareEgressStopped('state_invalid')


class Permit:
    """Separate inode lock + atomic journal; flock covers threads and processes.

    No automatic creation, renewal, reset or recovery in the request path.
    An in-flight record left by a killed process is never assumed unsent.
    """
    def __init__(self, directory=STATE_DIR):
        self.directory = Path(directory)

    @staticmethod
    def stopped(directory_fd):
        try:
            os.stat('STOP', dir_fd=directory_fd, follow_symlinks=False)
            return True
        except FileNotFoundError:
            return False

    def stop(self):
        """Set an irreversible stop marker without waiting for an in-flight lock.

        It cannot unsend a request. No subsequent operation may start; there is
        deliberately no automatic or CLI removal of this marker.
        """
        if not self.directory.is_absolute() or self.directory.resolve() != self.directory:
            raise TushareEgressStopped('state_invalid')
        fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            if stat.S_IMODE(info.st_mode) != 0o700 or info.st_uid != os.geteuid():
                raise TushareEgressStopped('state_invalid')
            try:
                marker = os.open('STOP', os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600, dir_fd=fd)
            except FileExistsError:
                return
            try: os.fsync(marker)
            finally: os.close(marker)
            os.fsync(fd)
        finally:
            os.close(fd)

    @contextmanager
    def locked(self):
        directory_fd = lock_fd = None
        try:
            if not self.directory.is_absolute() or self.directory.resolve() != self.directory:
                raise TushareEgressStopped('state_invalid')
            directory_fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            info = os.fstat(directory_fd)
            if stat.S_IMODE(info.st_mode) != 0o700 or info.st_uid != os.geteuid():
                raise TushareEgressStopped('state_invalid')
            lock_fd = os.open('gate.lock', os.O_RDWR | os.O_NOFOLLOW, dir_fd=directory_fd)
            _private_fd(lock_fd)
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise TushareEgressStopped('busy') from None
            yield directory_fd
        except FileNotFoundError:
            raise TushareEgressStopped('not_authorized') from None
        except OSError:
            raise TushareEgressStopped('state_io_failed') from None
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
            if directory_fd is not None:
                os.close(directory_fd)

    def read(self, directory_fd):
        fd = os.open('permit.json', os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
        with os.fdopen(fd, 'rb') as stream:
            _private_fd(stream.fileno())
            raw = stream.read(8193)
        if len(raw) > 8192:
            raise TushareEgressStopped('state_invalid')
        try:
            return _state(json.loads(raw, object_pairs_hook=_pairs))
        except (ValueError, TypeError, KeyError):
            raise TushareEgressStopped('state_invalid') from None

    def write(self, directory_fd, value):
        _state(value)
        # A leftover next file is evidence of interruption, not something to delete.
        fd = os.open('permit.next', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory_fd)
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, allow_nan=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace('permit.next', 'permit.json', src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        os.fsync(directory_fd)

    def perform(self, api_name, operation):
        with self.locked() as directory_fd:
            value = self.read(directory_fd)
            reason = None
            if self.stopped(directory_fd):
                reason = 'disabled'
            elif value['phase'] == 'in_flight':
                reason = 'previous_attempt_incomplete'
            elif value['phase'] != 'ready':
                reason = value['reason'] if value['reason'] != 'none' else 'disabled'
            elif time.time() >= value['expires_at']:
                reason = 'expired'
            elif value['expires_at'] - time.time() > MAX_TTL + 5:
                reason = 'state_invalid'
            elif api_name not in value['allowed_apis']:
                reason = 'api_not_approved'
            if reason:
                value.update(phase='blocked', reason=reason)
                self.write(directory_fd, value)
                raise TushareEgressStopped(reason, value['api_code'])
            # Reserve BEFORE opening the socket. Never roll a reservation back.
            value.update(phase='in_flight', used=value['used']+1, reason='none', api_code=None)
            self.write(directory_fd, value)
            try:
                if self.stopped(directory_fd):
                    raise TushareEgressStopped('disabled')
                result = operation()
            except BaseException as error:
                if isinstance(error, TushareEgressStopped):
                    stopped = error
                else:
                    stopped = TushareEgressStopped('interrupted')
                value.update(phase='blocked', reason=stopped.reason, api_code=stopped.api_code)
                self.write(directory_fd, value)
                if not isinstance(error, Exception):
                    raise
                raise stopped from None
            value.update(phase='ready' if value['used'] < value['budget'] else 'exhausted',
                         reason='none' if value['used'] < value['budget'] else 'budget_exhausted')
            if self.stopped(directory_fd):
                value.update(phase='disabled', reason='disabled')
            self.write(directory_fd, value)
            return result


def raise_if_egress_blocked():
    """Surface an error even if the SDK caught it and returned an empty table."""
    from tradingagents.utils.tushare_manual import CURRENT
    job = CURRENT.get()
    if job is not None:
        return job.assert_ready()
    permit = Permit()
    with permit.locked() as directory_fd:
        value = permit.read(directory_fd)
        if permit.stopped(directory_fd):
            raise TushareEgressStopped('disabled')
        if value['phase'] in ('blocked', 'disabled', 'in_flight'):
            reason = 'previous_attempt_incomplete' if value['phase'] == 'in_flight' else value['reason']
            raise TushareEgressStopped(reason, value['api_code'])


class UnixTLSConnection(http.client.HTTPSConnection):
    """Fixed Unix route; no TCP/DNS/proxy fallback. One connect only."""
    def __init__(self, timeout):
        context = ssl.create_default_context(cafile=CA_FILE)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        if not context.check_hostname or context.verify_mode != ssl.CERT_REQUIRED:
            raise TushareEgressStopped('tls_failed')
        super().__init__(HOST, 443, timeout=timeout, context=context)
        self._duration = timeout
        self._attempted = False
        self._expired = threading.Event()
        self._socket_lock = threading.Lock()
        self._active = None
        self._watchdog = None
        self._watchdog_id = 0
        self._closed = False
        self._created_at = time.monotonic()
        self._completed_requests = 0
        self._reading_headers = False

    def _adopt(self, sock):
        with self._socket_lock:
            if self._expired.is_set() or self._closed:
                sock.close()
                raise TimeoutError()
            self._active = sock

    def _expire(self, watchdog_id=None):
        with self._socket_lock:
            if watchdog_id is not None and watchdog_id != self._watchdog_id:
                return
            self._expired.set()
            if self._active is not None:
                try:
                    self._active.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self._active.close()

    def begin_request(self):
        """A fresh total deadline for an explicitly authorized HTTP attempt."""
        with self._socket_lock:
            if self._closed:
                raise TushareEgressStopped('network_failed')
            if self._expired.is_set():
                raise TimeoutError()
            self._watchdog_id += 1
            if self._watchdog is not None:
                self._watchdog.cancel()
            self._watchdog = threading.Timer(
                self._duration, self._expire, args=(self._watchdog_id,))
            self._watchdog.daemon = True
            self._watchdog.start()
            if self._active is not None:
                self._active.settimeout(self._duration)

    def finish_request(self):
        with self._socket_lock:
            if self._expired.is_set():
                raise TimeoutError()
            self._watchdog_id += 1  # A cancelled callback cannot expire the next request.
            if self._watchdog is not None:
                self._watchdog.cancel()
                self._watchdog = None
            self._completed_requests += 1

    def connect(self):
        if self._attempted or self._tunnel_host:
            raise TushareEgressStopped('network_failed')
        self._attempted = True
        self.begin_request()
        raw = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._adopt(raw)
        raw.settimeout(min(5, self._duration))
        raw.connect(SOCKET_PATH)
        secure = self._context.wrap_socket(raw, server_hostname=HOST, do_handshake_on_connect=False)
        self._adopt(secure)
        self.sock = secure
        secure.settimeout(self._duration)
        secure.do_handshake()

    def getresponse(self):
        self._reading_headers = True
        try:
            return super().getresponse()
        finally:
            self._reading_headers = False

    def close(self):
        if self._reading_headers:
            # HTTPConnection detaches a Connection: close socket as soon as
            # headers arrive. The response still owns its file descriptor;
            # retain the watchdog/active handle until its body has been read.
            super().close()
            return
        self.abort()

    def abort(self):
        with self._socket_lock:
            self._closed = True
            self._watchdog_id += 1
            if self._watchdog is not None:
                self._watchdog.cancel()
                self._watchdog = None
            if self._active is not None:
                try:
                    self._active.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self._active.close()
                self._active = None
            super().close()


_HISTORY_SESSION = ContextVar('tushare_history_connection_session', default=None)


class _HistoryConnectionSession:
    """One worker's connection, never its authorization or retry policy.

    Context propagation shares this object with asyncio.to_thread. The request
    slot rejects concurrency; close can interrupt a cancelled worker without
    waiting for that slot. Credentials only identify the in-memory cache key.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._slot = threading.Lock()
        self._cached = self._active = self._key = None
        self._last_used = 0
        self._closed = False

    @contextmanager
    def request_slot(self):
        if not self._slot.acquire(blocking=False):
            raise TushareEgressStopped('busy')
        try:
            yield
        finally:
            self._slot.release()

    def checkout(self, key):
        with self._lock:
            if self._closed:
                raise TushareEgressStopped('disabled')
            conn, self._cached = self._cached, None
            same_key = self._key == key
            self._key = key
            idle = time.monotonic() - self._last_used
        if conn is not None:
            try:
                reusable = (same_key and idle < 5
                    and time.monotonic() - conn._created_at < 120
                    and conn._completed_requests < 64 and conn.sock is not None
                    and not conn.sock.pending()
                    and not select.select([conn.sock], [], [], 0)[0])
                if reusable:
                    return conn
            except (OSError, ValueError):
                pass
            conn.close()
        return None

    def activate(self, conn):
        with self._lock:
            if self._closed:
                raise TushareEgressStopped('disabled')
            self._active = conn

    def retain(self, conn):
        with self._lock:
            if self._closed:
                return False
            self._cached, self._active = conn, None
            self._last_used = time.monotonic()
            return True

    def deactivate(self, conn):
        with self._lock:
            if self._active is conn:
                self._active = None

    def close(self):
        with self._lock:
            self._closed = True
            connections = {c for c in (self._cached, self._active) if c is not None}
            self._cached = self._active = self._key = None
        for conn in connections:
            conn.abort()


@contextmanager
def history_transport_session():
    """Opt in only for one history worker; every query still enters its gate."""
    session = _HistoryConnectionSession()
    token = _HISTORY_SESSION.set(session)
    try:
        yield session
    finally:
        _HISTORY_SESSION.reset(token)
        session.close()


class MainlandTransport:
    def __init__(self, token, classifier, timeout=30, permit=None):
        if not isinstance(token, str) or not token.strip() or len(token) > 8192:
            raise TushareEgressStopped('token_invalid')
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 30:
            raise TushareEgressStopped('invalid_request')
        self._token = token
        self._classifier = classifier
        self._timeout = timeout
        self._permit = permit if permit is not None else Permit()

    def query(self, api_name, fields='', **params):
        if os.environ.get('TA_TUSHARE_ROUTE') != 'mainland_unix':
            raise TushareEgressStopped('disabled')
        if (not isinstance(api_name, str) or not re.fullmatch('[a-z][a-z0-9_]{0,63}', api_name)
                or not isinstance(fields, str) or len(fields) > 8192):
            raise TushareEgressStopped('invalid_request')
        params.setdefault('ts_type_name', 'http://api.waditu.com/dataapi')
        try:
            body = json.dumps({'api_name':api_name, 'token':self._token, 'fields':fields,
                               'params':params}, allow_nan=False).encode('utf-8')
        except (TypeError, ValueError):
            raise TushareEgressStopped('invalid_request') from None
        if len(body) > 256 * 1024:
            raise TushareEgressStopped('invalid_request')
        from tradingagents.utils.tushare_manual import CURRENT
        job = CURRENT.get()
        if job is not None:
            return job.perform(api_name, fields, params, lambda: self._send(api_name, body))
        return self._permit.perform(api_name, lambda: self._send(api_name, body))

    def _send(self, api_name, body):
        session = _HISTORY_SESSION.get()
        if session is not None:
            with session.request_slot():
                return self._send_request(api_name, body, session)
        return self._send_request(api_name, body)

    def _send_request(self, api_name, body, session=None):
        conn = None
        owned = None
        try:
            if session is not None:
                conn = session.checkout((self._token, self._timeout))
            reused = conn is not None
            if conn is None:
                conn = UnixTLSConnection(self._timeout)
            owned = conn
            if session is not None:
                session.activate(conn)
            if reused:
                conn.begin_request()
            else:
                conn.connect()
            conn.request('POST', '/dataapi/'+api_name, body=body, headers={
                'Content-Type':'application/json', 'Accept':'*/*',
                'Connection':'keep-alive' if session is not None else 'close'})
            response = conn.getresponse()
            if 300 <= response.status < 400:
                raise TushareEgressStopped('redirect_blocked', http_status=response.status)
            if not 200 <= response.status < 300:
                raise TushareEgressStopped('http_error', http_status=response.status)
            raw = response.read(MAX_BODY+1)
            if conn._expired.is_set():
                raise TimeoutError()
            if len(raw) > MAX_BODY:
                raise TushareEgressStopped('response_too_large')
            data = json.loads(raw, object_pairs_hook=_pairs)
            if not isinstance(data, dict) or type(data.get('code')) is not int:
                raise TushareEgressStopped('response_invalid')
            if data['code'] != 0:
                diagnostic = self._classifier(data, self._token)
                match = re.fullmatch(r'tushare_api_rejected code=(?:-?\d{1,7}|unknown) '
                    r'reason=([a-z_,]+) window=(?:unknown|second|minute|hour|day)', diagnostic)
                reasons = match[1].split(',') if match else []
                cause = next((item for item in reasons if item in REASONS and item not in ('none',)), 'api_rejected')
                raise TushareEgressStopped(cause, data['code'])
            table = data.get('data')
            if (not isinstance(table, dict) or not isinstance(table.get('fields'), list)
                    or not all(isinstance(field, str) for field in table['fields'])
                    or not isinstance(table.get('items'), list)
                    or not all(isinstance(row, list) and len(row) == len(table['fields']) for row in table['items'])):
                raise TushareEgressStopped('response_invalid')
            if (session is not None and response.version == 11
                    and not response.will_close and response.isclosed()
                    and conn.sock is not None):
                conn.finish_request()
                if session.retain(conn):
                    conn = None  # The worker scope now owns the idle connection.
            return table
        except TushareEgressStopped:
            raise
        except ssl.SSLError:
            raise TushareEgressStopped('tls_failed') from None
        except (TimeoutError, socket.timeout):
            raise TushareEgressStopped('timeout') from None
        except (OSError, http.client.HTTPException):
            cause = 'timeout' if conn is not None and conn._expired.is_set() else 'network_failed'
            raise TushareEgressStopped(cause) from None
        except Exception:
            raise TushareEgressStopped('response_invalid') from None
        finally:
            if session is not None:
                session.deactivate(owned)
            if conn is not None:
                conn.close()
