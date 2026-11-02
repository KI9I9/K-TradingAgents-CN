"""No network: stale deadlines and production-host identity guard."""
import collections
import json
from pathlib import Path
import socket
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy'))
import build_route_assessment_console as console
from run_tushare_route_assessment import PROBE


class RouteAssessmentTests(unittest.TestCase):
    def test_cancelled_deadline_cannot_shutdown_the_next_socket(self):
        stale = collections.defaultdict(list)
        open_shutdowns = []
        sockets = []

        class Timer:
            def __init__(self, seconds, callback):
                self.seconds, self.callback = seconds, callback

            def start(self):
                pass

            def cancel(self):
                if self.seconds == 30:
                    stale[threading.get_ident()].append(self.callback)

        class Socket:
            def __init__(self, family, kind):
                self.family, self.closed = family, False
                sockets.append(self)

            def dup(self):
                return Socket(self.family, socket.SOCK_STREAM)

            def settimeout(self, value):
                assert 0 < value <= 30

            def connect(self, endpoint):
                assert endpoint == (('api.waditu.com', 443) if self.family == socket.AF_INET
                                    else '/run/ta-tushare-egress/upstream.sock')
                callbacks = stale.pop(threading.get_ident(), [])
                for callback in callbacks:
                    callback()  # Model a callback already running at cancel().

            def shutdown(self, how):
                if self.closed:
                    raise OSError('already closed')
                open_shutdowns.append(self)

            def close(self):
                self.closed = True

        class TLS:
            def __init__(self, raw):
                self.raw = raw

            def version(self):
                return 'TLSv1.2'

            def close(self):
                self.raw.close()

        class Context:
            def wrap_socket(self, raw, server_hostname):
                assert server_hostname == 'api.waditu.com'
                return TLS(raw)

        with tempfile.TemporaryDirectory() as directory:
            with patch('socket.socket', Socket), patch('ssl.create_default_context', Context), patch('threading.Timer', Timer):
                exec(compile(PROBE, 'probe', 'exec'), {'__file__': str(Path(directory) / 'probe.py')})
            result = json.loads((Path(directory) / 'result.json').read_text())
        self.assertEqual(open_shutdowns, [])
        self.assertTrue(all(s.closed for s in sockets))
        self.assertEqual(result['http_requests'], 0)
        self.assertEqual([len(r['observations']) for r in result['routes']], [3, 3])
        self.assertTrue(all(o['verified'] for r in result['routes'] for o in r['observations']))

    def test_console_refuses_wrong_host_before_touching_probe_directory(self):
        script = console.build()
        code = script.split("<<'TA_ROUTE_ASSESSMENT_PY'\n", 1)[1].rsplit('\nTA_ROUTE_ASSESSMENT_PY', 1)[0]
        inspect = json.dumps([dict(Id='wrong-container', Image=console.IMAGE)]).encode()
        with patch('subprocess.check_output', return_value=inspect), patch('subprocess.Popen') as launch:
            with patch('pathlib.Path.exists', side_effect=AssertionError('unexpected path lookup')):
                with self.assertRaisesRegex(SystemExit, 'wrong_host_or_backend_changed'):
                    exec(compile(code, 'console', 'exec'), {})
        launch.assert_not_called()


if __name__ == '__main__':
    unittest.main()
