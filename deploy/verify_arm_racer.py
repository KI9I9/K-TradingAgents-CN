"""Offline regression usable as a build gate; prohibits network activity."""
import contextlib
import importlib.metadata as metadata
import io
import json
import logging
from pathlib import Path
import struct
import sys

def deny_network(event, args):
    if event in {'socket.connect','socket.connect_ex','socket.getaddrinfo','socket.sendto'}:
        raise RuntimeError('NetworkNotAllowedInRegression')

sys.addaudithook(deny_network)
logging.disable(logging.CRITICAL)
checks = {}
try:
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        import py_mini_racer
        from py_mini_racer import py_mini_racer as implementation
        from akshare.stock.cons import hk_js_decode
        context = py_mini_racer.MiniRacer()
        checks['constant_eval'] = context.eval('1+1') == 2
        context.eval('function add(a,b) { return a+b; } function echo(v) { return v; }')
        checks['function_call'] = context.call('add', 17, 25) == 42
        fixture = {'text':'行情测试','values':[1,2.5,None,True], 'nested':{'ok':True}}
        checks['json_unicode_roundtrip'] = context.call('echo',fixture) == fixture
        context.eval(hk_js_decode)
        checks['akshare_decoder_compiles'] = context.eval('typeof d') == 'function'
        library = Path(implementation.EXTENSION_PATH)
        header = library.read_bytes()[:20]
        checks['native_library_aarch64'] = header[:4] == b'\x7fELF' and struct.unpack('<H',header[18:20])[0] == 183
        checks['versions_preserved'] = (metadata.version('akracer') == '0.0.14' and
                                        metadata.version('py-mini-racer') == '0.6.0' and
                                        metadata.version('akshare') == '1.19.1')
except Exception as exc:
    checks['error_category'] = type(exc).__name__
passed = len(checks) == 6 and all(v is True for v in checks.values())
print(json.dumps({'checks':checks,'all_passed':passed,'network_calls_allowed':False}))
raise SystemExit(0 if passed else 1)
