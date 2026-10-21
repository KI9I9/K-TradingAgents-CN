"""Offline tests of the migration logging patch; never import/start the application."""
import ast
import asyncio
import contextlib
import copy
import io
import logging
import os
from pathlib import Path
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
BASELINE = Path(os.environ.get('MIGRATION_LOG_BASELINE', '/opt/migration/logging-baseline'))
FILES = ('app/services/auth_service.py', 'app/routers/config.py', 'app/services/config_service.py',
         'tradingagents/llm_adapters/dashscope_openai_adapter.py',
         'tradingagents/dataflows/providers/hk/hk_stock.py')
SECRET = 'FAKE_CREDENTIAL_MUST_NEVER_APPEAR_987654321'
SECRET_NAMES = {'api_key', 'api_secret', 'final_api_key', 'api_key_from_kwargs', 'env_api_key',
    'truncated_db_key', 'truncated_db_secret', 'proxies', 'connection_string', 'payload',
    'token_data', 'base_url', 'final_base_url', 'api_base', 'api_base_normalized', 'url',
    'llm_config_data', 'request', 'item', 'data', 'result', 'error_detail', 'error_msg',
    'response', 'traceback', 'model_data'}
EXCEPTIONS = {'e', 'exc', 'error', 'config_error', 'reload_err', 'track_error'}


def deny_network(event, args):
    if event in {'socket.connect', 'socket.getaddrinfo', 'socket.sendto', 'subprocess.Popen'}:
        raise RuntimeError('Network/subprocess prohibited in this offline regression')


sys.addaudithook(deny_network)


def is_log(node):
    if not isinstance(node, ast.Call):
        return False
    if isinstance(node.func, ast.Name):
        return node.func.id == 'print'
    return (isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name)
            and ((node.func.value.id in {'logger', 'logging'} and node.func.attr in
                  {'debug', 'info', 'warning', 'error', 'exception', 'critical'})
                 or (node.func.value.id == 'traceback' and node.func.attr == 'print_exc')))


class StripDiagnostics(ast.NodeTransformer):
    def visit_Expr(self, node):
        return None if is_log(node.value) else self.generic_visit(node)


def normalize_approved_tushare_https(tree):
    """Reverse ONLY the two exact HTTPS edits for the logging-only comparison.

    Transport behavior is covered separately by test_tushare_https.py. All other
    non-logging AST nodes remain subject to the original strict comparison.
    """
    pairs = (
        ('from tradingagents.utils.tushare_https import create_tushare_client', 'import tushare as ts'),
        ('pro = create_tushare_client(api_key)', 'pro = ts.pro_api(api_key)'),
    )
    for new, old in pairs:
        expected = ast.dump(ast.parse(new).body[0])
        matches = []
        class Replace(ast.NodeTransformer):
            def generic_visit(self, node):
                if ast.dump(node) == expected:
                    matches.append(node)
                    return ast.copy_location(ast.parse(old).body[0], node)
                return super().generic_visit(node)
        tree = Replace().visit(tree)
        if len(matches) != 1:
            raise AssertionError('Expected exactly one approved Tushare HTTPS edit')
    return tree


class Value:
    def __init__(self, value='harmless'):
        self.value = value
    def __str__(self):
        return self.value
    __repr__ = __str__
    def __format__(self, spec):
        return self.value
    def __getattr__(self, name):
        return Value(SECRET if name in {'JWT_SECRET', 'api_key', 'api_secret', 'api_base', 'password'} else self.value)
    def __call__(self, *args, **kwargs):
        return self
    def __getitem__(self, key):
        return self
    def __len__(self):
        return 4
    def __iter__(self):
        return iter(())
    def __contains__(self, key):
        return False
    def __add__(self, other):
        return self


class Capture:
    def __init__(self):
        self.lines = []
    def write(self, message, *args, **kwargs):
        self.lines.append(str(message) % args if args else str(message))
    def __getattr__(self, name):
        return self.write


def function_from_main(name, namespace):
    namespace['__file__'] = str(ROOT / 'app/main.py')
    tree = ast.parse((ROOT / 'app/main.py').read_text())
    node = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
    node = copy.deepcopy(node)
    node.decorator_list = []
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), '<isolated-function>', 'exec'), namespace)
    return namespace[name]


class MigrationLoggingTests(unittest.TestCase):
    def test_01_all_changed_source_compiles(self):
        for relative in FILES + ('app/main.py',):
            with self.subTest(file=relative):
                compile((ROOT / relative).read_text(), relative, 'exec')

    def test_02_five_modules_have_no_nonlogging_logic_change(self):
        for relative in FILES:
            original = StripDiagnostics().visit(ast.parse((BASELINE / relative).read_text()))
            updated = StripDiagnostics().visit(ast.parse((ROOT / relative).read_text()))
            if relative == 'app/services/config_service.py':
                updated = normalize_approved_tushare_https(updated)
            with self.subTest(file=relative):
                self.assertEqual(ast.dump(original, include_attributes=False), ast.dump(updated, include_attributes=False))

    def test_03_every_changed_logging_call_with_synthetic_secrets(self):
        checked = 0
        for relative in FILES:
            old = [n for n in ast.walk(ast.parse((BASELINE / relative).read_text())) if is_log(n)]
            new = [n for n in ast.walk(ast.parse((ROOT / relative).read_text())) if is_log(n)]
            self.assertEqual(len(old), len(new), relative)
            for before, after in zip(old, new):
                if ast.dump(before) == ast.dump(after):
                    continue
                checked += 1
                capture = Capture()
                variables = {n.id: Value() for n in ast.walk(after) if isinstance(n, ast.Name)}
                variables.update({n: Value(SECRET) for n in SECRET_NAMES})
                variables.update({n: ValueError(SECRET) for n in EXCEPTIONS})
                variables.update({'logger': capture, 'logging': capture, 'print': capture.write,
                                  'type': type, 'len': len, 'str': str, 'repr': repr,
                                  'bool': bool, 'list': list, 'int': int, 'hasattr': hasattr})
                with self.subTest(file=relative, line=after.lineno):
                    eval(compile(ast.Expression(body=after), '<isolated-log>', 'eval'), variables)
                    self.assertNotIn(SECRET, '\n'.join(capture.lines))
        self.assertGreater(checked, 100)

    def test_04_no_exception_tracebacks_in_config_logging(self):
        for relative in FILES:
            for node in ast.walk(ast.parse((ROOT / relative).read_text())):
                if is_log(node):
                    self.assertFalse(isinstance(node.func, ast.Attribute) and node.func.attr in {'exception', 'print_exc'})
                    for keyword in node.keywords:
                        if keyword.arg in {'exc_info', 'stack_info'}:
                            self.assertIsInstance(keyword.value, ast.Constant)
                            self.assertIs(keyword.value.value, False)

    def test_05_summary_does_not_read_env_or_print_proxy(self):
        capture = Capture()
        async def empty_config():
            return types.SimpleNamespace(llm_configs=[], data_source_configs=[])
        service = types.ModuleType('app.services.config_service')
        service.config_service = types.SimpleNamespace(get_system_config=empty_config)
        settings = types.SimpleNamespace(model_config={}, HOST='127.0.0.1', PORT=8000, DEBUG=False,
            MONGODB_HOST='mongodb', REDIS_HOST='redis', MONGODB_PORT=27017, REDIS_PORT=6379,
            MONGODB_DATABASE='fixture', REDIS_DB=0, is_production=True, PROXY_ENABLED=True,
            HTTP_PROXY='http://user:' + SECRET + '@example.invalid', HTTPS_PROXY=SECRET, NO_PROXY=SECRET)
        fn = function_from_main('_print_config_summary', {'settings': settings})
        cwd = Path.cwd()
        with tempfile.TemporaryDirectory(prefix='migration-log-fixture-') as directory:
            try:
                os.chdir(directory)
                Path('.env').write_text('UNRECOGNIZED_VALUE=' + SECRET + '\n')
                with patch.dict(sys.modules, {'app.services.config_service': service}), patch('builtins.open', side_effect=AssertionError('env preview read')):
                    asyncio.run(fn(capture))
            finally:
                os.chdir(cwd)
        self.assertNotIn(SECRET, '\n'.join(capture.lines))
        self.assertNotIn('AssertionError', '\n'.join(capture.lines))
        self.assertIn('HTTP_PROXY: configured', '\n'.join(capture.lines))
        self.assertNotIn('Failed to print config summary', '\n'.join(capture.lines))

    def test_06_summary_exception_body_omitted(self):
        capture = Capture()
        class BrokenSettings:
            def __getattr__(self, name):
                raise ValueError(SECRET)
        fn = function_from_main('_print_config_summary', {'settings': BrokenSettings()})
        asyncio.run(fn(capture))
        self.assertNotIn(SECRET, '\n'.join(capture.lines))
        self.assertIn('ValueError', '\n'.join(capture.lines))

    def test_07_request_auth_and_session_never_logged(self):
        capture = Capture()
        fake_logging = types.SimpleNamespace(getLogger=lambda name: capture)
        fn = function_from_main('log_requests', {'Request': object, 'time': time, 'logging': fake_logging})
        request = types.SimpleNamespace(url=types.SimpleNamespace(path='/api/license/status'), method='GET',
            headers={'authorization': 'Bearer ' + SECRET}, scope={'session': True}, session={'user_id': SECRET})
        response = types.SimpleNamespace(status_code=200)
        async def next_handler(value):
            self.assertIs(value, request)
            return response
        self.assertIs(asyncio.run(fn(request, next_handler)), response)
        self.assertNotIn(SECRET, '\n'.join(capture.lines))
        self.assertIn('/api/license/status', '\n'.join(capture.lines))

    def test_08_real_jwt_success_expiry_and_validation_error(self):
        try:
            import jwt
            from pydantic import BaseModel
        except ImportError:
            self.skipTest('Run with the pinned backend dependencies for real JWT validation')
        tree = ast.parse((ROOT / 'app/services/auth_service.py').read_text())
        tree.body = [n for n in tree.body if not (isinstance(n, ast.ImportFrom) and (n.module or '').startswith('app.'))]
        from datetime import datetime, timezone
        settings = types.SimpleNamespace(JWT_SECRET=SECRET, JWT_ALGORITHM='HS256', ACCESS_TOKEN_EXPIRE_MINUTES=30)
        namespace = {'__name__': 'migration_offline_auth_test', 'settings': settings,
                     'now_tz': lambda: datetime.now(timezone.utc)}
        exec(compile(tree, '<isolated-auth-service>', 'exec'), namespace)
        service = namespace['AuthService']
        stream, logger = io.StringIO(), logging.getLogger(namespace['__name__'])
        old_level, old_propagate = logger.level, logger.propagate
        handler = logging.StreamHandler(stream)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        try:
            token = service.create_access_token(sub=SECRET, session_id=SECRET)
            decoded = service.verify_token(token)
            self.assertEqual(decoded.sub, SECRET)
            self.assertEqual(decoded.session_id, SECRET)
            self.assertIsNone(service.verify_token('invalid.' + SECRET))
            expired = jwt.encode({'sub': SECRET, 'exp': 1}, SECRET, algorithm='HS256')
            self.assertIsNone(service.verify_token(expired))
            malformed = jwt.encode({'sub': {'secret': SECRET}, 'exp': int(time.time()) + 60}, SECRET, algorithm='HS256')
            self.assertIsNone(service.verify_token(malformed))
            self.assertNotIn(SECRET, stream.getvalue())
            self.assertNotIn(SECRET[:10], stream.getvalue())
            self.assertNotIn(token, stream.getvalue())
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old_level)
            logger.propagate = old_propagate


if __name__ == '__main__':
    unittest.main(verbosity=2)
