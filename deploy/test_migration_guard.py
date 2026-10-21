"""Offline ASGI/lifespan regression; does not import/start the full application."""
import ast
import asyncio
import copy
from contextlib import asynccontextmanager
import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('migration_guard', ROOT / 'app/migration_guard.py')
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class GuardTests(unittest.TestCase):
    def test_disabled_delegates_without_changes(self):
        app = AsyncMock()
        with patch.dict(os.environ, {'TA_MIGRATION_REVIEW': '0'}):
            asyncio.run(guard.ReviewGuard(app)({'type': 'http', 'method': 'POST', 'path': '/api/analysis'}, AsyncMock(), AsyncMock()))
        app.assert_awaited_once()

    def test_review_default_deny_including_get_and_websocket(self):
        for kind, method, path in [('http', 'GET', '/api/stocks/refresh'), ('http', 'POST', '/api/analysis'),
                                   ('http', 'POST', '/api/auth/create-user'), ('websocket', 'GET', '/ws')]:
            app, send = AsyncMock(), AsyncMock()
            with patch.dict(os.environ, {'TA_MIGRATION_REVIEW': '1'}):
                asyncio.run(guard.ReviewGuard(app)({'type': kind, 'method': method, 'path': path}, AsyncMock(), send))
            app.assert_not_awaited()
            first = send.await_args_list[0].args[0]
            self.assertEqual(first.get('status', first.get('code')), 423 if kind == 'http' else 1008)

    def test_only_exact_auth_routes_are_allowed(self):
        for method, path in guard.ALLOW:
            app = AsyncMock()
            with patch.dict(os.environ, {'TA_MIGRATION_REVIEW': '1'}):
                asyncio.run(guard.ReviewGuard(app)({'type': 'http', 'method': method, 'path': path}, AsyncMock(), AsyncMock()))
            app.assert_awaited_once()

    def test_health_does_not_delegate_to_application_health(self):
        app, send = AsyncMock(), AsyncMock()
        with patch.dict(os.environ, {'TA_MIGRATION_REVIEW': '1'}):
            asyncio.run(guard.ReviewGuard(app)({'type': 'http', 'method': 'GET', 'path': '/api/health'}, AsyncMock(), send))
        app.assert_not_awaited()
        self.assertIn(b'"production_ready": false', send.await_args_list[1].args[0]['body'])

    def test_typo_flag_fails_closed(self):
        with patch.dict(os.environ, {'TA_MIGRATION_REVIEW': 'yes'}):
            with self.assertRaises(ValueError):
                guard.enabled()

    def test_review_lifespan_never_reaches_existing_startup(self):
        tree = ast.parse((ROOT / 'app/main.py').read_text())
        node = copy.deepcopy(next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'lifespan'))
        node.decorator_list = []
        namespace = {'FastAPI': object, 'migration_review_enabled': lambda: True,
                     'init_db': AsyncMock(), 'close_db': AsyncMock()}
        # No other startup symbol exists: executing any original setup fails.
        exec(compile(ast.Module(body=[node], type_ignores=[]), '<isolated-lifespan>', 'exec'), namespace)
        async def execute():
            async with asynccontextmanager(namespace['lifespan'])(object()):
                namespace['init_db'].assert_awaited_once_with(create_schema=False)
        asyncio.run(execute())
        namespace['close_db'].assert_awaited_once()

    def test_normal_database_init_default_keeps_schema_initialization(self):
        tree = ast.parse((ROOT / 'app/core/database.py').read_text())
        node = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'init_database')
        self.assertEqual(node.args.kwonlyargs[0].arg, 'create_schema')
        self.assertIs(node.args.kw_defaults[0].value, True)
        conditions = [n for n in ast.walk(node) if isinstance(n, ast.If) and isinstance(n.test, ast.Name) and n.test.id == 'create_schema']
        self.assertEqual(len(conditions), 1)
        self.assertIn('init_database_views_and_indexes', ast.unparse(conditions[0]))

    def test_script_entrypoint_adds_source_root_before_importing_app(self):
        tree = ast.parse((ROOT / 'deploy/migration_review_entrypoint.py').read_text())
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
        insertion = next(n for n in calls if ast.unparse(n.func) == 'sys.path.insert')
        launch = next(n for n in calls if ast.unparse(n.func) == 'uvicorn.run')
        self.assertEqual(ast.unparse(insertion.args[1]), 'str(Path(__file__).resolve().parent.parent)')
        self.assertLess(insertion.lineno, launch.lineno)


if __name__ == '__main__':
    unittest.main(verbosity=2)
