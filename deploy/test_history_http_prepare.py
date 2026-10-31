"""Verify real staging permissions under the private deployment umask."""
import ast
import os
from pathlib import Path
import tempfile
import unittest

from prepare_history_http_reuse import REMOTE


class PreparePermissionsTests(unittest.TestCase):
    def test_staged_code_readable_while_backup_remains_private(self):
        # Execute the actual remote save function, including its os.open mode,
        # under the production umask. Hash-only checks cannot find this bug.
        tree = ast.parse(REMOTE)
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'save')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ns = dict(root=root, os=os, json=__import__('json'))
            exec(compile(ast.Module(body=[function], type_ignores=[]), '<remote-save>', 'exec'), ns)
            old_umask = os.umask(0o077)
            try:
                ns['save']('staged/worker.py', b'pass\n', 0o644)
                ns['save']('backup/runtime.json', b'private\n')
            finally:
                os.umask(old_umask)
            self.assertEqual((root / 'staged/worker.py').stat().st_mode & 0o777, 0o644)
            self.assertEqual((root / 'backup/runtime.json').stat().st_mode & 0o777, 0o600)
            self.assertEqual((root / 'backup').stat().st_mode & 0o777, 0o700)
            self.assertEqual((root / 'staged/worker.py').read_bytes(), b'pass\n')


if __name__ == '__main__':
    unittest.main()
