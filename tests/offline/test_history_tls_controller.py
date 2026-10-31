"""Run the actual CLI method without loading private release files or SSH."""
import argparse
import ast
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch


class ControllerTests(unittest.TestCase):
    def controller(self):
        source=Path(__file__).resolve().parents[2]/'deploy/run_history_tls_recovery.py'
        tree=ast.parse(source.read_text())
        method=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
        namespace=dict(argparse=argparse,Path=Path,json=json,__doc__='operator test',
                       configure_transport=Mock(),transport=types.SimpleNamespace(ssh=Mock(return_value={})))
        for name in ('ROOT','BACKUP','LAUNCH','STATUS','DRIVER','UPLOAD_AND_LAUNCH'):
            namespace[name]='/unique/history-tls-recovery-20261030-v1'
        exec(compile(ast.Module(body=[method],type_ignores=[]),str(source),'exec'),namespace)
        return namespace

    def test_consumed_v1_cannot_execute_or_freeze(self):
        for action in ('execute','freeze'):
            state=self.controller()
            with patch.object(sys,'argv',['operator','--action',action,'--run','v1']):
                with self.assertRaisesRegex(SystemExit,'v1_already_consumed'):
                    state['main']()
            state['transport'].ssh.assert_not_called()
            state['configure_transport'].assert_not_called()

    def test_v2_uses_distinct_paths_before_any_remote_call(self):
        state=self.controller()
        with patch.object(sys,'argv',['operator','--action','status','--run','v2']):
            state['main']()
        for name in ('ROOT','BACKUP','LAUNCH','STATUS','DRIVER','UPLOAD_AND_LAUNCH'):
            self.assertEqual(state[name],'/unique/history-tls-recovery-20261030-v2')
        state['transport'].ssh.assert_called_once_with(state['STATUS'])

    def test_v1_status_remains_readable(self):
        state=self.controller()
        with patch.object(sys,'argv',['operator','--action','status']):
            state['main']()
        state['transport'].ssh.assert_called_once_with('/unique/history-tls-recovery-20261030-v1')


if __name__=='__main__':
    unittest.main()
