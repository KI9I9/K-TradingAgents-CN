"""Detached controller must STOP even on transport failure; no credential artifacts."""
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'deploy'))
import run_history_button_acceptance as runner


class DriverTest(unittest.TestCase):
    def exercise(self,outcome):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            (root/'package.json').write_text(json.dumps({'request':{},'code':'placeholder'}))
            calls=[]
            def run(args,**kwargs):
                calls.append((args,kwargs))
                if len(calls)==1:
                    self.assertEqual(json.loads(kwargs['input'])['auth']['password'],'test-secret')
                    if isinstance(outcome,Exception):raise outcome
                    return outcome
                return subprocess.CompletedProcess(args,0,'True\n','')
            code=runner.DRIVER.replace(runner.ROOT,folder)
            with patch('sys.stdin',io.StringIO(json.dumps({'auth':{'username':'test','password':'test-secret'}}))), patch('subprocess.run',run):
                exec(compile(code,'<detached-controller>','exec'),{})
            self.assertEqual(len(calls),2)
            self.assertIn('BatchGate(p).stop()',calls[1][0][-1])
            self.assertTrue(json.loads((root/'stop-confirmation.json').read_text())['stopped'])
            self.assertFalse(any('test-secret' in p.read_text() for p in root.iterdir()))
            return json.loads((root/'status.json').read_text())

    def test_success_stops(self):
        out=self.exercise(subprocess.CompletedProcess([],0,json.dumps({'status':'passed','new_gate_stopped':True,'used_requests':2}),''))
        self.assertEqual(out['stage'],'complete')

    def test_rejected_trigger_stops_without_retry(self):
        out=self.exercise(subprocess.CompletedProcess([],1,json.dumps({'status':'failed','reason':'original_button_rejected'}),''))
        self.assertEqual(out['stage'],'failed')

    def test_unknown_outcome_stops_without_retry(self):
        out=self.exercise(subprocess.TimeoutExpired('docker',260))
        self.assertEqual(out['stage'],'failed_unknown_outcome')
        self.assertFalse(out['automatic_retry'])


if __name__=='__main__':unittest.main()
