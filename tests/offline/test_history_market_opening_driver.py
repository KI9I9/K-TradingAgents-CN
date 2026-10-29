"""Daily permit must survive success, while ambiguous execution remains stopped."""
import io,json,subprocess,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'deploy'))
import run_history_market_opening as opening


class OpeningDriver(unittest.TestCase):
    def exercise(self,result,expected_calls):
        base=opening.base
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            (root/'package.json').write_text(json.dumps({'request':{},'code':'placeholder'}))
            calls=[]
            def run(args,**kwargs):
                calls.append(args)
                if len(calls)==1:
                    self.assertEqual(json.loads(kwargs['input'])['auth']['password'],'test-secret')
                    if isinstance(result,Exception):raise result
                    return result
                return subprocess.CompletedProcess(args,0,'True\n','')
            with patch('sys.stdin',io.StringIO(json.dumps({'auth':{'username':'test','password':'test-secret'}}))),patch('subprocess.run',run):
                exec(compile(base.DRIVER.replace(base.ROOT,folder),'<market-driver>','exec'),{})
            self.assertEqual(len(calls),expected_calls)
            if expected_calls==2:self.assertIn('g.stop()',calls[-1][-1])
            self.assertFalse(any('test-secret' in p.read_text() for p in root.iterdir()))
            return json.loads((root/'status.json').read_text())

    def test_success_keeps_permit_open(self):
        r=subprocess.CompletedProcess([],0,'BUTTON_ACCEPTANCE_JSON:'+json.dumps({'status':'passed','new_gate_stopped':False}), '')
        self.assertEqual(self.exercise(r,1)['stage'],'complete')

    def test_failure_stops_without_retry(self):
        r=subprocess.CompletedProcess([],1,'BUTTON_ACCEPTANCE_JSON:'+json.dumps({'status':'failed'}),'')
        self.assertEqual(self.exercise(r,2)['stage'],'failed')

    def test_timeout_stops_without_retry(self):
        self.assertEqual(self.exercise(subprocess.TimeoutExpired('docker',260),2)['stage'],'failed_unknown_outcome')

    def test_missing_result_stops(self):
        self.assertEqual(self.exercise(subprocess.CompletedProcess([],0,'no result',''),2)['stage'],'failed_unknown_outcome')


if __name__=='__main__':unittest.main()
