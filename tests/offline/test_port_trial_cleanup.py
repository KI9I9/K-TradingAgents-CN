"""Exercise actual timer payloads without network or privileged mutations."""
import contextlib
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'deploy'))
from tushare_port_trial_support import REMOTE_CLEANUP, LOCAL_RESTORE, RULE


class CleanupTests(unittest.TestCase):
    def remote(self,exists=True,delete_fails=False,stop_fails=False):
        calls=[];active=True
        def run(args,**kwargs):
            nonlocal exists,active
            calls.append(args)
            if args[0]=='iptables':
                self.assertEqual(args[5:],RULE)
                if args[3]=='-C':rc=0 if exists else 1
                else:
                    self.assertEqual(args[3],'-D')
                    rc=2 if delete_fails else 0
                    if not delete_fails:exists=False
                return subprocess.CompletedProcess(args,rc,stdout='')
            if args[:2]==['systemctl','stop']:
                self.assertEqual(args[2],'trial.service')
                if not stop_fails:active=False
                return subprocess.CompletedProcess(args,1 if stop_fails else 0,stdout='')
            if args[0]=='ss':return subprocess.CompletedProcess(args,0,stdout='LISTEN' if active else '')
            if args[:2]==['systemctl','show']:
                return subprocess.CompletedProcess(args,0,stdout=('ActiveState=active\nMainPID=123\n' if active else 'ActiveState=inactive\nMainPID=0\n'))
            self.fail('unexpected privileged command')
        with tempfile.TemporaryDirectory() as folder,patch('subprocess.run',side_effect=run):
            out=io.StringIO()
            with contextlib.redirect_stdout(out):exec(REMOTE_CLEANUP,dict(ROOT=folder,UNIT='trial.service',RULE=RULE))
            result=json.loads(out.getvalue())
            self.assertEqual(len(list(Path(folder).glob('cleanup-*.json'))),1)
        return result,calls

    def test_closes_both_resources(self):
        result,calls=self.remote()
        self.assertTrue(result['closed'])
        self.assertIn(['systemctl','stop','trial.service'],calls)
        self.assertFalse(any('ssh.service' in c or 'sshd.service' in c for c in calls))

    def test_absent_rule_is_idempotent(self):
        result,calls=self.remote(exists=False)
        self.assertTrue(result['closed'])
        self.assertFalse(any(c[0]=='iptables' and '-D' in c for c in calls))

    def test_failed_rule_delete_still_stops_listener(self):
        result,_=self.remote(delete_fails=True)
        self.assertTrue(result['port_absent'])
        self.assertFalse(result['rule_absent'])
        self.assertFalse(result['closed'])

    def test_failed_listener_stop_still_removes_rule(self):
        result,_=self.remote(stop_fails=True)
        self.assertTrue(result['rule_absent'])
        self.assertFalse(result['closed'])

    def restore(self,folder,override,fail_reload=False):
        root=Path(folder);dropin=root/'candidate.conf';original=root/'original.conf'
        if not original.exists():
            original.write_text('original22')
            (root/'manifest.json').write_text(json.dumps({'files':{str(original):{'sha256':hashlib.sha256(original.read_bytes()).hexdigest()}}}))
        if override is not None:dropin.write_text(override)
        calls=[]
        def run(args,**kwargs):
            calls.append(args)
            if fail_reload and args==['systemctl','daemon-reload']:raise subprocess.TimeoutExpired(args,15)
            return subprocess.CompletedProcess(args,0,stdout='')
        out=io.StringIO()
        with patch('subprocess.run',side_effect=run),patch('subprocess.check_output',return_value='/usr/bin/ssh -F original -N host'),contextlib.redirect_stdout(out):
            exec(LOCAL_RESTORE,dict(ROOT=folder,DROPIN=str(dropin),EXPECTED=hashlib.sha256(b'candidate443').hexdigest()))
        return json.loads(out.getvalue()),calls

    def test_restore_removes_only_own_override(self):
        with tempfile.TemporaryDirectory() as folder:
            result,calls=self.restore(folder,'candidate443')
            self.assertTrue(result['restored'])
            self.assertFalse((Path(folder)/'candidate.conf').exists())
            self.assertIn(['systemctl','restart','ta-tushare-forward.service'],calls)

    def test_restore_recovers_crash_after_override_unlink(self):
        with tempfile.TemporaryDirectory() as folder:
            result,_=self.restore(folder,'candidate443',fail_reload=True)
            self.assertFalse(result['restored'])
            self.assertFalse((Path(folder)/'candidate.conf').exists())
            result,calls=self.restore(folder,None)
            self.assertTrue(result['restored'])
            self.assertIn(['systemctl','daemon-reload'],calls)

    def test_changed_override_is_preserved_and_reported(self):
        with tempfile.TemporaryDirectory() as folder:
            result,calls=self.restore(folder,'other-owner-change')
            self.assertFalse(result['restored'])
            self.assertEqual((Path(folder)/'candidate.conf').read_text(),'other-owner-change')
            self.assertEqual(calls,[])


if __name__=='__main__':unittest.main()
