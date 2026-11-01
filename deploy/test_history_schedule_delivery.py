"""Actual file ownership/mode preservation and expiry rollback, with no services."""
from contextlib import redirect_stdout
import ast
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import renew_history_transport as renewal
from prepare_history_schedule import REMOTE


class DeliveryTests(unittest.TestCase):
    def test_actual_remote_writer_defeats_umask_for_app_sources_only(self):
        tree=ast.parse(REMOTE);node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='save')
        with tempfile.TemporaryDirectory() as tmp:
            ns=dict(root=Path(tmp),os=os,json=json)
            exec(compile(ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[])),'<save>','exec'),ns)
            old=os.umask(0o077)
            try:
                ns['save']('staged/module.py',b'pass\n',0o644)
                ns['save']('backup/module.py',b'pass\n')
            finally:os.umask(old)
            self.assertEqual((Path(tmp)/'staged/module.py').stat().st_mode&0o777,0o644)
            self.assertEqual((Path(tmp)/'backup/module.py').stat().st_mode&0o777,0o600)

    def exercise_renewal(self,fail=False):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);auth=base/'authorization.json';timer=base/'expire.timer'
            auth.write_text(json.dumps(dict(expiry_utc='old-deadline')));auth.chmod(0o640)
            timer.write_text('old timer');timer.chmod(0o644)
            others=[]
            for name in range(5):
                p=base/('fixed'+str(name));p.write_text('unchanged');p.chmod(0o600);others.append(p)
            watch=(auth,timer,*others);before={p:(p.read_bytes(),p.stat().st_mode&0o777,p.stat().st_uid,p.stat().st_gid) for p in watch}
            calls=[]
            def run(argv):
                calls.append(argv)
                if argv[:2]==['systemctl','is-active']:return 'active'
                if argv[:2]==['systemctl','show']:return 'Sun 2026-11-08 06:00:00 UTC'
                if argv[:2]==['date','-u']:return str(renewal.EXPIRY)
                if fail and argv[:2]==['systemctl','show']:raise RuntimeError('timer mismatch')
                return ''
            def invoke(action,runner):
                with patch('sys.argv',['renew','--action',action]),patch.object(renewal,'run',side_effect=runner),redirect_stdout(io.StringIO()):renewal.main()
            with patch.multiple(renewal,ROOT=base/'release',AUTH=auth,TIMER=timer,WATCH=watch),patch.object(renewal.os,'geteuid',return_value=0):
                invoke('prepare',run)
                if fail:
                    def bad_run(argv):
                        if argv[:2]==['systemctl','show']:raise RuntimeError('synthetic verification failure')
                        return run(argv)
                    with self.assertRaises(SystemExit):invoke('apply',bad_run)
                    self.assertEqual(json.loads((base/'release/result.json').read_text())['status'],'failed')
                    for p,expected in before.items():self.assertEqual((p.read_bytes(),p.stat().st_mode&0o777,p.stat().st_uid,p.stat().st_gid),expected)
                else:
                    invoke('apply',run)
                    self.assertEqual(json.loads(auth.read_text()),renewal.WINDOW)
                    self.assertEqual(timer.read_bytes(),renewal.NEW_TIMER)
                    for p in (auth,timer):self.assertEqual((p.stat().st_mode&0o777,p.stat().st_uid,p.stat().st_gid),before[p][1:])
                    for p in others:self.assertEqual(p.read_bytes(),before[p][0])
            self.assertFalse(any(c[:2]==['systemctl','restart'] and c[-1]=='ta-tushare-forward.service' for c in calls))

    def test_renewal_keeps_original_group_read_permissions_and_service_configuration(self):self.exercise_renewal()
    def test_timer_verification_failure_restores_exact_original_files(self):self.exercise_renewal(fail=True)


if __name__=='__main__':unittest.main()
