"""Exercise delivery failure boundaries and real scheduler/rollback checks."""
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import subprocess
import os
import zlib

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'deploy'))
import switch_paper_quote_supply as switch
import run_paper_supply_switch as runner


class SwitchBranches(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.ns={'__name__':'offline_switch'}
        exec(compile(Path(switch.__file__).read_text(),switch.__file__,'exec'),self.ns)
        self.actions=[];self.events=[];self.saved={}
        self.before=dict(modules={'paper':'old'},collections={'orders':'same'},runtime_files={'state':'same'})
        self.after=copy.deepcopy(self.before);self.after['modules']['paper']='new'
        root=Path(self.directory.name);(root/'backup').mkdir();(root/'backup/backend-inspect.json').write_text('{}')
        self.ns.update(ROOT=root,ATTEMPT=root,STATE={},PAYLOAD={'check_only':False},
                       REQUEST={'manifest':{'source_head':'synthetic'}},
                       preflight=lambda:copy.deepcopy(self.before),event=self.events.append,
                       save=lambda name,data:self.saved.update({name:data}),
                       stop_backend=lambda:self.actions.append('stop'),
                       up=lambda new:self.actions.append(('up',new)),healthy=lambda:None,
                       check_running=lambda new:self.actions.append(('verify',new)),
                       snapshot=lambda new:self.after if new else self.before,
                       api_probe=lambda name:self.actions.append(('api',name)),
                       original_check=lambda:self.actions.append('verify_originals'),
                       seal_check=lambda _:None,inspect=lambda:{},config=lambda _:dict(original=True))
        self.ns['PAYLOAD']['preflight']={'config_hashes':{'before':switch.digest(switch.canonical(dict(original=True)))}}

    def test_closed_permit_delivery_succeeds_without_opening_or_order(self):
        self.assertEqual(self.ns['main'](),0)
        self.assertEqual(self.actions,['stop',('up',True),('verify',True),('api','jobs-after.json')])
        self.assertEqual(self.saved['completion.json']['status'],'deployed_paper_permit_absent')
        self.assertFalse(self.saved['completion.json']['quote_permit_installed'])

    def test_check_only_does_not_stop_or_recreate(self):
        self.ns['PAYLOAD']['check_only']=True
        self.assertEqual(self.ns['main'](),0);self.assertEqual(self.actions,[])

    def test_backup_failure_never_stops_backend(self):
        self.ns['preflight']=lambda:(_ for _ in ()).throw(RuntimeError('backup_failed'))
        self.assertEqual(self.ns['main'](),1);self.assertEqual(self.actions,[])

    def test_changed_business_state_rolls_back_code_without_restoring_data(self):
        self.after['collections']['orders']='changed'
        self.assertEqual(self.ns['main'](),1)
        self.assertEqual(self.actions.count(('up',True)),1)
        self.assertEqual(self.actions.count(('up',False)),1)
        self.assertEqual(self.events[-1],'rolled_back_quote_delivery')
        self.assertEqual(self.ns['STATE']['failure_code'],'protected_state_changed')

    def test_uncertain_stop_failure_also_attempts_original_chain(self):
        calls=0
        def stop():
            nonlocal calls
            calls+=1;self.actions.append('stop')
            if calls==1:raise RuntimeError('stop_timeout')
        self.ns['stop_backend']=stop
        self.assertEqual(self.ns['main'](),1)
        self.assertIn(('up',False),self.actions)

    def test_failed_rollback_requests_stop_and_preserves_evidence(self):
        self.ns['healthy']=lambda:(_ for _ in ()).throw(RuntimeError('unhealthy'))
        self.assertEqual(self.ns['main'](),1)
        self.assertEqual(self.events[-1],'rollback_failed_stop_requested')
        self.assertEqual(self.actions[-1],'stop')

    def test_removed_failed_container_does_not_prevent_original_recreation(self):
        self.ns['inspect']=lambda:(_ for _ in ()).throw(RuntimeError('container_removed'))
        self.after['collections']['orders']='changed'
        self.assertEqual(self.ns['main'](),1)
        self.assertIn(('up',False),self.actions)
        self.assertEqual(self.events[-1],'rolled_back_quote_delivery')


class RealGuards(unittest.TestCase):
    def test_no_container_left_is_already_stopped(self):
        ns={'__name__':'offline_stop'}
        exec(compile(Path(switch.__file__).read_text(),switch.__file__,'exec'),ns)
        calls=[]
        def run(argv,**kw):calls.append(argv);return ''
        ns.update(run=run,chain=lambda _:['docker','compose','--project-name','synthetic'])
        ns['stop_backend']()
        self.assertEqual(len(calls),1)

    def test_scheduler_requires_both_history_jobs_enabled_idle_and_distant(self):
        now=datetime(2026,11,2,7,tzinfo=timezone.utc)
        report=dict(success=True,missing_jobs=['quotes_ingestion_service'],data=[
            dict(id=name,paused=name not in switch.HISTORY,has_running_execution=False,
                 next_run_time=(now+timedelta(hours=3)).isoformat() if name in switch.HISTORY else None)
            for name in switch.JOBS if name!='quotes_ingestion_service'])
        switch.check_jobs(report,now)
        for update,reason in [({'paused':True},'history_schedule_missing'),
                              ({'has_running_execution':True},'job_active'),
                              ({'next_run_time':(now+timedelta(minutes=9)).isoformat()},'history_due_within_switch_window')]:
            changed=copy.deepcopy(report)
            next(j for j in changed['data'] if j['id']=='tushare_historical_sync').update(update)
            with self.assertRaisesRegex(RuntimeError,reason):switch.check_jobs(changed,now)

    def test_retained_compose_and_sources_must_match_before_rollback(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);backup=root/'backup';(backup/'compose').mkdir(parents=True)
            compose=root/'compose.json';compose.write_bytes(b'original-compose')
            source=root/'original.py';source.write_bytes(b'original-source')
            (backup/'compose/0.source').write_bytes(compose.read_bytes())
            (backup/'complete.json').write_text(json.dumps(dict(files={
                'compose/0.source':switch.digest(compose.read_bytes())})))
            ns={'__name__':'offline_original'}
            exec(compile(Path(switch.__file__).read_text(),switch.__file__,'exec'),ns)
            ns.update(ROOT=root,PAYLOAD={'preflight':{'old_sources':{'/app/original.py':str(source)}}},
                      REQUEST={'modules':{'/app/original.py':switch.digest(source.read_bytes())}},
                      chain=lambda _:['docker','compose','-f',str(compose)])
            ns['original_check']()
            source.write_bytes(b'drift')
            with self.assertRaisesRegex(RuntimeError,'original_source_changed'):ns['original_check']()
            source.write_bytes(b'original-source');compose.write_bytes(b'drift')
            with self.assertRaisesRegex(RuntimeError,'original_compose_changed'):ns['original_check']()


class FrozenEnvelope(unittest.TestCase):
    def test_freeze_requires_real_success_and_credentials_stay_out_of_artifact_and_argv(self):
        with tempfile.TemporaryDirectory() as directory:
            work=Path(directory);repo=work/'repo';(repo/'deploy').mkdir(parents=True)
            controller=work/'prepare';controller.mkdir()
            release=work/'release';evidence=work/'evidence';evidence.mkdir()
            (repo/'deploy/switch_paper_quote_supply.py').write_text(Path(switch.__file__).read_text())
            code='pass\n';identity=switch.digest(code.encode())
            (controller/'controller.json').write_text(json.dumps(dict(code=code,
                request=dict(controller_sha256=identity,manifest={'source_head':'synthetic'}))))
            receipt=work/'preflight.json';receipt.write_text(json.dumps(dict(ok=False,stage='failed')))
            auth=work/'auth';auth.write_text('operator\nSECRET_CANARY\n');auth.chmod(0o600)
            with patch.multiple(runner,REPO=repo,CONTROLLER=controller,RELEASE=release,EVIDENCE=evidence), \
                 patch.object(runner.subprocess,'check_output',return_value=b''):
                with self.assertRaisesRegex(ValueError,'successful_production_preparation_required'):runner.freeze(receipt)
                self.assertFalse(release.exists())
                receipt.write_text(json.dumps(dict(ok=True,stage='prepared_no_switch',controller_sha256=identity)))
                previous=os.umask(0o077)
                try:frozen=runner.freeze(receipt)
                finally:os.umask(previous)
                raw=(release/'controller.json').read_bytes()
                self.assertEqual(switch.digest(raw),frozen['sha256'])
                self.assertNotIn(b'SECRET_CANARY',raw)
                self.assertEqual((release/'controller.json').stat().st_mode&0o777,0o600)
                calls=[]
                def ssh(argv,**kw):
                    self.assertNotIn('SECRET_CANARY',str(argv))
                    packet=json.loads(zlib.decompress(kw['input']))
                    compile(packet['code'],'<ssh-bootstrap>','exec')
                    self.assertEqual(packet['data']['auth']['password'],'SECRET_CANARY')
                    uploaded=json.loads(packet['data']['upload'])
                    self.assertNotIn('SECRET_CANARY',packet['data']['upload'])
                    calls.append(uploaded['payload']['check_only'])
                    return subprocess.CompletedProcess(argv,0,b'{"attempt":"fake"}',b'')
                with patch.object(runner.subprocess,'run',side_effect=ssh):
                    runner.remote('check',auth)
                    runner.remote('execute',auth)
                self.assertEqual(calls,[True,False])


if __name__=='__main__':
    unittest.main(verbosity=2)
