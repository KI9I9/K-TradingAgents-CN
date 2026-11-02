"""Delivery drift, real staged permissions and isolation; Docker/SSH are fake I/O."""
import base64
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zlib
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'deploy'))
import prepare_paper_quote_supply as prepare
import verify_compose_merge as verify


def compose_fixture(root):
    old = sorted(prepare.TARGETS-{prepare.NEW_TARGET})
    volumes = [dict(type='bind',source=str(root/('original-'+str(i)+'.py')),target=t,read_only=True)
               for i,t in enumerate(old)]
    before = {'services': {'backend': {'image': 'app:stable', 'volumes': volumes}}}
    override = {'services': {'backend': {'volumes': [
        dict(type='bind',source=str(root/'staged'/t.removeprefix('/app/')),target=t,
             read_only=True,bind={'create_host_path':False}) for t in sorted(prepare.TARGETS)]}}}
    after = copy.deepcopy(before)
    for volume in after['services']['backend']['volumes']:
        volume['source'] = next(v['source'] for v in override['services']['backend']['volumes'] if v['target']==volume['target'])
    after['services']['backend']['volumes'].append(copy.deepcopy(next(
        v for v in override['services']['backend']['volumes'] if v['target']==prepare.NEW_TARGET)))
    return before,after,override


class ComposeBoundary(unittest.TestCase):
    def test_remote_envelope_loads_actual_controller_without_running_cli(self):
        code=Path(prepare.__file__).read_text()
        envelope=prepare.remote_envelope(dict(code=code,request={}), 'prepare')
        # Replace only the external preparation call; execute the complete module
        # through the exact stdin/bootstrap path used over SSH.
        envelope['code']=envelope['code'].replace(
            'print(json.dumps(prepare_remote(REQUEST)))',
            "print(json.dumps({'module_file':__file__,'root':str(ROOT),'loaded':callable(prepare_remote)}))")
        result=subprocess.run([sys.executable,'-B','-c',prepare.REMOTE_BOOTSTRAP],
            input=zlib.compress(json.dumps(envelope).encode()),capture_output=True,check=True)
        receipt=json.loads(result.stdout)
        self.assertEqual(receipt['module_file'],str(prepare.ROOT/'controller.py'))
        self.assertTrue(receipt['loaded'])

    def test_exact_two_replacements_one_addition_and_reject_unrelated_changes(self):
        before,after,override = compose_fixture(Path('/release'))
        self.assertEqual(set(prepare.compose_change(before,after,override,verify)),prepare.TARGETS-{prepare.NEW_TARGET})
        for key,value in [('environment',{'PASSWORD':'SECRET_CANARY'}),('command',['changed']),('privileged',True)]:
            changed=copy.deepcopy(after);changed['services']['backend'][key]=value
            with self.assertRaisesRegex(ValueError,'compose_config_drift') as error:
                prepare.compose_change(before,changed,override,verify)
            self.assertNotIn('SECRET_CANARY',str(error.exception))
        changed=copy.deepcopy(after);changed['services']['backend']['volumes'][0]['read_only']=False
        with self.assertRaisesRegex(ValueError,'compose_config_drift'):
            prepare.compose_change(before,changed,override,verify)

    def test_existing_new_target_or_missing_original_refused(self):
        before,after,override = compose_fixture(Path('/release'))
        before['services']['backend']['volumes'].append(after['services']['backend']['volumes'][-1])
        with self.assertRaisesRegex(ValueError,'unexpected_existing_bind'):
            prepare.compose_change(before,after,override,verify)

    def test_native_import_only_mounts_code_not_runtime_or_secrets(self):
        info=dict(Image='synthetic-id',Config={'User':'1000:1000'},Mounts=[
            dict(Type='bind',Source='/private/password',Destination='/run/secrets/password'),
            dict(Type='volume',Source='/runtime',Destination='/app/runtime'),
            dict(Type='bind',Source='/old.py',Destination='/app/old.py')])
        argv=prepare.native_import_argv(info,{prepare.NEW_TARGET:'/new.py'},{})
        mounts=[argv[i+1] for i,arg in enumerate(argv) if arg=='--mount']
        self.assertEqual(len(mounts),2)
        self.assertTrue(all('readonly' in value and '/run/secrets/' not in value and '/app/runtime' not in value for value in mounts))
        self.assertEqual(argv[argv.index('--network')+1],'none')
        self.assertEqual(argv[argv.index('--user')+1],'1000:1000')


class RealStageWithFakeDocker(unittest.TestCase):
    def exercise(self,drift=False,import_fails=False):
        with tempfile.TemporaryDirectory() as directory:
            work=Path(directory);root=work/'release'
            before,after,override=compose_fixture(root)
            # Original files stay outside the release and must never be changed.
            originals={}
            for i,volume in enumerate(before['services']['backend']['volumes']):
                volume['source']=str(work/('original-'+str(i)+'.py'))
                path=Path(volume['source']);path.parent.mkdir(parents=True,exist_ok=True)
                raw=('original '+str(i)).encode();path.write_bytes(raw);originals[path]=raw
            mounts=[dict(Type='bind',Source=v['source'],Destination=v['target'],RW=False)
                    for v in before['services']['backend']['volumes']]
            seals={}
            for name in ('runtime-config.json','launch-context.json','activation-approval.json'):
                path=work/name;path.write_bytes(name.encode());seals[name]=prepare.digest(path.read_bytes())
                mounts.append(dict(Type='bind',Source=str(path),Destination='/run/secrets/'+name,RW=False))
                before['services']['backend']['volumes'].append(dict(type='bind',source=str(path),target='/run/secrets/'+name,read_only=True))
                after['services']['backend']['volumes'].append(copy.deepcopy(before['services']['backend']['volumes'][-1]))
            # Fill the same mount count without mounting private state into the import.
            for i in range(43-len(mounts)):
                source=work/('data'+str(i));source.mkdir()
                mounts.append(dict(Type='bind',Source=str(source),Destination='/data/'+str(i),RW=True))
                volume=dict(type='bind',source=str(source),target='/data/'+str(i),read_only=False)
                before['services']['backend']['volumes'].append(volume)
                after['services']['backend']['volumes'].append(copy.deepcopy(volume))
            compose=work/'compose.yml';compose.write_text('synthetic compose')
            labels={'com.docker.compose.project.working_dir':str(work),
                    'com.docker.compose.project':'synthetic',
                    'com.docker.compose.project.config_files':str(compose)}
            image=dict(Id='sha256:'+'a'*64,RepoTags=['app:stable'],Config=dict(Entrypoint=['python'],Cmd=['main.py'],Env=['PATH=/bin']))
            info=dict(Id='same-container',Image=image['Id'],Mounts=mounts,
                      State=dict(Running=True,Health={'Status':'healthy'}),
                      Config=dict(User='1000:1000',Labels=labels,Entrypoint=['python'],Cmd=['main.py'],Env=['PATH=/bin']))
            modules={v['target']:prepare.digest(Path(v['source']).read_bytes()) for v in before['services']['backend']['volumes'][:2]}
            modules[prepare.NEW_TARGET]=None
            source='pass\n'
            files={target:dict(path=target.removeprefix('/app/'),source=source,sha256=prepare.digest(source.encode())) for target in prepare.TARGETS}
            snapshot=dict(modules=modules,runtime={},runtime_files={'/app/runtime/state.json':base64.b64encode(b'old state').decode()},
                          generation='/app/runtime/generation',used=5597,preferences={'tushare_historical_sync':True},
                          collections={'paper_orders':{'count':0,'sha256':prepare.digest(b'')}},
                          history_counts={'stock_daily_quotes_unadjusted':94597},backups={'paper_orders':''})
            request=dict(controller_sha256='frozen-controller',verify=Path(verify.__file__).read_text(),image=image['Id'],
                         override=override,runtime='unused by fake IO',modules=modules,seals=seals,files=files,manifest={})
            calls=[];snapshot_count=0
            def run(argv,**kw):
                nonlocal snapshot_count
                calls.append(argv)
                if argv[:2]==['docker','inspect']:out=json.dumps([info])
                elif argv[:3]==['docker','image','inspect']:out=json.dumps([image])
                elif argv[:2]==['docker','compose']:out=json.dumps(after if kw.get('input') else before)
                elif argv[:2]==['docker','exec']:
                    snapshot_count+=1;value=copy.deepcopy(snapshot)
                    if drift and snapshot_count==2:value['used']+=1
                    if snapshot_count==2:value['backups']={}
                    out='PAPER_SNAPSHOT:'+json.dumps(value)
                elif argv[:2]==['docker','run']:
                    if import_fails:return subprocess.CompletedProcess(argv,1,'','synthetic import failure')
                    out='PAPER_IMPORT_OK:'+json.dumps(dict(uid=1000,full_app_import=True,permit_absent=True))
                else:raise AssertionError('unexpected external operation')
                return subprocess.CompletedProcess(argv,0,out,'')
            previous=os.umask(0o077)
            try:
                with patch.object(prepare,'ROOT',root),patch.object(prepare.subprocess,'run',side_effect=run):
                    result=prepare.prepare_remote(request)
            finally:os.umask(previous)
            self.assertTrue(all(path.read_bytes()==raw for path,raw in originals.items()))
            self.assertEqual(root.stat().st_mode&0o777,0o711)
            self.assertEqual((root/'staged').stat().st_mode&0o777,0o755)
            self.assertEqual((root/'backup').stat().st_mode&0o777,0o700)
            self.assertFalse(any(token in argv for argv in calls for token in ('stop','restart','up','down','rm')))
            for path in (root/'backup').rglob('*'):
                if path.is_file():self.assertEqual(path.stat().st_mode&0o777,0o600)
            for path in (root/'staged').rglob('*.py'):
                self.assertEqual(path.stat().st_mode&0o777,0o644)
            self.assertFalse((root/'staged/paper-quote-supply.json').exists())
            self.assertTrue((root/'preflight.json').is_file())
            if not drift and not import_fails:
                self.assertTrue(result['ok'],result)
                complete=json.loads((root/'backup/complete.json').read_text())
                for path,sha in complete['files'].items():
                    self.assertEqual(prepare.digest((root/'backup'/path).read_bytes()),sha)
                self.assertEqual(complete['rollback_argv'][-5:],['up','-d','--no-deps','--force-recreate','backend'])
            return result

    def test_backup_and_readable_stage_under_private_umask_without_switch(self):
        self.assertEqual(self.exercise()['stage'],'prepared_no_switch')

    def test_drift_during_import_refuses_delivery(self):
        self.assertEqual(self.exercise(drift=True)['reason'],'production_changed_during_preflight')

    def test_native_import_failure_keeps_original_files(self):
        result=self.exercise(import_fails=True)
        self.assertFalse(result['ok'])
        self.assertEqual(result['reason'],'details_withheld')


if __name__=='__main__':
    unittest.main(verbosity=2)
