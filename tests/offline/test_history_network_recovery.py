"""Real policy/journal/flock/fsync; fake DB matching and TLS handshake only."""
import asyncio
import ast
from copy import deepcopy
from datetime import datetime,timezone
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/'deploy'))
from deploy.history_sync_plan import build_plan
from deploy.history_batch_journal import Workspace
from tradingagents.utils.tushare_batch import provision,BJ


class Adapter:
    mismatch=False
    def __init__(self,*args,**kwargs):pass
    async def matches(self,item,payload):return not self.mismatch
    async def preflight(self,item):return item['observation_sha256']


adapter_module=types.ModuleType('deploy.history_batch_adapters');adapter_module.HistoricalAdapter=Adapter
spec=importlib.util.spec_from_file_location('operator_network_helper',ROOT/'deploy/history_tls_recovery.py')
helper=importlib.util.module_from_spec(spec)
with patch.dict(sys.modules,{'deploy.history_batch_adapters':adapter_module}):spec.loader.exec_module(helper)


class Recovery(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name);now=helper.time.time();today=datetime.fromtimestamp(now,BJ).date().isoformat()
        codes=['600000.SH','600001.SH','600002.SH']
        series={code:dict(data_source='tushare',market='CN',period='daily',count=1,latest='2026-10-27',
                         contract_verified=True,content_sha256='a'*64) for code in codes}
        self.plan=build_plan(codes,series,end='2026-10-28',now=datetime(2026,10,29,tzinfo=timezone.utc),
            price_adjustment='none',quota=dict(daily_requests=100,rolling_minute_requests=4,evidence_ref='synthetic'))
        self.gate=provision(root/'gate',dict(schema=2,codes=codes,min_date='2026-10-01',max_date='2026-11-06',
            expires_at=now+3600,max_requests=100,daily_requests=100,minute_requests=4,write_enabled=True))
        self.ws=Workspace(self.gate.directory/'runs');self.ws.provision();self.identity=self.ws.create(self.plan)
        self.folder=self.ws.path/self.identity
        first=json.loads((self.folder/(codes[0]+'.json')).read_text());first.update(phase='committed',fetch_attempts=1,payload={'saved':True})
        # Real journal verifies payload hashes when reloaded.
        from deploy.history_batch_journal import digest
        first['payload_sha256']=digest(first['payload'])
        (self.folder/(codes[0]+'.json')).write_text(json.dumps(first))
        item=json.loads((self.folder/(codes[1]+'.json')).read_text());item.update(phase='fetching',fetch_attempts=1)
        (self.folder/(codes[1]+'.json')).write_text(json.dumps(item))
        active=dict(id='b'*32,code=codes[1],start=self.plan['items'][1]['start'],end=self.plan['items'][1]['end'],sent=['daily'])
        with self.gate.locked() as fd:
            _,s=self.gate.read(fd);s.update(plan=self.plan,used=2,days={today:2},recent=[now,now],last_time=now,
                                          phase='blocked',reason='network_failed',active=active)
            self.gate.write(fd,s)
        self.gate.stop();self.state=deepcopy(s);self.active=active;Adapter.mismatch=False

    def inventory(self):
        return {str(p.relative_to(self.gate.directory)):hashlib.sha256(p.read_bytes()).hexdigest()
                for p in self.gate.directory.rglob('*') if p.is_file()}

    def recover(self,**kwargs):
        return asyncio.run(helper.recover_raw_network_verified(self.gate,None,
            expected_plan=self.identity,expected_used=2,expected_active=self.active,**kwargs))

    def test_check_has_zero_state_or_stop_changes(self):
        before=self.inventory()
        with patch.object(helper,'verify_fixed_tls',return_value=[{'version':'TLSv1.2'}]):result=self.recover(check_only=True)
        self.assertTrue(result['checked_only']);self.assertEqual(before,self.inventory())

    def test_resume_changes_only_failed_attempt_and_keeps_every_charged_request(self):
        original_first=(self.folder/'600000.SH.json').read_bytes();last=(self.folder/'600002.SH.json').read_bytes()
        with patch.object(helper,'verify_fixed_tls',return_value=[]):result=self.recover()
        self.assertFalse((self.gate.directory/'STOP').exists());self.assertEqual(result['resume_code'],'600001.SH')
        with self.gate.locked() as fd:_,state=self.gate.read(fd)
        self.assertEqual(state,dict(self.state,phase='ready',reason='none',active=None,api_code=None))
        self.assertEqual(original_first,(self.folder/'600000.SH.json').read_bytes())
        self.assertEqual(last,(self.folder/'600002.SH.json').read_bytes())
        self.assertEqual(json.loads((self.folder/'600001.SH.json').read_text())['phase'],'pending')
        self.assertTrue((self.gate.directory/('network-recovery-'+self.active['id'])/'ready.json').is_file())

    def test_mismatched_saved_data_does_not_probe_or_clear_stop(self):
        Adapter.mismatch=True;before=self.inventory()
        with patch.object(helper,'verify_fixed_tls') as probe:
            with self.assertRaisesRegex(RuntimeError,'write_outcome_unresolved'):self.recover()
        probe.assert_not_called();self.assertEqual(before,self.inventory())

    def test_tls_failure_does_not_change_any_ledger_byte(self):
        before=self.inventory()
        with patch.object(helper,'verify_fixed_tls',side_effect=OSError()):
            with self.assertRaisesRegex(RuntimeError,'tls_probe_failed'):self.recover()
        self.assertEqual(before,self.inventory())

    def test_changed_attempt_refuses_before_recovery(self):
        self.active=dict(self.active,id='c'*32);before=self.inventory()
        with patch.object(helper,'verify_fixed_tls') as probe:
            with self.assertRaises(RuntimeError):self.recover()
        probe.assert_not_called();self.assertEqual(before,self.inventory())


class Launcher(unittest.TestCase):
    def test_one_reservation_and_no_credentials_in_package_or_process_files(self):
        import run_history_network_recovery as runner
        raw=runner.canonical(dict(driver='pass'))
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'recovery';code=runner.LAUNCH.replace(runner.ROOT,str(root))
            payload=dict(package=raw.decode(),sha256=runner.sha(raw),check_only=False,attempt='test',
                         auth={'password':'SECRET_CANARY'})
            import zlib
            child=types.SimpleNamespace(pid=123,stdin=io.BytesIO())
            with patch.object(sys,'stdin',types.SimpleNamespace(buffer=io.BytesIO(zlib.compress(runner.canonical(payload))))), \
                 patch.object(runner.subprocess,'Popen',return_value=child) as launch:
                exec(code,{})
            self.assertEqual(launch.call_count,1)
            with patch.object(sys,'stdin',types.SimpleNamespace(buffer=io.BytesIO(zlib.compress(runner.canonical(payload))))), \
                 patch.object(runner.subprocess,'Popen') as launch:
                with self.assertRaises(FileExistsError):exec(code,{})
            launch.assert_not_called()
            for p in root.rglob('*'):
                if p.is_file():self.assertNotIn('SECRET_CANARY',p.read_text())


class RawBackup(unittest.TestCase):
    def test_streams_exact_bytes_and_never_overwrites_an_existing_backup(self):
        source=ROOT/'deploy/history_timeout_recovery.py'
        node=next(n for n in ast.parse(source.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='raw_history')
        class Cursor(list):
            def sort(self,*a):return self
            def batch_size(self,*a):return self
            def max_time_ms(self,*a):return self
        raw=[b'first\x00\xff',b'x'*(1024*1024+3)]
        class Collection:
            def find(self,q):return Cursor(types.SimpleNamespace(raw=v) for v in raw)
        db=types.SimpleNamespace(get_collection=lambda *a,**k:Collection())
        def require(v,r):
            if not v:raise RuntimeError(r)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            ns=dict(os=os,hashlib=hashlib,RUN=root,CodecOptions=lambda **kw:None,RawBSONDocument=object,require=require)
            exec(compile(ast.Module(body=[node],type_ignores=[]),str(source),'exec'),ns)
            expected=dict(count=2,sha256=hashlib.sha256(b''.join(raw)).hexdigest())
            self.assertEqual(ns['raw_history'](db,'history'),expected)
            self.assertEqual(list(root.iterdir()),[])
            self.assertEqual(ns['raw_history'](db,'history',backup=True),expected)
            path=root/'history.bson';self.assertEqual(path.read_bytes(),b''.join(raw))
            self.assertEqual(path.stat().st_mode&0o777,0o600)
            with self.assertRaises(FileExistsError):ns['raw_history'](db,'history',backup=True)
            self.assertEqual(path.read_bytes(),b''.join(raw))


if __name__=='__main__':unittest.main(verbosity=2)
