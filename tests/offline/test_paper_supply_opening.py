"""Real permit file operations; only API, clock and read-only DB snapshots fake."""
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import io
import subprocess
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'deploy'))
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import paper_supply_opening as opening
import run_paper_supply_opening as runner
from app.services import paper_quote_supply as supply
from app.services.quote_contract import CN_TZ
from datetime import datetime

NOW=datetime(2026,11,2,14,0,tzinfo=CN_TZ)


class Opening(unittest.TestCase):
    def exercise(self,fail=False,replace=False,link_then_fail=False,check_only=False):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'permit.json';calls=[]
            payload=dict(policy=dict(version=1,duration_seconds=86400,source='sina_l1',scope='paper_execution_sh_sz'),
                         auth={'username':'operator','password':'SECRET_CANARY'},query_code='pass',check_only=check_only)
            def exec_query(code,namespace):
                def request(url,token=None,body=None):
                    calls.append(url)
                    if url=='/api/auth/login':return dict(success=True,data={'access_token':'TOKEN_CANARY'})
                    if fail:raise RuntimeError('api_failure')
                    symbol=url.split('/quote/')[1].split('?')[0]
                    if symbol=='601059':return dict(success=True,data=dict(code=symbol,
                        current_price=None,low=None,high=None,pricing_basis=None))
                    return dict(success=True,data=dict(code=symbol,low=9,current_price=10,high=11,
                        pricing_basis=dict(source='sina_l1_on_demand',channel='upstream',as_of=NOW.isoformat(),
                            trading_status='normal',trading_status_raw='00',trading_status_source='sina_l1_field_32',
                            max_delay_seconds=90,payload_sha256='a'*64)))
                namespace['request']=request
            old_exec=exec
            def controlled_exec(code,namespace):
                return exec_query(code,namespace) if code=='pass' else old_exec(code,namespace)
            def snapshot(payload,permit=None):
                if permit is not None and replace:path.write_bytes(b'unexpected replacement')
                return dict(unchanged=True) if permit is None or not replace else dict(unchanged=False)
            original_install=opening.install
            def install(path,raw):
                original_install(path,raw)
                if link_then_fail:raise RuntimeError('directory_fsync_failure')
            with patch.object(supply,'cn_now',return_value=NOW),patch.object(opening,'snapshot',side_effect=snapshot), \
                 patch('builtins.exec',side_effect=controlled_exec),patch.object(opening.os,'geteuid',return_value=1000), \
                 patch.object(opening,'install',side_effect=install):
                result=opening.activate(payload,path)
            encoded=json.dumps(result)
            self.assertNotIn('SECRET_CANARY',encoded);self.assertNotIn('TOKEN_CANARY',encoded)
            exists=path.exists();raw=path.read_bytes() if exists else b''
            mode=path.stat().st_mode&0o777 if exists else None
            self.assertTrue(all('/orders' not in url and '/initialize' not in url for url in calls))
            return result,exists,raw,mode

    def test_finite_activation_checks_normal_etf_and_closed_suspension(self):
        result,exists,raw,mode=self.exercise()
        self.assertTrue(result['ok'],result);self.assertTrue(exists);self.assertEqual(mode,0o600)
        doc=json.loads(raw)
        self.assertEqual((supply.parse_quote_time(doc['expires_at'])-supply.parse_quote_time(doc['not_before'])).total_seconds(),86400)
        self.assertEqual(result['permit_sha256'],hashlib.sha256(raw).hexdigest())
        self.assertTrue(result['suspended_api_closed'])

    def test_api_failure_removes_only_created_permit(self):
        result,exists,_,_=self.exercise(fail=True)
        self.assertEqual(result['status'],'failed_closed');self.assertFalse(exists)

    def test_install_error_after_link_also_revokes(self):
        result,exists,_,_=self.exercise(link_then_fail=True)
        self.assertEqual(result['status'],'failed_closed');self.assertFalse(exists)

    def test_unexpected_replacement_is_preserved_and_not_reported_closed(self):
        result,exists,raw,_=self.exercise(replace=True)
        self.assertEqual(result['status'],'revoke_unproven');self.assertTrue(exists)
        self.assertEqual(raw,b'unexpected replacement')

    def test_check_only_does_not_install_permit(self):
        result,exists,_,_=self.exercise(check_only=True)
        self.assertEqual(result['status'],'ready_no_activation');self.assertFalse(exists)

    def test_existing_invalid_permit_is_preserved_before_any_api(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'permit';path.write_bytes(b'preexisting invalid file')
            payload=dict(policy=dict(version=1,duration_seconds=86400,source='sina_l1',scope='paper_execution_sh_sz'))
            with patch.object(supply,'cn_now',return_value=NOW),patch.object(opening.os,'geteuid',return_value=1000), \
                 patch.object(opening,'snapshot') as snapshot:
                result=opening.activate(payload,path)
            snapshot.assert_not_called()
            self.assertEqual(result['reason'],'permit_already_present')
            self.assertEqual(path.read_bytes(),b'preexisting invalid file')


class HostController(unittest.TestCase):
    def test_requires_accepted_delivery_and_health_before_starting_backend_program(self):
        for healthy in (True,False):
            with self.subTest(healthy=healthy),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);attempt=root/'activation/attempts/opening';attempt.mkdir(parents=True)
                old=root/'attempts/delivery';old.mkdir(parents=True)
                (root/'latest-attempt.json').write_text(json.dumps(dict(attempt='delivery')))
                (old/'completion.json').write_text(json.dumps(dict(status='deployed_paper_permit_absent',source_head='src')))
                payload=dict(attempt='opening',source_head='src',check_only=True,request={'image':'image'},
                             backend_code='# frozen code',auth={'password':'SECRET_CANARY'})
                info=dict(Id='container',Image='image',Mounts=[{}]*44,
                          State=dict(Running=True,Health={'Status':'healthy' if healthy else 'unhealthy'}))
                result=dict(ok=True,status='ready_no_activation',orders_created=0,permit_absent=True)
                calls=[]
                def run(argv,**kw):
                    self.assertNotIn('SECRET_CANARY',str(argv));calls.append(argv)
                    self.assertEqual(json.loads(kw['input'])['auth']['password'],'SECRET_CANARY')
                    return subprocess.CompletedProcess(argv,0,'PAPER_OPENING_RESULT:'+json.dumps(result),'')
                code=runner.REMOTE_CONTROLLER.replace(str(runner.ROOT),str(root))
                namespace={'__name__':'test_host'}
                with patch.object(sys,'stdin',io.StringIO(json.dumps(payload))), \
                     patch.object(subprocess,'check_output',return_value=json.dumps([info]).encode()), \
                     patch.object(subprocess,'run',side_effect=run):
                    try:
                        if healthy:exec(compile(code,'<host-opening>','exec'),namespace)
                        else:
                            with self.assertRaises(SystemExit):exec(compile(code,'<host-opening>','exec'),namespace)
                    finally:
                        if 'lock' in namespace:namespace['lock'].close()
                self.assertEqual(len(calls),1 if healthy else 0)
                for path in attempt.rglob('*'):
                    if path.is_file():self.assertNotIn('SECRET_CANARY',path.read_text())


if __name__=='__main__':unittest.main(verbosity=2)
