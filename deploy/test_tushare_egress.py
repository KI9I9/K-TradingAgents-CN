"""Synthetic tests, denied external networking, no app startup or production mounts."""
import ast
import asyncio
from datetime import datetime, date, timedelta
import hashlib
import http.client
import io
import json
import multiprocessing
import os
import functools
from pathlib import Path
import re
import socket
import ssl
import sys
import tempfile
import threading
import time
import traceback
import types
import unicodedata
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parent
from offline_tushare_support import SOURCE, egress, policy, authorize_new
TEST_ROOT = Path(tempfile.mkdtemp(prefix='ta-egress-offline-'))


def offline_only(event, args):
    if event in ('socket.getaddrinfo', 'socket.sendto', 'subprocess.Popen'):
        raise RuntimeError('OfflineNetworkOrProcessBlocked')
    if event in ('socket.connect', 'socket.bind'):
        address = args[1]
        if not isinstance(address, str) or not address.startswith(str(TEST_ROOT)+'/'):
            raise RuntimeError('OfflineExternalSocketBlocked')


sys.addaudithook(offline_only)
permit_admin = types.SimpleNamespace(authorize_new=authorize_new)

FAKE = 'SYNTHETIC_ONLY_NOT_A_REAL_TOKEN'
GOOD = {'code':0, 'data':{'fields':['ts_code'], 'items':[['SYNTHETIC.SZ']]}}


def classifier(data, token):
    reason = 'usage_limit' if data.get('msg') == 'synthetic_limit' else 'unclassified'
    return 'tushare_api_rejected code=40203 reason='+reason+' window=unknown'


def killed_attempt(directory):
    egress.Permit(directory).perform('stock_basic', lambda: os._exit(0))


def hold_process(directory, channel):
    with egress.Permit(directory).locked():
        channel.send('locked')
        channel.recv()


class OfflineTests(unittest.TestCase):
    def setUp(self):
        self.directory = TEST_ROOT/self.id().split('.')[-1]
        self.permit = self.new_permit()
        self.sent = []
        self.reply = json.dumps(GOOD).encode()
        self.status = 200
        self.error = None
        outer = self

        class FakeConnection:
            def __init__(self, timeout):
                self._expired = threading.Event()
            def connect(self):
                if outer.error is not None:
                    raise outer.error
            def request(self, method, path, **kwargs):
                outer.sent.append((method, path, kwargs))
            def getresponse(self):
                return types.SimpleNamespace(status=outer.status, read=lambda n:outer.reply[:n])
            def close(self):
                pass

        self.fake_connection = FakeConnection
        self.route = patch.dict(os.environ, {'TA_TUSHARE_ROUTE':'mainland_unix'})
        self.route.start()
        self.addCleanup(self.route.stop)

    def new_permit(self, suffix='', budget=5, ttl=300):
        directory = Path(str(self.directory)+suffix)
        permit_admin.authorize_new(directory, budget, ttl, ['stock_basic'])
        return egress.Permit(directory)

    def state(self, permit=None):
        permit = permit or self.permit
        with permit.locked() as fd:
            return permit.read(fd)

    def client(self, permit=None):
        return egress.MainlandTransport(FAKE, classifier, permit=permit or self.permit)

    def query(self, client=None, **params):
        with patch.object(egress, 'UnixTLSConnection', self.fake_connection):
            return (client or self.client()).query('stock_basic', **params)

    def test_success_payload_and_nonsecret_state(self):
        table = self.query(limit=1)
        self.assertEqual(table, GOOD['data'])
        method, path, options = self.sent[0]
        self.assertEqual((method,path),('POST','/dataapi/stock_basic'))
        body=json.loads(options['body'])
        self.assertEqual(body, {'api_name':'stock_basic','token':FAKE,'fields':'',
            'params':{'limit':1,'ts_type_name':'http://api.waditu.com/dataapi'}})
        self.assertEqual(self.state()['used'],1)
        for file in self.directory.iterdir():
            self.assertNotIn(FAKE.encode(),file.read_bytes())
        self.assertEqual(self.directory.stat().st_mode & 0o777,0o700)
        self.assertEqual((self.directory/'permit.json').stat().st_mode & 0o777,0o600)

    def test_budget_global_across_new_clients(self):
        for _ in range(5): self.query(self.client())
        with self.assertRaisesRegex(egress.TushareEgressStopped,'budget_exhausted'):self.query(self.client())
        self.assertEqual(len(self.sent),5)
        self.assertEqual(self.state()['used'],5)

    def test_api_rejection_latches_and_no_token_switch(self):
        self.reply=json.dumps({'code':40203,'msg':'synthetic_limit'}).encode()
        for token in (FAKE, 'SYNTHETIC_SECOND_TOKEN'):
            client=egress.MainlandTransport(token,classifier,permit=egress.Permit(self.directory))
            with self.assertRaisesRegex(egress.TushareEgressStopped,'usage_limit'):self.query(client)
        self.assertEqual(len(self.sent),1)
        self.assertEqual(self.state()['api_code'],40203)

    def test_no_network_authorization_by_default(self):
        client=self.client(egress.Permit(TEST_ROOT/'missing'))
        with self.assertRaisesRegex(egress.TushareEgressStopped,'not_authorized'):self.query(client)
        with patch.dict(os.environ,{'TA_TUSHARE_ROUTE':'direct'}):
            with self.assertRaisesRegex(egress.TushareEgressStopped,'disabled'):self.query()
        self.assertEqual(self.sent,[])
        self.assertEqual(self.state()['used'],0)

    def test_expired_permit_does_not_send(self):
        with patch.object(egress.time,'time',return_value=time.time()+601):
            with self.assertRaisesRegex(egress.TushareEgressStopped,'expired'):self.query()
        self.assertEqual(self.sent,[])

    def test_unapproved_api_locks_without_send(self):
        with self.assertRaisesRegex(egress.TushareEgressStopped,'api_not_approved'):
            self.client().query('daily')
        self.assertEqual(self.state()['used'],0)

    def test_network_tls_timeout_fail_once(self):
        for index,error in enumerate([OSError('synthetic'),ssl.SSLCertVerificationError('synthetic'),TimeoutError('synthetic')]):
            permit=self.new_permit(str(index))
            self.error=error
            with self.assertRaises(egress.TushareEgressStopped):self.query(self.client(permit))
            self.error=None
            with self.assertRaises(egress.TushareEgressStopped):self.query(self.client(permit))
            self.assertEqual(self.state(permit)['used'],1)
        self.assertEqual(self.sent,[])

    def test_redirect_and_http_errors_never_follow(self):
        for status in (301,302,307,401,403,429,500):
            permit=self.new_permit(str(status));self.status=status
            with self.assertRaises(egress.TushareEgressStopped):self.query(self.client(permit))
            with self.assertRaises(egress.TushareEgressStopped):self.query(self.client(permit))
        self.assertEqual(len(self.sent),7)

    def test_invalid_or_oversized_responses_latch(self):
        bodies=[b'',b'not json',b'{"code":0,"code":0}',b'{}',json.dumps({'code':True}).encode(),
            json.dumps({'code':0,'data':{'fields':['a'],'items':[['a','b']]}}).encode(),b'x'*(egress.MAX_BODY+1)]
        for index,body in enumerate(bodies):
            permit=self.new_permit(str(index));self.reply=body
            with self.assertRaises(egress.TushareEgressStopped):self.query(self.client(permit))
            self.assertEqual(self.state(permit)['phase'],'blocked')

    def test_error_trace_and_state_do_not_echo_response(self):
        self.reply=json.dumps({'code':40203,'msg':FAKE,'detail':'SYNTHETIC_RESPONSE_BODY'}).encode()
        try:self.query()
        except egress.TushareEgressStopped:
            text=traceback.format_exc()
        self.assertNotIn(FAKE,text)
        self.assertNotIn('SYNTHETIC_RESPONSE_BODY',text)
        self.assertNotIn(FAKE,json.dumps(self.state()))

    def test_invalid_request_does_not_consume_permit(self):
        for kwargs in ({'fields':[]},{'value':float('nan')},{'value':'x'*(256*1024)}):
            with self.assertRaisesRegex(egress.TushareEgressStopped,'invalid_request'):self.query(**kwargs)
        for api in ('../daily','https://example.invalid',''):
            with self.assertRaises(egress.TushareEgressStopped):self.client().query(api)
        self.assertEqual(self.state()['used'],0)

    def test_threads_cannot_send_in_parallel(self):
        entered=threading.Event();release=threading.Event();completed=[]
        def operation():entered.set();release.wait(3);return 'ok'
        def worker():completed.append(self.permit.perform('stock_basic',operation))
        thread=threading.Thread(target=worker);thread.start()
        try:
            self.assertTrue(entered.wait(2))
            with self.assertRaisesRegex(egress.TushareEgressStopped,'busy'):self.query(self.client())
        finally:release.set();thread.join(3)
        self.assertEqual(completed,['ok']);self.assertEqual(self.sent,[])

    def test_stop_marker_does_not_wait_for_current_request(self):
        entered=threading.Event();release=threading.Event();completed=[]
        def operation():entered.set();release.wait(3);return 'already_sent_result'
        thread=threading.Thread(target=lambda:completed.append(self.permit.perform('stock_basic',operation)))
        thread.start()
        try:
            self.assertTrue(entered.wait(2))
            self.permit.stop()
            self.assertFalse(release.is_set())
            self.assertTrue((self.directory/'STOP').exists())
        finally:release.set();thread.join(3)
        self.assertEqual(completed,['already_sent_result'])
        with self.assertRaisesRegex(egress.TushareEgressStopped,'disabled'):self.query()
        self.assertEqual(self.state()['used'],1)
        self.assertEqual(self.sent,[])

    def test_process_lock_and_crash_survive_recreation(self):
        context=multiprocessing.get_context('fork')
        a,b=context.Pipe();process=context.Process(target=hold_process,args=(self.directory,b));process.start()
        try:
            self.assertTrue(a.poll(3));self.assertEqual(a.recv(),'locked')
            with self.assertRaisesRegex(egress.TushareEgressStopped,'busy'):self.query()
        finally:a.send('release');process.join(3);a.close();b.close()
        self.assertEqual(process.exitcode,0)
        process=context.Process(target=killed_attempt,args=(self.directory,));process.start();process.join(3)
        self.assertEqual(process.exitcode,0)
        self.assertEqual(self.state()['phase'],'in_flight')
        with self.assertRaisesRegex(egress.TushareEgressStopped,'previous_attempt_incomplete'):self.query()
        self.assertEqual(self.state()['used'],1);self.assertEqual(self.sent,[])

    def test_unexpected_permissions_and_links_refused(self):
        self.directory.chmod(0o755)
        with self.assertRaisesRegex(egress.TushareEgressStopped,'state_invalid'):self.query()
        self.directory.chmod(0o700)
        os.link(self.directory/'permit.json',self.directory/'preserved-hardlink')
        with self.assertRaisesRegex(egress.TushareEgressStopped,'state_invalid'):self.query()
        other=TEST_ROOT/'symlinked-permit';other.symlink_to(self.directory,target_is_directory=True)
        with self.assertRaises(egress.TushareEgressStopped):self.query(self.client(egress.Permit(other)))
        self.assertEqual(self.sent,[])

    def test_missing_corrupt_and_partial_state_refused(self):
        (self.directory/'permit.json').write_text('{"schema":1,"schema":1}')
        with self.assertRaisesRegex(egress.TushareEgressStopped,'state_invalid'):self.query()
        permit=self.new_permit('partial')
        (permit.directory/'permit.next').write_text('synthetic interrupted write')
        with self.assertRaisesRegex(egress.TushareEgressStopped,'state_io_failed'):self.query(self.client(permit))
        self.assertEqual(self.sent,[])

    def test_journal_failure_prevents_send(self):
        with patch.object(self.permit,'write',side_effect=OSError('synthetic')):
            with self.assertRaisesRegex(egress.TushareEgressStopped,'state_io_failed'):self.query()
        self.assertEqual(self.sent,[])

    def test_completion_journal_failure_keeps_attempt_uncertain(self):
        original=self.permit.write;writes=[]
        def failing(fd,value):
            writes.append(value['phase'])
            if len(writes)==2:raise OSError('synthetic commit failure')
            return original(fd,value)
        with patch.object(self.permit,'write',side_effect=failing):
            with self.assertRaisesRegex(egress.TushareEgressStopped,'state_io_failed'):self.query()
        self.assertEqual(self.state()['phase'],'in_flight')
        with self.assertRaisesRegex(egress.TushareEgressStopped,'previous_attempt_incomplete'):self.query()
        self.assertEqual(len(self.sent),1)

    def test_real_classifier_and_adapter_delegate_to_guarded_transport(self):
        filename='tradingagents/utils/tushare_https.py'
        source=(SOURCE/filename).read_text()
        tree=ast.parse(source);tree.body=[n for n in tree.body if not isinstance(n,(ast.Import,ast.ImportFrom))]
        dataframe=Mock(side_effect=lambda items,columns:{'fields':columns,'items':items})
        ns=dict(re=re,unicodedata=unicodedata,partial=functools.partial,pd=types.SimpleNamespace(DataFrame=dataframe),
            MainlandTransport=egress.MainlandTransport,TushareEgressStopped=egress.TushareEgressStopped)
        exec(compile(tree,'<adapter-only>','exec'),ns)
        with patch.object(egress,'Permit',return_value=self.permit),patch.object(egress,'UnixTLSConnection',self.fake_connection):
            api=ns['create_tushare_client'](FAKE)
            self.assertEqual(api.stock_basic(limit=1),GOOD['data'])
            self.reply=json.dumps({'code':40203,'msg':'每分钟最多访问50次 '+FAKE}).encode()
            with self.assertRaisesRegex(egress.TushareEgressStopped,'usage_limit'):api.stock_basic(limit=1)
            with self.assertRaisesRegex(egress.TushareEgressStopped,'usage_limit'):api.stock_basic(limit=1)
        self.assertEqual(len(self.sent),2)
        self.assertEqual(dataframe.call_count,1)

    def test_authorization_cannot_reset_existing_state(self):
        original=(self.directory/'permit.json').read_bytes()
        with self.assertRaises(FileExistsError):permit_admin.authorize_new(self.directory,1,300,['stock_basic'])
        self.assertEqual(original,(self.directory/'permit.json').read_bytes())
        for budget in (0,6,True):
            with self.assertRaises(ValueError):permit_admin.authorize_new(TEST_ROOT/'invalid',budget,300,['stock_basic'])

    def test_fixed_unix_tls_route_and_no_second_connect(self):
        raw=Mock();secure=Mock();ctx=Mock(check_hostname=True,verify_mode=ssl.CERT_REQUIRED)
        ctx.wrap_socket.return_value=secure
        with patch.object(egress.ssl,'create_default_context',return_value=ctx) as ca,patch.object(egress.socket,'socket',return_value=raw) as sock:
            conn=egress.UnixTLSConnection(2)
            try:
                conn.connect()
                sock.assert_called_once_with(socket.AF_UNIX,socket.SOCK_STREAM)
                raw.connect.assert_called_once_with(egress.SOCKET_PATH)
                self.assertEqual(ctx.wrap_socket.call_args.kwargs['server_hostname'],egress.HOST)
                self.assertEqual(ca.call_args.kwargs['cafile'],egress.CA_FILE)
                with self.assertRaises(egress.TushareEgressStopped):conn.connect()
            finally:conn.close()

    def test_watchdog_interrupts_slow_handshake(self):
        released=threading.Event();raw=Mock();secure=Mock()
        secure.shutdown.side_effect=lambda *_:released.set()
        def handshake():
            if not released.wait(2):raise AssertionError('watchdog did not interrupt')
            raise OSError('synthetic interrupted TLS')
        secure.do_handshake.side_effect=handshake
        ctx=Mock(check_hostname=True,verify_mode=ssl.CERT_REQUIRED);ctx.wrap_socket.return_value=secure
        with patch.object(egress.ssl,'create_default_context',return_value=ctx),patch.object(egress.socket,'socket',return_value=raw):
            conn=egress.UnixTLSConnection(0.05);started=time.monotonic()
            try:
                with self.assertRaises(OSError):conn.connect()
                self.assertTrue(conn._expired.is_set());self.assertLess(time.monotonic()-started,1)
            finally:conn.close()

    def test_local_unix_socket_replacement_without_container_or_tcp(self):
        path=str(TEST_ROOT/'socket-current');old=str(TEST_ROOT/'socket-retained')
        listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
        try:listener.bind(path);os.chmod(path,0o660);listener.listen(1)
        except BaseException:listener.close();raise
        client=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
        try:
            client.connect(path);peer,_=listener.accept();peer.close()
            self.assertEqual(os.stat(path).st_mode & 0o777,0o660)
        finally:client.close();listener.close()
        os.rename(path,old)
        listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);listener.bind(path);listener.listen(1)
        client=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
        try:client.connect(path);peer,_=listener.accept();peer.close()
        finally:client.close();listener.close()
        self.assertNotEqual(os.stat(path).st_ino,os.stat(old).st_ino)


class IntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sources={p:(SOURCE/p).read_text() for p in ('tradingagents/utils/tushare_https.py', 'tradingagents/dataflows/providers/china/tushare.py', 'app/worker/tushare_sync_service.py')}

    def classes(self, filename, classname, **extra):
        tree=ast.parse(self.sources[filename]);cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name==classname)
        module=ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0),cls],type_ignores=[])
        ns=dict(BaseStockDataProvider=object,os=os,asyncio=asyncio,datetime=datetime,date=date,timedelta=timedelta,
            TUSHARE_AVAILABLE=True,TushareEgressStopped=egress.TushareEgressStopped,logger=Mock(),
            _manual_current=policy.CURRENT, _manual_frame=policy.validate_frame, **extra)
        exec(compile(ast.fix_missing_locations(module),'<patched-class>','exec'),ns)
        obj=ns[classname].__new__(ns[classname]);obj.logger=Mock()
        return obj,ns

    def test_all_broad_class_handlers_propagate_stop(self):
        count=0
        for filename,classname in [('tradingagents/dataflows/providers/china/tushare.py','TushareProvider'),('app/worker/tushare_sync_service.py','TushareSyncService')]:
            tree=ast.parse(self.sources[filename]);cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name==classname)
            for node in ast.walk(cls):
                if isinstance(node,ast.Try):
                    for index,handler in enumerate(node.handlers):
                        if isinstance(handler.type,ast.Name) and handler.type.id=='Exception':
                            self.assertEqual(node.handlers[index-1].type.id,'TushareEgressStopped');count+=1
        self.assertEqual(count,92)

    def test_provider_does_not_try_second_token_after_stop(self):
        factory=Mock(side_effect=egress.TushareEgressStopped('usage_limit'))
        obj,_=self.classes('tradingagents/dataflows/providers/china/tushare.py','TushareProvider',create_tushare_client=factory)
        obj.api=None;obj.config={'token':'SYNTHETIC_ENV'};obj._get_token_from_database=lambda:FAKE
        with self.assertRaisesRegex(egress.TushareEgressStopped,'usage_limit'):obj._ensure_api_client(validate=True)
        self.assertEqual(factory.call_count,1)

    def test_duplicate_token_is_not_validated_twice(self):
        api=Mock();api.stock_basic.return_value=types.SimpleNamespace(empty=True);factory=Mock(return_value=api)
        obj,_=self.classes('tradingagents/dataflows/providers/china/tushare.py','TushareProvider',create_tushare_client=factory)
        obj.api=None;obj.config={'token':FAKE};obj._get_token_from_database=lambda:FAKE
        self.assertFalse(obj._ensure_api_client(validate=True));self.assertEqual(factory.call_count,1)

    def test_history_service_does_not_return_success_stats_on_stop(self):
        binder=Mock(side_effect=egress.TushareEgressStopped('usage_limit'))
        obj,_=self.classes('app/worker/tushare_sync_service.py','TushareSyncService',bind_loop_safe_mongo_db=binder)
        with self.assertRaisesRegex(egress.TushareEgressStopped,'usage_limit'):
            asyncio.run(obj.sync_historical_data())

    def test_batch_gather_cannot_hide_stopped_requests(self):
        obj,_=self.classes('app/worker/tushare_sync_service.py','TushareSyncService')
        async def stopped(symbol):raise egress.TushareEgressStopped('usage_limit')
        obj._get_and_save_quotes=stopped
        with self.assertRaisesRegex(egress.TushareEgressStopped,'usage_limit'):
            asyncio.run(obj._process_quotes_batch(['SYNTHETIC']))

    def test_guard_helper_surfaces_sdk_suppressed_error(self):
        directory=TEST_ROOT/'integration-permit'
        permit_admin.authorize_new(directory,1,300,['stock_basic'])
        permit=egress.Permit(directory)
        with self.assertRaises(egress.TushareEgressStopped):
            permit.perform('stock_basic',lambda: (_ for _ in ()).throw(egress.TushareEgressStopped('usage_limit',40203)))
        with patch.object(egress,'Permit',return_value=permit):
            with self.assertRaisesRegex(egress.TushareEgressStopped,'usage_limit'):
                egress.raise_if_egress_blocked()

    def test_sdk_swallowed_error_is_surfaced(self):
        stopped=egress.TushareEgressStopped('usage_limit')
        check=Mock(side_effect=stopped)
        obj,_=self.classes('tradingagents/dataflows/providers/china/tushare.py','TushareProvider',
            ts=types.SimpleNamespace(pro_bar=lambda **kw:None),raise_if_egress_blocked=check)
        obj.api=object();obj.is_available=lambda:True;obj._normalize_ts_code=lambda s:s
        obj._format_date=lambda s:s;obj._is_etf=lambda s:False
        with self.assertRaisesRegex(egress.TushareEgressStopped,'usage_limit'):
            asyncio.run(obj.get_historical_data('SYNTHETIC','20260101','20260102'))
        check.assert_called_once()

    def test_transport_factory_has_no_direct_query_path(self):
        tree=ast.parse(self.sources['tradingagents/utils/tushare_https.py'])
        cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='HttpsTushareClient')
        query=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='query')
        text=ast.unparse(query)
        self.assertNotIn('requests',text);self.assertIn('MainlandTransport',text)
        self.assertNotIn('session',text)

    def test_sdk_replaced_error_is_surfaced(self):
        check=Mock(side_effect=egress.TushareEgressStopped('usage_limit'))
        sdk=types.SimpleNamespace(pro_bar=Mock(side_effect=OSError('ERROR.')))
        obj,_=self.classes('tradingagents/dataflows/providers/china/tushare.py','TushareProvider',
            ts=sdk,raise_if_egress_blocked=check)
        obj.api=object();obj.is_available=lambda:True;obj._normalize_ts_code=lambda s:s
        obj._format_date=lambda s:s;obj._is_etf=lambda s:False
        with self.assertRaisesRegex(egress.TushareEgressStopped,'usage_limit'):
            asyncio.run(obj.get_historical_data('SYNTHETIC','20260101','20260102'))
        check.assert_called_once()



if __name__ == '__main__':
    print(json.dumps({'synthetic_artifacts_retained':str(TEST_ROOT),'external_network':'denied','production_services_touched':False}),flush=True)
    unittest.main(verbosity=2)
