"""Finite, explicitly provisioned history policy; fixed plan scope, no renew/reset API.

Uses the existing CURRENT transport dispatch. Old manual/global permits and their
STOP files remain untouched. Only a separately approved private policy can run.
"""
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
import hashlib
import fcntl
import re
import stat
import json
import math
import os
from pathlib import Path
import time
from uuid import uuid4

from tradingagents.utils.tushare_egress import Permit, TushareEgressStopped, _pairs, _private_fd
from tradingagents.utils.tushare_manual import CURRENT, BJ
from deploy.history_sync_plan import _code as stock_code

DIRECTORY = '/app/runtime/tushare-history-batch-v1'
REGISTRY = '/app/runtime/tushare-history-generations'


def sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


GENERATION_REASONS = frozenset(('generation_changed','generation_incomplete',
    'predecessor_not_stopped','predecessor_changed','write_outcome_unresolved'))


class GenerationStopped(TushareEgressStopped):
    def __init__(self, reason):
        super().__init__('state_invalid')
        if reason in GENERATION_REASONS:
            self.reason = reason
            RuntimeError.__init__(self, 'history_generation_stopped reason=' + reason)


def require(value, reason='state_invalid'):
    if not value:
        raise GenerationStopped(reason) if reason in GENERATION_REASONS else TushareEgressStopped(reason)


def policy_check(p):
    try:
        require(set(p) == {'schema','codes','min_date','max_date','expires_at','max_requests',
                          'daily_requests','minute_requests','write_enabled'})
        require(type(p['schema']) is int and p['schema'] == 1 and p['write_enabled'] is True)
        require(isinstance(p['codes'],list) and 1 <= len(p['codes']) <= 10000
                and p['codes'] == sorted(set(stock_code(c) for c in p['codes'])))
        lo,hi=(datetime.strptime(p[k],'%Y-%m-%d').date() for k in ('min_date','max_date'))
        require(0 <= (hi-lo).days <= 373)  # 366-day item window plus up to seven days of permit life
        for k,limit in [('max_requests',100000),('daily_requests',20000),('minute_requests',200)]:
            require(type(p[k]) is int and 2 <= p[k] <= limit)
        require(type(p['expires_at']) in (int,float) and math.isfinite(p['expires_at']))
        return p
    except (KeyError,TypeError,ValueError):
        raise TushareEgressStopped('state_invalid') from None


class BatchGate(Permit):
    def __init__(self,directory=DIRECTORY):
        super().__init__(directory)
        self.admission_path = self.directory / "admission.lock"
        self.selection = None

    def assert_selected(self):
        if self.selection is not None:
            registry, expected = self.selection
            require(_selection(registry) == expected, "generation_changed")

    def _read_file(self,fd,name):
        raw_fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW,dir_fd=fd)
        with os.fdopen(raw_fd,'rb') as f:
            _private_fd(f.fileno());raw=f.read(16*1024*1024+1)
        require(len(raw)<=16*1024*1024)
        try: return json.loads(raw,object_pairs_hook=_pairs)
        except (ValueError,TypeError): raise TushareEgressStopped('state_invalid') from None

    def read(self,fd):
        policy=policy_check(self._read_file(fd,'policy.json'))
        state=self._read_file(fd,'state.json')
        try:
            require(set(state)=={'policy_sha256','phase','reason','api_code','used','days','recent','last_time','plan','active'})
            require(state['policy_sha256']==sha(policy) and state['phase'] in ('ready','in_flight','blocked'))
            require(type(state['used']) is int and 0<=state['used']<=policy['max_requests'])
            require(isinstance(state['days'],dict) and sum(state['days'].values())==state['used'])
            for day,count in state['days'].items():
                require(datetime.strptime(day,'%Y-%m-%d').strftime('%Y-%m-%d')==day)
                require(type(count) is int and 0<=count<=policy['daily_requests'])
            require(type(state['last_time']) in (int,float) and math.isfinite(state['last_time']))
            require(isinstance(state['recent'],list) and len(state['recent'])<=policy['minute_requests'])
            require(all(type(t) in (int,float) and math.isfinite(t) and 0<t<=state['last_time'] for t in state['recent']))
            require(state['recent']==sorted(state['recent']))
            require(state['phase']!='in_flight' or state['active'] is not None)
            require(state['phase']!='ready' or state['reason']=='none')
            if state['plan'] is not None: self._validate_plan(policy,state['plan'])
            if state['active'] is not None:
                a=state['active'];require(set(a)=={'id','code','start','end','sent'})
                require(isinstance(a['id'],str) and len(a['id'])==32 and a['sent'] in ([],['daily'],['daily','adj_factor']))
                require(state['plan'] is not None and any(all(a[k]==i[k] for k in ('code','start','end')) for i in state['plan']['items']))
            return policy,state
        except (KeyError,TypeError,ValueError): raise TushareEgressStopped('state_invalid') from None

    def _validate_plan(self,policy,plan):
        require(isinstance(plan,dict) and 'plan_sha256' in plan)
        plain={k:v for k,v in plan.items() if k!='plan_sha256'}
        require(plan['plan_sha256']==sha(plain))
        from deploy.history_batch_journal import validate_plan
        validate_plan(plan)
        require(plan['pool']==policy['codes'] and len(plan['items'])==len(policy['codes']), 'invalid_request')
        require([i['code'] for i in plan['items']]==policy['codes'])
        for i in plan['items']:
            if i['status']=='blocked':
                continue
            require(i['status']=='candidate' and i['request_cost']==2, 'invalid_request')
            require(policy['min_date']<=i['start']<=i['end']<=policy['max_date'], 'invalid_request')
            require(0<=(datetime.strptime(i['end'],'%Y-%m-%d')-datetime.strptime(i['start'],'%Y-%m-%d')).days<366,'invalid_request')

    def write(self,fd,state):
        # Policy is immutable; state replacement is durable before each request.
        raw=json.dumps(state,sort_keys=True,allow_nan=False).encode()
        require(len(raw)<=16*1024*1024)
        out=os.open('state.next',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=fd)
        with os.fdopen(out,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
        os.replace('state.next','state.json',src_dir_fd=fd,dst_dir_fd=fd);os.fsync(fd)

    def check(self,fd,p,s):
        if self.stopped(fd): raise TushareEgressStopped('disabled')
        if s['phase']!='ready':
            raise TushareEgressStopped('previous_attempt_incomplete' if s['phase']=='in_flight' else s['reason'],s['api_code'])
        now=time.time()
        require(s['last_time']<=now<p['expires_at'],'expired')
        return now

    def status(self):
        with self.locked() as fd:
            p,s=self.read(fd);self.check(fd,p,s)
            return deepcopy(p),deepcopy(s)

    def bind(self,plan):
        with self.locked() as fd:
            p,s=self.read(fd);now=self.check(fd,p,s)
            require(s['active'] is None,'busy');self._validate_plan(p,plan)
            # Switching away from an unfinished plan is prohibited by the caller's
            # workspace lock and committed-state check; no quota counters reset here.
            s.update(plan=deepcopy(plan),last_time=now);self.write(fd,s)

    def authorize(self,identity,item,action):
        require(action in ('fetch','write','reconcile'),'invalid_request')
        with self.locked() as fd:
            p,s=self.read(fd);now=self.check(fd,p,s)
            require(s['plan'] is not None and s['plan']['plan_sha256']==identity,'invalid_request')
            require(item in s['plan']['items'] and item['status']=='candidate','invalid_request')
            if action=='fetch':
                day=datetime.fromtimestamp(now,BJ).date().isoformat()
                require(s['used']+2<=p['max_requests'] and s['days'].get(day,0)+2<=p['daily_requests'],'budget_exhausted')
            return True

    def begin(self,item):
        with self.locked() as fd:
            p,s=self.read(fd);now=self.check(fd,p,s)
            require(s['active'] is None,'busy')
            require(s['plan'] is not None and item in s['plan']['items'] and item['status']=='candidate','invalid_request')
            day=datetime.fromtimestamp(now,BJ).date().isoformat()
            require(s['used']+2<=p['max_requests'] and s['days'].get(day,0)+2<=p['daily_requests'],'budget_exhausted')
            identity=uuid4().hex
            s.update(active=dict(id=identity,code=item['code'],start=item['start'],end=item['end'],sent=[]),last_time=now)
            self.write(fd,s)
        return BatchJob(self,identity,item)


class NoDailyData(TushareEgressStopped):
    """Successful, accounted daily response with zero rows; not an API failure."""
    def __init__(self):
        super().__init__('response_invalid')


class BatchJob:
    def __init__(self,gate,identity,item):
        self.gate,self.identity,self.item=gate,identity,deepcopy(item)
        self.approved=dict(codes=[item['code']],start=item['start'].replace('-',''),end=item['end'].replace('-',''))
        self.daily_dates=None
        self.empty_daily=None

    @contextmanager
    def context(self):
        token=CURRENT.set(self)
        try:yield self
        finally:CURRENT.reset(token)

    def assert_ready(self):
        with self.gate.locked() as fd:
            p,s=self.gate.read(fd);self.gate.check(fd,p,s)
            require(s['active'] is not None and s['active']['id']==self.identity,'previous_attempt_incomplete')
            if self.empty_daily is not None:
                raise NoDailyData()

    def fail(self,reason='interrupted'):
        self.gate.stop()  # keep old/manual STOPs untouched

    def finish(self):
        with self.gate.locked() as fd:
            p,s=self.gate.read(fd);self.gate.check(fd,p,s)
            require(s['active'] is not None and s['active']['id']==self.identity
                    and (s['active']['sent']==['daily','adj_factor'] or
                         (self.empty_daily is not None and s['active']['sent']==['daily'])),'previous_attempt_incomplete')
            s['active']=None;self.gate.write(fd,s)

    def perform(self,api,fields,params,operation):
        require(api in ('daily','adj_factor') and fields==''
                and not set(params)-{'ts_code','start_date','end_date','offset','limit','ts_type_name'}
                and params.get('ts_code')==self.item['code']
                and params.get('start_date')==self.approved['start'] and params.get('end_date')==self.approved['end']
                and params.get('offset') is None and params.get('limit') is None
                and params.get('ts_type_name','http://api.waditu.com/dataapi')=='http://api.waditu.com/dataapi','invalid_request')
        while True:
            with self.gate.locked() as fd:
                p,s=self.gate.read(fd);now=self.gate.check(fd,p,s)
                a=s['active'];require(a is not None and a['id']==self.identity,'previous_attempt_incomplete')
                require(a['sent']==([] if api=='daily' else ['daily']),'invalid_request')
                day=datetime.fromtimestamp(now,BJ).date().isoformat()
                require(s['used']<p['max_requests'] and s['days'].get(day,0)<p['daily_requests'],'budget_exhausted')
                recent=[t for t in s['recent'] if now-t<60]
                if len(recent)<p['minute_requests']:
                    s['used']+=1;s['days'][day]=s['days'].get(day,0)+1;s['recent']=recent+[now]
                    a['sent'].append(api);s.update(last_time=now,phase='in_flight');self.gate.write(fd,s)
                    try:
                        require(not self.gate.stopped(fd),'disabled')
                        result=operation();self.validate_table(api,result)
                        require(not self.gate.stopped(fd),'disabled')
                    except BaseException as error:
                        s.update(phase='blocked',reason=error.reason if isinstance(error,TushareEgressStopped) else 'interrupted',
                                 api_code=getattr(error,'api_code',None));self.gate.write(fd,s)
                        raise
                    s.update(phase='ready',reason='none');self.gate.write(fd,s)
                    return result
            time.sleep(0.25)

    def validate_table(self,api,result):
        try:
            fields,rows=result['fields'],result['items']
            required={'ts_code','trade_date'}|({'open','high','low','close','vol'} if api=='daily' else {'adj_factor'})
            require(len(fields)==len(set(fields)) and required<=set(fields) and 0<=len(rows)<=366,'response_invalid')
            if not rows:
                require(api=='daily','response_invalid')
                self.empty_daily=deepcopy(result)
                self.daily_dates=set()
                return
            dates=set()
            for values in rows:
                require(len(values)==len(fields),'response_invalid');r=dict(zip(fields,values))
                day=r['trade_date'];require(datetime.strptime(day,'%Y%m%d').strftime('%Y%m%d')==day,'response_invalid')
                require(r['ts_code']==self.item['code'] and self.approved['start']<=day<=self.approved['end'] and day not in dates,'response_invalid')
                dates.add(day)
                for key in required-{'ts_code','trade_date'}:
                    require(type(r[key]) in (int,float) and math.isfinite(r[key]) and (r[key]>=0 if key=='vol' else r[key]>0),'response_invalid')
            if api=='daily':self.daily_dates=dates
            else:require(dates==self.daily_dates,'response_invalid')
        except (KeyError,TypeError,ValueError):raise TushareEgressStopped('response_invalid') from None


def provision(directory,policy):
    """Explicit deployment step only. Existing paths/permits cannot be reused."""
    policy_check(policy);now=time.time();require(now<policy['expires_at']<=now+7*86400,'expired')
    gate=BatchGate(directory);gate.directory.mkdir(mode=0o700)
    for name,value in [('policy.json',policy),('gate.lock',None),('admission.lock',None)]:
        fd=os.open(gate.directory/name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        with os.fdopen(fd,'wb') as f:
            if value is not None:f.write(json.dumps(value,sort_keys=True,allow_nan=False).encode())
            f.flush();os.fsync(f.fileno())
    with gate.locked() as fd:
        gate.write(fd,dict(policy_sha256=sha(policy),phase='ready',reason='none',api_code=None,
            used=0,days={},recent=[],last_time=now,plan=None,active=None))
    return gate


def _selection(registry):
    """Missing registry is legacy mode; malformed existing registry never falls back."""
    registry = Path(registry)
    try: info = registry.lstat()
    except FileNotFoundError: return None
    require(stat.S_ISDIR(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o700
            and info.st_uid == os.geteuid() and registry.resolve() == registry, 'state_invalid')
    try:
        fd = os.open(registry/'active.json', os.O_RDONLY|os.O_NOFOLLOW)
    except FileNotFoundError:
        raise GenerationStopped('generation_incomplete') from None
    with os.fdopen(fd, 'rb') as f:
        _private_fd(f.fileno()); raw = f.read(8193)
    require(len(raw) <= 8192)
    try:
        value = json.loads(raw, object_pairs_hook=_pairs)
        require(set(value) == {'schema','generation','policy_sha256','retirement_sha256'})
        require(type(value['schema']) is int and value['schema'] == 1)
        require(all(isinstance(value[k], str) and re.fullmatch('[a-f0-9]{32}' if k == 'generation' else '[a-f0-9]{64}', value[k])
                    for k in ('generation','policy_sha256','retirement_sha256')))
    except (ValueError, TypeError, KeyError):
        raise TushareEgressStopped('state_invalid') from None
    return value


def selected_gate(registry=REGISTRY, legacy=DIRECTORY):
    """Read-only selection. HTTP never provisions/renews a generation."""
    registry, legacy = Path(registry), Path(legacy)
    selection = _selection(registry)
    gate = BatchGate(registry/selection['generation'] if selection else legacy)
    gate.admission_path = legacy/'admission.lock'  # shared by ALL generations
    gate.selection = (registry, selection)
    if selection:
        with gate.locked() as fd:
            policy, _ = gate.read(fd)
        require(sha(policy) == selection['policy_sha256'], 'state_invalid')
        receipt = gate.directory/'retirement.json'
        try: fd = os.open(receipt, os.O_RDONLY|os.O_NOFOLLOW)
        except OSError: raise TushareEgressStopped('state_io_failed') from None
        with os.fdopen(fd, 'rb') as f:
            _private_fd(f.fileno()); raw = f.read(16*1024*1024+1)
        require(len(raw) <= 16*1024*1024 and hashlib.sha256(raw).hexdigest() == selection['retirement_sha256'])
    return gate


async def activate_generation(policy, collection, *, registry=REGISTRY, legacy=DIRECTORY):
    """Explicit operator action: STOP predecessor, reconcile it, create a NEW budget.

    Never exposed by the trigger endpoint. No provider/network client is created.
    No old files or task statuses are changed. A fresh policy needs fresh approval.
    """
    from deploy.history_batch_journal import Workspace
    from deploy.history_batch_adapters import HistoricalAdapter
    registry, legacy = Path(registry), Path(legacy)
    policy_check(policy)
    fd = os.open(legacy/'admission.lock', os.O_RDWR|os.O_NOFOLLOW)
    created = None
    try:
        _private_fd(fd)
        try: fcntl.flock(fd, fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError: raise TushareEgressStopped('busy') from None
        previous = selected_gate(registry, legacy)
        previous.assert_selected()
        with previous.locked() as gate_fd:
            require(previous.stopped(gate_fd), 'predecessor_not_stopped')
            _, state = previous.read(gate_fd)
        require(state['active'] is None and state['phase'] == 'ready', 'previous_attempt_incomplete')
        plan = state['plan']
        if plan is not None:
            workspace = Workspace(previous.directory/'runs')
            adapter = HistoricalAdapter(collection, None, None, None, None)
            with workspace.locked():
                _, _, states = workspace._load(plan['plan_sha256'])
                for item in plan['items']:
                    saved = states[item['code']]
                    if saved['phase']=='skipped' and item['status']=='blocked':
                        continue
                    require(saved['phase'] in ('committed','writing','fetched') and saved['payload'] is not None,
                            'previous_attempt_incomplete')
                    require(await adapter.matches(item, saved['payload']), 'write_outcome_unresolved')
        old_hashes = {str(p.relative_to(previous.directory)):hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in previous.directory.rglob('*') if p.is_file()}
        receipt = dict(previous_directory=str(previous.directory), previous_selection=previous.selection[1],
                       plan_sha256=plan['plan_sha256'] if plan else None,
                       all_saved_data_matched=True, previous_hashes=old_hashes)
        # Registry is created only by this explicit action. Interrupted creation
        # leaves a closed/incomplete registry; never fall back to an old permit.
        registry.mkdir(mode=0o700, exist_ok=True)
        identity = uuid4().hex
        created = BatchGate(registry/identity)
        gate = provision(created.directory, policy)
        Workspace(gate.directory/'runs').provision()
        raw = json.dumps(receipt, sort_keys=True, allow_nan=False).encode()
        out = os.open(gate.directory/'retirement.json', os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600)
        with os.fdopen(out,'wb') as f: f.write(raw); f.flush(); os.fsync(f.fileno())
        directory_fd = os.open(gate.directory, os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(directory_fd)
        finally: os.close(directory_fd)
        require(old_hashes == {str(p.relative_to(previous.directory)):hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in previous.directory.rglob('*') if p.is_file()}, 'predecessor_changed')
        value = dict(schema=1, generation=identity, policy_sha256=sha(policy), retirement_sha256=hashlib.sha256(raw).hexdigest())
        out = os.open(registry/'active.next', os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600)
        with os.fdopen(out,'w') as f: json.dump(value,f,sort_keys=True); f.flush(); os.fsync(f.fileno())
        os.replace(registry/'active.next', registry/'active.json')
        parent = os.open(registry, os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(parent)
        finally: os.close(parent)
        return selected_gate(registry, legacy)
    except BaseException:
        if created is not None and created.directory.is_dir(): created.stop()
        raise
    finally:
        os.close(fd)
