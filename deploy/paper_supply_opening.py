"""Backend-only finite quote activation and read-only API acceptance.

Run only after the separately approved delivery. No order or quote-data writes.
Failure removes only the exact permit created by this attempt, preserving any
unexpected replacement for investigation. Credentials stay in memory.
"""
from datetime import timedelta
import hashlib
import io
import json
import math
import os
from pathlib import Path
import signal
import sys
import types
import uuid
from contextlib import redirect_stdout

PERMIT=Path('/app/runtime/paper-quote-supply.json')
ABSENT_CHECK="assert not Path('/app/runtime/paper-quote-supply.json').exists(),'paper_permit_already_present'"
HISTORY_IDLE_CHECK="assert generation.parent==root and state['active'] is None and state['phase']=='ready' and not (generation/'STOP').exists(),'history_not_idle'"
HISTORY_ISOLATION_GUARD = r'''
assert generation.parent==root,'history_generation_invalid'
history_stop=generation/'STOP'
assert not history_stop.is_symlink(),'history_stop_symlink'
history_ready=(state['phase']=='ready' and state['active'] is None and not history_stop.exists())
history_active=state['active']
history_blocked=(state['phase']=='blocked' and history_stop.is_file()
    and state.get('reason') in ('network_failed','tls_failed','timeout')
    and isinstance(history_active,dict) and history_active.get('sent')==['daily'])
assert history_ready or history_blocked,'history_not_quiescent'
history_isolation='stopped_transport_failure' if history_blocked else 'ready'
'''


def isolated_history_snapshot(code):
    """Freeze an opening-only guard; delivery's idle-only guard stays intact.

    A stopped history attempt is not a running worker. The existing DB check
    still rejects running/cancelling executions, and both snapshots include
    the exact STOP bytes as well as the policy, counters and unresolved attempt.
    This function never reopens history or changes its recovery policy.
    """
    paths_anchor="runtime_files={str(p):base64.b64encode(p.read_bytes()).decode() for p in paths}"
    report_anchor="generation=str(generation),used=state['used'],preferences=prefs"
    require(all(code.count(anchor)==1 for anchor in
                (HISTORY_IDLE_CHECK,paths_anchor,report_anchor)), 'snapshot_history_guard_changed')
    code=code.replace(HISTORY_IDLE_CHECK,HISTORY_ISOLATION_GUARD)
    code=code.replace(paths_anchor,
        "if history_blocked:paths.append(history_stop)\n"+paths_anchor)
    return code.replace(report_anchor,
        "history_isolation=history_isolation,"+report_anchor)


def require(value,reason):
    if not value:raise RuntimeError(reason)


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':')).encode()


def install(path,raw):
    require(not os.path.lexists(path),'permit_already_present')
    temporary=path.with_name('.paper-quote-'+hashlib.sha256(raw).hexdigest()+'.tmp')
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    try:
        with os.fdopen(fd,'wb') as stream:
            stream.write(raw);stream.flush();os.fsync(stream.fileno())
        os.link(temporary,path,follow_symlinks=False)  # Does not replace any file.
    finally:
        temporary.unlink()
    fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)


def revoke(path,raw):
    require(not path.is_symlink() and path.read_bytes()==raw,'permit_changed_do_not_remove')
    path.unlink()
    fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)


def snapshot(payload,permit=None):
    request=dict(runtime=payload['request']['runtime'],entry_hash=payload['entry_hash'],
                 modules=payload['modules'],backup=False)
    code=payload['snapshot_code'];require(code.count(ABSENT_CHECK)==1,'snapshot_guard_changed')
    if permit is not None:
        code=code.replace(ABSENT_CHECK,"assert Path('/app/runtime/paper-quote-supply.json').read_bytes()=="+
                          repr(canonical(permit))+",'paper_permit_changed'")
    original=sys.stdin;out=io.StringIO()
    try:
        sys.stdin=io.StringIO(json.dumps(request))
        with redirect_stdout(out):exec(compile(code,'<opening-snapshot>','exec'),{'__name__':'opening_snapshot'})
    finally:sys.stdin=original
    rows=[s.split(':',1)[1] for s in out.getvalue().splitlines() if s.startswith('PAPER_SNAPSHOT:')]
    require(len(rows)==1,'snapshot_invalid')
    return json.loads(rows[0])


def validate_quote(response,symbol,now):
    from app.services.quote_contract import parse_quote_time
    require(response.get('success') is True,'quote_api_failed')
    data=response['data'];require(data.get('code')==symbol,'quote_symbol_changed')
    prices=[data.get(key) for key in ('low','current_price','high')]
    require(all(type(p) in (int,float) and math.isfinite(p) and p>0 for p in prices)
            and prices[0]<=prices[1]<=prices[2],'quote_prices_invalid')
    basis=data.get('pricing_basis') or {};stamp=parse_quote_time(basis.get('as_of'))
    require(basis.get('source')=='sina_l1_on_demand' and basis.get('channel')=='upstream'
            and basis.get('trading_status')=='normal' and basis.get('trading_status_raw')=='00'
            and basis.get('trading_status_source')=='sina_l1_field_32','quote_basis_invalid')
    require(stamp is not None and stamp.date()==now.date()
            and -60<=(now-stamp).total_seconds()<=90,'quote_time_invalid')
    require(type(basis.get('max_delay_seconds')) is int and 0<basis['max_delay_seconds']<=90
            and isinstance(basis.get('payload_sha256'),str) and len(basis['payload_sha256'])==64,'quote_proof_invalid')
    return dict(symbol=symbol,quote_time=basis['as_of'],source=basis['source'],
                delay_seconds=(now-stamp).total_seconds(),payload_sha256=basis['payload_sha256'])


def activate(payload,path=PERMIT):
    from app.services.paper_quote_supply import cn_now,read_permit,in_continuous_session
    created=False;raw=None;success=False
    result=dict(ok=False,status='not_started',orders_created=0)
    try:
        require(os.geteuid()==1000,'unexpected_backend_uid')
        policy=payload['policy']
        require(policy==dict(version=1,duration_seconds=86400,source='sina_l1',scope='paper_execution_sh_sz'),
                'activation_policy_changed')
        now=cn_now();require(in_continuous_session(now),'outside_continuous_session')
        require(now.minute+now.hour*60 < (11*60+25 if now.hour<12 else 14*60+55),'too_close_to_session_end')
        require(read_permit(now,path) is None and not os.path.lexists(path),'permit_already_present')
        before=snapshot(payload)
        helper=types.ModuleType('query');exec(payload['query_code'],helper.__dict__)
        auth=payload['auth']
        if 'token' in auth:token=auth['token']
        else:
            login=helper.request('/api/auth/login',body=auth)
            require(login.get('success') is True,'login_failed');token=login['data']['access_token']
        if payload.get('check_only') is True:
            return dict(ok=True,status='ready_no_activation',permit_absent=True,orders_created=0)
        start=cn_now();permit=dict(version=1,enabled=True,source='sina_l1',scope='paper_execution_sh_sz',
                                  not_before=start.isoformat(),expires_at=(start+timedelta(seconds=86400)).isoformat(),
                                  activation_id=uuid.uuid4().hex)
        raw=canonical(permit);install(path,raw);created=True
        require(read_permit(cn_now(),path)==permit,'installed_permit_invalid')
        quotes=[]
        for symbol,market in [('600000','CN'),('159865','ETF')]:
            response=helper.request('/api/paper/quote/'+symbol+'?market='+market,token=token)
            quotes.append(validate_quote(response,symbol,cn_now()))
        suspended=helper.request('/api/paper/quote/601059?market=CN',token=token)
        require(suspended.get('success') is True and
                suspended['data'].get('code')=='601059' and
                all(key in suspended['data'] and suspended['data'][key] is None
                    for key in ('current_price','low','high','pricing_basis')),
                'suspended_api_not_closed')
        after=snapshot(payload,permit);require(before==after,'protected_state_changed')
        require(read_permit(cn_now(),path)==permit and in_continuous_session(cn_now()),'permit_or_session_changed')
        success=True;result.update(ok=True,status='activated',quotes=quotes,suspended_api_closed=True,
            permit_sha256=hashlib.sha256(raw).hexdigest(),not_before=permit['not_before'],expires_at=permit['expires_at'],
            protected_state_unchanged=True)
    except BaseException as error:
        result.update(status='failed_before_opening',error_type=type(error).__name__,
                      reason=str(error) if type(error) is RuntimeError else 'details_withheld')
    finally:
        if raw is not None and not success:
            # install() can link successfully and then fail while syncing the
            # directory; inspect our unique bytes even if it never returned.
            owned=False
            try:owned=not path.is_symlink() and path.read_bytes()==raw
            except OSError:pass
            if created or owned:
                try:
                    if os.path.lexists(path):revoke(path,raw)
                    result.update(status='failed_closed',permit_absent=True)
                except BaseException as error:result.update(status='revoke_unproven',revoke_error_type=type(error).__name__)
    return result


if __name__=='__main__':
    os.umask(0o077)
    def expired(*_):raise TimeoutError('activation_deadline')
    signal.signal(signal.SIGALRM,expired);signal.alarm(100)
    result=activate(json.load(sys.stdin))
    signal.alarm(0)
    print('PAPER_OPENING_RESULT:'+json.dumps(result))
    raise SystemExit(0 if result['ok'] else 1)
