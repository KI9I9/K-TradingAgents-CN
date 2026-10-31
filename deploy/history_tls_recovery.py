"""Operator-only reconciliation after repaired TLS transport; no app/HTTP reset.

Keeps certificate/hostname verification and every charged request. This helper is
shipped inside a frozen one-use operator package, not installed in the backend.
"""
from copy import deepcopy
from datetime import datetime
import fcntl
import os
import time

from tradingagents.utils.tushare_batch import require, BJ
from tradingagents.utils.tushare_egress import TushareEgressStopped, _private_fd
from deploy.history_batch_journal import Workspace, _atomic, _sync_dir
from deploy.history_batch_adapters import HistoricalAdapter


def verify_fixed_tls():
    """Handshake only: no API body, token or quota. Never relax TLS validation."""
    from tradingagents.utils.tushare_egress import UnixTLSConnection
    observations=[]
    for _ in range(3):
        # Match MainlandTransport's existing request deadline. On this route a
        # verified handshake can take >10s; do not weaken certificate checks or
        # increase the application's 30s total request budget.
        conn=UnixTLSConnection(30)
        start=time.monotonic()
        try:
            conn.connect()
            observations.append(dict(version=conn.sock.version(),
                                     elapsed=round(time.monotonic()-start,3)))
        finally:
            conn.close()
    return observations


async def recover_raw_tls(gate, collection, *, expected_plan, expected_used):
    return await _recover_raw_transport(gate,collection,expected_plan=expected_plan,
                                       expected_used=expected_used,expected_reason='tls_failed')


async def recover_raw_timeout_verified(gate, collection, *, expected_plan, expected_used):
    return await _recover_raw_transport(gate,collection,expected_plan=expected_plan,
                                       expected_used=expected_used,expected_reason='timeout')


async def _recover_raw_transport(gate, collection, *, expected_plan, expected_used, expected_reason):
    """Reconcile exactly one blocked daily attempt, then remove STOP last.

    Full database/journal backups are a caller precondition. As with the timeout
    recovery, pending inventories and every committed payload must still match.
    A successful TLS handshake today does not classify the old SSL exception;
    this is an explicit operator recovery, never an automatic TLS retry.
    """
    require(expected_reason in ('tls_failed','timeout'),'invalid_recovery_reason')
    fd=os.open(gate.admission_path,os.O_RDWR|os.O_NOFOLLOW)
    try:
        _private_fd(fd)
        try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise TushareEgressStopped('busy') from None
        gate.assert_selected()
        workspace=Workspace(gate.directory/'runs')
        with workspace.locked(),gate.locked() as gate_fd:
            p,s=gate.read(gate_fd)
            require(gate.stopped(gate_fd) and p['schema']==2
                    and s['phase']=='blocked' and s['reason']==expected_reason
                    and s['api_code'] is None and s['used']==expected_used
                    and s['plan'] is not None and s['plan']['plan_sha256']==expected_plan
                    and s['active'] is not None and s['active']['sent']==['daily'],
                    'previous_attempt_incomplete')
            def check_budget():
                now=time.time()
                require(s['last_time']<=now<p['expires_at'],'expired')
                day=datetime.fromtimestamp(now,BJ).date().isoformat()
                require(s['used']<p['max_requests'] and s['days'].get(day,0)<p['daily_requests'],
                        'budget_exhausted')
                return now
            check_budget()
            workspace._not_stopped()
            folder,plan,states=workspace._load(expected_plan)
            require(plan==s['plan'],'state_invalid')
            active=s['active']
            require([c for c,v in states.items() if v['phase']=='fetching']==[active['code']],
                    'previous_attempt_incomplete')
            adapter=HistoricalAdapter(collection,None,None,None,None,price_adjustment='none')
            for item in plan['items']:
                saved=states[item['code']]
                if saved['phase']=='skipped':continue
                if saved['phase']=='committed':
                    require(await adapter.matches(item,saved['payload']),'write_outcome_unresolved')
                else:
                    require(saved['phase'] in ('pending','fetching'),'write_outcome_unresolved')
                    if saved['phase']=='fetching':
                        require(all(active[k]==item[k] for k in ('code','start','end')),'state_invalid')
                    require(await adapter.preflight(item)==item['observation_sha256'],
                            'write_outcome_unresolved')
            # This isolated operator process holds all locks through the probes.
            try:
                probes=verify_fixed_tls()
            except OSError as error:
                # Gate.locked normalizes OSError (including SSL errors) to
                # state_io_failed. Preserve the probe stage and safe class name.
                raise RuntimeError('tls_probe_failed:'+type(error).__name__) from None
            now=check_budget()
            gate.assert_selected()
            prefix='tls-recovery-' if expected_reason=='tls_failed' else 'timeout-recovery-'
            receipt=gate.directory/(prefix+active['id'])
            receipt.mkdir(mode=0o700)
            _sync_dir(gate.directory)
            _atomic(receipt/'before.json',dict(state=s,item=states[active['code']],stopped=True,
                    verified_all_items=True,verified_at=now,tls_probes=probes))
            saved=deepcopy(states[active['code']]);saved.update(phase='pending',fetch_attempts=0)
            _atomic(folder/(active['code']+'.json'),saved)
            s.update(active=None,phase='ready',reason='none',api_code=None)
            gate.write(gate_fd,s)
            _atomic(receipt/'ready.json',dict(plan_sha256=expected_plan,used=s['used'],
                    days=s['days'],resume_code=active['code'],previous_reason=expected_reason))
            gate.assert_selected()
            os.unlink('STOP',dir_fd=gate_fd)
            os.fsync(gate_fd)
            return dict(plan_sha256=expected_plan,used=s['used'],resume_code=active['code'],
                        receipt=str(receipt),previous_reason=expected_reason,tls_probes=probes)
    finally:
        os.close(fd)
