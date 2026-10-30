"""140-point path: original service/gate/provider/store with synthetic SDK only."""
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

from test_history_button import ButtonTests, button, gate_module, CURRENT
from test_history_batch_adapters import ReadCollection, raw_json
from test_tushare_history import frame, history
from deploy.history_batch_journal import Workspace
from app.services.historical_contract import HistoricalContractError


class BasicTierTests(ButtonTests):
    # Reuse setup helpers, not the qfq-specific inherited test cases.
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.old = raw_json(self.col.rows)
        self.legacy_col = self.col
        policy, _ = self.generation_args()
        policy.update(schema=2, max_requests=7500, daily_requests=7500, minute_requests=40)
        self.gate = gate_module.provision(Path(self.tmp.name)/'raw', policy)
        self.workspace = Workspace(self.gate.directory/'runs'); self.workspace.provision()
        self.col = ReadCollection(); self.db.stock_daily_quotes_unadjusted = self.col
        self.history = history.HistoricalDataService(unadjusted_stock=True)
        self.service = button.HistoryBatchService(self.gate,self.workspace,self.col,self.executions,
            self.history,self.factory,clock=lambda:self.clock)
        self.submitpatch.stop()
        self.submitpatch = patch.object(button,'submit_history_batch',side_effect=self.service.submit)
        self.submitpatch.start()
        self.raw = frame(('20261018','20261019')).rename(columns={'volume':'vol'})
        def daily(**kwargs):
            job = CURRENT.get()
            def operation():
                self.sent.append('daily')
                if self.send_failure: raise self.send_failure
                return dict(fields=list(self.raw.columns),items=self.raw.values.tolist())
            result=job.perform('daily','',kwargs,operation)
            import pandas as pd
            return pd.DataFrame(result['items'], columns=result['fields'])
        self.provider.api.daily.side_effect=daily
        self.sdk.pro_bar.side_effect=AssertionError('Basic account must never request pro_bar')

    async def test_basic_end_to_end_and_replay(self):
        self.assertTrue(await self.service.submit())
        await asyncio.gather(*list(button._TASKS))
        self.assertEqual(self.sent,['daily'])
        self.assertEqual(self.state()['used'],1)
        self.assertEqual(self.state()['plan']['candidate_requests'],1)
        self.assertEqual(self.executions.rows['1']['status'],'success')
        self.assertEqual(self.executions.rows['1']['price_adjustment'],'none')
        self.assertEqual(len(self.col.rows),2)
        self.assertEqual(self.col.rows[0]['source_endpoint'],'daily')
        self.assertEqual(self.col.rows[0]['price_adjustment'],'none')
        self.assertEqual(self.col.rows[0]['volume'],123.45)
        self.assertEqual(self.col.rows[0]['amount'],678900.)
        self.assertEqual(raw_json(self.legacy_col.rows), self.old)
        before=raw_json(self.col.rows)
        await self.service.submit(); await asyncio.gather(*list(button._TASKS))
        self.assertEqual(raw_json(self.col.rows),before)
        self.assertEqual(self.sent,['daily'])
        self.sdk.pro_bar.assert_not_called()

    async def test_cross_basis_writes_rejected(self):
        data=frame(); data.attrs['historical_contract'].update(endpoint='daily',price_adjustment='none')
        with self.assertRaisesRegex(HistoricalContractError,'storage_basis'):
            await history.HistoricalDataService().save_historical_data('600000',data,'tushare')
        with self.assertRaisesRegex(HistoricalContractError,'storage_basis'):
            await self.history.save_historical_data('600000',frame(),'tushare')
        self.assertFalse(self.col.rows)
        self.assertEqual(raw_json(self.legacy_col.rows), self.old)

    async def test_gate_rejects_adj_factor_before_request(self):
        policy,state=self.gate.status()
        plan,_=await self.service._prepare(policy,state,True)
        job=self.gate.begin(plan['items'][0])
        operation=Mock()
        with self.assertRaisesRegex(RuntimeError,'invalid_request'):
            job.perform('adj_factor','',dict(ts_code='600000.SH',start_date=job.approved['start'],end_date=job.approved['end']),operation)
        operation.assert_not_called(); self.assertEqual(self.state()['used'],0)

    async def test_raw_read_does_not_fallback_to_qfq(self):
        from test_history_batch_adapters import ReadCursor
        class SortedCursor(ReadCursor):
            def sort(self, key, direction):
                self.rows.sort(key=lambda r:r[key],reverse=direction<0);return self
            def limit(self, n):self.rows=self.rows[:n];return self
        def find(query):
            return SortedCursor([deepcopy(r) for r in self.col.rows if all(r.get(k)==v for k,v in query.items())])
        self.col.find=find
        reader=history.HistoricalDataService()
        self.assertEqual(await reader.get_historical_data('600000',data_source='tushare',period='daily',price_adjustment='none'),[])
        await self.service.submit();await asyncio.gather(*list(button._TASKS))
        rows=await reader.get_historical_data('600000',data_source='tushare',period='daily',price_adjustment='none',limit=1)
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['trade_date'],'2026-10-19')
        self.assertEqual(rows[0]['price_adjustment'],'none')
        self.assertEqual(raw_json(self.legacy_col.rows),self.old)

    async def test_basic_policy_caps(self):
        policy,_=self.gate.status()
        for k,v in [('minute_requests',41),('daily_requests',7501)]:
            bad=dict(policy);bad[k]=v
            with self.assertRaises(RuntimeError):gate_module.policy_check(bad)

    async def test_retire_rejection_preserves_records_evidence_and_debits(self):
        from tradingagents.utils.tushare_egress import TushareEgressStopped
        self.send_failure=TushareEgressStopped('usage_limit',40203)
        await self.service.submit();await asyncio.gather(*list(button._TASKS))
        policy=json.loads((self.gate.directory/'policy.json').read_text())
        before={str(p):p.read_bytes() for p in self.gate.directory.rglob('*') if p.is_file()}
        with self.assertRaises(RuntimeError):
            await gate_module.activate_generation(policy,self.col,registry=Path(self.tmp.name)/'registry',legacy=self.gate.directory)
        new=await gate_module.activate_generation(policy,self.col,registry=Path(self.tmp.name)/'registry',legacy=self.gate.directory,retire_rejected=True)
        self.assertEqual(new.status()[1]['used'],1)
        self.assertEqual(new.status()[1]['days'],self.state()['days'])
        self.assertEqual(before,{str(p):p.read_bytes() for p in self.gate.directory.rglob('*') if p.is_file()})
        self.assertFalse(self.col.rows)
        self.assertEqual(raw_json(self.legacy_col.rows), self.old)

    async def test_unknown_request_cannot_retire(self):
        from tradingagents.utils.tushare_egress import TushareEgressStopped
        self.send_failure=TushareEgressStopped('network_error')
        await self.service.submit();await asyncio.gather(*list(button._TASKS))
        policy=json.loads((self.gate.directory/'policy.json').read_text())
        with self.assertRaises(RuntimeError):
            await gate_module.activate_generation(policy,self.col,registry=Path(self.tmp.name)/'registry',legacy=self.gate.directory,retire_rejected=True)

# Do not inherit qfq test methods; retain only fixture/helper methods above.
for name in list(vars(ButtonTests)):
    if name.startswith('test_') and name not in BasicTierTests.__dict__:
        setattr(BasicTierTests,name,None)
del ButtonTests

class QfqRetirementTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejected_adj_factor_can_retire_without_resetting_old_state(self):
        from test_history_button import ButtonTests as Fixture
        from tradingagents.utils.tushare_egress import TushareEgressStopped
        fixture=Fixture('test_new_generation_uses_original_button_and_preserves_legacy_stop')
        await fixture.asyncSetUp()
        try:
            def reject_factor(**kwargs):
                job=CURRENT.get()
                params=dict(ts_code='600000.SH',start_date=kwargs['start_date'],end_date=kwargs['end_date'])
                job.perform('daily','',params,lambda:dict(fields=['ts_code','trade_date','open','high','low','close','vol'],
                    items=[['600000.SH','20261018',10.,12.,9.,11.,123.45]]))
                def rejected():raise TushareEgressStopped('usage_limit',40203)
                job.perform('adj_factor','',params,rejected)
            fixture.sdk.pro_bar.side_effect=reject_factor
            await fixture.service.submit();await asyncio.gather(*list(button._TASKS))
            self.assertEqual(fixture.state()['used'],2)
            policy,paths=fixture.generation_args();policy.update(schema=2,minute_requests=40,daily_requests=7500,max_requests=30000)
            before={str(p):p.read_bytes() for p in fixture.gate.directory.rglob('*') if p.is_file()}
            original=raw_json(fixture.col.rows)
            # A changed baseline is not eligible for retirement.
            fixture.col.rows[0]['close']=10.9
            with self.assertRaisesRegex(RuntimeError,'write_outcome_unresolved'):
                await gate_module.activate_generation(policy,fixture.col,**paths,retire_rejected=True)
            fixture.col.rows[0]['close']=11.
            new=await gate_module.activate_generation(policy,fixture.col,**paths,retire_rejected=True)
            self.assertEqual(new.status()[1]['used'],2)
            self.assertEqual(raw_json(fixture.col.rows),original)
            self.assertEqual(before,{str(p):p.read_bytes() for p in fixture.gate.directory.rglob('*') if p.is_file()})
        finally:
            await fixture.asyncTearDown();fixture.doCleanups()

if __name__=='__main__':unittest.main()
