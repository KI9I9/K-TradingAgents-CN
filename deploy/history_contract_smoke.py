#!/usr/bin/env python3
"""Pure memory smoke for installed historical contract. No provider/DB imports."""
import argparse
from datetime import datetime, timezone
import importlib.util
from pathlib import Path

import pandas as pd


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--app-root',type=Path,default=Path(__file__).resolve().parents[1])
    args=parser.parse_args()
    source=args.app_root/'app/services/historical_contract.py'
    spec=importlib.util.spec_from_file_location('installed_history_contract',source)
    contract=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(contract)
    frame=pd.DataFrame([dict(trade_date='20261026',open=10.,high=12.,low=9.,close=11.,vol=123.,amount=456.)])
    frame.attrs['historical_contract']=dict(source='tushare',symbol='600000',period='daily',
        start_date='20261025',end_date='20261026',volume_unit='lot',amount_unit='thousand_CNY',
        endpoint='pro_bar',price_adjustment='qfq')
    original=frame.copy(deep=True)
    docs=contract.tushare_documents('600000',frame,'CN','daily',datetime(2026,10,28,tzinfo=timezone.utc))
    assert docs[0]['volume']==123 and docs[0]['amount']==456000
    assert 'is_final' not in docs[0] and 'trading_status' not in docs[0]
    pd.testing.assert_frame_equal(frame,original)
    try:
        contract.validate_existing_contract([dict(trade_date='2026-10-25')],'qfq')
    except contract.HistoricalContractError:
        pass
    else:
        raise AssertionError('unproven legacy data was accepted')
    print('history contract smoke OK (memory only; no upstream/database acceptance)')


if __name__=='__main__':
    main()
