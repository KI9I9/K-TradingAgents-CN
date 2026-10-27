#!/usr/bin/env python3
"""纯内存护栏冒烟；installed 模式先重建封印启动配置，不触碰数据库/网络。"""
import argparse
from datetime import datetime, timedelta
import importlib.util
import json
import os
from pathlib import Path
import sys


def require(condition, reason):
    if not condition:
        raise AssertionError(reason)


def smoke(contract):
    now = datetime(2026, 10, 27, 14, 0, tzinfo=contract.CN_TZ)
    require(contract.quotes_write_enabled() is False, "写闸必须关闭")
    doc, unset, reason = contract.prepare_quote_write(
        {"close": 10, "high": 11, "low": 9}, "600000", "2026-10-27", now)
    require(doc is None and reason == "quotes_write_disabled", reason)
    quote = {"trade_date": "2026-10-27", "quote_time_source": "fetch",
             "fetched_at": (now - timedelta(minutes=5)).isoformat(),
             "close": 10, "high": 11, "low": 9}
    require(contract.quote_is_usable(quote, now) == (False, "trading_status_missing"), "状态未知必须拒绝")
    bar = {"trade_date": "2026-10-27", "close": 10, "high": 11, "low": 9,
           "updated_at": "2026-10-27T15:10:00+08:00"}
    require(contract.daily_bar_is_final(bar, now.replace(hour=16)) == (False, "bar_finality_unproven"),
            "日线最终性未知必须拒绝")
    price, basis, reason = contract.resolve_cn_price({"market_quotes": quote}, now, strict=False)
    require(price == 10 and basis["channel"] == "display", (price, basis, reason))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--installed", action="store_true")
    parser.add_argument("--runtime-entrypoint")
    parser.add_argument("--expected-entrypoint-sha256")
    parser.add_argument("--secrets-dir", default="/run/secrets")
    parser.add_argument("--app-root", default="/app")
    args = parser.parse_args()
    if args.installed:
        if not args.runtime_entrypoint or not args.expected_entrypoint_sha256:
            parser.error("--installed 必须提供已核验入口路径与完整 SHA256，不能用 exec 环境代替启动配置")
        from quote_guard_runtime import reconstruct
        try:
            effective, report = reconstruct(args.runtime_entrypoint,
                                            args.expected_entrypoint_sha256, args.secrets_dir)
        except Exception:
            print("guard smoke FAILED: 启动配置证据不满足")
            return 1
        os.environ.update(effective)  # 仅改变检查子进程；不改变服务、封印或数据库。
        sys.path.insert(0, args.app_root)
        from app.services import quote_contract as contract
        expected_path = Path(args.app_root) / "app/services/quote_contract.py"
        if Path(contract.__file__).resolve() != expected_path.resolve():
            print("guard smoke FAILED: 导入路径不符")
            return 1
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    else:
        path = Path(__file__).resolve().parents[1] / "app/services/quote_contract.py"
        spec = importlib.util.spec_from_file_location("quote_guard_contract", path)
        contract = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(contract)
    smoke(contract)
    print(f"guard smoke OK (isolated process): {contract.__file__}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
