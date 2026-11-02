"""Single public read via the real adapter, or replay a saved single response.

No database access, permit installation, Tushare request, or paper order.
Use during a continuous session for a positive freshness result; a noon/closed
sample can only establish connectivity/layout, never successful execution.
"""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.services.paper_quote_supply import cn_now, fetch_sina, parse_sina_quote, source_symbol


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", required=True)
    choice = parser.add_mutually_exclusive_group(required=True)
    choice.add_argument("--request-public", action="store_true")
    choice.add_argument("--input", type=Path)
    args = parser.parse_args()
    source_symbol(args.symbol)
    try:
        raw = await asyncio.wait_for(fetch_sina(args.symbol), 5) if args.request_public else args.input.read_bytes()
        now = cn_now()
        try:
            doc = parse_sina_quote(raw, args.symbol, now)
            result = dict(usable=True, trade_date=doc["trade_date"], quote_time=doc["quote_time"],
                          trading_status=doc["trading_status"], reason="ok")
        except ValueError as exc:
            result = dict(usable=False, reason=str(exc))
        result.update(symbol=args.symbol, fetched_at=now.isoformat(), bytes=len(raw),
                      sha256=hashlib.sha256(raw).hexdigest(), database_writes=0)
    except Exception as exc:
        result = dict(usable=False, symbol=args.symbol, error_type=type(exc).__name__, database_writes=0)
    print(json.dumps(result))
    return 0 if result["usable"] else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
