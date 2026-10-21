"""Hard deadlines for blocking SDK calls, including DNS and native extensions.

Cancelling an asyncio thread does not stop its HTTP requests. These narrowly
allowlisted reads run in a child which is killed and reaped before returning a
timeout/cancellation. No credentials or raw SDK errors enter the wire protocol.
"""
import asyncio
import contextlib
import io
import json
import logging
import os
import subprocess
import sys
import threading
import time

IS_WORKER = False
OPERATIONS = {"stock_list", "trade_date", "daily_basic", "akshare_history"}


class DataFetchNetworkError(RuntimeError):
    def __init__(self, message, circuit=None):
        super().__init__(message)
        self.circuit = circuit


def _decode(raw):
    value = json.loads(raw)
    if not value.get("ok"):
        category = value.get("category", "SDKError")
        if category == "network":
            raise DataFetchNetworkError("数据源网络请求失败", value.get("circuit"))
        raise RuntimeError("数据源返回错误，未取得新数据")
    if "frame" in value:
        import pandas as pd
        frame = value["frame"]
        result = None if frame is None else pd.DataFrame(frame["data"], columns=frame["columns"])
        if "circuit" in value:
            return result, value["circuit"]
        return (result, value["source"]) if "source" in value else result
    return value.get("value")


def fetch(operation, arguments=None, *, timeout=45, cancel=None):
    if operation not in OPERATIONS or not 0 < timeout <= 180:
        raise ValueError("Invalid bounded data fetch")
    deadline = time.monotonic() + timeout
    payload = json.dumps({"operation": operation, "arguments": arguments or {}}).encode()
    with subprocess.Popen(
        # Execute the file directly: -m imports tradingagents/__init__ before
        # the worker can silence configuration/log initialization output.
        [sys.executable, "-B", os.path.abspath(__file__)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    ) as process:
        try:
            first = True
            while True:
                if cancel is not None and cancel.is_set():
                    raise TimeoutError("数据源请求已取消")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("数据源请求超时；请求进程已终止")
                try:
                    output, _ = process.communicate(payload if first else None, timeout=min(0.2, remaining))
                    break
                except subprocess.TimeoutExpired:
                    first = False
            if process.returncode or len(output) > 32 * 1024 * 1024:
                raise RuntimeError("数据源请求进程失败，未取得新数据")
            return _decode(output)
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate()


async def fetch_async(operation, arguments=None, *, timeout=45):
    cancel = threading.Event()
    task = asyncio.create_task(asyncio.to_thread(fetch, operation, arguments, timeout=timeout, cancel=cancel))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        cancel.set()
        # Do not report cancellation while the request process is still alive.
        try:
            await asyncio.shield(task)
        except Exception:
            pass
        raise


def _perform(operation, arguments):
    if operation == "akshare_history":
        from tradingagents.dataflows.providers.china.akshare import AKShareProvider, AKShareNetworkError
        provider = AKShareProvider()
        arguments = dict(arguments)
        state = arguments.pop("circuit", {})
        provider._history_failure_count = state.get("failures", 0)
        provider._history_retry_after = state.get("retry_after", 0.0)
        def circuit():
            return {"failures": provider._history_failure_count, "retry_after": provider._history_retry_after}
        try:
            return asyncio.run(provider.get_historical_data(**arguments)), circuit()
        except AKShareNetworkError:
            raise DataFetchNetworkError("数据源网络请求失败", circuit()) from None
    from app.services.data_sources.manager import DataSourceManager
    # Reuse the application's request headers/default timeout in this fresh
    # interpreter too; importing the adapter alone does not initialize them.
    from tradingagents.dataflows.providers.china.akshare import AKShareProvider
    AKShareProvider()
    manager = DataSourceManager()
    sources = arguments.get("preferred_sources")
    if operation == "stock_list":
        return manager.get_stock_list_with_fallback(sources, True)
    if operation == "daily_basic":
        return manager.get_daily_basic_with_fallback(arguments["trade_date"], sources)
    return manager.find_latest_trade_date_with_fallback(sources), None


def _main():
    global IS_WORKER
    IS_WORKER = True
    # File execution starts with utils/ on sys.path, not the project root.
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    # Importing the module by its canonical name must see the same worker flag.
    sys.modules["tradingagents.utils.bounded_data_fetch"] = sys.modules[__name__]
    logging.disable(logging.CRITICAL)
    packet = {"ok": False, "category": "SDKError"}
    try:
        request = json.loads(sys.stdin.buffer.read(8192))
        operation = request["operation"]
        if operation not in OPERATIONS:
            raise ValueError("Invalid operation")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result, source = _perform(operation, request["arguments"])
        packet = {"ok": True}
        if operation != "trade_date":
            packet["frame"] = None if result is None else json.loads(result.to_json(orient="split", date_format="iso", index=False))
            if operation != "akshare_history":
                packet["source"] = source
            else:
                packet["circuit"] = source
        else:
            packet["value"] = result
    except Exception as error:
        from requests.exceptions import ConnectionError, Timeout, HTTPError
        packet["category"] = "network" if isinstance(error, (ConnectionError, Timeout, HTTPError, DataFetchNetworkError, TimeoutError)) else "SDKError"
        if isinstance(error, DataFetchNetworkError):
            packet["circuit"] = error.circuit
    sys.stdout.write(json.dumps(packet))


if __name__ == "__main__":
    _main()
