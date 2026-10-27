#!/usr/bin/env python3
"""只读重建入口配置；不启动应用、不连接数据库或网络、不输出配置值。

这证明封印文件与容器继承环境合成后的启动配置，不证明运行中进程未被修改。
必须同时核验实际入口/模块哈希、重建结果和调度状态。
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path


# 与实际配置默认值不同的缺省值会导致误放行；这里只允许显式 false。
REQUIRED_DISABLED = ("QUOTES_INGEST_ENABLED", "AKSHARE_QUOTES_SYNC_ENABLED")
FALSE_VALUES = {"0", "false", "no", "off"}


def check_environment(inherited, injected):
    effective = dict(inherited)
    effective.update(injected)  # 与入口 os.environ.update(env) 顺序相同
    checked = {}
    for key in ("QUOTES_WRITE_ENABLED", *REQUIRED_DISABLED):
        raw = effective.get(key)
        if raw is None:
            if key != "QUOTES_WRITE_ENABLED":
                raise ValueError("task_flag_missing")
        elif not isinstance(raw, str) or raw.strip().lower() not in FALSE_VALUES:
            raise ValueError("required_gate_not_explicitly_disabled")
        checked[key] = {"enabled": False, "source": "sealed" if key in injected else
                        "container" if key in inherited else "contract_default"}
    return effective, checked


def reconstruct(entrypoint, expected_hash, secrets_dir, inherited=None):
    path = Path(entrypoint)
    data = path.read_bytes()
    if len(expected_hash) != 64 or hashlib.sha256(data).hexdigest() != expected_hash:
        raise ValueError("entrypoint_hash_mismatch")
    # 执行已核验的同一份字节，避免哈希后另读一次路径。
    module = importlib.util.module_from_spec(importlib.util.spec_from_loader("verified_entrypoint", loader=None))
    module.__file__ = str(path)
    exec(compile(data, str(path), "exec"), module.__dict__)
    root = Path(secrets_dir)
    runtime = module.private_bytes(root / "runtime-config.json")
    context = module.parse(module.private_bytes(root / "launch-context.json"))
    approval = module.parse(module.private_bytes(root / "activation-approval.json"))
    injected = module.validated_environment(runtime, context, approval)
    effective, checked = check_environment(dict(os.environ) if inherited is None else inherited, injected)
    report = {"scope": "startup_configuration_reconstruction_not_live_process",
              "entrypoint_sha256": expected_hash,
              "runtime_sha256": hashlib.sha256(runtime).hexdigest(), "gates": checked}
    return effective, report


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--entrypoint", required=True)
    parser.add_argument("--expected-entrypoint-sha256", required=True)
    parser.add_argument("--secrets-dir", default="/run/secrets")
    args = parser.parse_args()
    try:
        _, report = reconstruct(args.entrypoint, args.expected_entrypoint_sha256, args.secrets_dir)
    except Exception:
        print("RUNTIME_CHECK_FAILED: 入口、封印或关闭配置证据不满足；不输出配置内容")
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
