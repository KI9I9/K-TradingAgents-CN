#!/usr/bin/env python3
"""比对「重建前/重建后」的 compose 合并结果，防止改挂载时动到别的东西。

只用标准库：`docker compose config --format json` 输出的 image 已是镜像 ID，
因此可以直接做等价比较，不需要拉取或猜测 tag。

用法：
    python3 verify_compose_merge.py before.json after.json \
        --allow backend:/app/app/routers/paper.py,/app/app/services/quote_contract.py

退出码 0 表示通过；任何一项不符即非 0，调用方必须停止，不得继续重建。
"""
import argparse
import json
import sys


def norm_entry(value):
    if value is None:
        return []
    return list(value)


def norm_volumes(service):
    """把 compose 的挂载列表规范化为 (type, source, target, read_only) 集合。"""
    out = {}
    for item in service.get("volumes") or []:
        if not isinstance(item, dict):
            raise SystemExit(f"不支持的挂载写法: {item!r}")
        key = (item.get("type"), item.get("source"), item.get("target"), bool(item.get("read_only")))
        out[key] = item
    return out


def describe(key):
    return f"{key[0]}|{key[1]}|{key[2]}|ro={key[3]}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("before")
    ap.add_argument("after")
    ap.add_argument("--allow", action="append", default=[],
                    help="service:target1,target2 —— 允许新增的挂载目标")
    args = ap.parse_args()

    before = json.load(open(args.before))
    after = json.load(open(args.after))

    allowed = {}
    for spec in args.allow:
        svc, targets = spec.split(":", 1)
        allowed.setdefault(svc, set()).update(t.strip() for t in targets.split(",") if t.strip())

    errors = []

    if set(before["services"]) != set(after["services"]):
        errors.append(f"服务集合变化: {sorted(before['services'])} -> {sorted(after['services'])}")

    for name, b in before["services"].items():
        a = after["services"].get(name)
        if a is None:
            continue

        if b.get("image") != a.get("image"):
            errors.append(f"[{name}] 镜像变化: {b.get('image')} -> {a.get('image')}")

        if norm_entry(b.get("entrypoint")) != norm_entry(a.get("entrypoint")):
            errors.append(f"[{name}] entrypoint 变化: {b.get('entrypoint')} -> {a.get('entrypoint')}")

        if norm_entry(b.get("command")) != norm_entry(a.get("command")):
            errors.append(f"[{name}] command 变化: {b.get('command')} -> {a.get('command')}")

        bv, av = norm_volumes(b), norm_volumes(a)
        missing = [describe(k) for k in bv if k not in av]
        if missing:
            errors.append(f"[{name}] 原有挂载缺失: {missing}")

        added = [k for k in av if k not in bv]
        unexpected = [describe(k) for k in added if k[2] not in allowed.get(name, set())]
        if unexpected:
            errors.append(f"[{name}] 出现非预期挂载: {unexpected}")

        expect_added = allowed.get(name, set())
        if expect_added:
            got = {k[2] for k in added}
            if got != expect_added:
                errors.append(f"[{name}] 新增挂载不符预期: 期望 {sorted(expect_added)} 实际 {sorted(got)}")
            for key in added:
                if not key[1] or not key[1].startswith("/"):
                    errors.append(f"[{name}] 新增挂载 source 不是绝对路径: {describe(key)}")

    if errors:
        print("MERGE_CHECK_FAILED")
        for e in errors:
            print(" -", e)
        return 1

    summary = ", ".join(
        f"{n}={len(norm_volumes(after['services'][n]))}" for n in sorted(after["services"])
    )
    print(f"MERGE_CHECK_OK services_mounts[{summary}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
