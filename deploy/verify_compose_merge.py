#!/usr/bin/env python3
"""比对 compose 合并结果与运行容器，确保「只新增指定挂载」且其它配置未漂移。

只用标准库。`docker compose config --format json` 的 image 是否已是镜像 ID
取决于输入写法，因此本脚本**不假设**，而是额外与运行容器的 Image ID 比对。

用法：
    python3 verify_compose_merge.py before.json after.json \
        --allow backend:/app/app/routers/paper.py=/srv/paper.py \
        --allow backend:/app/app/services/quote_contract.py=/srv/quote_contract.py \
        [--running before-backend.json]

`--allow` 语法：service:target=source，默认还要求 read_only=True。
退出码非 0 即必须停止，不得继续重建。
"""
import argparse
import json
import sys


def norm_list(value):
    return list(value) if value else []


def norm_env(service):
    env = service.get("environment") or {}
    if isinstance(env, list):
        out = {}
        for item in env:
            key, _, val = str(item).partition("=")
            out[key] = val
        return out
    return {str(k): "" if v is None else str(v) for k, v in env.items()}


def mount_map(service):
    out = {}
    for item in service.get("volumes") or []:
        if not isinstance(item, dict):
            raise SystemExit(f"不支持的挂载写法: {item!r}")
        out[(item.get("type"), item.get("source"), item.get("target"),
             bool(item.get("read_only")))] = item
    return out


def desc(key):
    return f"{key[0]}|{key[1]}|{key[2]}|ro={key[3]}"


def parse_allow(specs):
    allowed = {}
    for spec in specs:
        try:
            svc, rest = spec.split(":", 1)
            target, source = rest.split("=", 1)
        except ValueError:
            raise SystemExit(f"--allow 格式应为 service:target=source，收到 {spec!r}")
        allowed.setdefault(svc, {})[target.strip()] = source.strip()
    return allowed


def compare_running(inspect_path, merged_before, errors):
    raw = json.load(open(inspect_path))
    c = raw[0] if isinstance(raw, list) else raw
    backend = merged_before["services"]["backend"]

    if c.get("Image") != backend.get("image"):
        errors.append(
            f"[running] 运行容器镜像 ID 与合并配置不一致: {c.get('Image')} vs {backend.get('image')}"
        )
    if norm_list(c.get("Config", {}).get("Entrypoint")) != norm_list(backend.get("entrypoint")):
        errors.append("[running] entrypoint 与合并配置不一致")
    if norm_list(c.get("Config", {}).get("Cmd")) != norm_list(backend.get("command")):
        errors.append("[running] command 与合并配置不一致")

    running = {}
    for m in c.get("Mounts") or []:
        running[(m.get("Type"), m.get("Source"), m.get("Destination"), not m.get("RW", True))] = m
    compose = mount_map(backend)

    missing = [desc(k) for k in running if k not in compose]
    extra = [desc(k) for k in compose if k not in running]
    if missing:
        errors.append(f"[running] 运行容器有、合并配置没有: {missing}")
    if extra:
        errors.append(f"[running] 合并配置有、运行容器没有: {extra}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("before")
    ap.add_argument("after")
    ap.add_argument("--allow", action="append", default=[])
    ap.add_argument("--allow-env", action="append", default=[],
                    help="service:KEY —— 允许变化的环境变量")
    ap.add_argument("--running", help="重建前的 docker inspect JSON，用于核对无漂移")
    args = ap.parse_args()

    before = json.load(open(args.before))
    after = json.load(open(args.after))
    allowed = parse_allow(args.allow)
    allowed_env = {}
    for spec in args.allow_env:
        svc, key = spec.split(":", 1)
        allowed_env.setdefault(svc, set()).add(key)

    errors = []

    if set(before["services"]) != set(after["services"]):
        errors.append(f"服务集合变化: {sorted(before['services'])} -> {sorted(after['services'])}")

    for name, b in before["services"].items():
        a = after["services"].get(name)
        if a is None:
            continue

        if b.get("image") != a.get("image"):
            errors.append(f"[{name}] 镜像变化: {b.get('image')} -> {a.get('image')}")
        if norm_list(b.get("entrypoint")) != norm_list(a.get("entrypoint")):
            errors.append(f"[{name}] entrypoint 变化")
        if norm_list(b.get("command")) != norm_list(a.get("command")):
            errors.append(f"[{name}] command 变化")

        be, ae = norm_env(b), norm_env(a)
        for key in sorted(set(be) | set(ae)):
            if be.get(key) != ae.get(key) and key not in allowed_env.get(name, set()):
                errors.append(f"[{name}] 环境变量变化: {key} {be.get(key)!r} -> {ae.get(key)!r}")

        bv, av = mount_map(b), mount_map(a)
        missing = [desc(k) for k in bv if k not in av]
        if missing:
            errors.append(f"[{name}] 原有挂载缺失或属性变化: {missing}")

        added = [k for k in av if k not in bv]
        expect = allowed.get(name, {})
        for key in added:
            if key[2] not in expect:
                errors.append(f"[{name}] 出现非预期挂载: {desc(key)}")
                continue
            if not key[3]:
                errors.append(f"[{name}] 新增挂载未设只读: {desc(key)}")
            if key[1] != expect[key[2]]:
                errors.append(f"[{name}] 新增挂载源不符: {key[2]} 期望 {expect[key[2]]} 实际 {key[1]}")

        got = {k[2] for k in added}
        if expect and got != set(expect):
            errors.append(f"[{name}] 新增挂载集合不符: 期望 {sorted(expect)} 实际 {sorted(got)}")

    if args.running:
        compare_running(args.running, before, errors)

    if errors:
        print("MERGE_CHECK_FAILED")
        for e in errors:
            print(" -", e)
        return 1

    summary = ", ".join(
        f"{n}={len(mount_map(after['services'][n]))}" for n in sorted(after["services"])
    )
    print(f"MERGE_CHECK_OK services_mounts[{summary}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
