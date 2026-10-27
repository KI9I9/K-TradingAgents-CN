#!/usr/bin/env python3
"""离线比对 Compose JSON；只允许列明的只读 bind 新增。

--running SERVICE=container.json 必须同时提供 --image SERVICE=image.json。
image.json 应由 docker image inspect <before 中的 image> 获取；只接受相同 Image ID。
本工具不调用 Docker，不能证明证据的新鲜度，也不替代健康/任务/有效运行配置检查。
"""
import argparse
import json
import sys


def load(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def one(raw):
    if isinstance(raw, list):
        if len(raw) != 1:
            raise ValueError("inspect 必须恰好包含一个对象")
        raw = raw[0]
    if not isinstance(raw, dict):
        raise ValueError("inspect 对象格式错误")
    return raw


def norm_env(service):
    env = service.get("environment") or {}
    if isinstance(env, list):
        result = {}
        for item in env:
            if not isinstance(item, str) or "=" not in item:
                raise ValueError("环境变量必须已解析")
            key, val = item.split("=", 1)
            if key in result:
                raise ValueError("重复环境变量")
            result[key] = val
        return result
    if not isinstance(env, dict) or any(not isinstance(v, str) for v in env.values()):
        raise ValueError("环境变量必须为已解析字符串映射")
    return env


def mount_map(service):
    result = {}
    for item in service.get("volumes") or []:
        if not isinstance(item, dict) or not isinstance(item.get("target"), str):
            raise ValueError("需要 Compose 规范化长格式挂载")
        target = item["target"]
        if target in result:
            raise ValueError("挂载目标重复")
        result[target] = item
    return result


def parse_allow(specs):
    allowed = {}
    for spec in specs:
        svc, rest = spec.split(":", 1)
        target, source = rest.split("=", 1)
        if not svc or not target.startswith("/") or not source.startswith("/"):
            raise ValueError("--allow 格式应为 service:/target=/source")
        if target in allowed.setdefault(svc, {}):
            raise ValueError("--allow 目标重复")
        allowed[svc][target] = source
    return allowed


def compare(before, after, allowed):
    errors = []
    if {k: v for k, v in before.items() if k != "services"} != {
            k: v for k, v in after.items() if k != "services"}:
        errors.append("顶层 Compose 配置变化")
    bs, ats = before["services"], after["services"]
    if set(bs) != set(ats):
        errors.append("服务集合变化")
    if set(allowed) - set(bs):
        errors.append("允许清单包含未知服务")
    for name in bs.keys() & ats.keys():
        b, a = bs[name], ats[name]
        # 比较所有服务字段，包括 networks / privileged / healthcheck / restart 等。
        for key in (b.keys() | a.keys()) - {"volumes"}:
            if (key in b) != (key in a) or b.get(key) != a.get(key):
                errors.append(f"[{name}] 配置变化: {key}")  # 不输出可能包含凭据的值
        bv, av = mount_map(b), mount_map(a)
        for target in bv:
            if av.get(target) != bv[target]:
                errors.append(f"[{name}] 原挂载缺失或属性变化: {target}")
        added = av.keys() - bv.keys()
        expect = allowed.get(name, {})
        if added != expect.keys():
            errors.append(f"[{name}] 新增挂载集合不符")
        for target in added & expect.keys():
            v = av[target]
            if (v.get("type") != "bind" or v.get("source") != expect[target]
                    or v.get("read_only") is not True):
                errors.append(f"[{name}] 新挂载必须为指定源的只读 bind: {target}")
            # create_host_path=false 防止源缺失时 Docker 自动创建目录。
            if v.get("bind") != {"create_host_path": False} or set(v) != {
                    "type", "source", "target", "read_only", "bind"}:
                errors.append(f"[{name}] 新挂载选项不符: {target}")
    return errors


def argv(value):
    if value is None or value == "":
        return []
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ValueError("命令需要 Compose 解析为 argv 数组；不猜测字符串 shell 语义")
    return value


def compare_running(service, container, image, before):
    errors = []
    cfg = before["services"][service]
    c, im = one(container), one(image)
    rc, ic = c["Config"], im["Config"]
    ref = cfg.get("image")
    if not isinstance(im.get("Id"), str) or not im["Id"].startswith("sha256:"):
        errors.append(f"[{service}] 缺少镜像 ID")
    if c.get("Image") != im.get("Id"):
        errors.append(f"[{service}] 运行镜像 ID 与当前引用解析结果不符")
    if ref not in [im.get("Id"), *(im.get("RepoTags") or []), *(im.get("RepoDigests") or [])]:
        errors.append(f"[{service}] image inspect 未覆盖 Compose 镜像引用")
    entry = cfg.get("entrypoint")
    expected_entry = ic.get("Entrypoint") if entry is None else entry
    command = cfg.get("command")
    expected_command = (ic.get("Cmd") if entry is None else None) if command is None else command
    if argv(rc.get("Entrypoint")) != argv(expected_entry):
        errors.append(f"[{service}] 运行 entrypoint 不符")
    if argv(rc.get("Cmd")) != argv(expected_command):
        errors.append(f"[{service}] 运行 command 不符")
    if cfg.get("env_file"):
        errors.append(f"[{service}] env_file 尚未解析")
    expected_env = norm_env({"environment": ic.get("Env")})
    expected_env.update(norm_env(cfg))
    actual_env = norm_env({"environment": rc.get("Env")})
    for key in expected_env.keys() | actual_env.keys():
        if (key in expected_env) != (key in actual_env) or expected_env.get(key) != actual_env.get(key):
            errors.append(f"[{service}] 容器环境变量不符: {key}")
    expected_mounts = {}
    for target, m in mount_map(cfg).items():
        kind, source = m.get("type"), m.get("source")
        if kind == "volume":
            source = before.get("volumes", {}).get(source, {}).get("name")
            if not source:
                errors.append(f"[{service}] volume 名称未解析: {target}")
        elif kind != "bind":
            errors.append(f"[{service}] 暂不支持验证此挂载类型: {target}")
        expected_mounts[target] = (kind, source, m.get("read_only", False))
    actual_mounts = {}
    for m in c.get("Mounts", []):
        target = m["Destination"]
        if target in actual_mounts or type(m.get("RW")) is not bool:
            errors.append(f"[{service}] 运行挂载证据不完整: {target}")
        actual_mounts[target] = (m.get("Type"), m.get("Name") if m.get("Type") == "volume"
                                 else m.get("Source"), not m.get("RW", True))
    if expected_mounts != actual_mounts:
        errors.append(f"[{service}] 运行挂载路径/源/读写属性不符")
    return errors


def service_paths(specs):
    result = {}
    for spec in specs:
        service, path = spec.split("=", 1)
        if not service or not path or service in result:
            raise ValueError("需要不重复的 SERVICE=文件路径")
        result[service] = path
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("before")
    ap.add_argument("after")
    ap.add_argument("--allow", action="append", default=[])
    ap.add_argument("--running", action="append", default=[], metavar="SERVICE=JSON")
    ap.add_argument("--image", action="append", default=[], metavar="SERVICE=JSON")
    args = ap.parse_args()
    try:
        before, after = load(args.before), load(args.after)
        errors = compare(before, after, parse_allow(args.allow))
        running, images = service_paths(args.running), service_paths(args.image)
        if running.keys() != images.keys():
            errors.append("--running 与 --image 的服务集合必须一致")
        for service in running.keys() & images.keys():
            errors.extend(compare_running(service, load(running[service]), load(images[service]), before))
    except (ValueError, KeyError, TypeError, OSError):
        # 不输出异常原文，避免 JSON 内容或环境值泄漏。
        errors = ["输入证据格式错误或不可读"]
    if errors:
        print("MERGE_CHECK_FAILED")
        for error in errors:
            print(" -", error)
        return 1
    print("MERGE_CHECK_OK; running_checked=" + (",".join(sorted(running)) or "none"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
