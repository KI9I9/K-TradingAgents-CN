#!/usr/bin/env python3
"""通过正常鉴权读取生产调度状态；凭据仅经 SSH stdin 传递。

--credentials-file 使用两行用户名/密码，会产生正常登录会话及审计记录。
--token-file 使用已有 JWT。输出只含目标任务状态，不保存/输出密码或令牌。
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import stat
import subprocess

TARGET_JOBS = ("akshare_quotes_sync", "quotes_ingestion_service", "etf_sync", "tushare_realtime_quotes_hourly")

REMOTE = r'''
import json, sys, urllib.request

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('redirect_refused')

def request(path, token=None, body=None):
    headers={'Content-Type':'application/json'}
    if token is not None: headers['Authorization']='Bearer '+token
    req=urllib.request.Request('http://127.0.0.1:8000'+path,
        data=json.dumps(body).encode() if body is not None else None, headers=headers)
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
    with opener.open(req,timeout=15) as response:
        return json.load(response)

def query(payload, fetch=request):
    auth=payload['auth']
    if 'token' in auth:
        token=auth['token']
    else:
        login=fetch('/api/auth/login',body=auth)
        if login.get('success') is not True: raise ValueError('login_failed')
        token=login['data']['access_token']
    response=fetch('/api/scheduler/jobs',token=token)
    if response.get('success') is not True or not isinstance(response.get('data'),list):
        raise ValueError('jobs_response_invalid')
    wanted=set(payload['jobs'])
    result=[]
    seen=set()
    for job in response['data']:
        ident=job.get('id')
        if ident not in wanted: continue
        if ident in seen:raise ValueError('duplicate_job')
        seen.add(ident)
        result.append({k:job[k] for k in ['id','paused','next_run_time','has_running_execution'] if k in job})
    return {'success':True,'data':result,'missing_jobs':sorted(wanted-seen)}

if __name__=='__main__':
    try:
        print(json.dumps(query(json.load(sys.stdin))))
    except Exception:
        print('JOBS_QUERY_FAILED: authentication, transport or response invalid')
        sys.exit(1)
'''


def read_auth(path, credentials=False):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "r", encoding="utf-8") as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_uid != os.geteuid() or info.st_nlink != 1 or not 0 < info.st_size <= 16384):
            raise ValueError("需要当前用户所有、权限 600 的普通凭据文件")
        raw = stream.read()
    lines = raw.splitlines()
    if credentials:
        if len(lines) != 2 or not all(lines):
            raise ValueError("凭据文件必须恰好两行：用户名、密码")
        return {"username": lines[0], "password": lines[1]}
    token = raw.strip()
    if not token or any(c.isspace() for c in token):
        raise ValueError("Token 文件格式错误")
    return {"token": token}


def check_jobs(report):
    """缺失 ingestion 由封印配置关闭的独立证据解释；其他任务必须可见。"""
    errors = []
    jobs = {job["id"]: job for job in report["data"]}
    for ident in TARGET_JOBS:
        if ident not in jobs:
            if ident != "quotes_ingestion_service":
                errors.append(ident + ":missing")
            continue
        job = jobs[ident]
        if job.get("paused") is not True or job.get("has_running_execution") is not False:
            errors.append(ident + ":not_explicitly_paused_and_idle")
        if "next_run_time" not in job or job["next_run_time"] is not None:
            errors.append(ident + ":next_run_time_not_null")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    auth = parser.add_mutually_exclusive_group(required=True)
    auth.add_argument("--token-file", type=Path)
    auth.add_argument("--credentials-file", type=Path)
    parser.add_argument("--ssh-config", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("输出文件已存在；不得覆盖验收证据")
    try:
        payload = {"auth": read_auth(args.token_file or args.credentials_file, bool(args.credentials_file)),
                   "jobs": TARGET_JOBS}
        remote = shlex.join(["sudo", "-n", "docker", "exec", "-i", args.container, "python3", "-B", "-c", REMOTE])
        result = subprocess.run(["ssh", "-F", args.ssh_config, "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                                 args.host, remote], input=json.dumps(payload), text=True, capture_output=True, timeout=60)
        if result.returncode:
            raise ValueError("remote_failed")
        report = json.loads(result.stdout)
        if report.get("success") is not True:
            raise ValueError("query_failed")
        errors = check_jobs(report)
        fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            json.dump(report, stream, indent=2)
    except Exception:
        print("JOBS_CHECK_FAILED: 凭据、连接或响应不满足；未输出敏感值")
        return 1
    print(json.dumps({"jobs_checked": [j["id"] for j in report["data"]],
                      "missing_jobs": report["missing_jobs"], "errors": errors}, ensure_ascii=False))
    if "quotes_ingestion_service" in report["missing_jobs"]:
        print("需要独立封印配置证据证明 QUOTES_INGEST_ENABLED=false；此结果不单独批准部署。")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
