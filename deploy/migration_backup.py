#!/usr/bin/env python3
"""迁移预检与归档校验；正式停写通过 migration_window.py 的显式窗口协调。"""
import argparse
import gzip
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import subprocess
import tarfile
import time
from migration_archive import APP_VOLUMES, file_sha256, seal, verify

PROJECT = 'k-tradingagents'

# Only reads the existing Mongo container's environment. Credentials stay inside that
# container and its restricted tmpfs file; neither argv nor stdout contains their value.
MONGO_CONFIG_SCRIPT = r'''
try {
  const fs = require('fs');
  const username = process.env.MONGO_INITDB_ROOT_USERNAME;
  const password = process.env.MONGO_INITDB_ROOT_PASSWORD;
  if (!username || !password) process.exit(2);
  const directory = fs.mkdtempSync('/dev/shm/k-tradingagents-mongo-');
  fs.chmodSync(directory, 0o700);
  const path = directory + '/tools.json';
  fs.writeFileSync(path, JSON.stringify({
    uri: 'mongodb://' + encodeURIComponent(username) + '@127.0.0.1:27017/?authSource=admin',
    password: password
  }), {mode: 0o600, flag: 'wx'});
  process.stdout.write('TA_MIGRATION_CONFIG=' + JSON.stringify({path: path}) + '\n');
  process.exit(0);
} catch (_) { process.exit(3); }
'''


MONGO_BASELINE_SCRIPT = r'''
try {
  const username = process.env.MONGO_INITDB_ROOT_USERNAME;
  const password = process.env.MONGO_INITDB_ROOT_PASSWORD;
  if (!username || !password) process.exit(2);
  const connection = new Mongo('mongodb://' + encodeURIComponent(username) + ':' +
    encodeURIComponent(password) + '@127.0.0.1:27017/?authSource=admin');
  const admin = connection.getDB('admin');
  const ttl = admin.runCommand({getParameter: 1, ttlMonitorEnabled: 1});
  if (ttl.ok !== 1 || ttl.ttlMonitorEnabled !== false) process.exit(4);
  const database = connection.getDB('k_tradingagents');
  // A new task can start between the pre-stop check and stopping the backend.
  // Reject such a frozen snapshot rather than carry interrupted work forward.
  const active = ['queued','pending','running','processing','cancelling','scheduled'];
  for (const name of ['analysis_tasks','unified_analysis_tasks','stock_sync_tasks','favorites_sync_tasks','scheduler_executions']) {
    if (database.getCollection(name).countDocuments({status:{$in:active}}) !== 0) process.exit(7);
  }
  // View definitions are recorded in each view's options. Do not treat the
  // internal system.views catalog as an ordinary collection: countDocuments
  // uses aggregation and is not authorized there even for this root role.
  const baseline = database.getCollectionInfos().filter(info => info.name !== 'system.views')
    .sort((a,b) => a.name.localeCompare(b.name)).map(info => ({
    name: info.name, type: info.type, options: info.options,
    count: database.getCollection(info.name).countDocuments({}),
    indexes: info.type === 'view' ? [] : database.getCollection(info.name).getIndexes().sort((a,b) => a.name.localeCompare(b.name))
  }));
  process.stdout.write('TA_MIGRATION_BASELINE=' + EJSON.stringify(baseline) + '\n');
  process.exit(0);
} catch (_) { process.exit(3); }
'''


def mongo_baseline(container):
    result = run(['docker', 'exec', '--user', '0', '-i', container, 'mongosh',
                  '--nodb', '--quiet', '--file', '/dev/stdin'], input=MONGO_BASELINE_SCRIPT,
                 capture_output=True, text=True, timeout=300)
    records = [line[len('TA_MIGRATION_BASELINE='):] for line in result.stdout.splitlines()
               if line.startswith('TA_MIGRATION_BASELINE=')]
    if len(records) != 1:
        raise RuntimeError('冻结基线读取失败；不输出原始结果')
    baseline = json.loads(records[0])
    if not isinstance(baseline, list) or not baseline:
        raise ValueError('冻结基线为空或格式错误')
    return baseline


def confirm_stopped():
    for service in ('frontend', 'backend', 'redis'):
        if output(['docker', 'inspect', '--format', '{{.State.Running}}', f'{PROJECT}-{service}-1']) != 'false':
            raise RuntimeError('源服务未保持停止，拒绝完成快照')


def save_backend_cache(destination):
    # docker cp works with a stopped container; do not restart it to read its layer.
    process = subprocess.Popen(['docker', 'cp', f'{PROJECT}-backend-1:/app/tradingagents/dataflows/cache/.', '-'],
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 600
        with gzip.open(destination, 'wb') as target, selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError('容器缓存导出超时')
                if not selector.select(timeout=1):
                    continue
                block = os.read(process.stdout.fileno(), 1024 * 1024)
                if not block:
                    break
                target.write(block)
        process.stdout.close()
        if process.wait(timeout=120) != 0:
            raise RuntimeError('容器卷外缓存导出失败')
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def prepare_mongo_tools_config(container):
    result = run(['docker', 'exec', '--user', '0', '-i', container, 'mongosh',
                  '--nodb', '--quiet', '--file', '/dev/stdin'],
                 input=MONGO_CONFIG_SCRIPT, capture_output=True, text=True, timeout=30)
    try:
        # mongosh can emit startup notices even with --quiet. Parse only our tagged
        # record; never forward other stdout/stderr to logs or the conversation.
        records = [line[len('TA_MIGRATION_CONFIG='):] for line in result.stdout.splitlines()
                   if line.startswith('TA_MIGRATION_CONFIG=')]
        if len(records) != 1:
            raise ValueError
        path = json.loads(records[0])['path']
        if not isinstance(path, str) or not re.fullmatch(r'/dev/shm/k-tradingagents-mongo-[A-Za-z0-9]{6}/tools\.json', path):
            raise ValueError
    except (ValueError, KeyError, TypeError):
        raise RuntimeError('MongoDB 工具临时配置创建失败；不输出原始结果') from None
    return path


def run(args, **kwargs):
    # Docker/Compose may include configuration details in diagnostics. Capture
    # by default; callers may deliberately direct archive bytes to a private file.
    if not kwargs.get('capture_output'):
        kwargs.setdefault('stdout', subprocess.PIPE)
        kwargs.setdefault('stderr', subprocess.PIPE)
    kwargs.setdefault('timeout', 30)
    return subprocess.run(args, check=True, **kwargs)


def output(args):
    return run(args, capture_output=True, text=True).stdout.strip()


def inspect(root):
    services = {}
    for service in ['backend', 'frontend', 'mongodb', 'redis']:
        name = f'{PROJECT}-{service}-1'
        data = json.loads(output(['docker', 'inspect', name]))[0]
        labels = data['Config'].get('Labels') or {}
        if labels.get('com.docker.compose.project') != PROJECT or labels.get('com.docker.compose.service') != service:
            raise ValueError('源容器不属于预期项目/服务')
        services[service] = {
            'name': name, 'id': data['Id'], 'running': data['State']['Running'], 'image_id': data['Image'],
            'image': data['Config']['Image'],
            'mounts': [{'name': m.get('Name'), 'destination': m['Destination'], 'type': m['Type']} for m in data['Mounts']],
        }
    volumes = {m['name'] for s in services.values() for m in s['mounts'] if m['type'] == 'volume'}
    for suffix in APP_VOLUMES:
        if f'{PROJECT}_{suffix}' not in volumes:
            raise ValueError('缺少预期数据卷：' + suffix)
    images = {}
    for s in services.values():
        image = json.loads(output(['docker', 'image', 'inspect', s['image_id']]))[0]
        images[s['image']] = {k: image.get(k) for k in ['Id', 'Architecture', 'Os', 'Size', 'RepoDigests']}
    return {
        'project': PROJECT, 'root': str(root), 'services': services, 'images': images,
        'git_commit': output(['git', '-C', str(root), 'rev-parse', 'HEAD']),
        'git_branch': output(['git', '-C', str(root), 'branch', '--show-current']),
        'git_status': output(['git', '-C', str(root), 'status', '--short']),
        'free_bytes': shutil.disk_usage(root).free,
    }


def save_source(root, destination):
    """打包受版本控制和未忽略的改动文件，部署密钥单独保存。"""
    names = run(['git', '-C', str(root), 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], capture_output=True).stdout.decode().split('\0')
    with tarfile.open(destination, 'w:gz') as archive:
        for name in sorted(set(names)):
            if not name:
                continue
            rel = Path(name)
            if rel.is_absolute() or '..' in rel.parts:
                raise ValueError('源码路径超出仓库')
            if rel.name == '.env' or (rel.name.startswith('.env.') and rel.name not in {'.env.example', '.env.sample', '.env.template'}):
                continue
            path = root / rel
            if path.is_symlink():
                path.resolve().relative_to(root.resolve())
            if path.exists():
                archive.add(path, arcname=name, recursive=False)


def backup(root, destination, inventory, *, success_state, failure_state, write_freeze_confirmed=False, runtime_config=None):
    if not write_freeze_confirmed:
        raise ValueError('必须先确认已批准的停写窗口、外部写入隔离及 TTL 临时暂停')
    if not isinstance(runtime_config, dict) or runtime_config.get('schema') != 1:
        raise ValueError('必须保存原容器解析后的运行配置，不允许重新猜测或生成凭据')
    if success_state not in {'keep-stopped', 'restart-source'} or failure_state not in {'keep-stopped', 'restart-source'}:
        raise ValueError('必须显式选择成功和失败后的源服务状态')
    if destination.exists():
        raise ValueError('备份目录已存在，拒绝覆盖')
    destination.mkdir(parents=True, mode=0o700)
    os.chmod(destination, 0o700)
    previous_umask = os.umask(0o077)
    stopped = []
    completed = False
    try:
        container = f'{PROJECT}-mongodb-1'
        # Read-only gate: the separate approved window must pause TTL first. This
        # tool never changes TTL silently and never guesses whether writers exist.
        mongo_baseline(container)
        # 先停止入口和应用，再停止 Redis；MongoDB 保持运行以便逻辑导出。
        for service in ['frontend', 'backend', 'redis']:
            if inventory['services'][service]['running']:
                stopped.append(service)
                run(['docker', 'stop', '--time', '60', inventory['services'][service]['id']], timeout=90)
        confirm_stopped()
        before = mongo_baseline(container)
        save_source(root, destination/'source.tar.gz')
        shutil.copy2(root/'.env', destination/'.env')
        os.chmod(destination/'.env', 0o600)
        (destination/'inventory.json').write_text(json.dumps(inventory, ensure_ascii=False, indent=2))
        (destination/'runtime-config.json').write_text(json.dumps(runtime_config, ensure_ascii=True, indent=2))
        config_path = prepare_mongo_tools_config(container)
        # Kept for user-confirmed cleanup, not unlinked automatically on success/error.
        (destination/'temporary-credential-files.json').write_text(json.dumps({
            'container': container, 'paths': [config_path], 'mode': '600',
            'parent_mode': '700', 'storage': 'container_tmpfs',
        }, indent=2))
        archive = destination/'mongodb.archive.gz'
        with archive.open('wb') as f:
            run(['docker', 'exec', '--user', '0', container, 'mongodump',
                 '--config=' + config_path, '--db=k_tradingagents', '--archive', '--gzip'],
                stdout=f, stderr=subprocess.PIPE, timeout=900)
        with archive.open('rb') as f:
            run(['docker', 'exec', '--user', '0', '-i', container, 'mongorestore',
                 '--config=' + config_path, '--nsInclude=k_tradingagents.*', '--archive',
                 '--gzip', '--dryRun'], stdin=f, capture_output=True, timeout=900)
        (destination/'restore-dry-run.log').write_text('应用命名空间 dry-run 成功；未保留原始工具输出。\n')
        save_backend_cache(destination/'backend-cache.tar.gz')
        # 从只读挂载导出冷备份，保留文件权限和所有权。
        for suffix in APP_VOLUMES:
            print('备份数据卷：', suffix, flush=True)
            # Check immediately before mount: Docker otherwise creates a missing
            # named volume silently. volume-nocopy also prevents image copy-up.
            volume = json.loads(output(['docker', 'volume', 'inspect', f'{PROJECT}_{suffix}']))
            if len(volume) != 1 or volume[0].get('Name') != f'{PROJECT}_{suffix}':
                raise ValueError('源卷不存在或身份不符')
            with (destination/f'{suffix}.tar.gz').open('wb') as f:
                run(['docker','run','--rm','--network','none','--user','0',
                     '--read-only','--cap-drop','ALL','--cap-add','DAC_OVERRIDE',
                     '--security-opt','no-new-privileges:true','--pids-limit','64','--memory','256m',
                     '--mount',f'type=volume,src={PROJECT}_{suffix},dst=/volume,readonly,volume-nocopy',
                     '--entrypoint','tar',inventory['services']['backend']['image_id'],
                     '-czf','-','-C','/volume','.'], stdout=f, stderr=subprocess.PIPE, timeout=600)
        confirm_stopped()
        after = mongo_baseline(container)
        if before != after:
            raise ValueError('冻结前后集合记录数/索引/视图不一致')
        (destination/'snapshot-metadata.json').write_text(json.dumps({
            'schema': 1, 'writers_stopped': True, 'ttl_monitor_enabled': False,
            'before': before, 'after': after,
        }, ensure_ascii=True, indent=2))
        seal(destination)
        completed = True
        print('备份完成：', destination)
    finally:
        # Final cutover must keep writers stopped; a rehearsal may explicitly restart.
        selected_state = success_state if completed else failure_state
        failures = []
        if selected_state == 'restart-source':
            for service in reversed(stopped):
                try:
                    run(['docker','start',inventory['services'][service]['id']], timeout=60)
                except subprocess.CalledProcessError:
                    failures.append(service)
        os.umask(previous_umask)
        if failures:
            raise RuntimeError('本地服务恢复失败，需检查：' + ', '.join(failures))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument('--inventory-out', type=Path)
    parser.add_argument('--execute-backup', type=Path, metavar='NEW_DIRECTORY')
    parser.add_argument('--success-state', choices=['keep-stopped', 'restart-source'])
    parser.add_argument('--failure-state', choices=['keep-stopped', 'restart-source'])
    parser.add_argument('--confirmed-write-freeze', action='store_true',
                        help='仅在已批准停写、隔离外部写入并临时暂停 TTL 后使用；本工具不修改 TTL')
    parser.add_argument('--verify', type=Path, metavar='BACKUP_DIRECTORY')
    args = parser.parse_args()
    if args.execute_backup:
        parser.error('正式快照请使用 migration_window.py snapshot；需要窗口记录、容量与 TTL 恢复保护')
    if args.execute_backup and (not args.success_state or not args.failure_state or not args.confirmed_write_freeze):
        parser.error('执行备份前必须显式指定成功/失败状态，并确认停写窗口、外部写入隔离及 TTL 暂停')
    if args.verify:
        try:
            print(json.dumps(verify(args.verify)))
        except Exception as error:
            print(json.dumps({'status': 'failed', 'error_category': type(error).__name__}))
            raise SystemExit(1) from None
    else:
        inventory = inspect(args.root)
        if args.inventory_out:
            args.inventory_out.write_text(json.dumps(inventory, ensure_ascii=False, indent=2))
        print(json.dumps(inventory, ensure_ascii=False, indent=2))
        if args.execute_backup:
            try:
                backup(args.root, args.execute_backup, inventory,
                       success_state=args.success_state, failure_state=args.failure_state,
                       write_freeze_confirmed=args.confirmed_write_freeze)
            except Exception as error:
                print('备份失败；错误类别：' + type(error).__name__ + '；保留受限文件等待检查。')
                raise SystemExit(1) from None
