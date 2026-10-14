#!/usr/bin/env python3
"""迁移预检与一致性备份；默认只读，--execute-backup 才会短暂停服。"""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
from datetime import datetime

PROJECT = 'k-tradingagents'
APP_VOLUMES = ['app_data', 'app_logs', 'app_runtime', 'app_results', 'app_reports', 'app_uploads', 'app_config', 'redis_data']


def run(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def output(args):
    return run(args, capture_output=True, text=True).stdout.strip()


def inspect(root):
    services = {}
    for service in ['backend', 'frontend', 'mongodb', 'redis']:
        name = f'{PROJECT}-{service}-1'
        data = json.loads(output(['docker', 'inspect', name]))[0]
        assert data['Config']['Labels']['com.docker.compose.project'] == PROJECT
        services[service] = {
            'name': name, 'running': data['State']['Running'], 'image_id': data['Image'],
            'image': data['Config']['Image'],
            'mounts': [{'name': m.get('Name'), 'destination': m['Destination'], 'type': m['Type']} for m in data['Mounts']],
        }
    volumes = {m['name'] for s in services.values() for m in s['mounts'] if m['type'] == 'volume'}
    for suffix in APP_VOLUMES:
        assert f'{PROJECT}_{suffix}' in volumes, f'缺少预期数据卷：{suffix}'
    images = {}
    for s in services.values():
        image = json.loads(output(['docker', 'image', 'inspect', s['image']]))[0]
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


def backup(root, destination, inventory):
    if destination.exists():
        raise ValueError('备份目录已存在，拒绝覆盖')
    destination.mkdir(parents=True, mode=0o700)
    os.chmod(destination, 0o700)
    stopped = []
    try:
        # 先停止入口和应用，再停止 Redis；MongoDB 保持运行以便逻辑导出。
        for service in ['frontend', 'backend', 'redis']:
            if inventory['services'][service]['running']:
                stopped.append(service)
                run(['docker', 'compose', 'stop', service], cwd=root)
        save_source(root, destination/'source.tar.gz')
        shutil.copy2(root/'.env', destination/'.env')
        os.chmod(destination/'.env', 0o600)
        (destination/'inventory.json').write_text(json.dumps(inventory, ensure_ascii=False, indent=2))
        archive = destination/'mongodb.archive.gz'
        with archive.open('wb') as f:
            run(['docker','exec',f'{PROJECT}-mongodb-1','sh','-c',
                 'exec mongodump --username "$MONGO_INITDB_ROOT_USERNAME" --password "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin --archive --gzip'], stdout=f, stderr=subprocess.PIPE)
        with archive.open('rb') as f:
            result = run(['docker','exec','-i',f'{PROJECT}-mongodb-1','sh','-c',
                 'exec mongorestore --username "$MONGO_INITDB_ROOT_USERNAME" --password "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin --archive --gzip --dryRun'], stdin=f, capture_output=True)
        (destination/'restore-dry-run.log').write_bytes(result.stdout+result.stderr)
        # 从只读挂载导出冷备份，保留文件权限和所有权。
        for suffix in APP_VOLUMES:
            print('备份数据卷：', suffix, flush=True)
            with (destination/f'{suffix}.tar.gz').open('wb') as f:
                run(['docker','run','--rm','--network','none','--user','0',
                     '--mount',f'type=volume,src={PROJECT}_{suffix},dst=/volume,readonly',
                     '--entrypoint','tar',inventory['services']['backend']['image'],
                     '-czf','-','-C','/volume','.'], stdout=f, stderr=subprocess.PIPE)
        checksums = {}
        for p in destination.iterdir():
            if p.is_file():
                with p.open('rb') as f:
                    checksums[p.name] = {'bytes': p.stat().st_size, 'sha256': hashlib.file_digest(f, 'sha256').hexdigest()}
        (destination/'checksums.json').write_text(json.dumps(checksums, indent=2))
        verify(destination)
        (destination/'COMPLETE').write_text('备份及完整性检查通过；恢复前仍须核对目标环境。\n')
        print('备份完成：', destination)
    finally:
        # 失败也恢复原本运行的本地服务，避免备份异常导致长期停服。
        failures = []
        for service in reversed(stopped):
            try:
                run(['docker','compose','start',service],cwd=root)
            except subprocess.CalledProcessError:
                failures.append(service)
        if failures:
            raise RuntimeError('本地服务恢复失败，需检查：' + ', '.join(failures))


def verify(destination):
    manifest = json.loads((destination/'checksums.json').read_text())
    for name, expected in manifest.items():
        if Path(name).name != name:
            raise ValueError('非法备份文件路径')
        path = destination/name
        with path.open('rb') as f:
            assert hashlib.file_digest(f, 'sha256').hexdigest() == expected['sha256'], name
        assert path.stat().st_size == expected['bytes'], name
        if name.endswith('.gz'):
            with gzip.open(path,'rb') as f:
                while f.read(1024*1024):
                    pass
    print('校验和与压缩流完整性检查通过。')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument('--inventory-out', type=Path)
    parser.add_argument('--execute-backup', type=Path, metavar='NEW_DIRECTORY')
    parser.add_argument('--verify', type=Path, metavar='BACKUP_DIRECTORY')
    args = parser.parse_args()
    if args.verify:
        verify(args.verify)
    else:
        inventory = inspect(args.root)
        if args.inventory_out:
            args.inventory_out.write_text(json.dumps(inventory, ensure_ascii=False, indent=2))
        print(json.dumps(inventory, ensure_ascii=False, indent=2))
        if args.execute_backup:
            backup(args.root, args.execute_backup, inventory)
