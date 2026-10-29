#!/usr/bin/env python3
"""Local disposable MongoDB acceptance. No production settings, mounts or data.

Requires local Docker access via sudo -n and preinstalled isolated Python test deps.
The image is not pulled implicitly; use --image with a locally available MongoDB image.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from pymongo import MongoClient


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image',default='mongo:7.0')
    parser.add_argument('--test', choices=['test_mongo_history.py', 'test_mongo_history_write.py', 'test_mongo_history_batch.py'], default='test_mongo_history.py')
    args=parser.parse_args()
    docker=['sudo','-n','docker']
    name='ta-history-test-'+uuid.uuid4().hex[:12]
    try:
        subprocess.run(docker+['run','--detach','--name',name,'--pull','never',
            '--label','codex.task=history-contract-test','--memory','512m','--cpus','1',
            '--publish','127.0.0.1::27017',
            args.image,'mongod','--bind_ip_all','--wiredTigerCacheSizeGB','0.25'],
            check=True,capture_output=True,text=True,timeout=40)
        info=json.loads(subprocess.check_output(docker+['inspect',name]))[0]
        binding=info['NetworkSettings']['Ports']['27017/tcp'][0]
        assert binding['HostIp']=='127.0.0.1'
        uri='mongodb://127.0.0.1:'+binding['HostPort']
        ready=False
        with MongoClient(uri,serverSelectionTimeoutMS=1000) as client:
            for _ in range(15):
                try:
                    client.admin.command('ping')
                    ready=True
                    break
                except Exception:
                    time.sleep(1)
        if not ready: raise RuntimeError('test_mongodb_not_ready')
        env=dict(os.environ,HISTORY_TEST_MONGO_URI=uri)
        root=Path(__file__).resolve().parents[1]
        result=subprocess.run([sys.executable,'-B',str(root/'tests/history'/args.test)],
                               cwd=root,env=env,timeout=90)
        print(json.dumps({'image_id':info['Image'],'test_exit_code':result.returncode,
                          'production_access':False,'data':'synthetic only'}))
        return result.returncode
    finally:
        cleanup=subprocess.run(docker+['rm','--force','--volumes',name],capture_output=True,text=True,timeout=30)
        if cleanup.returncode:
            raise RuntimeError('test_container_cleanup_needs_review:'+name)


if __name__=='__main__':
    raise SystemExit(main())
