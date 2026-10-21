"""PREPARE ONLY: no outage/backup/restore/transfer commands are accepted here.

Reads the existing source deployment and writes only a new private preparation
record outside its checkout. The wider window coordinator is NOT authorized by
running this entrypoint. All diagnostics are field-free categories and stages.
"""
import argparse
import json
import os
from pathlib import Path

import migration_window as window


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('/home/marvin/project/K-TradingAgents-CN'))
    parser.add_argument('--window', type=Path, required=True)
    args = parser.parse_args(argv)
    os.umask(0o077)
    stage = 'initializing'

    def record_stage(value):
        nonlocal stage
        stage = value

    try:
        result = window.prepare(args.root, args.window, report_stage=record_stage)
        result.update(source_modified=False, business_backup_created=False,
                      window=str(args.window.absolute()))
        print(json.dumps(result))
        return 0
    except BaseException as error:
        result = {'status': 'failed', 'failed_stage': stage,
                  'error_category': type(error).__name__, 'source_modified': False,
                  'business_backup_created': False}
        if isinstance(error, window.CapacityError):
            result['capacity'] = error.budget
        print(json.dumps(result))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
