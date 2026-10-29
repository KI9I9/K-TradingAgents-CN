"""Read a local JSON inventory and print a plan. Never connects to any service."""
import argparse
import importlib.util
import json
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('inventory', type=Path)
    parser.add_argument('--end', required=True)
    parser.add_argument('--now', required=True, help='ISO timestamp with explicit timezone')
    parser.add_argument('--bootstrap-start')
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location('history_plan', ROOT/'deploy/history_sync_plan.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    try:
        data = json.loads(args.inventory.read_text())
        if not isinstance(data, dict) or set(data) - {'symbols', 'series', 'quota'}:
            raise ValueError('invalid_inventory')
        result = module.build_plan(data['symbols'], data['series'], end=args.end,
            now=datetime.fromisoformat(args.now), bootstrap_start=args.bootstrap_start,
            quota=data.get('quota'))
    except (ValueError, KeyError, TypeError):
        parser.exit(2, 'Invalid inventory or planning window; no action performed.\n')
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
