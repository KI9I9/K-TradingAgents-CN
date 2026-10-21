"""Load original runtime values from a restricted file, then run LIMITED review.

Never switches into production; use a different, explicitly approved launch for
cutover. All raw application stdout/stderr is discarded in this temporary mode
because imported third-party code is outside the targeted logging patch audit.
"""
import json
import logging
import os
from pathlib import Path
import stat
import sys


def main():
    original_stdout = os.dup(1)
    try:
        path = Path('/run/secrets/runtime-config.json')
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > 4 * 1024 * 1024:
            raise ValueError('Runtime config permissions or size invalid')
        data = json.loads(path.read_text())
        values = data['backend_environment']
        if data['schema'] != 1 or not isinstance(values, dict):
            raise ValueError('Unsupported runtime configuration')
        if any(not values.get(key) for key in ('JWT_SECRET', 'CSRF_SECRET', 'MONGODB_USERNAME', 'MONGODB_PASSWORD', 'REDIS_PASSWORD')):
            raise ValueError('Original credentials incomplete')
        # Values never enter process argv or are regenerated. Runtime-only host
        # overrides point to the dedicated review services, never back to WSL.
        os.environ.update(values)
        os.environ.update({'TA_MIGRATION_REVIEW': '1', 'MONGODB_HOST': 'mongodb',
                           'REDIS_HOST': 'redis', 'DOTENV_FILE': '/dev/null'})
        logging.disable(sys.maxsize)
        with open(os.devnull, 'wb') as sink:
            os.dup2(sink.fileno(), 1)
            os.dup2(sink.fileno(), 2)
        # Running a script by filename puts deploy/, not /app, on sys.path.
        # The project installs tradingagents only; app remains source-local.
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        import uvicorn
        uvicorn.run('app.main:app', host='0.0.0.0', port=8000, workers=1,
                    log_config=None, access_log=False)
        return 0
    except BaseException as error:
        os.write(original_stdout, (json.dumps({'status': 'failed', 'error_category': type(error).__name__}) + '\n').encode())
        return 1
    finally:
        os.close(original_stdout)


if __name__ == '__main__':
    raise SystemExit(main())
