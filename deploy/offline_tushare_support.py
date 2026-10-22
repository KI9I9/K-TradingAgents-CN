"""Test-only source loading and synthetic permits; no application startup or CLI."""
import importlib.util
import os
from pathlib import Path
import sys
import time
import types

SOURCE = Path(__file__).resolve().parent.parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# The standalone regression processes need the transport modules only, not the
# package's application/logging initialization or any configured providers.
for name in ('tradingagents', 'tradingagents.utils'):
    if name not in sys.modules:
        package = types.ModuleType(name)
        package.__path__ = [str(SOURCE / name.replace('.', '/'))]
        sys.modules[name] = package

egress = load('tradingagents.utils.tushare_egress',
              SOURCE / 'tradingagents/utils/tushare_egress.py')
policy = load('tradingagents.utils.tushare_manual',
              SOURCE / 'tradingagents/utils/tushare_manual.py')


def deny_external(event, args):
    if event in ('socket.connect', 'socket.getaddrinfo', 'socket.bind',
                 'socket.sendto', 'subprocess.Popen'):
        raise RuntimeError('OfflineOnly')


def authorize_new(directory, budget, ttl, apis):
    """Create a fresh synthetic fixture; cannot reuse/reset an existing permit."""
    if (type(budget) is not int or not 1 <= budget <= egress.MAX_PERMIT
            or type(ttl) is not int or not 1 <= ttl <= egress.MAX_TTL):
        raise ValueError('InvalidBoundedPermit')
    value = egress._state(dict(schema=1, phase='ready', budget=budget, used=0,
        expires_at=time.time()+ttl, allowed_apis=list(apis), reason='none', api_code=None))
    directory = Path(directory)
    if not directory.is_absolute() or directory.parent.resolve() != directory.parent:
        raise ValueError('CanonicalParentRequired')
    directory.mkdir(mode=0o700)
    fd = os.open(directory/'gate.lock', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.fsync(fd)
    os.close(fd)
    permit = egress.Permit(directory)
    with permit.locked() as root_fd:
        permit.write(root_fd, value)
    return value
