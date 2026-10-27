"""部署控制流测试：真实原地写入＋临时文件；Docker/DB/API 均为替身。"""
import importlib.util
from pathlib import Path
import tempfile
import unittest
import io
import urllib.error
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]


def load():
    spec = importlib.util.spec_from_file_location('quote_switch', ROOT/'deploy/switch_quote_guard_v12.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SwitchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.m = load()
        self.m.ROOT = self.root
        self.m.STAGED = self.root/'staged'
        self.m.BACKUP = self.root/'backup'
        self.m.STAGED.mkdir()
        self.m.BACKUP.mkdir()
        self.source = self.root/'source.py'
        self.source.write_bytes(b'original_long_content')
        self.source.chmod(0o640)
        (self.m.STAGED/'source.py').write_bytes(b'new')
        (self.m.BACKUP/'source.py').write_bytes(self.source.read_bytes())
        stat = self.source.stat()
        self.original = {'inode':stat.st_ino, 'device':stat.st_dev, 'backup':'source.py'}
        self.m.ORIGINALS = {str(self.source):self.original}
        self.m.REPLACEMENTS = [{'host_source':str(self.source),'path':'source.py',
                               'baseline_sha256':self.m.digest(self.source.read_bytes())}]
        self.m.MANIFEST = {'source_head':'test'}
        self.m.PAYLOAD = {'seal_hashes':{}, 'seal_host_paths':{}}
        self.calls = []
        self.m.preflight = Mock()
        self.m.stop_backend = Mock(side_effect=lambda **k:self.calls.append('stop'))
        self.m.db_probe = Mock(return_value={'hashes':{'market_quotes':'same'}})
        self.m.check_running = Mock()
        self.m.config = Mock(return_value={})
        self.m.verify_module_hashes = Mock(return_value={})
        self.m.seal_probe = Mock(return_value={})
        self.m.smoke = Mock()
        self.m.api_probe = Mock()
        self.m.frontend_http_probe = Mock(return_value={'verified_http_resources':3})
        self.m.inspect = Mock(return_value={'State':{'Running':True},'Mounts':[]})
        self.m.healthy = Mock()
        self.m.up = Mock(side_effect=lambda svc,new:self.calls.append(('up',svc,new)))

    def test_inplace_shorter_write_preserves_inode_permissions(self):
        self.m.overwrite(self.source,b'new',self.original)
        self.assertEqual(self.source.read_bytes(),b'new')
        self.assertEqual(self.source.stat().st_ino,self.original['inode'])
        self.assertEqual(self.source.stat().st_mode&0o777,0o640)

    def test_inode_drift_refuses_before_modifying(self):
        with self.assertRaises(RuntimeError):
            self.m.overwrite(self.source,b'new',{**self.original,'inode':self.original['inode']+1})
        self.assertEqual(self.source.read_bytes(),b'original_long_content')

    def test_preflight_failure_never_stops_or_overwrites(self):
        self.m.preflight.side_effect = RuntimeError('preflight_refused')
        self.assertEqual(self.m.main(),1)
        self.m.stop_backend.assert_not_called()
        self.m.up.assert_not_called()
        self.assertEqual(self.source.read_bytes(),b'original_long_content')

    def test_check_only_never_stops_or_overwrites(self):
        self.m.PAYLOAD['check_only'] = True
        self.assertEqual(self.m.main(),0)
        self.m.stop_backend.assert_not_called()
        self.assertEqual(self.source.read_bytes(),b'original_long_content')

    def test_success_stops_before_write_and_validates_both_services(self):
        overwrite = self.m.overwrite
        def write(*args):
            self.assertEqual(self.calls,['stop'])
            self.calls.append('overwrite')
            overwrite(*args)
        self.m.overwrite = write
        self.assertEqual(self.m.main(),0)
        self.assertEqual(self.calls,['stop','overwrite',('up','backend',True),('up','frontend',True)])
        self.assertEqual(self.m.STATE['stage'],'complete')
        self.assertEqual(self.source.read_bytes(),b'new')
        self.m.smoke.assert_called_once()
        self.m.api_probe.assert_called_once()

    def test_backend_failure_restores_original_before_restart(self):
        def up(service,new):
            self.calls.append(('up',service,new))
            if new:raise RuntimeError('new_backend_failed')
            self.assertEqual(self.source.read_bytes(),b'original_long_content')
        self.m.up.side_effect = up
        self.assertEqual(self.m.main(),1)
        self.assertEqual(self.m.STATE['stage'],'rolled_back')
        self.assertEqual(self.calls,['stop',('up','backend',True),'stop',('up','backend',False)])
        self.assertEqual(self.source.stat().st_ino,self.original['inode'])

    def test_unsafe_rollback_preferences_prevent_restart(self):
        self.m.up.side_effect = RuntimeError('new_backend_failed')
        self.m.db_probe.side_effect = [{'hashes':{}},RuntimeError('unsafe_supply_preference')]
        self.assertEqual(self.m.main(),1)
        self.assertEqual(self.m.STATE['stage'],'rollback_failed_backend_stop_requested')
        self.assertEqual(self.m.up.call_count,1)
        self.assertEqual(self.m.stop_backend.call_count,3)

    def test_frontend_failure_rolls_back_both_chains(self):
        def up(service,new):
            self.calls.append(('up',service,new))
            if service=='frontend' and new:raise RuntimeError('frontend_failed')
        self.m.up.side_effect = up
        self.assertEqual(self.m.main(),1)
        self.assertEqual(self.m.STATE['stage'],'rolled_back')
        self.assertEqual(self.calls[-2:],[('up','backend',False),('up','frontend',False)])
        self.assertEqual(self.source.read_bytes(),b'original_long_content')


class FrontendReadinessTests(unittest.TestCase):
    def setUp(self):
        self.m = load()
        self.expected = {'index.html': self.m.digest(b'expected')}

    def test_connection_refused_then_ready_checks_bytes(self):
        opener = Mock()
        opener.open.side_effect = [urllib.error.URLError(ConnectionRefusedError()), io.BytesIO(b'expected')]
        with patch('urllib.request.build_opener', return_value=opener), patch('time.sleep') as sleep:
            result = self.m.verify_frontend_resources(self.expected)
        self.assertEqual(result, {'verified_http_resources': 1, 'attempts': 2})
        sleep.assert_called_once()

    def test_wrong_content_is_not_retried(self):
        opener = Mock()
        opener.open.return_value = io.BytesIO(b'wrong')
        with patch('urllib.request.build_opener', return_value=opener), patch('time.sleep') as sleep:
            with self.assertRaisesRegex(RuntimeError, 'frontend_http_hash_mismatch'):
                self.m.verify_frontend_resources(self.expected)
        sleep.assert_not_called()

    def test_http_error_is_not_retried(self):
        opener = Mock()
        opener.open.side_effect = urllib.error.HTTPError('http://frontend/index.html', 404, 'missing', {}, None)
        with patch('urllib.request.build_opener', return_value=opener), patch('time.sleep') as sleep:
            with self.assertRaisesRegex(RuntimeError, 'frontend_http_status_failed'):
                self.m.verify_frontend_resources(self.expected)
        sleep.assert_not_called()

    def test_unavailable_listener_has_bounded_deadline(self):
        opener = Mock()
        opener.open.side_effect = urllib.error.URLError(ConnectionRefusedError())
        with patch('urllib.request.build_opener', return_value=opener), patch('time.sleep') as sleep, \
                patch('time.monotonic', side_effect=[0, 0, 45]):
            with self.assertRaisesRegex(RuntimeError, 'frontend_http_ready_timeout'):
                self.m.verify_frontend_resources(self.expected)
        self.assertEqual(opener.open.call_count, 1)
        sleep.assert_not_called()


if __name__=='__main__':
    unittest.main(verbosity=2,buffer=True)
