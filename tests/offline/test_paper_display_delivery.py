"""Delivery failure paths, without Docker, network or production files."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch


spec = importlib.util.spec_from_file_location('delivery', 'deploy/paper_display_delivery.py')
delivery = importlib.util.module_from_spec(spec); spec.loader.exec_module(delivery)


class Delivery(unittest.TestCase):
    def test_two_source_overwrite_preserves_inode_and_supports_partial_rollback(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root); sources = {}
            for i, target in enumerate(sorted(delivery.TARGETS)):
                source = root/str(i); source.write_bytes(b'original'); source.chmod(0o644)
                backup = root/'backup/sources'/target.removeprefix('/app/')
                backup.parent.mkdir(parents=True, exist_ok=True); backup.write_bytes(b'original')
                sources[target] = {'host_source': str(source), 'source': 'candidate'}
            with patch.object(delivery, 'REQUEST', {'sources': sources}, create=True), \
                 patch.object(delivery, 'ATTEMPT', root, create=True):
                first, second = sorted(sources)
                source = Path(sources[first]['host_source']); before = source.stat()
                delivery.overwrite(first, b'candidate')
                self.assertEqual(source.read_bytes(), b'candidate')
                self.assertEqual((source.stat().st_ino, source.stat().st_mode), (before.st_ino, before.st_mode))
                # Only the first write occurred: both targets must still be restorable.
                for target in (first, second): delivery.overwrite(target, b'original')
                self.assertTrue(all(Path(item['host_source']).read_bytes() == b'original' for item in sources.values()))

    def test_api_failure_rolls_back_both_sources_without_publishing(self):
        self.failure_case('api')

    def test_failed_application_start_still_restarts_original(self):
        self.failure_case('start')

    def failure_case(self, failure):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root); sources = {t: {'source': 'candidate'} for t in delivery.TARGETS}
            for target in sources:
                backup = root/'backup/sources'/target.removeprefix('/app/')
                backup.parent.mkdir(parents=True, exist_ok=True); backup.write_bytes(b'original')
            events = []; write = Mock(); publish = Mock()
            start = Mock(side_effect=[RuntimeError('start_failed'), None] if failure == 'start' else None)
            with patch.multiple(delivery, REQUEST={'sources': sources}, PAYLOAD={'check_only': False},
                 ATTEMPT=root, STATE={}, create=True), \
                 patch.multiple(delivery, preflight=Mock(return_value={}), event=events.append,
                    stop=Mock(), start=start, running=Mock(), snapshot=Mock(return_value={}),
                    protected=lambda x: x, jobs=Mock(), overwrite=write, save=Mock(),
                    publish_frontend=publish,
                    accept_api=Mock(side_effect=RuntimeError('api_failed') if failure == 'api' else None)):
                self.assertEqual(delivery.main(), 1)
            self.assertEqual(events[-1], 'rolled_back')
            self.assertEqual(write.call_count, 4)
            self.assertEqual(start.call_count, 2)
            publish.assert_not_called()
            self.assertEqual({call.args[0] for call in write.call_args_list[-2:]}, set(sources))
            self.assertTrue(all(call.args[1] == b'original' for call in write.call_args_list[-2:]))


if __name__ == '__main__':
    unittest.main()
