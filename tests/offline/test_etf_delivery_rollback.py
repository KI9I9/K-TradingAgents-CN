"""Exercise delivery failures without Docker, networking or production paths."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('delivery',ROOT/'deploy/etf_history_delivery.py')
d=importlib.util.module_from_spec(spec);spec.loader.exec_module(d)


class Rollback(unittest.TestCase):
    def test_absent_replacement_does_not_prevent_rollback_stop(self):
        with patch.object(d,'REQUEST',{'chain':['docker','compose','--project-name','test']},create=True), \
             patch.object(d,'run',return_value='') as run, patch.object(d,'info') as info:
            d.stop()
        info.assert_not_called()
        self.assertEqual(run.call_count,1)

    def test_failed_recreation_restores_original_chain_without_database_restore(self):
        before={'modules':{},'collections':{},'runtime':{}}
        up=Mock(side_effect=[RuntimeError('replacement removed'),None])
        with patch.multiple(d,STATE={},PAYLOAD={'check_only':False},ATTEMPT=Path('/unused'),create=True), \
             patch.object(d,'event') as event,patch.object(d,'save'), \
             patch.object(d,'preflight',return_value=before),patch.object(d,'stop') as stop, \
             patch.object(d,'up',up),patch.object(d,'running') as running, \
             patch.object(d,'snapshot',return_value=before),patch.object(d,'jobs'):
            self.assertEqual(d.main(),1)
        self.assertEqual([c.args[0] for c in up.call_args_list],[True,False])
        self.assertEqual(stop.call_count,2)
        running.assert_called_once_with(False)
        self.assertEqual(event.call_args.args,('rolled_back',))

    def test_failed_preflight_never_stops_live_backend(self):
        with patch.multiple(d,STATE={},create=True),patch.object(d,'event'), \
             patch.object(d,'preflight',side_effect=RuntimeError('active history task')), \
             patch.object(d,'stop') as stop,patch.object(d,'up') as up:
            self.assertEqual(d.main(),1)
        stop.assert_not_called();up.assert_not_called()

    def test_frontend_rollback_keeps_new_assets_but_restores_old_index(self):
        before={'modules':{},'collections':{},'runtime':{}}
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);front=root/'front';front.mkdir();(front/'index.html').write_text('new')
            (front/'new-hash.js').write_text('keep')
            backup=root/'backup/frontend';backup.mkdir(parents=True);(backup/'index.html').write_text('old')
            with patch.multiple(d,STATE={},PAYLOAD={'check_only':False},ATTEMPT=root,create=True), \
                 patch.object(d,'event') as event,patch.object(d,'preflight',return_value=before), \
                 patch.object(d,'stop'),patch.object(d,'up'),patch.object(d,'running'), \
                 patch.object(d,'snapshot',return_value=before),patch.object(d,'jobs'), \
                 patch.object(d,'publish_frontend',side_effect=RuntimeError('publish failed')), \
                 patch.object(d,'frontend_root',return_value=front):
                self.assertEqual(d.main(),1)
            self.assertEqual((front/'index.html').read_text(),'old')
            self.assertEqual((front/'new-hash.js').read_text(),'keep')
            self.assertEqual(event.call_args.args,('rolled_back',))


if __name__=='__main__':unittest.main()
