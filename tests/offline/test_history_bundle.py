import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('history_bundle',ROOT/'deploy/build_history_contract_bundle.py')
bundle=importlib.util.module_from_spec(spec)
spec.loader.exec_module(bundle)


class HistoryBundleTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.repo=self.root/'repo'
        self.output=self.root/'release'
        for path in bundle.FILES+bundle.TOOLS:
            target=self.repo/path
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_text('new '+path)
        self.audit={'module_hashes':{p:bundle.sha(('old '+p).encode()) for p in bundle.FILES[1:]},
                    'runtime':{'runtime_sha256':'sealed'}}
        self.audit['module_hashes']['app/services/quote_contract.py']='guarded-v12'
        self.dirty=False
        self.changed=list(bundle.FILES)
    def git(self, args):
        command=args[3:]
        if command[0]=='status':return b' M x' if self.dirty else b''
        if command[0]=='diff':return '\n'.join(self.changed).encode()
        if command[0]=='rev-parse':return b'committed-head\n'
        if command[0]=='show':return ('old '+command[1].split(':',1)[1]).encode()
        raise AssertionError(command)
    def build(self, release_dir=Path('/srv/history/staged')):
        with patch.object(bundle.subprocess,'check_output',side_effect=self.git):
            return bundle.build(self.repo,self.output,release_dir,self.audit)
    def test_four_readonly_mounts_and_exact_frozen_hashes(self):
        manifest=self.build()
        self.assertEqual(len(manifest['files']),4)
        self.assertEqual(manifest['expected_backend_mounts_after'],31)
        volumes=json.loads((self.output/'compose.history.json').read_text())['services']['backend']['volumes']
        self.assertEqual({v['target'] for v in volumes},{'/app/'+p for p in bundle.FILES})
        self.assertTrue(all(v['read_only'] and not v['bind']['create_host_path'] for v in volumes))
        for line in (self.output/'SHA256SUMS').read_text().splitlines():
            digest,path=line.split('  ')
            self.assertFalse(Path(path).is_absolute())
            self.assertEqual(digest,hashlib.sha256((self.output/path).read_bytes()).hexdigest())
        with self.assertRaises(FileExistsError):self.build()
    def test_dirty_tree_cannot_be_frozen(self):
        self.dirty=True
        with self.assertRaisesRegex(ValueError,'commit_before'):self.build()
        self.assertFalse(self.output.exists())
    def test_unlisted_app_change_is_rejected(self):
        self.changed.append('app/unreviewed.py')
        with self.assertRaisesRegex(ValueError,'scope_mismatch'):self.build()
        self.assertFalse(self.output.exists())
    def test_production_baseline_mismatch_is_rejected(self):
        self.audit['module_hashes'][bundle.FILES[1]]='wrong'
        with self.assertRaisesRegex(ValueError,'baseline_mismatch'):self.build()
        self.assertFalse(self.output.exists())
    def test_relative_target_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'absolute_production'):self.build(Path('relative'))
        self.assertFalse(self.output.exists())


if __name__=='__main__':unittest.main()
