"""Verify the production-only BSON acceptance on original records, across batches."""
import hashlib
from pathlib import Path
import tempfile
import unittest
from bson import BSON
from bson.raw_bson import RawBSONDocument
from verify_history_schedule import preserved


class Cursor(list):
    def max_time_ms(self,*_):return self


class Records:
    def __init__(self,rows):self.rows=rows
    def find(self,query):return Cursor(r for r in self.rows if r['_id'] in query['_id']['$in'])


class RecordTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'old.bson'
        self.raw=[BSON.encode(dict(_id=i,close=10.0)) for i in range(503)]
        self.path.write_bytes(b''.join(self.raw))
        self.expected=dict(count=503,sha256=hashlib.sha256(self.path.read_bytes()).hexdigest())
        self.rows=[RawBSONDocument(r) for r in self.raw]

    def test_preserves_all_originals_but_allows_new_rows(self):
        self.rows.append(RawBSONDocument(BSON.encode(dict(_id=600,close=12.0))))
        self.assertEqual(preserved(Records(self.rows),self.path,self.expected),503)

    def test_original_changed_after_first_batch_is_rejected(self):
        self.rows[-1]=RawBSONDocument(BSON.encode(dict(_id=502,close=99.0)))
        with self.assertRaisesRegex(RuntimeError,'original_record_changed'):preserved(Records(self.rows),self.path,self.expected)

    def test_truncated_backup_is_rejected(self):
        self.path.write_bytes(self.path.read_bytes()[:-1])
        with self.assertRaisesRegex(RuntimeError,'backup_truncated'):preserved(Records(self.rows),self.path,self.expected)


if __name__=='__main__':unittest.main()
