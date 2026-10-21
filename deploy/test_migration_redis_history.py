"""Reject unsafe restore prerequisites without writing to either database."""
import copy
import pickle
import unittest
from unittest.mock import Mock
from migration_redis_history import copy_terminal_history, mongo_terminal_status, parse_terminal_exception


def policy(task='cancelled-fixture'):
    return {'schema': 1, 'kind': 'approved_single_terminal_difference', 'purpose': 'rehearsal_only',
            'snapshot_manifest_sha256': 'a' * 64,
            'entries': [{'task_id': task, 'mongo_collection': 'unified_analysis_tasks',
                         'mongo_status': 'cancelled', 'redis_status': 'failed'}]}


class MemoryRedis:
    """Test-only DUMP/RESTORE double; pickle handles fixed binary fixtures only."""
    def __init__(self, values=None):
        self.values = copy.deepcopy(values or {})
        self.write_calls = 0
        self.pipeline_calls = 0

    def dbsize(self): return len(self.values)
    def exists(self, *keys): return sum(key in self.values for key in keys)
    def type(self, key):
        value = self.values.get(key)
        return b'set' if isinstance(value, set) else b'hash' if isinstance(value, dict) else b'none'
    def pttl(self, key): return -1 if key in self.values else -2
    def scard(self, key): return len(self.values[key])
    def smembers(self, key): return self.values[key].copy()
    def hget(self, key, field): return self.values[key].get(field)
    def hgetall(self, key): return self.values[key].copy()
    def scan_iter(self, **kwargs): return iter(self.values)
    def dump(self, key): return pickle.dumps(self.values[key])

    def pipeline(self, **kwargs):
        self.pipeline_calls += 1
        parent = self
        class Pipeline:
            def __init__(self): self.pending = []
            def restore(self, key, ttl, body, replace=False):
                if ttl != 0 or replace or key in parent.values: raise AssertionError()
                self.pending.append((key, body))
            def execute(self):
                for key, body in self.pending:
                    parent.values[key] = pickle.loads(body)
                    parent.write_calls += 1
        return Pipeline()


class RedisHistoryGates(unittest.TestCase):
    def lookup(self, legacy, unified):
        database = {}
        for name, rows in (('analysis_tasks', legacy), ('unified_analysis_tasks', unified)):
            database[name] = Mock()
            database[name].find.return_value.limit.return_value = rows
        return database

    def test_real_task_field_and_either_collection(self):
        for legacy, unified in (([{'status': 'completed'}], []), ([], [{'status': 'completed'}]),
                                ([{'status': 'completed'}], [{'status': 'completed'}])):
            database = self.lookup(legacy, unified)
            self.assertEqual(mongo_terminal_status(database, b'fixture'), b'completed')
            database['analysis_tasks'].find.assert_called_once_with({'task_id': 'fixture'}, {'status': 1, '_id': 0})

    def test_missing_duplicate_nonterminal_or_conflicting_mongo_rejected(self):
        for legacy, unified in (([], []), ([{'status': 'completed'}] * 2, []),
                                ([{'status': 'processing'}], []),
                                ([{'status': 'failed'}], [{'status': 'completed'}])):
            with self.assertRaises(ValueError):
                mongo_terminal_status(self.lookup(legacy, unified), b'fixture')

    def clients(self):
        source, target = Mock(), Mock()
        target.dbsize.return_value = 0
        source.exists.return_value = 0
        source.scan_iter.return_value = []
        return source, target

    def test_nonempty_target_rejected(self):
        source, target = self.clients()
        target.dbsize.return_value = 1
        with self.assertRaises(ValueError):
            copy_terminal_history(source, target, Mock())
        source.exists.assert_not_called()
        target.pipeline.assert_not_called()

    def test_unfinished_queue_rejected(self):
        source, target = self.clients()
        source.exists.return_value = 1
        with self.assertRaises(ValueError):
            copy_terminal_history(source, target, Mock())
        target.pipeline.assert_not_called()

    def test_unknown_permanent_key_rejected(self):
        source, target = self.clients()
        source.scan_iter.return_value = [b'unknown-fixture']
        source.pttl.return_value = -1
        with self.assertRaises(ValueError):
            copy_terminal_history(source, target, Mock())
        target.pipeline.assert_not_called()

    def test_mongo_mismatch_rejected(self):
        source, target = self.clients()
        source.exists.side_effect = lambda *keys: int(keys == (b'qa:completed',))
        source.type.return_value = b'set'
        source.pttl.return_value = -1
        source.scard.return_value = 1
        source.smembers.return_value = {b'fixture-task'}
        with self.assertRaises(ValueError):
            copy_terminal_history(source, target, lambda task_id: b'failed')
        target.pipeline.assert_not_called()

    def exception(self):
        return parse_terminal_exception(policy(), 'a' * 64, rehearsal_only=True)

    def terminal_fixture(self):
        return MemoryRedis({b'qa:completed': {b'completed-fixture'}, b'qa:failed': {b'cancelled-fixture'},
            b'qa:task:completed-fixture': {b'status': b'completed', b'extra': b'\x00\xfffixture'},
            b'qa:task:cancelled-fixture': {b'status': b'failed', b'extra': b'\x00\xffpreserved'}})

    def test_policy_is_single_task_snapshot_bound_and_rehearsal_only(self):
        self.assertEqual(self.exception().task_id, b'cancelled-fixture')
        self.assertNotIn('cancelled-fixture', repr(self.exception()))
        for digest, rehearsal in (('b' * 64, True), ('a' * 64, False), ('a' * 64, 1)):
            with self.assertRaises(ValueError):
                parse_terminal_exception(policy(), digest, rehearsal_only=rehearsal)

    def test_policy_rejects_wildcards_extra_entries_and_broader_statuses(self):
        bad = []
        value = policy(); value['entries'] *= 2; bad.append(value)
        value = policy(); value['entries'] = []; bad.append(value)
        value = policy(); value['snapshot_manifest_sha256'] = '*'; bad.append(value)
        value = policy(); value['entries'][0]['mongo_status'] = 'processing'; bad.append(value)
        value = policy(); value['entries'][0]['redis_status'] = 'completed'; bad.append(value)
        value = policy(); value['entries'][0]['mongo_collection'] = 'analysis_tasks'; bad.append(value)
        value = policy(); value['entries'][0]['task_id'] = ''; bad.append(value)
        value = policy(); value['entries'][0]['task_id'] = 'x' * 257; bad.append(value)
        value = policy(); value['allow_any'] = True; bad.append(value)
        for value in bad:
            with self.assertRaises(ValueError):
                parse_terminal_exception(value, 'a' * 64, rehearsal_only=True)

    def test_cutover_policy_requires_exact_purpose_fresh_manifest_and_source_freeze(self):
        final = policy()
        final['purpose'] = 'cutover_staging'
        final['snapshot_manifest_sha256'] = 'c' * 64
        exception = parse_terminal_exception(final, 'c' * 64, source_frozen=True)
        self.assertEqual(exception.task_id, b'cancelled-fixture')
        self.assertNotIn('cancelled-fixture', repr(exception))
        for flags in ({}, {'rehearsal_only': True}, {'source_frozen': 1},
                      {'rehearsal_only': True, 'source_frozen': True}):
            with self.subTest(flags=flags), self.assertRaises(ValueError):
                parse_terminal_exception(final, 'c' * 64, **flags)
        with self.assertRaises(ValueError):
            parse_terminal_exception(final, 'a' * 64, source_frozen=True)
        with self.assertRaises(ValueError):
            parse_terminal_exception(policy(), 'a' * 64, source_frozen=True)

    def test_cutover_exception_preserves_same_narrow_history_rules(self):
        final = policy()
        final['purpose'] = 'cutover_staging'
        exception = parse_terminal_exception(final, 'a' * 64, source_frozen=True)
        source, target = self.terminal_fixture(), MemoryRedis()
        original = copy.deepcopy(source.values)
        result = copy_terminal_history(source, target,
            {b'completed-fixture': b'completed', b'cancelled-fixture': b'cancelled'}.__getitem__,
            exception=exception)
        self.assertEqual(target.values, original)
        self.assertEqual(source.values, original)
        self.assertEqual(result['approved_terminal_exceptions_used'], 1)
        self.assertEqual(source.write_calls, 0)
        for mutation in ('queue', 'unknown', 'hash', 'other_difference'):
            source, target = self.terminal_fixture(), MemoryRedis()
            statuses = {b'completed-fixture': b'completed', b'cancelled-fixture': b'cancelled'}
            if mutation == 'queue': source.values[b'qa:ready'] = {b'fixture'}
            if mutation == 'unknown': source.values[b'unknown'] = {b'fixture'}
            if mutation == 'hash': source.values[b'qa:task:cancelled-fixture'][b'status'] = b'completed'
            if mutation == 'other_difference': statuses[b'completed-fixture'] = b'failed'
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                copy_terminal_history(source, target, statuses.__getitem__, exception=exception)
            self.assertEqual(target.pipeline_calls, 0)

    def test_cancelled_default_and_other_identity_are_rejected(self):
        for task, exception in ((b'cancelled-fixture', None), (b'other-fixture', self.exception())):
            with self.assertRaises(ValueError):
                mongo_terminal_status(self.lookup([], [{'status': 'cancelled'}]), task, exception=exception)

    def test_exact_cancelled_record_is_returned_without_normalizing_status(self):
        database = self.lookup([], [{'status': 'cancelled'}])
        self.assertEqual(mongo_terminal_status(database, b'cancelled-fixture', exception=self.exception()), b'cancelled')
        for collection in database.values():
            collection.update_one.assert_not_called()
            collection.delete_one.assert_not_called()

    def test_exception_does_not_accept_changed_missing_or_duplicate_mongo_record(self):
        for legacy, unified in (([], []), ([], [{'status': 'failed'}]), ([], [{'status': 'processing'}]),
                                ([], [{'status': 'cancelled'}] * 2),
                                ([{'status': 'failed'}], [{'status': 'cancelled'}]),
                                ([{'status': 'cancelled'}], [])):
            with self.assertRaises(ValueError):
                mongo_terminal_status(self.lookup(legacy, unified), b'cancelled-fixture', exception=self.exception())

    def test_exact_exception_preserves_all_redis_fields_without_rewriting_source(self):
        source, target = self.terminal_fixture(), MemoryRedis()
        original = copy.deepcopy(source.values)
        statuses = {b'completed-fixture': b'completed', b'cancelled-fixture': b'cancelled'}
        result = copy_terminal_history(source, target, statuses.__getitem__, exception=self.exception())
        self.assertEqual(target.values, original)
        self.assertEqual(source.values, original)
        self.assertEqual(source.write_calls, 0)
        self.assertEqual(result, {'tasks': 2, 'keys': 4, 'all_fields_equal': True,
            'approved_terminal_exceptions_used': 1, 'historical_status_difference_preserved': True})
        self.assertEqual(statuses[b'cancelled-fixture'], b'cancelled')
        self.assertEqual(target.hget(b'qa:task:cancelled-fixture', b'status'), b'failed')

    def test_other_difference_or_missing_approved_difference_prevents_all_writes(self):
        for statuses in ({b'completed-fixture': b'failed', b'cancelled-fixture': b'cancelled'},
                         {b'completed-fixture': b'completed', b'cancelled-fixture': b'failed'},
                         {b'completed-fixture': b'completed', b'cancelled-fixture': b'processing'}):
            source, target = self.terminal_fixture(), MemoryRedis()
            with self.assertRaises(ValueError):
                copy_terminal_history(source, target, statuses.__getitem__, exception=self.exception())
            self.assertEqual(target.pipeline_calls, 0)
            self.assertEqual(target.dbsize(), 0)

    def test_default_still_rejects_the_approved_difference_without_explicit_exception(self):
        source, target = self.terminal_fixture(), MemoryRedis()
        with self.assertRaises(ValueError):
            copy_terminal_history(source, target,
                {b'completed-fixture': b'completed', b'cancelled-fixture': b'cancelled'}.__getitem__)
        self.assertEqual(target.pipeline_calls, 0)

    def test_exception_does_not_bypass_unfinished_queue_unknown_key_or_hash_difference(self):
        for mutation in ('queue', 'unknown', 'hash'):
            source, target = self.terminal_fixture(), MemoryRedis()
            if mutation == 'queue': source.values[b'qa:ready'] = {b'fixture'}
            if mutation == 'unknown': source.values[b'unknown'] = {b'fixture'}
            if mutation == 'hash': source.values[b'qa:task:cancelled-fixture'][b'status'] = b'completed'
            with self.assertRaises(ValueError):
                copy_terminal_history(source, target,
                    {b'completed-fixture': b'completed', b'cancelled-fixture': b'cancelled'}.__getitem__,
                    exception=self.exception())
            self.assertEqual(target.pipeline_calls, 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
