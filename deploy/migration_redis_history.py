"""Copy verified permanent terminal history only, from a frozen isolated Redis.

Callers must supply a MongoDB terminal-status lookup and an empty isolated target.
No application import, queue replay, credential parsing, or credential output.
Real restore requires approval and source/cold-clone isolation outside this module.
"""
from dataclasses import dataclass, field
import re

SETS = {b'qa:completed': b'completed', b'qa:failed': b'failed'}
MAX_TASKS = 10000


@dataclass(frozen=True)
class TerminalException:
    # Never expose business identity through repr/logging.
    task_id: bytes = field(repr=False)


def parse_terminal_exception(policy, manifest_sha256, *, rehearsal_only=False, source_frozen=False):
    """One exact task + manifest + purpose; never promote a rehearsal approval.

    Cutover staging additionally requires an explicit frozen-source assertion.
    This permits isolated restoration only, NOT production startup. The caller
    must obtain a fresh policy approved for the final snapshot; changing these
    flags does not convert the old rehearsal policy into a cutover approval.
    """
    if (type(rehearsal_only) is not bool or type(source_frozen) is not bool
            or rehearsal_only == source_frozen):
        raise ValueError('Terminal exception purpose confirmation required')
    purpose = 'rehearsal_only' if rehearsal_only else 'cutover_staging'
    if (not isinstance(policy, dict)
            or set(policy) != {'schema', 'kind', 'purpose', 'snapshot_manifest_sha256', 'entries'}
            or type(policy['schema']) is not int or policy['schema'] != 1
            or policy['kind'] != 'approved_single_terminal_difference'
            or policy['purpose'] != purpose
            or not isinstance(manifest_sha256, str) or not re.fullmatch('[0-9a-f]{64}', manifest_sha256)
            or policy['snapshot_manifest_sha256'] != manifest_sha256
            or not isinstance(policy['entries'], list) or len(policy['entries']) != 1):
        raise ValueError('Terminal exception scope mismatch')
    entry = policy['entries'][0]
    if (not isinstance(entry, dict)
            or set(entry) != {'task_id', 'mongo_collection', 'mongo_status', 'redis_status'}
            or entry['mongo_collection'] != 'unified_analysis_tasks'
            or entry['mongo_status'] != 'cancelled' or entry['redis_status'] != 'failed'
            or not isinstance(entry['task_id'], str)):
        raise ValueError('Unsupported terminal exception')
    task_id = entry['task_id'].encode('utf-8', errors='strict')
    if not task_id or len(task_id) > 256 or b'\0' in task_id:
        raise ValueError('Invalid exception identity')
    return TerminalException(task_id)


def mongo_terminal_status(database, task_id, *, exception=None):
    """Use the two source-code task collections and their real task_id field.

    simple_analysis_service and task_analysis_service use task_id, NOT Mongo _id.
    Accept corroborating duplicates across the legacy/unified collections only
    when both terminal statuses agree; reject duplicates within either one.
    """
    identifier = task_id.decode('utf-8', errors='strict')
    statuses = []
    matched = {}
    for name in ('analysis_tasks', 'unified_analysis_tasks'):
        rows = list(database[name].find({'task_id': identifier}, {'status': 1, '_id': 0}).limit(2))
        if len(rows) > 1:
            raise ValueError('Duplicate Mongo task identity')
        matched[name] = rows
        if rows:
            status = rows[0].get('status')
            approved_cancelled = (exception is not None and task_id == exception.task_id
                                  and name == 'unified_analysis_tasks' and status == 'cancelled')
            if status not in ('completed', 'failed') and not approved_cancelled:
                raise ValueError('Mongo task is not terminal')
            statuses.append(status)
    if exception is not None and task_id == exception.task_id:
        if (matched['analysis_tasks'] or len(matched['unified_analysis_tasks']) != 1
                or statuses != ['cancelled']):
            raise ValueError('Approved Mongo record changed')
    if not statuses or len(set(statuses)) != 1:
        raise ValueError('Missing or conflicting Mongo terminal records')
    return statuses[0].encode()


def copy_terminal_history(source, target, mongo_status, *, exception=None):
    if target.dbsize() != 0:
        raise ValueError('Target Redis must be empty')
    if source.exists(b'qa:ready', b'qa:processing'):
        raise ValueError('Unfinished queue state requires review; do not drop or replay it')
    tasks, keys, payloads, exceptions_used = {}, set(), {}, set()
    for key, status in SETS.items():
        if not source.exists(key):
            continue
        if source.type(key) != b'set' or source.pttl(key) != -1:
            raise ValueError('Terminal set type or lifetime mismatch')
        if source.scard(key) > MAX_TASKS:
            raise ValueError('Terminal history needs manual size review')
        for task_id in source.smembers(key):
            if task_id in tasks or not task_id or len(task_id) > 256:
                raise ValueError('Invalid or conflicting terminal task')
            actual = mongo_status(task_id)
            if actual != status:
                if (exception is None or task_id != exception.task_id
                        or actual != b'cancelled' or status != b'failed'):
                    raise ValueError('MongoDB terminal status mismatch')
                exceptions_used.add(task_id)
            tasks[task_id] = status
        keys.add(key)
    if len(tasks) > MAX_TASKS:
        raise ValueError('Terminal history needs manual size review')
    if exception is not None and exceptions_used != {exception.task_id}:
        raise ValueError('Approved exception absent or no longer matches')
    for task_id, status in tasks.items():
        key = b'qa:task:' + task_id
        if (source.type(key) != b'hash' or source.pttl(key) != -1
                or source.hget(key, b'status') != status):
            raise ValueError('Terminal task hash mismatch')
        keys.add(key)
    # Unknown permanent state must be reviewed, not silently called garbage.
    for count, key in enumerate(source.scan_iter(count=1000), start=1):
        if count > 1000000:
            raise ValueError('Redis scan exceeds reviewed size')
        if source.pttl(key) == -1 and key not in keys:
            raise ValueError('Unclassified permanent Redis key')
    total = 0
    for key in keys:
        payload = source.dump(key)
        if payload is None or len(payload) > 16 * 1024 * 1024:
            raise ValueError('Missing or oversized terminal payload')
        total += len(payload)
        if total > 128 * 1024 * 1024:
            raise ValueError('Terminal history exceeds reviewed size')
        payloads[key] = payload
    if target.dbsize() != 0:
        raise ValueError('Target Redis changed during review')
    pipe = target.pipeline(transaction=True)
    for key, payload in sorted(payloads.items()):
        pipe.restore(key, 0, payload, replace=False)
    pipe.execute()
    if set(target.scan_iter()) != keys:
        raise ValueError('Restored key set differs')
    for key in keys:
        equal = (source.smembers(key) == target.smembers(key) if key in SETS
                 else source.hgetall(key) == target.hgetall(key))
        if not equal or target.pttl(key) != -1:
            raise ValueError('Restored history differs')
    result = {'tasks': len(tasks), 'keys': len(keys), 'all_fields_equal': True}
    if exception is not None:
        result.update(approved_terminal_exceptions_used=1, historical_status_difference_preserved=True)
    return result
