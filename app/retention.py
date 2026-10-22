"""Bounded maintenance of disposable evidence; replication state is never aged out."""
import os
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone


GROUP = 'lab-range-bluegreen-v4'
LOCK = 70261025
BATCH_SIZE = 500
MAX_BATCHES = 20


@dataclass(frozen=True)
class Settings:
    log_retention_days: int = 7
    audit_retention_days: int = 7
    kafka_retention_days: int = 7
    log_cleanup_enabled: bool = True
    log_cleanup_interval_hours: int = 24

    @classmethod
    def from_env(cls):
        values = {}
        for name in ('log_retention_days', 'audit_retention_days', 'kafka_retention_days',
                     'log_cleanup_interval_hours'):
            values[name] = int(os.getenv(name.upper(), str(getattr(cls, name))))
            if values[name] < 1:
                raise ValueError(name.upper() + ' must be a positive integer')
        enabled = os.getenv('LOG_CLEANUP_ENABLED', 'true').lower()
        if enabled not in ('true', 'false'):
            raise ValueError('LOG_CLEANUP_ENABLED must be true or false')
        return cls(**values, log_cleanup_enabled=enabled == 'true')


def topic_configs(settings, events, audit):
    if events == audit:
        raise ValueError('KAFKA_TOPIC and KAFKA_AUDIT_TOPIC must differ')
    common = {'cleanup.policy': 'delete', 'retention.bytes': '-1', 'segment.ms': '3600000'}
    return {
        # Broker expiry ignores consumer offsets. Only our acknowledged frontier
        # may expire data events, including while the consumer is offline.
        events: {**common, 'retention.ms': '-1', 'message.timestamp.type': 'LogAppendTime'},
        audit: {**common, 'retention.ms': str(settings.kafka_retention_days * 86400000)},
    }


def configure_topics(admin, configs):
    from confluent_kafka.admin import ConfigResource, ConfigEntry, AlterConfigOpType, ResourceType
    resources = []
    for topic, config in configs.items():
        resource = ConfigResource(ResourceType.TOPIC, topic)
        for name, value in config.items():
            resource.add_incremental_config(ConfigEntry(name, value, incremental_operation=AlterConfigOpType.SET))
        resources.append(resource)
    for future in admin.incremental_alter_configs(resources, request_timeout=8).values():
        future.result(timeout=10)


SCHEMA_SQL = '''
CREATE TABLE IF NOT EXISTS replica.log_cleanup_state (
 job TEXT PRIMARY KEY, document JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS replica.log_cleanup_history (
 id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 finished_at TIMESTAMPTZ NOT NULL, document JSONB NOT NULL
);
GRANT SELECT ON replica.log_cleanup_state TO monitor_reader;
'''


def ensure_schema(conn):
    with conn.transaction():
        conn.execute("SET LOCAL lock_timeout = '1s'")
        conn.execute("SET LOCAL statement_timeout = '5s'")
        conn.execute('SELECT pg_advisory_xact_lock(70261023)')
        conn.execute(SCHEMA_SQL)


# No run, cycle, staging, generation, hash, checkpoint or journal is deleted.
# Terminal run IDs and cycle IDs are also replay deduplication markers.
POLICIES = {
    'transitions': ('transitioned_at', '''id <> (SELECT max(id) FROM replica.transitions)'''),
    'source_catalog_observations': ('observed_at', '''EXISTS (
        SELECT 1 FROM replica.source_catalog_observations newer
        WHERE newer.source_key=t.source_key AND newer.observation_id>t.observation_id)'''),
    'discovered_schema_history': ('observed_at', '''EXISTS (
        SELECT 1 FROM replica.discovered_schema_history newer
        WHERE newer.table_key=t.table_key AND newer.id>t.id)'''),
    'log_cleanup_history': ('finished_at', 'TRUE'),
}


class PruneFailure(RuntimeError):
    def __init__(self, error, removed):
        super().__init__(str(error))
        self.removed = removed


def prune(conn, schema, table, predicate, params, dry_run=False):
    """Short transactions, skip row locks, bounded work even with a large backlog."""
    from psycopg import sql
    relation = sql.Identifier(schema, table)
    total = 0
    try:
        for _ in range(1 if dry_run else MAX_BATCHES):
            with conn.transaction():
                conn.execute("SET LOCAL lock_timeout = '1s'")
                conn.execute("SET LOCAL statement_timeout = '5s'")
                if dry_run:
                    return conn.execute(sql.SQL('SELECT count(*) FROM {} t WHERE ').format(relation)
                                        + sql.SQL(predicate), params).fetchone()[0]
                query = sql.SQL('''WITH candidates AS (
                    SELECT t.ctid FROM {} t WHERE {} LIMIT %s FOR UPDATE OF t SKIP LOCKED
                ) DELETE FROM {} t USING candidates c WHERE t.ctid=c.ctid''').format(
                    relation, sql.SQL(predicate), relation)
                removed = conn.execute(query, (*params, BATCH_SIZE)).rowcount
            total += removed
            if removed < BATCH_SIZE:
                break
    except Exception as exc:
        raise PruneFailure(exc, total) from exc
    return total


def pending(conn):
    return any(conn.execute(query).fetchone()[0] for query in (
        "SELECT EXISTS(SELECT 1 FROM replica.sync_runs WHERE status IN ('extracting','loading'))",
        "SELECT EXISTS(SELECT 1 FROM replica.discovered_runs WHERE status='loading')",
        "SELECT EXISTS(SELECT 1 FROM replica.discovered_cycles WHERE status='loading')",
        "SELECT EXISTS(SELECT 1 FROM replica.schema_backfill_runs WHERE status='loading')",
        "SELECT EXISTS(SELECT 1 FROM replica.schema_state WHERE pending_hash IS NOT NULL)",
    ))


def expire_events(conn, admin, topic, cutoff, dry_run=False):
    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import OffsetSpec
    if pending(conn):
        return {'status': 'preserved_pending', 'removed_offsets': 0}
    metadata = admin.list_topics(topic=topic, timeout=8)
    if metadata.topics[topic].error or set(metadata.topics[topic].partitions) != {0}:
        raise RuntimeError('Event retention requires the managed single-partition topic')
    partition = TopicPartition(topic, 0)
    group = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(GROUP, [partition])], require_stable=True,
        request_timeout=8)[GROUP].result(timeout=10)
    if len(group.topic_partitions) != 1:
        raise RuntimeError('Missing consumer offset evidence')
    committed = group.topic_partitions[0]
    if (committed.topic, committed.partition) != (topic, 0):
        raise RuntimeError('Unexpected consumer offset evidence')
    if committed.error or committed.offset < 0:
        return {'status': 'preserved_no_committed_offset', 'removed_offsets': 0}

    def offset(spec):
        return admin.list_offsets({partition: spec}, request_timeout=8)[partition].result(timeout=10).offset

    earliest = offset(OffsetSpec.earliest())
    by_age = offset(OffsetSpec.for_timestamp(int(cutoff.timestamp() * 1000)))
    if by_age < 0:
        by_age = offset(OffsetSpec.latest())
    boundary = min(committed.offset, by_age)
    result = {'status': 'dry_run' if dry_run else 'ok', 'earliest_offset': earliest,
              'committed_offset': committed.offset, 'delete_before_offset': boundary,
              'eligible_offsets': max(0, boundary - earliest), 'removed_offsets': 0}
    if not dry_run and boundary > earliest:
        # Never pass -1 (OFFSET_END), and never delete unacknowledged messages.
        removed = admin.delete_records([TopicPartition(topic, 0, boundary)],
                                       request_timeout=8, operation_timeout=5)
        low = next(iter(removed.values())).result(timeout=10).low_watermark
        result['removed_offsets'] = max(0, low - earliest)
    return result


def latest(conn, job='automatic'):
    row = conn.execute('SELECT document FROM replica.log_cleanup_state WHERE job=%s', (job,)).fetchone()
    return row[0] if row else None


def cleanup(conn, settings, *, admin=None, topic=None, dry_run=False, automatic=False, now=None):
    """Caller owns an autocommit connection; history survives partial failures."""
    from psycopg.types.json import Jsonb
    started = now or datetime.now(timezone.utc)
    tick = time.monotonic()
    if not conn.execute('SELECT pg_try_advisory_lock(%s)', (LOCK,)).fetchone()[0]:
        return {'status': 'busy'}
    try:
        previous = latest(conn)
        interval = timedelta(minutes=1) if previous and previous['status'] == 'error' else timedelta(
            hours=settings.log_cleanup_interval_hours)
        if automatic and (not settings.log_cleanup_enabled or (previous and
                datetime.fromisoformat(previous['finished_at']) +
                interval > started)):
            return {'status': 'not_due' if settings.log_cleanup_enabled else 'disabled'}
        report = {'status': 'dry_run' if dry_run else 'ok', 'started_at': started.isoformat(),
                  'dry_run': dry_run, 'settings': asdict(settings), 'removed': {}, 'eligible': {}, 'errors': []}
        for table, (stamp, extra) in POLICIES.items():
            days = settings.log_retention_days if table == 'log_cleanup_history' else settings.audit_retention_days
            cutoff = started - timedelta(days=days)
            try:
                count = prune(conn, 'replica', table, f't.{stamp} < %s AND ({extra})', (cutoff,), dry_run)
                report['eligible' if dry_run else 'removed'][table] = count
            except Exception as exc:
                if not dry_run:
                    report['removed'][table] = getattr(exc, 'removed', 0)
                report['errors'].append({'component': table, 'error': str(exc)[:500]})
        if admin is not None:
            try:
                report['kafka'] = expire_events(conn, admin, topic,
                    started - timedelta(days=settings.kafka_retention_days), dry_run)
            except Exception as exc:
                report['errors'].append({'component': 'kafka', 'error': str(exc)[:500]})
        if report['errors']:
            report['status'] = 'error'
        report.update(finished_at=datetime.now(timezone.utc).isoformat(),
                      duration_seconds=round(time.monotonic() - tick, 3))
        # Dry runs have separate bookkeeping and never postpone scheduled cleanup.
        job = 'dry_run' if dry_run else 'automatic' if automatic else 'manual'
        with conn.transaction():
            conn.execute('''INSERT INTO replica.log_cleanup_state VALUES(%s,%s)
                ON CONFLICT(job) DO UPDATE SET document=excluded.document''', (job, Jsonb(report)))
            conn.execute('INSERT INTO replica.log_cleanup_history(finished_at,document) VALUES(%s,%s)',
                         (report['finished_at'], Jsonb(report)))
        return report
    finally:
        conn.execute('SELECT pg_advisory_unlock(%s)', (LOCK,))


def worker(dsn, admin_factory, topic, log, stopping):
    import psycopg
    settings = Settings.from_env()
    while not stopping.is_set():
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                report = cleanup(conn, settings, admin=admin_factory(), topic=topic, automatic=True)
            if report['status'] not in ('not_due', 'disabled', 'busy'):
                log('log_cleanup', **report)
        except Exception as exc:
            log('log_cleanup_failed', error=str(exc)[:500])
        stopping.wait(30)


def start_worker(dsn, admin_factory, topic, log):
    Settings.from_env()  # Fail early on invalid configuration.
    stopping = threading.Event()
    threading.Thread(target=worker, args=(dsn, admin_factory, topic, log, stopping),
                     name='log-cleanup', daemon=True).start()
    return stopping
