"""Copy local audit metadata to the monitor DB; never write to the mirror DB.

The source read and the projection publication each use one transaction. Failure
of either leaves the previous complete projection available. Business payloads,
staging rows and journals never leave the replication database.
"""
import json
import os
import signal
import threading
from datetime import datetime, timezone
from tempfile import SpooledTemporaryFile

import psycopg
import monitor


CATALOG_RUN_SQL = '''SELECT r.run_id::text,t.engine,t.source_schema,t.source_table,
    r.mode,r.status,r.manifest,r.base_generation,r.result_generation,r.full_snapshot,
    r.delta_count,r.seen_count,r.inserted_count,r.updated_count,r.deleted_count,
    r.logical_bytes,r.error,r.started_at,r.finished_at
    FROM replica.discovered_runs r JOIN replica.discovered_tables t USING(table_key)'''
CATALOG_RUN_KEYS = ('run_id','engine','schema','table','mode','status','manifest',
    'base_generation','result_generation','full_snapshot','delta_count','seen_count',
    'inserted_count','updated_count','deleted_count','logical_bytes','error','started_at','finished_at')
LEGACY_RUN_SQL = '''SELECT r.run_id::text,r.mode,r.status,r.sweep_low,r.sweep_high,r.cutoff,
    r.data_schema_hash,r.delta_count,r.sweep_count,r.inserted_count,r.updated_count,
    r.deleted_count,r.logical_kafka_bytes,r.error,r.started_at,r.finished_at,r.result_version,
    r.source_engine,r.sweep_target_max_id,CASE WHEN r.source_engine='postgresql' THEN v.manifest END
    FROM replica.sync_runs r LEFT JOIN replica.schema_versions v ON v.schema_hash=r.data_schema_hash'''
LEGACY_RUN_KEYS = ('run_id','mode','status','sweep_low','sweep_high','source_cutoff',
    'data_schema_hash','delta_count','sweep_count','inserted_count','updated_count','deleted_count',
    'logical_kafka_bytes','error','started_at','finished_at','result_version','source_engine',
    'sweep_target_max_id','manifest')


def documents(source, mode):
    """Stream all historical evidence, including runs outside the UI's recent 40."""
    with source.cursor(name='monitor_runs') as cur:
        cur.execute(CATALOG_RUN_SQL if mode == 'catalog' else LEGACY_RUN_SQL)
        keys = CATALOG_RUN_KEYS if mode == 'catalog' else LEGACY_RUN_KEYS
        for row in cur:
            record = {key: monitor._iso(value) for key, value in zip(keys, row)}
            yield {'kind': 'run', 'key': record['run_id'], 'document': record}
    if mode == 'catalog':
        with source.cursor(name='monitor_tables') as cur:
            cur.execute('SELECT table_key,observed_manifest,observed_at,status FROM replica.discovered_tables')
            for key, manifest, stamp, status in cur:
                yield {'kind': 'table_key', 'key': key, 'document': {
                    'manifest': manifest, 'observed_at': monitor._iso(stamp), 'status': status}}
    with source.cursor(name='monitor_sources') as cur:
        cur.execute('''SELECT DISTINCT ON (source_key) source_key,native_manifest,observed_at,classification
            FROM replica.source_catalog_observations ORDER BY source_key,observation_id DESC''')
        for key, manifest, stamp, status in cur:
            yield {'kind': 'source_key', 'key': key, 'document': {
                'manifest': manifest, 'observed_at': monitor._iso(stamp), 'status': status}}


def write_documents(target, mode, batch):
    target.execute('''INSERT INTO monitoring.details(mode,kind,detail_key,document)
        SELECT %s,item.kind,item.key,item.document
        FROM jsonb_to_recordset(%s::jsonb) AS item(kind text,key text,document jsonb)
        ON CONFLICT(mode,kind,detail_key) DO UPDATE SET document=excluded.document
        WHERE monitoring.details.document IS DISTINCT FROM excluded.document''',
        (mode, json.dumps(batch, ensure_ascii=False)))


def write_state(target, mode, now, source_status, snapshot=None):
    target.execute('''INSERT INTO monitoring.collection_state
        (mode,snapshot,attempted_at,collected_at,source_status)
        VALUES(%s,%s::jsonb,%s,CASE WHEN %s THEN %s END,%s::jsonb)
        ON CONFLICT(mode) DO UPDATE SET attempted_at=excluded.attempted_at,
        source_status=excluded.source_status,
        snapshot=coalesce(excluded.snapshot,monitoring.collection_state.snapshot),
        collected_at=coalesce(excluded.collected_at,monitoring.collection_state.collected_at)''',
        (mode, json.dumps(snapshot) if snapshot is not None else None, now,
         source_status['ok'], now, json.dumps(source_status)))


def collect_once(source_dsn, target_dsn, mode, *, connect=psycopg.connect, now=None):
    stamp = now or datetime.now(timezone.utc)
    source_status = {'ok': False, 'connected': False, 'error': None, 'error_code': None}
    # Source commits before the projection transaction starts. Spill historical
    # metadata to a temporary file instead of growing memory with the audit log.
    with SpooledTemporaryFile(mode='w+t', max_size=8 * 1024 * 1024) as spool:
        try:
            with connect(source_dsn, connect_timeout=3) as source:
                source_status['connected'] = True
                source.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
                source.execute("SET LOCAL statement_timeout = '10s'")
                source.execute("SET LOCAL lock_timeout = '2s'")
                snapshot = monitor._query_db(source, stamp)
                for item in documents(source, mode):
                    spool.write(json.dumps(item, ensure_ascii=False) + '\n')
        except Exception as exc:
            source_status['error'] = f'{type(exc).__name__}: {str(exc)[:230]}'
            source_status['error_code'] = getattr(exc, 'sqlstate', None)
            with connect(target_dsn, connect_timeout=3) as target:
                write_state(target, mode, stamp, source_status)
            print(json.dumps({'event': 'monitor_collection_failed', 'error': source_status['error']}), flush=True)
            return False
        # Target failures propagate without marking the mirror DB unavailable.
        spool.seek(0)
        with connect(target_dsn, connect_timeout=3) as target:
            batch = []
            for line in spool:
                batch.append(json.loads(line))
                if len(batch) >= 250:
                    write_documents(target, mode, batch)
                    batch = []
            if batch:
                write_documents(target, mode, batch)
            source_status['ok'] = True
            write_state(target, mode, stamp, source_status, snapshot)
    return True


def main():
    interval = int(os.getenv('LAB_MONITOR_COLLECTION_SECONDS', '10'))
    if interval < 1:
        raise ValueError('Collection interval must be positive')
    stopping = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stopping.set())
    while not stopping.is_set():
        try:
            collect_once(os.environ['LOCAL_DSN'], os.environ['MONITOR_WRITE_DSN'], monitor.SYNC_MODE)
        except Exception as exc:
            print(json.dumps({'event': 'monitor_store_unavailable',
                              'error': f'{type(exc).__name__}: {str(exc)[:230]}'}), flush=True)
        stopping.wait(interval)


if __name__ == '__main__':
    main()
