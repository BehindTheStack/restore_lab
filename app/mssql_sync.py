"""SQL Server read-only range reconciliation to Kafka and PostgreSQL blue/green.

Deliberately supports one audited five-column contract, numeric BIGINT PK and
transaction-level SNAPSHOT isolation. No CDC, DDL, or timestamp assumptions.
The source-range payload is read to the Python producer to calculate portable
SHA-256; only differing rows and range membership are written to Kafka.
"""
from __future__ import annotations

import hashlib
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import psycopg
import pymssql

import schema_evolution as evo
import source_catalog as catalog

from canonical_contract import COLUMNS, COLUMN_TYPES, canonical, fingerprint, validate_native, portable_manifest
MAX_SWEEP_WIDTH = int(os.environ.get('MAX_SQLSERVER_SWEEP_WIDTH', '100000'))


def observe_and_gate(conn, local, source_key='mssql-dbo-contracts'):
    """Record observed DDL in JSONB even if subsequently blocked by the gate."""
    native = catalog.observe_mssql(conn, os.getenv('MSSQL_SCHEMA', 'dbo'),
                                    os.getenv('MSSQL_TABLE', 'contracts'))
    with local.transaction():
        catalog.persist(local, source_key, native)
    validate_native(native)
    document = portable_manifest(native)
    h = evo.signature(document)
    state = evo.current(local)
    if state['pending']:
        raise RuntimeError('Schema migration pending; refuse SQL Server data synchronization')
    if state['active'] is None:
        with local.transaction():
            local.execute('''INSERT INTO replica.schema_versions
                 (schema_hash,manifest,status,activated_at)
                 VALUES (%s,%s,'active',clock_timestamp()) ON CONFLICT DO NOTHING''',
                 (h, psycopg.types.json.Jsonb(document)))
            local.execute('''UPDATE replica.schema_state SET active_hash=%s
                 WHERE singleton AND active_hash IS NULL''', (h,))
        state = evo.current(local)
    if state['active'] != h:
        raise RuntimeError('SQL Server DDL drift: approved schema no longer matches source; migration/review required')
    return h, catalog.sha(native)


def _connect():
    required = ['MSSQL_HOST', 'MSSQL_USER', 'MSSQL_PASSWORD', 'MSSQL_DATABASE']
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError('Missing SQL Server configuration: ' + ', '.join(missing))
    # Read-only is guaranteed by SQL Server GRANTs, not a driver hint.
    return pymssql.connect(server=os.environ['MSSQL_HOST'],
                           port=int(os.getenv('MSSQL_PORT', '1433')),
                           user=os.environ['MSSQL_USER'],
                           password=os.environ['MSSQL_PASSWORD'],
                           database=os.environ['MSSQL_DATABASE'],
                           charset='UTF-8', login_timeout=8, timeout=45,
                           autocommit=True)


def _execute(conn, sql, params=()):
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall() if cursor.description is not None else []


def ensure_snapshot(conn):
    rows = _execute(conn, '''SELECT snapshot_isolation_state
                  FROM sys.databases WHERE name=DB_NAME()''')
    if not rows or rows[0][0] != 1:
        raise RuntimeError('SQL Server ALLOW_SNAPSHOT_ISOLATION is OFF. '
                           'Read-only middleware cannot enable it; fail closed to avoid false DELETE')
    # NOLOCK/READ UNCOMMITTED would allow false deletion; never use them.
    _execute(conn, 'SET TRANSACTION ISOLATION LEVEL SNAPSHOT')
    _execute(conn, 'SET LOCK_TIMEOUT 8000')
    _execute(conn, 'BEGIN TRANSACTION')
    # Force versioned snapshot acquisition before querying data/DDL.
    _execute(conn, 'SELECT DB_NAME()')


def _source_sql():
    # Both SQL identifiers are validated; never interpolate unchecked names.
    schema = catalog.valid_identifier(os.getenv('MSSQL_SCHEMA', 'dbo'))
    table = catalog.valid_identifier(os.getenv('MSSQL_TABLE', 'contracts'))
    return f'[{schema}].[{table}]'


def source_rows(conn, low, high, batch_size=250):
    sql = ('SELECT [id],[contract_code],[amount],[status],[description] FROM '
           + _source_sql())
    params = ()
    if low is not None:
        sql += ' WHERE [id] BETWEEN %s AND %s'
        params = (low, high)
    sql += ' ORDER BY [id]'
    with conn.cursor() as cur:
        cur.execute(sql, params)
        last = None
        while True:
            chunk = cur.fetchmany(batch_size)
            if not chunk:
                break
            for row in chunk:
                obj = canonical(row)
                if last is not None and obj['id'] <= last:
                    raise RuntimeError('SQL Server non-monotonic or duplicate PK range')
                last = obj['id']
                yield obj


def fetch_local_range(local, active_slot, low, high):
    # This is deliberately bounded by MAX_SQLSERVER_SWEEP_WIDTH.
    import lab
    sql = ('SELECT id,contract_code,amount,status,description FROM '
           + lab.slot_table(active_slot)
           + ' WHERE id BETWEEN %s AND %s ORDER BY id')
    return {r[0]: canonical(r) for r in local.execute(sql, (low, high))}


def ensure_proxy():
    import lab
    name = 'remote_mssql'
    target = lab.proxy_request('GET', '/proxies/' + name, allow_missing=True)
    if target is None:
        target = lab.proxy_request('POST', '/proxies', {
            'name':name, 'listen':'0.0.0.0:15433',
            'upstream':'remote-sqlserver:1433'})
    return target


def poll(mode, sweep_width=1000, batch_size=250, throttle_ms=0, from_id=None):
    """Expose EXACT V5 Kafka protocol and consumer, switch only source adapter."""
    import lab
    if mode not in ('bootstrap', 'incremental'):
        raise ValueError(mode)
    if not 1 <= batch_size <= 10000 or throttle_ms < 0:
        raise ValueError('Invalid batch size/throttle')
    if mode == 'incremental' and not 1 <= sweep_width <= MAX_SWEEP_WIDTH:
        raise ValueError('SQL Server sweep-width must be in [1,MAX_SQLSERVER_SWEEP_WIDTH]')
    lab.kafka_ready()
    if os.environ.get('MSSQL_HOST') == 'toxiproxy':
        ensure_proxy()
    run_id = str(uuid4())
    started = time.monotonic()
    with psycopg.connect(lab.LOCAL, autocommit=True) as local:
        if not local.execute('SELECT pg_try_advisory_lock(70261021)').fetchone()[0]:
            raise RuntimeError('Another producer is running')
        if local.execute("SELECT count(*) FROM replica.sync_runs WHERE status IN ('extracting','loading')").fetchone()[0]:
            raise RuntimeError('Prior batch pending; resolve first')
        state = lab.active_checkpoint(local)
        if mode == 'bootstrap' and state['version'] is not None:
            raise RuntimeError('Bootstrap only allowed on an empty destination')
        if mode == 'incremental' and state['version'] is None:
            raise RuntimeError('Run SQL Server bootstrap first')
        publisher = lab.producer()
        errors, started_event = [], False
        n_delta, n_seen, kafka_bytes, n_remote, source_payload_json_bytes = 0, 0, 0, 0, 0
        delta_hash, key_hash = hashlib.sha256(), hashlib.sha256()
        scan_low = scan_high = None
        with _connect() as source:
            in_tx = False
            try:
                ensure_snapshot(source)
                in_tx = True
                schema_hash, native_hash = observe_and_gate(source, local)
                lab.configure_schema(local)
                if tuple(lab.FIELDS) != COLUMNS:
                    raise RuntimeError('SQL Server adapter cannot sync an unapproved projection')
                remote_now = _execute(source, 'SELECT SYSUTCDATETIME()')[0][0]
                if not isinstance(remote_now, datetime):
                    raise RuntimeError('Unexpected SQL Server clock type')
                cutoff = remote_now.replace(tzinfo=timezone.utc)
                local_values = {}
                if mode == 'incremental':
                    max_remote = _execute(source, 'SELECT ISNULL(MAX([id]),0) FROM ' + _source_sql())[0][0]
                    max_local = local.execute('SELECT COALESCE(MAX(id),0) FROM '
                        + lab.slot_table(state['slot'])).fetchone()[0]
                    max_id = max(max_remote, max_local)
                    scan_low = (from_id if from_id is not None else
                        state['next_sweep_id'] if state['next_sweep_id'] <= max_id else 1)
                    scan_high = scan_low + sweep_width - 1
                    local_values = fetch_local_range(local, state['slot'], scan_low, scan_high)
                local.execute('''INSERT INTO replica.sync_runs
                    (run_id,mode,status,base_version,cutoff,sweep_low,sweep_high,data_schema_hash)
                    VALUES (%s,%s,'extracting',%s,%s,%s,%s,%s)''',
                    (run_id, mode, state['version'], cutoff, scan_low, scan_high, schema_hash))
                event = {'run_id': run_id, 'kind':'start', 'schema_version':lab.SCHEMA,
                         'mode':mode, 'base_version':state['version'], 'cutoff':cutoff.isoformat(),
                         'sweep_low':scan_low, 'sweep_high':scan_high,
                         'sweep_target_max_id':max_id if mode == 'incremental' else None,
                         'data_schema_hash':schema_hash, 'source_engine':'sqlserver'}
                lab.emit(publisher, lab.TOPIC, event, errors)
                lab.flush(publisher, errors)
                started_event = True
                lab.audit(publisher, run_id, 'started', engine='sqlserver',
                          native_hash=native_hash)
                for row in source_rows(source, scan_low, scan_high, batch_size):
                    n_remote += 1
                    source_payload_json_bytes += len(lab.obj_bytes(row))
                    id_ = row['id']
                    if mode == 'incremental':
                        lab.digest_one(key_hash, {'id': id_})
                        kafka_bytes += lab.emit(publisher, lab.TOPIC,
                            {'run_id':run_id, 'kind':'seen','schema_version':lab.SCHEMA,
                             'data_schema_hash':schema_hash,'id':id_}, errors)
                        n_seen += 1
                    if mode == 'bootstrap' or (id_ not in local_values or
                            fingerprint(row) != fingerprint(local_values[id_])):
                        delta = lab.delta_item(id_, 'upsert', row)
                        lab.digest_one(delta_hash, delta)
                        kafka_bytes += lab.emit(publisher,lab.TOPIC,
                            {'run_id':run_id,'kind':'delta','schema_version':lab.SCHEMA,
                             'data_schema_hash':schema_hash,**delta}, errors)
                        n_delta += 1
                    if (n_seen+n_delta) % batch_size == 0:
                        lab.flush(publisher, errors)
                        if throttle_ms:
                            time.sleep(throttle_ms / 1000)
                lab.flush(publisher, errors)
                # Reobserve DDL while snapshot is still open. Metadata is not versioned
                # by SQL Server: error/changed catalog BLOCKS promotion.
                native_after = catalog.observe_mssql(source, os.getenv('MSSQL_SCHEMA','dbo'),
                                                            os.getenv('MSSQL_TABLE','contracts'))
                if catalog.sha(native_after) != native_hash:
                    raise RuntimeError('SQL Server schema changed during range scan')
                _execute(source, 'COMMIT TRANSACTION')
                in_tx = False
                end = {'run_id':run_id, 'kind':'end', 'schema_version':lab.SCHEMA,
                    'delta_count':n_delta, 'delta_sha256':delta_hash.hexdigest(),
                    'sweep_count':n_seen, 'sweep_sha256':key_hash.hexdigest(),
                    'deep_count':0, 'deep_sha256':hashlib.sha256().hexdigest(),
                    'logical_kafka_bytes':kafka_bytes,'data_schema_hash':schema_hash}
                lab.emit(publisher, lab.TOPIC, end, errors)
                lab.flush(publisher, errors)
                lab.audit(publisher,run_id,'extracted',engine='sqlserver',
                          changed=n_delta,observed_ids=n_seen,bytes_json=kafka_bytes)
                lab.log('mssql_range_emitted',run_id=run_id,mode=mode,
                        range_low=scan_low,range_high=scan_high,
                        remote_rows=n_remote, changed_rows=n_delta,seen_ids=n_seen,
                        remote_payload_json_bytes=source_payload_json_bytes,
                        kafka_bytes=kafka_bytes,
                        duration_s=round(time.monotonic()-started,2))
                return run_id
            except Exception as exc:
                if in_tx:
                    try:
                        _execute(source, 'ROLLBACK TRANSACTION')
                    except Exception:
                        pass
                if started_event:
                    try:
                        abort_err=[]
                        lab.emit(publisher,lab.TOPIC,
                           {'run_id':run_id,'kind':'abort','schema_version':lab.SCHEMA,
                            'data_schema_hash':schema_hash,'reason':str(exc)[:500]}, abort_err)
                        lab.flush(publisher,abort_err)
                    except Exception:
                        lab.log('mssql_abort_event_failed',run_id=run_id)
                local.execute('''UPDATE replica.sync_runs SET status='failed',error=%s,
                     finished_at=clock_timestamp() WHERE run_id=%s
                     AND status IN ('extracting','loading')''', (str(exc)[:500],run_id))
                raise
