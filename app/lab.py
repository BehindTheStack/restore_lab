"""Read-only PostgreSQL range-fingerprint reconciliation -> Kafka -> blue/green.

No remote updated_at, deleted_at, CDC, WAL or plugins. One table/one partition.
Audits one complete PK range per run, propagates changes only; eventual convergence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
import schema_evolution as schema_evo
import source_catalog

REMOTE = os.environ['REMOTE_DSN']
DIRECT = os.environ['REMOTE_DIRECT_DSN']
LOCAL = os.environ['LOCAL_DSN']
BROKER = os.environ['KAFKA_BOOTSTRAP']
TOPIC = os.environ.get('KAFKA_TOPIC', 'lab.sync.events.v5')
AUDIT_TOPIC = os.environ.get('KAFKA_AUDIT_TOPIC', 'lab.sync.audit.v5')
TOXI = os.environ.get('TOXIPROXY_URL', 'http://toxiproxy:8474')
SCHEMA = 'contracts/v5'
REMOTE_ENGINE = os.environ.get('REMOTE_ENGINE', 'postgresql').lower()
if REMOTE_ENGINE not in ('postgresql', 'sqlserver'):
    raise RuntimeError('REMOTE_ENGINE must be postgresql or sqlserver')
BASE_FIELDS = ['id', 'contract_code', 'amount', 'status', 'description']
FIELDS = BASE_FIELDS.copy()
EXPECTED_COLS = [
    ('id', 'bigint'), ('contract_code', 'text'), ('amount', 'numeric'),
    ('status', 'text'), ('description', 'text')
]


def log(event, **data):
    print(json.dumps({'ts': datetime.now(timezone.utc).isoformat(), 'event': event,
                      **data}, default=str, ensure_ascii=False), flush=True)


def obj_bytes(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False).encode('utf8')


def digest_one(h, item):
    h.update(obj_bytes(item))
    h.update(b'\n')


def normalized(row):
    if row is None:
        return None
    result=dict(zip(FIELDS,row))
    result['id']=int(result['id'])
    result['amount']=format(Decimal(result['amount']), 'f')
    return result


def delta_item(id_, op, row):
    return {'id': int(id_), 'op': op, 'row': row}


def kafka_ready():
    admin = AdminClient({'bootstrap.servers': BROKER, 'socket.timeout.ms': 5000})
    for attempt in range(30):
        try:
            topics = [NewTopic(t, num_partitions=1, replication_factor=1,
                               config={'cleanup.policy': 'delete',
                                       'retention.ms': '604800000'})
                      for t in (TOPIC, AUDIT_TOPIC)]
            for f in admin.create_topics(topics).values():
                try:
                    f.result(timeout=8)
                except Exception as exc:
                    if 'TOPIC_ALREADY_EXISTS' not in str(exc):
                        raise
            info = admin.list_topics(topic=TOPIC, timeout=8)
            if len(info.topics[TOPIC].partitions) != 1:
                raise RuntimeError('V3 demo assumes single topic partition for total ordering')
            return
        except Exception as exc:
            if attempt == 29:
                raise
            log('kafka_waiting', attempt=attempt + 1, error=str(exc))
            time.sleep(2)


def proxy_request(method, path, payload=None, allow_missing=False):
    req = urllib.request.Request(TOXI + path, method=method,
                                 data=None if payload is None else obj_bytes(payload),
                                 headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = resp.read()
        return json.loads(data) if data else {}
    except urllib.error.HTTPError as exc:
        if allow_missing and exc.code == 404:
            return None
        raise


def ensure_proxy():
    state = proxy_request('GET', '/proxies/remote_pg', allow_missing=True)
    if state is None:
        state = proxy_request('POST', '/proxies', {
            'name': 'remote_pg', 'listen': '0.0.0.0:15432',
            'upstream': 'remote-db:5432'})
    return state


def chaos(args):
    target = 'remote_mssql' if args.target == 'sqlserver' else 'remote_pg'
    if args.target == 'sqlserver':
        import mssql_sync
        initialize = mssql_sync.ensure_proxy
    else:
        initialize = ensure_proxy
    if args.action == 'init':
        log('proxy_initialized', state=initialize(), target=args.target)
        return
    initialize()
    if args.action == 'status':
        log('proxy_status', state=proxy_request('GET', '/proxies/' + target))
    elif args.action in ('up', 'down'):
        proxy_request('POST', '/proxies/' + target, {'enabled': args.action == 'up'})
        log('proxy_changed', action=args.action)
    elif args.action == 'clear':
        for item in proxy_request('GET', '/proxies/' + target)['toxics']:
            proxy_request('DELETE', '/proxies/' + target + '/toxics/' + item['name'])
        proxy_request('POST', '/proxies/' + target, {'enabled': True})
        log('proxy_cleared')
    else:
        if args.amount is None or args.amount < 0:
            raise ValueError('Provide nonnegative amount for toxic')
        for item in proxy_request('GET', '/proxies/' + target)['toxics']:
            if item['name'] == args.action:
                proxy_request('DELETE', '/proxies/' + target + '/toxics/' + args.action)
        attrs = ({'latency': args.amount, 'jitter': args.amount // 5}
                 if args.action == 'latency' else {'timeout': args.amount}
                 if args.action == 'timeout' else {'rate': args.amount})
        proxy_request('POST', '/proxies/' + target + '/toxics', {
            'name': args.action, 'type': args.action, 'stream': 'downstream',
            'toxicity': 1.0, 'attributes': attrs})
        log('toxic_added', action=args.action, amount=args.amount)


def producer():
    return Producer({'bootstrap.servers': BROKER, 'enable.idempotence': True,
                     'acks': 'all', 'compression.type': 'gzip',
                     'message.timeout.ms': 30000, 'linger.ms': 5})


def emit(p, topic, event, errors):
    b = obj_bytes(event)
    if len(b) > 950_000:
        raise RuntimeError('Event >950kB: reduce synthetic row size or use chunked storage')

    def callback(err, _):
        if err:
            errors.append(str(err))

    for attempt in range(1000):
        try:
            p.produce(topic, key=event['run_id'].encode(), value=b,
                      on_delivery=callback)
            break
        except BufferError:
            p.poll(.05)
    else:
        raise RuntimeError('Kafka local producer queue full')
    p.poll(0)
    if errors:
        raise RuntimeError(f'Kafka delivery failed: {errors[:2]}')
    return len(b)


def flush(p, errors):
    remaining = p.flush(60)
    if remaining or errors:
        raise RuntimeError(f'Kafka flush: undelivered={remaining} errors={errors[:2]}')


def audit(p, run_id, kind, **data):
    errors = []
    try:
        emit(p, AUDIT_TOPIC, {'run_id': run_id, 'kind': kind,
                              'schema_version': SCHEMA, **data}, errors)
        flush(p, errors)
    except Exception as exc:
        log('audit_failed_non_authoritative', error=str(exc))


def remote_schema(conn, local):
    """Gate all extractors on approved pg_catalog state; no remote DDL."""
    schema_hash = schema_evo.gate(conn,local)
    configure_schema(local)
    return schema_hash


def configure_schema(local):
    """Active fields only. During additive backfill use legacy projection."""
    global FIELDS, FINGERPRINT_SQL
    FIELDS=schema_evo.fields(local)
    if any(not schema_evo.IDENT.fullmatch(f) for f in FIELDS):
        raise RuntimeError('Unapproved identifier in active schema')
    FINGERPRINT_SQL=("encode(sha256(convert_to(jsonb_build_array(" +
                     ','.join(FIELDS) + ")::text, 'UTF8')), 'hex')")


def remote_select():
    return ','.join(FIELDS)


def active_checkpoint(conn):
    r = conn.execute('SELECT slot, version, cutoff, next_sweep_id, row_count '
                     'FROM replica.active_state WHERE singleton').fetchone()
    return {'slot': r[0], 'version': r[1], 'cutoff': r[2],
            'next_sweep_id': r[3], 'row_count': r[4]}


# The same built-in PostgreSQL sha256(convert_to(jsonb_build_array(...)::text,
# UTF8)) is evaluated remotely and locally. Hash agreement is a strong
# fingerprint but not proof of source-wide transactional consistency.
# Field types, projection and session parameters must remain identical.
FINGERPRINT_SQL = "encode(sha256(convert_to(jsonb_build_array(id,contract_code,amount,status,description)::text, 'UTF8')), 'hex')"


def diff_keys(remote, local):
    """Compute changed and missing IDs from two COMPLETE sorted PK/hash ranges."""
    rd, ld = dict(remote), dict(local)
    return sorted(k for k,h in rd.items() if ld.get(k) != h), sorted(ld.keys()-rd.keys())


def poll(mode, sweep_width=1000, batch_size=250, throttle_ms=0, from_id=None):
    """Audit one *complete* PK interval in one read-only repeatable-read snapshot.

    Send full payload via Kafka only for changed/new IDs. SEEN conveys all IDs in
    the audited interval so the consumer can infer physical DELETE atomically.
    """
    if mode not in ('bootstrap', 'incremental'):
        raise ValueError(mode)
    if mode == 'incremental' and sweep_width < 1:
        raise ValueError('incremental always requires sweep_width >= 1')
    kafka_ready()
    ensure_proxy()
    run_id = str(uuid4())
    started = time.monotonic()
    with psycopg.connect(LOCAL, autocommit=True) as lock:
        if not lock.execute('SELECT pg_try_advisory_lock(70261021)').fetchone()[0]:
            raise RuntimeError('Another producer is running')
        if lock.execute("SELECT COUNT(*) FROM replica.sync_runs WHERE status IN ('extracting','loading')").fetchone()[0]:
            raise RuntimeError('Previous batch pending; resolve before continuing')
        base = active_checkpoint(lock)
        if schema_evo.current(lock)['pending'] is not None:
            raise RuntimeError('Schema migration pending: ordinary synchronization frozen until activation or cancellation')
        if mode == 'bootstrap' and base['version'] is not None:
            raise RuntimeError('Bootstrap permitted only on an empty destination')
        if mode == 'incremental' and base['version'] is None:
            raise RuntimeError('First execute bootstrap')
        p = producer()
        errors = []
        started_event = False
        bytes_sent = 0
        try:
            with psycopg.connect(REMOTE, autocommit=True) as source:
                source.execute('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
                source.execute("SET LOCAL statement_timeout = '45s'")
                source.execute("SET LOCAL idle_in_transaction_session_timeout = '120s'")
                source.execute("SET LOCAL TIME ZONE 'UTC'")
                data_schema_hash = remote_schema(source, lock)
                cutoff = source.execute('SELECT transaction_timestamp()').fetchone()[0]
                scan_low = scan_high = None
                observed = []
                missing_ids = []
                if mode == 'incremental':
                    max_remote = source.execute('SELECT COALESCE(MAX(id),0) FROM public.contracts').fetchone()[0]
                    max_local = lock.execute('SELECT COALESCE(MAX(id),0) FROM '+slot_table(base['slot'])).fetchone()[0]
                    max_id = max(max_remote, max_local)
                    scan_low = (from_id if from_id is not None else
                        base['next_sweep_id'] if base['next_sweep_id'] <= max_id else 1)
                    scan_high = scan_low + sweep_width - 1
                    # Both comparisons use bounded, indexed PK ranges.
                    observed = source.execute(f'''SELECT id,{FINGERPRINT_SQL}
                        FROM public.contracts WHERE id BETWEEN %s AND %s
                        ORDER BY id''', (scan_low,scan_high)).fetchall()
                    local = lock.execute(f'''SELECT id,{FINGERPRINT_SQL}
                        FROM {slot_table(base['slot'])} WHERE id BETWEEN %s AND %s
                        ORDER BY id''', (scan_low,scan_high)).fetchall()
                    changed_ids, missing_ids = diff_keys(observed, local)
                    # Fetch full rows only for changed/new IDs in the SAME remote snapshot.
                    changed_rows = []
                    for offset in range(0, len(changed_ids), batch_size):
                        ids = changed_ids[offset:offset+batch_size]
                        changed_rows.extend(source.execute(f'''SELECT {remote_select()}
                            FROM public.contracts WHERE id = ANY(%s) ORDER BY id''', (ids,)).fetchall())
                    if [r[0] for r in changed_rows] != changed_ids:
                        raise RuntimeError('Remote rows disappeared between fingerprint and full fetch')
                lock.execute('''INSERT INTO replica.sync_runs
                    (run_id,mode,status,base_version,cutoff,sweep_low,sweep_high,data_schema_hash)
                    VALUES(%s,%s,'extracting',%s,%s,%s,%s,%s)''',
                    (run_id,mode,base['version'],cutoff,scan_low,scan_high,data_schema_hash))
                emit(p, TOPIC, {'run_id':run_id,'kind':'start','schema_version':SCHEMA,
                     'mode':mode,'base_version':base['version'],'cutoff':cutoff.isoformat(),
                     'sweep_low':scan_low,'sweep_high':scan_high,
                     'sweep_target_max_id':max_id if mode == 'incremental' else None,
                     'source_engine':'postgresql',
                     'data_schema_hash':data_schema_hash}, errors)
                flush(p,errors)
                started_event = True
                audit(p,run_id,'start',mode=mode,range_low=scan_low,range_high=scan_high)
                delta_hash,d_count = hashlib.sha256(),0
                if mode == 'bootstrap':
                    with source.cursor(name='bootstrap_data') as cur:
                        cur.execute(f'''SELECT {remote_select()}
                            FROM public.contracts ORDER BY id''')
                        while True:
                            rows = cur.fetchmany(batch_size)
                            if not rows: break
                            for rec in rows:
                                item=delta_item(rec[0], 'upsert', normalized(rec))
                                digest_one(delta_hash,item)
                                bytes_sent += emit(p,TOPIC,{'run_id':run_id,'kind':'delta',
                                    'schema_version':SCHEMA,'data_schema_hash':data_schema_hash,**item},errors)
                                d_count+=1
                            flush(p,errors)
                            if throttle_ms: time.sleep(throttle_ms/1000)
                else:
                    for rec in changed_rows:
                        item=delta_item(rec[0], 'upsert', normalized(rec))
                        digest_one(delta_hash,item)
                        bytes_sent += emit(p,TOPIC,{'run_id':run_id,'kind':'delta',
                           'schema_version':SCHEMA,'data_schema_hash':data_schema_hash,**item},errors)
                        d_count+=1
                        if d_count % batch_size == 0:
                            flush(p,errors)
                            if throttle_ms: time.sleep(throttle_ms/1000)
                    flush(p,errors)
                key_hash,key_count = hashlib.sha256(),0
                if mode == 'incremental':
                    # All remote keys must be published, otherwise deletes are unsafe.
                    for id_,fingerprint in observed:
                        digest_one(key_hash,{'id':id_})
                        bytes_sent+=emit(p,TOPIC,{'run_id':run_id,'kind':'seen',
                           'schema_version':SCHEMA,'data_schema_hash':data_schema_hash,'id':id_},errors)
                        key_count+=1
                        if key_count % batch_size == 0: flush(p,errors)
                    flush(p,errors)
                source.execute('COMMIT')
                end={'run_id':run_id,'kind':'end','schema_version':SCHEMA,
                    'delta_count':d_count,'delta_sha256':delta_hash.hexdigest(),
                    'sweep_count':key_count,'sweep_sha256':key_hash.hexdigest(),
                    'deep_count':0,'deep_sha256':hashlib.sha256().hexdigest(),
                    'logical_kafka_bytes':bytes_sent,'data_schema_hash':data_schema_hash}
                emit(p,TOPIC,end,errors)
                flush(p,errors)
                audit(p,run_id,'extracted',changes=d_count,delete_candidates=len(missing_ids),
                    examined_keys=key_count)
                log('range_emitted',run_id=run_id,mode=mode,range_low=scan_low,
                    range_high=scan_high,observed_remote_keys=key_count,
                    changed_payloads=d_count,delete_candidates=len(missing_ids),
                    bytes_json=bytes_sent,duration_s=round(time.monotonic()-started,2))
        except Exception as exc:
            log('producer_failed',run_id=run_id,error=str(exc))
            if started_event:
                try:
                    abort_err=[]
                    emit(p,TOPIC,{'run_id':run_id,'kind':'abort',
                        'schema_version':SCHEMA,'reason':str(exc)[:500]},abort_err)
                    flush(p,abort_err)
                except Exception as err:
                    log('abort_event_failed',error=str(err))
            lock.execute('''UPDATE replica.sync_runs SET status='failed',error=%s,
               finished_at=clock_timestamp() WHERE run_id=%s AND status IN ('extracting','loading')''',
               (str(exc)[:500],run_id))
            raise


def slot_table(slot):
    if slot not in ('blue', 'green'):
        raise ValueError('Invalid slot')
    return 'replica.' + slot + '_contracts'


def point_view(conn, slot):
    """Static UNION-ALL view points to whichever slot active_state selects.

    No CREATE OR REPLACE VIEW in each promotion; updating active_state is atomic.
    """
    if slot not in ('blue', 'green'):
        raise ValueError('Invalid slot')



def put_row(conn, slot, id_, row):
    table = slot_table(slot)
    if row is None:
        conn.execute(f'DELETE FROM {table} WHERE id=%s', (id_,))
        return
    if not set(BASE_FIELDS).issubset(set(row)) or not set(row).issubset(set(FIELDS)) or int(row['id']) != int(id_):
        raise ValueError(f'Payload incompatible with active schema {FIELDS}: {id_}')
    # Older journal entries predate added nullable columns: absent -> NULL.
    vals=[row.get(k) for k in FIELDS]
    cols=','.join(FIELDS)
    placeholders=','.join('%s' for _ in FIELDS)
    update=', '.join(f'{c}=EXCLUDED.{c}' for c in FIELDS if c!='id')
    conn.execute(f'''INSERT INTO {table} ({cols}) VALUES ({placeholders})
        ON CONFLICT(id) DO UPDATE SET {update}''', vals)


def current_row(conn, slot, id_):
    rec = conn.execute(f'''SELECT {remote_select()}
        FROM {slot_table(slot)} WHERE id=%s''', (id_,)).fetchone()
    return normalized(rec) if rec else None


def ancestry(conn, ver):
    chain = []
    while ver is not None:
        if ver in chain:
            raise RuntimeError('Cyclic generation ancestry')
        chain.append(ver)
        parent = conn.execute('SELECT parent_version FROM replica.generations WHERE version=%s',
                              (ver,)).fetchone()
        if not parent:
            raise RuntimeError(f'Generation {ver} missing')
        ver = parent[0]
    return chain


def replay_journal(conn, version, slot, field):
    for id_, payload in conn.execute(f'''SELECT id,{field}
        FROM replica.change_journal WHERE version=%s ORDER BY id''', (version,)):
        put_row(conn, slot, id_, payload)


def align_standby(conn, slot, from_version, to_version):
    """Reconcile standby by journal paths, including branches after a rollback."""
    if from_version == to_version:
        return
    if from_version is None or to_version is None:
        raise RuntimeError('Standby has no bootstrap: refusing expensive full clone')
    source_chain = ancestry(conn, from_version)
    target_chain = ancestry(conn, to_version)
    common = next((v for v in target_chain if v in set(source_chain)), None)
    if common is None:
        raise RuntimeError('No common ancestor: cannot align blue/green')
    for version in source_chain[:source_chain.index(common)]:
        replay_journal(conn, version, slot, 'before_row')
    for version in reversed(target_chain[:target_chain.index(common)]):
        replay_journal(conn, version, slot, 'after_row')
    conn.execute('UPDATE replica.slot_state SET version=%s WHERE slot=%s',
                 (to_version, slot))
    log('standby_aligned', slot=slot, from_version=from_version,
        to_version=to_version, undo=len(source_chain[:source_chain.index(common)]),
        redo=len(target_chain[:target_chain.index(common)]))


def prune_journal(conn):
    """Keep only the mutation paths required to transform one physical slot into the other."""
    versions = dict(conn.execute('SELECT slot,version FROM replica.slot_state'))
    b, g = versions['blue'], versions['green']
    if b is None or g is None:
        return
    bchain, gchain = ancestry(conn, b), ancestry(conn, g)
    common = next((v for v in bchain if v in set(gchain)), None)
    if common is None:
        raise RuntimeError('Cannot prune without common ancestor')
    keep = list(set(bchain[:bchain.index(common)] + gchain[:gchain.index(common)]))
    if keep:
        conn.execute('DELETE FROM replica.change_journal WHERE NOT(version=ANY(%s))',
                     (keep,))
    else:
        conn.execute('DELETE FROM replica.change_journal')


def staged_hashes(conn, run_id):
    dh, dc = hashlib.sha256(), 0
    with conn.cursor(name='digest_delta') as cur:
        cur.execute('''SELECT id,op,payload FROM replica.ingest_delta
                       WHERE run_id=%s ORDER BY id''', (run_id,))
        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            for id_, op, payload in rows:
                digest_one(dh, delta_item(id_, op, payload))
                dc += 1
    kh, kc = hashlib.sha256(), 0
    with conn.cursor(name='digest_sweep') as cur:
        cur.execute('SELECT id FROM replica.ingest_keys WHERE run_id=%s ORDER BY id',
                    (run_id,))
        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            for (id_,) in rows:
                digest_one(kh, {'id': id_})
                kc += 1
    deep_h, deep_c = hashlib.sha256(), 0
    with conn.cursor(name='digest_deep') as cur:
        cur.execute('SELECT id,payload FROM replica.ingest_deep WHERE run_id=%s ORDER BY id',
                    (run_id,))
        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            for id_, payload in rows:
                digest_one(deep_h, {'id':id_, 'row':payload})
                deep_c += 1
    return dc, dh.hexdigest(), kc, kh.hexdigest(), deep_c, deep_h.hexdigest()


def on_end(conn, evt):
    run_id = evt['run_id']
    info = conn.execute('''SELECT mode,status,base_version,cutoff,sweep_low,sweep_high
        FROM replica.sync_runs WHERE run_id=%s FOR UPDATE''', (run_id,)).fetchone()
    if not info or info[1] not in ('extracting', 'loading'):
        return
    mode, _, base_version, cutoff, sweep_low, sweep_high = info
    st = schema_evo.current(conn)
    if evt.get('data_schema_hash')!=st['active']:
        raise RuntimeError('Data schema changed between extract and promotion')
    configure_schema(conn)
    dc, dh, kc, kh, deep_c, deep_h = staged_hashes(conn, run_id)
    if ((dc, dh, kc, kh, deep_c, deep_h) !=
        (evt['delta_count'], evt['delta_sha256'],
         evt['sweep_count'], evt['sweep_sha256'],
         evt.get('deep_count', 0), evt.get('deep_sha256', hashlib.sha256().hexdigest()))):
        conn.execute('''UPDATE replica.sync_runs SET status='rejected',
            error='Kafka delta/key count or SHA256 mismatch', finished_at=clock_timestamp(),
            delta_count=%s, actual_count=%s WHERE run_id=%s''',
                     (evt['delta_count'], dc, run_id))
        conn.execute('DELETE FROM replica.ingest_delta WHERE run_id=%s', (run_id,))
        conn.execute('DELETE FROM replica.ingest_keys WHERE run_id=%s', (run_id,))
        conn.execute('DELETE FROM replica.ingest_deep WHERE run_id=%s', (run_id,))
        log('batch_rejected', run_id=run_id, reason='integrity checksum/count')
        return
    # Serialize promotion with rollback and any other consumer transaction.
    conn.execute('SELECT version FROM replica.active_state WHERE singleton FOR UPDATE')
    state = active_checkpoint(conn)
    if state['version'] != base_version:
        conn.execute('''UPDATE replica.sync_runs SET status='rejected',
            error='Base generation changed since extraction', finished_at=clock_timestamp()
            WHERE run_id=%s''', (run_id,))
        conn.execute('DELETE FROM replica.ingest_delta WHERE run_id=%s', (run_id,))
        conn.execute('DELETE FROM replica.ingest_keys WHERE run_id=%s', (run_id,))
        conn.execute('DELETE FROM replica.ingest_deep WHERE run_id=%s', (run_id,))
        log('batch_rejected', run_id=run_id, reason='stale base')
        return
    if mode == 'bootstrap':
        if state['version'] is not None:
            raise RuntimeError('Bootstrap attempted with nonempty destination')
        if dc == 0:
            raise RuntimeError('Empty bootstrap forbidden')
        # Bootstrap builds both physical tables ONCE. Incremental cycles never do this.
        for slot in ('blue', 'green'):
            conn.execute('TRUNCATE ' + slot_table(slot))
            conn.execute(f'''INSERT INTO {slot_table(slot)}
                (id,contract_code,amount,status,description)
                SELECT id,payload->>'contract_code',
                    (payload->>'amount')::numeric, payload->>'status',
                    payload->>'description'
                FROM replica.ingest_delta
                WHERE run_id=%s AND op='upsert' ORDER BY id''', (run_id,))
        ver = conn.execute('''INSERT INTO replica.generations
            (parent_version,run_id,cutoff,row_count,next_sweep_id,data_schema_hash)
            VALUES (NULL,%s,%s,%s,1,%s) RETURNING version''',
            (run_id, cutoff, dc, evt['data_schema_hash'])).fetchone()[0]
        conn.execute('UPDATE replica.slot_state SET version=%s', (ver,))
        point_view(conn, 'blue')
        conn.execute('''UPDATE replica.active_state SET slot='blue',version=%s,
            cutoff=%s,row_count=%s,next_sweep_id=1 WHERE singleton''',
                     (ver, cutoff, dc))
        conn.execute('''UPDATE replica.sync_runs SET status='promoted',
            result_version=%s, delta_count=%s, actual_count=%s,
            logical_kafka_bytes=%s, inserted_count=%s, updated_count=0,
            deleted_count=0,finished_at=clock_timestamp() WHERE run_id=%s''',
            (ver, dc, dc, evt['logical_kafka_bytes'], dc, run_id))
        conn.execute('''INSERT INTO replica.transitions(action,to_slot,to_version)
            VALUES ('promote','blue',%s)''', (ver,))
        conn.execute('DELETE FROM replica.ingest_delta WHERE run_id=%s', (run_id,))
        log('bootstrap_promoted', run_id=run_id, rows=dc, version=ver)
        return

    active_slot = state['slot']
    inactive = 'green' if active_slot == 'blue' else 'blue'
    standby_ver = conn.execute('SELECT version FROM replica.slot_state WHERE slot=%s FOR UPDATE',
                               (inactive,)).fetchone()[0]
    changes = {}
    for id_, op, payload in conn.execute('''SELECT id,op,payload FROM replica.ingest_delta
        WHERE run_id=%s ORDER BY id''', (run_id,)):
        before = current_row(conn, active_slot, id_)
        after = payload if op == 'upsert' else None
        if before != after:
            changes[id_] = (before, after)
    if sweep_low is not None:
        # The deep range sweep is optional, for finding stale content with bad timestamps.
        for id_, payload in conn.execute('''SELECT id,payload FROM replica.ingest_deep
            WHERE run_id=%s ORDER BY id''', (run_id,)):
            before = current_row(conn, active_slot, id_)
            if before != payload:
                changes[id_] = (before, payload)
        # A hard-delete is only inferred from a COMPLETE ID sweep over the exact range.
        missing = conn.execute(f'''SELECT a.id FROM {slot_table(active_slot)} a
            WHERE a.id BETWEEN %s AND %s
              AND NOT EXISTS (SELECT 1 FROM replica.ingest_keys k
                WHERE k.run_id=%s AND k.id=a.id)''',
            (sweep_low, sweep_high, run_id)).fetchall()
        for (id_,) in missing:
            before = current_row(conn, active_slot, id_)
            if before is not None:
                changes[id_] = (before, None)
    ins = sum(1 for a, b in changes.values() if a is None and b is not None)
    delete = sum(1 for a, b in changes.values() if a is not None and b is None)
    updates = sum(1 for a, b in changes.values() if a is not None and b is not None)
    fraction = float(os.environ.get('MAX_REMOVE_FRACTION', '0.20'))
    local_in_range = (conn.execute(f'SELECT COUNT(*) FROM {slot_table(active_slot)} WHERE id BETWEEN %s AND %s', (sweep_low,sweep_high)).fetchone()[0] if sweep_low is not None else 0)
    if delete and ((state['row_count'] and delete / state['row_count'] > fraction) or
                   (local_in_range and delete / local_in_range > fraction)):
        conn.execute('''UPDATE replica.sync_runs SET status='rejected',
            error=%s,deleted_count=%s,finished_at=clock_timestamp() WHERE run_id=%s''',
            (f'Deletion guard {delete}/{local_in_range} within interval; global={state["row_count"]}, max={fraction:.2%}',
             delete, run_id))
        conn.execute('DELETE FROM replica.ingest_delta WHERE run_id=%s', (run_id,))
        conn.execute('DELETE FROM replica.ingest_keys WHERE run_id=%s', (run_id,))
        conn.execute('DELETE FROM replica.ingest_deep WHERE run_id=%s', (run_id,))
        log('batch_rejected', run_id=run_id, reason='deletion guard', deletes=delete)
        return
    # Only delta mutations and journal replay; NO TRUNCATE or full table clone.
    # Run this AFTER all guards, so rejected batches never alter the previous slot.
    align_standby(conn, inactive, standby_ver, state['version'])
    row_count = state['row_count'] + ins - delete
    next_scan = sweep_high + 1 if sweep_high is not None else state['next_sweep_id']
    ver = conn.execute('''INSERT INTO replica.generations
        (parent_version,run_id,cutoff,row_count,next_sweep_id,data_schema_hash)
        VALUES (%s,%s,%s,%s,%s,%s) RETURNING version''',
        (state['version'], run_id, cutoff, row_count, next_scan, evt['data_schema_hash'])).fetchone()[0]
    for id_, (before, after) in sorted(changes.items()):
        put_row(conn, inactive, id_, after)
        conn.execute('''INSERT INTO replica.change_journal
            (version,id,before_row,after_row) VALUES (%s,%s,%s,%s)''',
            (ver, id_, psycopg.types.json.Jsonb(before), psycopg.types.json.Jsonb(after)))
    # Per-key read-back, not an O(700GB) hash/count scan per cycle.
    for id_, (_, after) in changes.items():
        if current_row(conn, inactive, id_) != after:
            raise RuntimeError(f'Read-back mismatch after applying id={id_}')
    conn.execute('UPDATE replica.slot_state SET version=%s WHERE slot=%s',
                 (ver, inactive))
    point_view(conn, inactive)
    conn.execute('''UPDATE replica.active_state SET slot=%s,version=%s,cutoff=%s,
        row_count=%s,next_sweep_id=%s WHERE singleton''',
        (inactive, ver, cutoff, row_count, next_scan))
    conn.execute('''UPDATE replica.sync_runs SET status='promoted', result_version=%s,
        delta_count=%s,actual_count=%s,sweep_count=%s,
        inserted_count=%s,updated_count=%s,deleted_count=%s,
        logical_kafka_bytes=%s,finished_at=clock_timestamp() WHERE run_id=%s''',
        (ver, dc, dc, kc, ins, updates, delete, evt['logical_kafka_bytes'], run_id))
    conn.execute('''INSERT INTO replica.transitions
        (action,from_slot,to_slot,from_version,to_version)
        VALUES ('promote',%s,%s,%s,%s)''',
        (active_slot, inactive, state['version'], ver))
    conn.execute('DELETE FROM replica.ingest_delta WHERE run_id=%s', (run_id,))
    conn.execute('DELETE FROM replica.ingest_keys WHERE run_id=%s', (run_id,))
    conn.execute('DELETE FROM replica.ingest_deep WHERE run_id=%s', (run_id,))
    # Coverage evidence is committed in the SAME transaction as blue-green promotion.
    # Rejected/failed/aborted batches NEVER write a checkpoint.
    conn.execute('''INSERT INTO replica.range_checkpoints
        (run_id,sweep_low,sweep_high,target_max_id,source_engine,version)
        SELECT run_id,sweep_low,sweep_high,sweep_target_max_id,
               COALESCE(source_engine,'postgresql'),%s
        FROM replica.sync_runs WHERE run_id=%s AND sweep_low IS NOT NULL
        ON CONFLICT (run_id) DO NOTHING''', (ver,run_id))
    prune_journal(conn)
    log('incremental_promoted', run_id=run_id, version=ver,
        from_slot=active_slot, to_slot=inactive, inserted=ins, updated=updates,
        deleted=delete, received_events=dc, sweep_keys=kc, live_rows=row_count)


def handle_event(conn, event):
    run_id = event['run_id']
    kind = event['kind']
    if kind.startswith('schema_backfill_'):
        if event.get('schema_version')!=SCHEMA:
            raise RuntimeError('Unknown schema-backfill protocol version')
        schema_evo.handle_backfill(conn,event)
        return
    configure_schema(conn)
    if kind=='start' and event.get('data_schema_hash')!=schema_evo.current(conn)['active']:
        raise RuntimeError('Data batch schema is not active; refusing Kafka events')
    if event.get('schema_version') != SCHEMA:
        raise RuntimeError('Unknown event schema: ' + str(event.get('schema_version')))
    if kind == 'start':
        exists = conn.execute('SELECT status FROM replica.sync_runs WHERE run_id=%s',
                              (run_id,)).fetchone()
        if not exists:
            # Enables replay of retained Kafka history into a new local DB.
            conn.execute('''INSERT INTO replica.sync_runs
                (run_id,mode,status,base_version,cutoff,sweep_low,sweep_high,data_schema_hash)
                VALUES(%s,%s,'loading',%s,%s,%s,%s,%s)''',
                (run_id, event['mode'], event['base_version'],event['cutoff'],
                 event.get('sweep_low'),event.get('sweep_high'),event['data_schema_hash']))
        elif exists[0] == 'extracting':
            conn.execute("UPDATE replica.sync_runs SET status='loading' WHERE run_id=%s",
                         (run_id,))
        conn.execute('''UPDATE replica.sync_runs
            SET source_engine=%s, sweep_target_max_id=%s
            WHERE run_id=%s''',
            (event.get('source_engine','postgresql'),
             event.get('sweep_target_max_id'),run_id))
        return
    exists = conn.execute('SELECT status FROM replica.sync_runs WHERE run_id=%s',
                          (run_id,)).fetchone()
    if not exists:
        raise RuntimeError(f'Missing start for Kafka batch {run_id}')
    if exists[0] == 'failed' and kind == 'abort':
        conn.execute('DELETE FROM replica.ingest_delta WHERE run_id=%s', (run_id,))
        conn.execute('DELETE FROM replica.ingest_keys WHERE run_id=%s', (run_id,))
        conn.execute('DELETE FROM replica.ingest_deep WHERE run_id=%s', (run_id,))
        return
    if exists[0] not in ('extracting', 'loading'):
        return
    stored_hash=conn.execute('SELECT data_schema_hash FROM replica.sync_runs WHERE run_id=%s',
                             (run_id,)).fetchone()[0]
    if event.get('data_schema_hash',stored_hash)!=stored_hash:
        raise RuntimeError('Mixed schema versions in one Kafka data run')  # idempotent replay of promoted, rejected or aborted run
    if kind == 'delta':
        row = event.get('row')
        if event['op'] not in ('upsert','delete'):
            raise RuntimeError('Unsupported operation')
        if event['op'] == 'upsert' and (row is None or set(row) != set(FIELDS)):
            raise RuntimeError('Invalid upsert payload')
        conn.execute('''INSERT INTO replica.ingest_delta(run_id,id,op,payload)
            VALUES(%s,%s,%s,%s)
            ON CONFLICT(run_id,id) DO UPDATE
            SET op=EXCLUDED.op,payload=EXCLUDED.payload''',
            (run_id,event['id'],event['op'],psycopg.types.json.Jsonb(row)))
    elif kind == 'deep':
        payload = event['row']
        if set(payload) != set(FIELDS):
            raise RuntimeError('Invalid deep sweep payload')
        conn.execute('''INSERT INTO replica.ingest_deep(run_id,id,payload)
            VALUES(%s,%s,%s) ON CONFLICT(run_id,id) DO UPDATE SET payload=EXCLUDED.payload''',
            (run_id,event['id'],psycopg.types.json.Jsonb(payload)))
    elif kind == 'seen':
        conn.execute('''INSERT INTO replica.ingest_keys(run_id,id)
            VALUES(%s,%s) ON CONFLICT DO NOTHING''', (run_id,event['id']))
    elif kind == 'abort':
        conn.execute('''UPDATE replica.sync_runs SET status='aborted',error=%s,
            finished_at=clock_timestamp() WHERE run_id=%s''',
                     (event.get('reason','Source abort'),run_id))
        conn.execute('DELETE FROM replica.ingest_delta WHERE run_id=%s', (run_id,))
        conn.execute('DELETE FROM replica.ingest_keys WHERE run_id=%s', (run_id,))
        conn.execute('DELETE FROM replica.ingest_deep WHERE run_id=%s', (run_id,))
        log('batch_aborted', run_id=run_id)
    elif kind == 'end':
        on_end(conn, event)
    else:
        raise RuntimeError('Unknown kind '+str(kind))


def consume():
    kafka_ready()
    while True:
        c = Consumer({'bootstrap.servers': BROKER,
                      'group.id': 'lab-range-bluegreen-v4',
                      'auto.offset.reset': 'earliest',
                      'enable.auto.commit': False,
                      'enable.auto.offset.store': False,
                      'max.poll.interval.ms': 1800000})
        try:
            c.subscribe([TOPIC])
            log('consumer_started', topic=TOPIC)
            while True:
                messages = c.consume(num_messages=200, timeout=1)
                if not messages:
                    continue
                with psycopg.connect(LOCAL) as conn:
                    with conn.transaction():
                        for msg in messages:
                            if msg.error():
                                raise RuntimeError(str(msg.error()))
                            if msg.partition() != 0:
                                raise RuntimeError('Single Kafka partition required')
                            handle_event(conn,json.loads(msg.value()))
                # DB transaction commits first; Kafka offset commits second.
                c.commit(offsets=[TopicPartition(TOPIC,0,messages[-1].offset()+1)],
                         asynchronous=False)
        except KeyboardInterrupt:
            break
        except Exception as exc:
            log('consumer_retrying', error=str(exc))
            time.sleep(3)
        finally:
            c.close()


def rollback():
    with psycopg.connect(LOCAL) as conn:
        with conn.transaction():
            conn.execute('SELECT version FROM replica.active_state WHERE singleton FOR UPDATE')
            active = active_checkpoint(conn)
            if not active['slot']:
                raise RuntimeError('Nothing active to roll back')
            st=schema_evo.current(conn)
            if st['pending']:
                raise RuntimeError('Rollback disabled while schema backfill is pending')
            configure_schema(conn)
            other = 'green' if active['slot'] == 'blue' else 'blue'
            other_ver = conn.execute('''SELECT version FROM replica.slot_state
                WHERE slot=%s FOR UPDATE''', (other,)).fetchone()[0]
            if other_ver is None or other_ver == active['version']:
                raise RuntimeError('No previous validated generation in inactive slot')
            old_epoch=conn.execute('SELECT data_schema_hash FROM replica.generations WHERE version=%s',
                                   (other_ver,)).fetchone()[0]
            if old_epoch!=st['active']:
                raise RuntimeError('Rollback to previous schema epoch forbidden; retain old versioned view')
            r = conn.execute('''SELECT cutoff,row_count,next_sweep_id
                FROM replica.generations WHERE version=%s''', (other_ver,)).fetchone()
            point_view(conn, other)
            conn.execute('''UPDATE replica.active_state
                SET slot=%s,version=%s,cutoff=%s,row_count=%s,next_sweep_id=%s
                WHERE singleton''', (other,other_ver,*r))
            conn.execute('''INSERT INTO replica.transitions
                (action,from_slot,to_slot,from_version,to_version)
                VALUES('rollback',%s,%s,%s,%s)''',
                (active['slot'],other,active['version'],other_ver))
            log('rollback_done', from_version=active['version'], to_version=other_ver,
                from_slot=active['slot'], to_slot=other)


def status():
    with psycopg.connect(LOCAL) as conn:
        active = active_checkpoint(conn)
        schema_state=schema_evo.current(conn)
        slots = conn.execute('''SELECT slot,version,
            pg_total_relation_size(('replica.'||slot||'_contracts')::regclass)
            FROM replica.slot_state ORDER BY slot''').fetchall()
        journal_count = conn.execute('SELECT COUNT(*) FROM replica.change_journal').fetchone()[0]
        runs = conn.execute('''SELECT run_id,mode,status,base_version,result_version,
            delta_count,sweep_count,inserted_count,updated_count,deleted_count,
            logical_kafka_bytes,error,started_at,finished_at
            FROM replica.sync_runs ORDER BY started_at DESC LIMIT 15''').fetchall()
    log('lab_status', active=active, schema=schema_state, journal_entries=journal_count,
        slots=[{'slot':s,'version':v,'physical_bytes':b} for s,v,b in slots],
        runs=[dict(zip(['id','mode','status','base','result','delta_events',
             'sweep_keys','inserts','updates','deletes','bytes_json',
             'error','started','finished'], r)) for r in runs])

def seed(rows, description_bytes, confirm_large, dry_run=False):
    if rows < 1 or description_bytes < 0 or description_bytes > 240_000:
        raise ValueError('rows>=1, 0<=description_bytes<=240000')
    estimate = rows * (description_bytes + 150)
    log('seed_plan', rows=rows, description_bytes=description_bytes,
        estimated_logical_bytes=estimate, estimated_mib=round(estimate / 2**20, 2),
        note='Estimated uncompressed row payload, NOT PostgreSQL physical size')
    if dry_run:
        return
    if estimate > 512 * 2**20 and not confirm_large:
        raise RuntimeError('Refusing >512MiB seed without --confirm-large')
    admin_dsn = os.environ.get('REMOTE_ADMIN_DSN')
    if not admin_dsn:
        raise RuntimeError('REMOTE_ADMIN_DSN only permitted for lab fixture setup')
    with psycopg.connect(admin_dsn) as conn:
        with conn.transaction():
            conn.execute('TRUNCATE public.contracts')
            # Data generated entirely on fake remote, using pgcrypto-free MD5
            # blocks that vary by row+block. Estimates aren't disk-size guarantees.
            conn.execute('''INSERT INTO public.contracts
                (id,contract_code,amount,status,description)
                SELECT g, 'CON-' || lpad(g::text,10,'0'),
                       (g * 17.31)::numeric(14,2),
                       CASE WHEN g %% 3 = 0 THEN 'closed' ELSE 'active' END,
                       CASE WHEN %s=0 THEN '' ELSE left((
                           SELECT string_agg(md5(g::text || ':' || s::text),'' ORDER BY s)
                           FROM generate_series(0,(%s-1)/32) s
                       ), %s) END
                FROM generate_series(1,%s) g''',
                (description_bytes, max(description_bytes, 1), description_bytes, rows))
        physical = conn.execute('SELECT pg_total_relation_size(%s::regclass)',
                                ('public.contracts',)).fetchone()[0]
        dbsize = conn.execute('SELECT pg_database_size(current_database())').fetchone()[0]
    log('seed_measured', rows=rows, physical_table_including_indexes_bytes=physical,
        physical_table_mib=round(physical/2**20, 2), database_bytes=dbsize,
        warning='No fixed equivalence between rows, PostgreSQL physical GB and Kafka GB')


def load(workers, duration, multiplier):
    stop_at = time.monotonic() + duration
    guard = threading.Lock()
    latency = []
    failures = 0

    def worker(i):
        nonlocal failures
        try:
            with psycopg.connect(DIRECT, autocommit=True) as conn:
                conn.execute("SET statement_timeout = '10s'")
                while time.monotonic() < stop_at:
                    started = time.monotonic()
                    try:
                        conn.execute('''SELECT SUM(c.amount) FROM public.contracts c
                                        CROSS JOIN generate_series(1,%s) s
                                        WHERE c.id %% 5 = 0''', (multiplier,)).fetchone()
                        with guard:
                            latency.append(time.monotonic() - started)
                    except Exception:
                        with guard:
                            failures += 1
        except Exception:
            with guard:
                failures += 1

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(worker, range(workers)))
    ms = sorted(x*1000 for x in latency)
    def p(q):
        return round(ms[min(len(ms)-1,int(len(ms)*q))],1) if ms else None
    log('read_load_finished', workers=workers, duration_s=duration,
        queries=len(ms), failures=failures, qps=round(len(ms)/max(duration,1),1),
        p50_ms=p(.5), p95_ms=p(.95), p99_ms=p(.99))



def mutate(op, id_):
    """Fixture only: artificially change fake remote using its ADMIN DSN."""
    admin = os.environ.get('REMOTE_ADMIN_DSN')
    if not admin:
        raise RuntimeError('Fixture administrator not available to reader-only services')
    with psycopg.connect(admin) as conn:
        with conn.transaction():
            if op == 'insert':
                conn.execute('''INSERT INTO public.contracts
                    (id,contract_code,amount,status,description)
                    VALUES (%s,%s,777.00,'active','new synthetic contract')''',
                    (id_,'NEW-' + str(id_)))
            elif op == 'update':
                cur = conn.execute('''UPDATE public.contracts SET amount=amount+42 WHERE id=%s''', (id_,))
                if cur.rowcount != 1:
                    raise RuntimeError('ID not found')
            elif op == 'hard-delete':
                cur = conn.execute('DELETE FROM public.contracts WHERE id=%s', (id_,))
                if cur.rowcount != 1:
                    raise RuntimeError('ID not found')
            else:
                raise ValueError('Invalid fixture mutation')
    log('fixture_mutated', op=op, id=id_)


def abandon(run_id, force):
    """Operator-only: publish ordered ABORT for an abandoned batch.

    Must verify END has not already been published/queued for this run.
    """
    if not force:
        raise RuntimeError('Requires --force after inspecting Kafka and local status')
    kafka_ready()
    with psycopg.connect(LOCAL, autocommit=True) as conn:
        if not conn.execute('SELECT pg_try_advisory_lock(70261021)').fetchone()[0]:
            raise RuntimeError('Producer is active; refusing operator abort')
        row = conn.execute('SELECT status FROM replica.sync_runs WHERE run_id=%s',
                           (run_id,)).fetchone()
        if not row or row[0] not in ('extracting', 'loading'):
            raise RuntimeError('Batch not pending')
        p = producer()
        errors = []
        emit(p, TOPIC, {'run_id': run_id, 'kind': 'abort',
             'schema_version': SCHEMA,
             'reason': 'manual operator-abandon'}, errors)
        flush(p, errors)
        log('operator_abort_published', run_id=run_id,
            warning='If END was already queued, promotion might have happened first')


def schema_cli(args):
    # Schema events use a dedicated audit topic; backfill uses data topic.
    if args.schema_command=='abandon':
        if not args.force:
            raise RuntimeError('Inspect topic/order before --force; never abort after END')
        kafka_ready()
        p=producer();errs=[]
        emit(p,TOPIC,{'run_id':args.run_id,'kind':'schema_backfill_abort',
                       'schema_version':SCHEMA,'schema_hash':None,
                       'reason':'manual recovery after incomplete batch'},errs)
        flush(p,errs)
        log('schema_backfill_abort_queued',run_id=args.run_id)
        return
    if args.schema_command=='status':
        with psycopg.connect(LOCAL) as local:
            st=schema_evo.current(local)
            events=local.execute('''SELECT first_id,last_id,rows_checked FROM
                 replica.schema_backfill_intervals ORDER BY committed_at DESC LIMIT 10''').fetchall()
            log('schema_state',state=st,completed_intervals=events)
        return
    kafka_ready()
    ensure_proxy()
    p=producer()
    errs=[]
    def audit_schema(kind,**data):
        emit(p,AUDIT_TOPIC,{'run_id':str(uuid4()),'kind':kind,
                           'schema_version':SCHEMA,**data},errs)
        flush(p,errs)
    with psycopg.connect(REMOTE,autocommit=True) as remote:
        with psycopg.connect(LOCAL) as local:
            if args.schema_command=='plan':
                info=schema_evo.plan(remote,local,emit_audit=audit_schema)
            elif args.schema_command=='apply':
                with local.transaction():
                    # Migration barrier: synchronize both physical slots BEFORE
                    # modifying DDL. Pre-migration rollback history is retired.
                    local.execute('SELECT version FROM replica.active_state WHERE singleton FOR UPDATE')
                    state=active_checkpoint(local)
                    if state['version'] is None:
                        raise RuntimeError('Bootstrap required before schema evolution')
                    configure_schema(local)
                    other='green' if state['slot']=='blue' else 'blue'
                    before=local.execute('SELECT version FROM replica.slot_state WHERE slot=%s FOR UPDATE',
                                         (other,)).fetchone()[0]
                    align_standby(local,other,before,state['version'])
                    prune_journal(local)
                    info=schema_evo.prepare(remote,local)
                audit_schema('SCHEMA_PREPARED',**info)
            elif args.schema_command=='backfill':
                def send(ev):
                    ev['schema_version']=SCHEMA
                    emit(p,TOPIC,ev,errs)
                info=schema_evo.backfill_emit(remote,local,args.sweep_width,send,
                                             lambda:flush(p,errs))
            elif args.schema_command=='activate':
                with local.transaction():
                    info=schema_evo.activate(remote,local)
                if info.get('active_schema_hash'):
                    audit_schema('SCHEMA_ACTIVATED',**info)
            else:
                raise ValueError('Unsupported schema subcommand')
    log('schema_'+args.schema_command,**info)



def catalog_cli(args):
    """Observe source native schema as dictionary, persist JSONB history locally.

    SQLServer support here is intentionally SCHEMA OBSERVATION, not (yet) row
    replication: its keyset/canonical-row/hash adapter remains engine-specific.
    """
    source_key=args.source_key
    with psycopg.connect(LOCAL) as local:
        if args.catalog_command=='history':
            data=source_catalog.history(local,source_key,args.limit)
            log('catalog_history',source=source_key,observations=data)
            return
        if args.engine=='postgresql':
            with psycopg.connect(REMOTE, autocommit=True) as remote:
                remote.execute('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
                manifest=source_catalog.observe_pg(remote)
                remote.execute('COMMIT')
        elif args.engine=='sqlserver':
            try:
                import pymssql
            except ImportError as exc:
                raise RuntimeError('Install pymssql to observe SQL Server') from exc
            required=('MSSQL_HOST','MSSQL_USER','MSSQL_PASSWORD','MSSQL_DATABASE')
            missing=[v for v in required if not os.environ.get(v)]
            if missing: raise RuntimeError('Missing MSSQL connection environment: '+','.join(missing))
            with pymssql.connect(server=os.environ['MSSQL_HOST'],
                                port=int(os.environ.get('MSSQL_PORT','1433')),
                                user=os.environ['MSSQL_USER'],
                                password=os.environ['MSSQL_PASSWORD'],
                                database=os.environ['MSSQL_DATABASE'],
                                login_timeout=8,timeout=20,
                                charset='UTF-8') as remote:
                manifest=source_catalog.observe_mssql(remote,args.db_schema,args.table)
                # Two independent passes reject most concurrent DDL during read.
                if source_catalog.sha(manifest)!=source_catalog.sha(
                       source_catalog.observe_mssql(remote,args.db_schema,args.table)):
                    raise RuntimeError('SQL Server schema changed during catalog observation')
        else:
            raise RuntimeError('Unsupported source engine')
        if args.catalog_command=='inspect':
            log('catalog_inspected',source=source_key,engine=args.engine,
                native=manifest,logical=source_catalog.projected(manifest),
                native_hash=source_catalog.sha(manifest),
                logical_hash=source_catalog.sha(source_catalog.projected(manifest)))
            return
        if args.catalog_command!='observe':
            raise RuntimeError('Unknown catalog subcommand')
        with local.transaction():
            result=source_catalog.persist(local,source_key,manifest)
        log('catalog_observed',**result)
        if result['changed']:
            try:
                kafka_ready()
                audit(producer(),str(uuid4()),'SOURCE_DDL_OBSERVED',
                      source=source_key,engine=manifest['engine'],
                      native_hash=result['native_hash'],
                      logical_hash=result['logical_hash'],
                      classification=result['classification'],
                      changes=result['changes'],
                      native_changes=result['native_changes'])
            except Exception as exc:
                log('catalog_audit_kafka_unavailable',error=str(exc),
                    note='Database observation committed; Kafka audit is non-authoritative')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('bootstrap')
    incremental = sub.add_parser('incremental')
    incremental.add_argument('--sweep-width', type=int, default=1000)
    incremental.add_argument('--from-id', type=int, default=None,
                             help='Lab override: inspect explicit PK range start')
    incremental.add_argument('--batch-size', type=int, default=250)
    incremental.add_argument('--throttle-ms', type=int, default=0)
    sub.add_parser('consume')
    sub.add_parser('status')
    catalog=sub.add_parser('catalog',help='Read-only schema catalog for PostgreSQL or SQL Server')
    catalog_sub=catalog.add_subparsers(dest='catalog_command',required=True)
    for cmd in ('inspect','observe','history'):
        c=catalog_sub.add_parser(cmd)
        c.add_argument('--source-key',default='contracts-source')
        if cmd!='history':
            c.add_argument('--engine',choices=['postgresql','sqlserver'],
                           default=os.environ.get('REMOTE_ENGINE','postgresql'))
            c.add_argument('--db-schema',default='dbo')
            c.add_argument('--table',default='contracts')
        else:
            c.add_argument('--limit',type=int,default=20)
    sub.add_parser('rollback')
    schema_cmd=sub.add_parser('schema',help='Observe, plan and approve local schema evolution')
    sch_sub=schema_cmd.add_subparsers(dest='schema_command',required=True)
    sch_sub.add_parser('plan')
    sch_sub.add_parser('apply',help='Approve ONLY one appended nullable TEXT column')
    bs=sch_sub.add_parser('backfill',help='Publish one range of added column via Kafka')
    bs.add_argument('--sweep-width',type=int,default=1000)
    sch_sub.add_parser('activate',help='Append view columns after all ranges are validated')
    ab=sch_sub.add_parser('abandon',help='Operator abort of incomplete Kafka backfill')
    ab.add_argument('--run-id',required=True)
    ab.add_argument('--force',action='store_true')
    sch_sub.add_parser('status')
    abandoned = sub.add_parser('abandon')
    abandoned.add_argument('--run-id', required=True)
    abandoned.add_argument('--force', action='store_true')
    ch = sub.add_parser('chaos')
    ch.add_argument('action', choices=['init','status','up','down','clear',
                                      'latency','timeout','bandwidth'])
    ch.add_argument('amount', nargs='?', type=int)
    ch.add_argument('--target',choices=['postgresql','sqlserver'],default='postgresql')
    sched = sub.add_parser('schedule')
    sched.add_argument('--interval', type=int, default=60)
    sched.add_argument('--sweep-width', type=int, default=1000)
    ld = sub.add_parser('load')
    ld.add_argument('--workers', type=int, default=8)
    ld.add_argument('--duration', type=int, default=60)
    ld.add_argument('--multiplier', type=int, default=50)
    sd = sub.add_parser('seed')
    sd.add_argument('--rows', type=int, default=10000)
    sd.add_argument('--description-bytes', type=int, default=1024)
    sd.add_argument('--target-mib', type=float)
    sd.add_argument('--dry-run', action='store_true')
    sd.add_argument('--confirm-large', action='store_true')
    mu = sub.add_parser('mutate')
    mu.add_argument('op', choices=['insert','update','hard-delete'])
    mu.add_argument('id', type=int)
    args = parser.parse_args()
    if args.command == 'bootstrap':
        if REMOTE_ENGINE == 'sqlserver':
            import mssql_sync
            mssql_sync.poll('bootstrap')
        else:
            poll('bootstrap')
    elif args.command == 'incremental':
        if (args.sweep_width < 1 or args.batch_size < 1 or args.throttle_ms < 0 or
            (args.from_id is not None and args.from_id < 1)):
            parser.error('sweep-width>=1; batch-size>=1; throttle-ms>=0; from-id>=1')
        if REMOTE_ENGINE == 'sqlserver':
            import mssql_sync
            mssql_sync.poll('incremental', sweep_width=args.sweep_width,
                batch_size=args.batch_size, throttle_ms=args.throttle_ms,
                from_id=args.from_id)
        else:
            poll('incremental', sweep_width=args.sweep_width,
                batch_size=args.batch_size, throttle_ms=args.throttle_ms,
                from_id=args.from_id)
    elif args.command == 'consume':
        consume()
    elif args.command == 'status':
        status()
    elif args.command == 'catalog':
        catalog_cli(args)
    elif args.command == 'rollback':
        rollback()
    elif args.command == 'schema':
        schema_cli(args)
    elif args.command == 'abandon':
        abandon(args.run_id,args.force)
    elif args.command == 'chaos':
        chaos(args)
    elif args.command == 'load':
        load(args.workers,args.duration,args.multiplier)
    elif args.command == 'seed':
        rows = args.rows
        if args.target_mib is not None:
            if args.target_mib <= 0:
                parser.error('target-mib must be >0')
            rows = max(1,math.ceil(args.target_mib * 2**20 /
                                   (args.description_bytes + 150)))
        seed(rows,args.description_bytes,args.confirm_large,args.dry_run)
    elif args.command == 'mutate':
        mutate(args.op,args.id)
    elif args.command == 'schedule':
        if args.interval < 1 or args.sweep_width < 1:
            parser.error('interval>=1; sweep-width>=1')
        while True:
            try:
                if REMOTE_ENGINE == 'sqlserver':
                    import mssql_sync
                    mssql_sync.poll('incremental', sweep_width=args.sweep_width)
                else:
                    poll('incremental', sweep_width=args.sweep_width)
            except Exception as exc:
                log('scheduled_incremental_failed', error=str(exc))
            time.sleep(args.interval)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as exc:
        log('fatal', error=str(exc))
        sys.exit(1)
