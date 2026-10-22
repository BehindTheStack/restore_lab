"""Per-table Kafka staging, typed views and journal-backed blue/green publication."""
import os
from uuid import UUID

from psycopg import sql
from psycopg.types.json import Jsonb

from discovery_catalog import document, signature, validate_manifest


SCHEMA_SQL = '''
CREATE TABLE IF NOT EXISTS replica.discovered_tables (
 table_key TEXT PRIMARY KEY, engine TEXT NOT NULL, source_schema TEXT NOT NULL, source_table TEXT NOT NULL,
 destination_schema TEXT NOT NULL, destination_table TEXT NOT NULL,
 observed_manifest JSONB NOT NULL, manifest JSONB, schema_hash TEXT,
 active_slot TEXT CHECK(active_slot IN ('blue','green')), active_generation BIGINT,
 blue_generation BIGINT, green_generation BIGINT, row_count BIGINT NOT NULL DEFAULT 0,
 status TEXT NOT NULL DEFAULT 'new', missing_cycles INTEGER NOT NULL DEFAULT 0,
 last_error TEXT, observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 checked_at TIMESTAMPTZ, published_at TIMESTAMPTZ,
 UNIQUE(destination_schema,destination_table)
);
CREATE TABLE IF NOT EXISTS replica.discovered_cycles (
 cycle_id UUID PRIMARY KEY, engine TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'loading',
 started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), finished_at TIMESTAMPTZ, error TEXT
);
CREATE TABLE IF NOT EXISTS replica.discovered_runs (
 run_id UUID PRIMARY KEY, cycle_id UUID REFERENCES replica.discovered_cycles(cycle_id),
 table_key TEXT NOT NULL REFERENCES replica.discovered_tables(table_key),
 mode TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'loading', manifest JSONB NOT NULL,
 base_generation BIGINT, result_generation BIGINT, full_snapshot BOOLEAN NOT NULL,
 delta_count BIGINT, seen_count BIGINT, inserted_count BIGINT, updated_count BIGINT, deleted_count BIGINT,
 logical_bytes BIGINT, error TEXT, started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS discovered_runs_recent ON replica.discovered_runs(started_at DESC);
CREATE TABLE IF NOT EXISTS replica.discovered_generations (
 generation BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 table_key TEXT NOT NULL REFERENCES replica.discovered_tables(table_key),
 parent_generation BIGINT REFERENCES replica.discovered_generations(generation),
 run_id UUID NOT NULL UNIQUE REFERENCES replica.discovered_runs(run_id),
 schema_hash TEXT NOT NULL, row_count BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS replica.discovered_blue (
 table_key TEXT NOT NULL REFERENCES replica.discovered_tables(table_key), row_key TEXT NOT NULL,
 payload JSONB NOT NULL, row_hash TEXT NOT NULL, PRIMARY KEY(table_key,row_key)
);
CREATE TABLE IF NOT EXISTS replica.discovered_green (LIKE replica.discovered_blue INCLUDING ALL);
CREATE TABLE IF NOT EXISTS replica.discovered_delta (
 run_id UUID NOT NULL REFERENCES replica.discovered_runs(run_id), row_key TEXT NOT NULL,
 seq BIGINT NOT NULL, payload JSONB NOT NULL, row_hash TEXT NOT NULL,
 PRIMARY KEY(run_id,row_key), UNIQUE(run_id,seq)
);
CREATE TABLE IF NOT EXISTS replica.discovered_seen (
 run_id UUID NOT NULL REFERENCES replica.discovered_runs(run_id), row_key TEXT NOT NULL,
 seq BIGINT NOT NULL, row_hash TEXT NOT NULL, PRIMARY KEY(run_id,row_key), UNIQUE(run_id,seq)
);
CREATE TABLE IF NOT EXISTS replica.discovered_journal (
 generation BIGINT NOT NULL REFERENCES replica.discovered_generations(generation), row_key TEXT NOT NULL,
 before_row JSONB, after_row JSONB, before_hash TEXT, after_hash TEXT, PRIMARY KEY(generation,row_key)
);
CREATE TABLE IF NOT EXISTS replica.discovered_schema_history (
 id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, table_key TEXT NOT NULL,
 manifest JSONB NOT NULL, schema_hash TEXT NOT NULL, classification TEXT NOT NULL,
 observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
GRANT SELECT ON replica.discovered_tables,replica.discovered_cycles,replica.discovered_runs,
 replica.discovered_generations,replica.discovered_schema_history TO monitor_reader;
GRANT SELECT ON replica.generations TO monitor_reader;
'''


def ensure_schema(conn):
    with conn.transaction():
        conn.execute('SELECT pg_advisory_xact_lock(70261023)')
        conn.execute(SCHEMA_SQL)


def one(conn, statement, args=()):
    cur = conn.execute(statement,args)
    row = cur.fetchone()
    return dict(zip((c.name for c in cur.description),row)) if row else None


def state(conn, key, lock=False):
    return one(conn,'SELECT * FROM replica.discovered_tables WHERE table_key=%s' +
               (' FOR UPDATE' if lock else ''),(key,))


def slot_table(slot):
    if slot not in ('blue','green'):
        raise ValueError('Invalid slot')
    return sql.Identifier('replica','discovered_' + slot)


def row_key(manifest, payload, position):
    return signature([payload[k] for k in manifest['primary_key']] if manifest['primary_key']
                     else ['ordinal',position])


def register(conn, manifest):
    old = state(conn,manifest['key'])
    if not old or signature(old['observed_manifest']) != signature(manifest):
        conn.execute('''INSERT INTO replica.discovered_schema_history
            (table_key,manifest,schema_hash,classification) VALUES (%s,%s,%s,%s)''',
            (manifest['key'],Jsonb(manifest),signature(manifest),'new' if not old else 'changed'))
    conn.execute('''INSERT INTO replica.discovered_tables
        (table_key,engine,source_schema,source_table,destination_schema,destination_table,observed_manifest)
        VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(table_key) DO UPDATE
        SET observed_manifest=excluded.observed_manifest,observed_at=clock_timestamp()''',
        (manifest['key'],manifest['engine'],manifest['schema'],manifest['table'],
         manifest['destination_schema'],manifest['destination_table'],Jsonb(manifest)))


def ownership(conn, manifest):
    return one(conn,'''SELECT c.relkind,obj_description(c.oid,'pg_class') AS marker
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=%s AND c.relname=%s''',
        (manifest['destination_schema'],manifest['destination_table']))


def publish_view(conn, manifest):
    validate_manifest(manifest)
    marker = 'kafka-restore-lab:discovery:' + manifest['key']
    existing = ownership(conn,manifest)
    if existing and (existing['relkind'] != 'v' or existing['marker'] != marker):
        raise RuntimeError('Destination name belongs to an unmanaged object; refusing overwrite')
    name = sql.Identifier(manifest['destination_schema'],manifest['destination_table'])
    conn.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(manifest['destination_schema'])))
    if existing:
        # RESTRICT is deliberate: never destroy views owned by downstream users.
        conn.execute(sql.SQL('DROP VIEW {} RESTRICT').format(name))
    fields = sql.SQL(',').join(sql.SQL('(r.payload->>{})::{} AS {}').format(
        sql.Literal(col['name']),sql.SQL(col['target_type']),sql.Identifier(col['name']))
        for col in manifest['columns'])
    branches = []
    for slot in ('blue','green'):
        branches.append(sql.SQL('''SELECT {} FROM {} r
            JOIN replica.discovered_tables s ON s.table_key=r.table_key
            WHERE r.table_key={} AND s.active_slot={} ''').format(
            fields,slot_table(slot),sql.Literal(manifest['key']),sql.Literal(slot)))
    conn.execute(sql.SQL('CREATE VIEW {} AS ').format(name) + sql.SQL(' UNION ALL ').join(branches))
    conn.execute(sql.SQL('COMMENT ON VIEW {} IS {}').format(name,sql.Literal(marker)))


def retire_view(conn, item):
    manifest = item['manifest'] or item['observed_manifest']
    existing = ownership(conn,manifest)
    if existing:
        if existing['marker'] != 'kafka-restore-lab:discovery:' + item['table_key'] or existing['relkind'] != 'v':
            raise RuntimeError('Refusing to retire an unmanaged destination object')
        conn.execute(sql.SQL('DROP VIEW {} RESTRICT').format(
            sql.Identifier(item['destination_schema'],item['destination_table'])))


def align(conn, item, destination):
    old = item[destination + '_generation']
    current = item['active_generation']
    if old == current:
        return
    target_parent = conn.execute('SELECT parent_generation FROM replica.discovered_generations WHERE generation=%s',
                                 (current,)).fetchone()[0]
    if target_parent == old:
        version, row, hash_ = current, 'after_row', 'after_hash'
    else:
        old_parent = conn.execute('SELECT parent_generation FROM replica.discovered_generations WHERE generation=%s',
                                  (old,)).fetchone()
        if not old_parent or old_parent[0] != current:
            raise RuntimeError('Slots are not adjacent validated generations; refusing alignment')
        version, row, hash_ = old, 'before_row', 'before_hash'
    conn.execute(sql.SQL('''DELETE FROM {} r USING replica.discovered_journal j
        WHERE r.table_key=%s AND j.generation=%s AND r.row_key=j.row_key AND j.{} IS NULL''').format(
        slot_table(destination),sql.Identifier(row)),(item['table_key'],version))
    conn.execute(sql.SQL('''INSERT INTO {}(table_key,row_key,payload,row_hash)
        SELECT %s,row_key,{},{} FROM replica.discovered_journal WHERE generation=%s AND {} IS NOT NULL
        ON CONFLICT(table_key,row_key) DO UPDATE SET payload=excluded.payload,row_hash=excluded.row_hash''').format(
        slot_table(destination),sql.Identifier(row),sql.Identifier(hash_),sql.Identifier(row)),
        (item['table_key'],version))


def stream_digest(conn, relation, run_id, payload_fields):
    import hashlib
    digest = hashlib.sha256()
    count = 0
    with conn.cursor(name='validate_' + relation) as cur:
        cur.execute(sql.SQL('SELECT {} FROM {} WHERE run_id=%s ORDER BY seq').format(
            sql.SQL(',').join(sql.Identifier(x) for x in payload_fields),
            sql.Identifier('replica','discovered_' + relation)),(run_id,))
        for row in cur:
            digest.update((document(dict(zip(payload_fields,row))) + '\n').encode())
            count += 1
    return count,digest.hexdigest()


def promote(conn, run, event):
    key, run_id = run['table_key'],run['run_id']
    item = state(conn,key,True)
    manifest = run['manifest']
    if item['active_generation'] != run['base_generation']:
        raise RuntimeError('Base generation changed since extraction')
    for relation,fields in (('seen',('row_key','row_hash')),('delta',('row_key','payload','row_hash'))):
        count, hash_ = stream_digest(conn,relation,run_id,fields)
        if count != event[relation + '_count'] or hash_ != event[relation + '_sha256']:
            raise RuntimeError('Kafka count/hash mismatch: ' + relation)
    destination = 'green' if item['active_slot'] == 'blue' else 'blue'
    if item['active_generation'] is not None:
        align(conn,item,destination)
    if run['full_snapshot']:
        conn.execute(sql.SQL('DELETE FROM {} WHERE table_key=%s').format(slot_table(destination)),(key,))
    conn.execute(sql.SQL('''INSERT INTO {}(table_key,row_key,payload,row_hash)
        SELECT %s,row_key,payload,row_hash FROM replica.discovered_delta WHERE run_id=%s
        ON CONFLICT(table_key,row_key) DO UPDATE SET payload=excluded.payload,row_hash=excluded.row_hash''').format(
        slot_table(destination)),(key,run_id))
    conn.execute(sql.SQL('''DELETE FROM {} r WHERE r.table_key=%s AND NOT EXISTS
        (SELECT 1 FROM replica.discovered_seen s WHERE s.run_id=%s AND s.row_key=r.row_key)''').format(
        slot_table(destination)),(key,run_id))
    mismatch = conn.execute(sql.SQL('''SELECT count(*) FROM replica.discovered_seen s LEFT JOIN {} r
        ON r.table_key=%s AND r.row_key=s.row_key WHERE s.run_id=%s AND r.row_hash IS DISTINCT FROM s.row_hash''').format(
        slot_table(destination)),(key,run_id)).fetchone()[0]
    if mismatch:
        raise RuntimeError('Published range content does not match source fingerprints')
    row_count = event['seen_count']
    generation = conn.execute('''INSERT INTO replica.discovered_generations
        (table_key,parent_generation,run_id,schema_hash,row_count) VALUES(%s,%s,%s,%s,%s)
        RETURNING generation''',(key,item['active_generation'],run_id,signature(manifest),row_count)).fetchone()[0]
    if item['active_slot']:
        before = sql.SQL('SELECT row_key,payload,row_hash FROM {} WHERE table_key={}').format(
            slot_table(item['active_slot']),sql.Literal(key))
    else:
        before = sql.SQL('SELECT NULL::text AS row_key,NULL::jsonb AS payload,NULL::text AS row_hash WHERE false')
    conn.execute(sql.SQL('''INSERT INTO replica.discovered_journal
        (generation,row_key,before_row,after_row,before_hash,after_hash)
        SELECT %s,coalesce(a.row_key,b.row_key),a.payload,b.payload,a.row_hash,b.row_hash
        FROM ({}) a FULL JOIN (SELECT row_key,payload,row_hash FROM {} WHERE table_key=%s) b
        ON a.row_key=b.row_key WHERE a.row_hash IS DISTINCT FROM b.row_hash''').format(
        before,slot_table(destination)),(generation,key))
    inserted,updated,deleted = conn.execute('''SELECT
        count(*) FILTER(WHERE before_row IS NULL),
        count(*) FILTER(WHERE before_row IS NOT NULL AND after_row IS NOT NULL),
        count(*) FILTER(WHERE after_row IS NULL)
        FROM replica.discovered_journal WHERE generation=%s''',(generation,)).fetchone()
    # Without a PK the copy is exact, but INSERT vs UPDATE cannot be inferred.
    if not manifest['primary_key'] and item['active_generation'] is not None:
        inserted = updated = deleted = None
    removed = deleted if deleted is not None else max(0,item['row_count']-row_count)
    limit = float(os.getenv('MAX_REMOVE_FRACTION','0.20'))
    if removed and item['row_count'] and removed / item['row_count'] > limit:
        raise RuntimeError(f'Deletion guard: {removed}/{item["row_count"]}, limit={limit:.2%}')
    if item['active_generation'] is None:
        other = 'green' if destination == 'blue' else 'blue'
        conn.execute(sql.SQL('DELETE FROM {} WHERE table_key=%s').format(slot_table(other)),(key,))
        conn.execute(sql.SQL('INSERT INTO {} SELECT * FROM {} WHERE table_key=%s').format(
            slot_table(other),slot_table(destination)),(key,))
        conn.execute(sql.SQL('UPDATE replica.discovered_tables SET {}=%s WHERE table_key=%s').format(
            sql.Identifier(other + '_generation')),(generation,key))
    conn.execute(sql.SQL('''UPDATE replica.discovered_tables SET active_slot=%s,active_generation=%s,
        {}=%s,manifest=%s,schema_hash=%s,row_count=%s,status='active',missing_cycles=0,last_error=NULL,
        checked_at=clock_timestamp(),published_at=clock_timestamp() WHERE table_key=%s''').format(
        sql.Identifier(destination + '_generation')),
        (destination,generation,generation,Jsonb(manifest),signature(manifest),row_count,key))
    if item['schema_hash'] != signature(manifest) or not ownership(conn,manifest):
        publish_view(conn,manifest)
    # Force casts before commit: lazy views must not publish invalid values.
    fields = sql.SQL(',').join(sql.SQL('count({})').format(sql.Identifier(c['name'])) for c in manifest['columns'])
    conn.execute(sql.SQL('SELECT {} FROM {}').format(fields,
        sql.Identifier(manifest['destination_schema'],manifest['destination_table']))).fetchone()
    conn.execute('''UPDATE replica.discovered_runs SET status='promoted',result_generation=%s,
        delta_count=%s,seen_count=%s,inserted_count=%s,updated_count=%s,deleted_count=%s,
        logical_bytes=%s,finished_at=clock_timestamp() WHERE run_id=%s''',
        (generation,event['delta_count'],event['seen_count'],inserted,updated,deleted,event['logical_bytes'],run_id))


def cleanup_staging(conn, run_id):
    for relation in ('delta','seen'):
        conn.execute(sql.SQL('DELETE FROM {} WHERE run_id=%s').format(
            sql.Identifier('replica','discovered_' + relation)),(run_id,))


def handle(conn, event):
    kind = event['kind']
    if kind == 'table_inventory':
        cycle_id = str(UUID(event['cycle_id']))
        cycle = one(conn,'SELECT * FROM replica.discovered_cycles WHERE cycle_id=%s FOR UPDATE',(cycle_id,))
        if not cycle or cycle['status'] == 'completed':
            return
        observed = set(event['visible_keys'])
        known = set(event.get('known_keys',event['visible_keys']))
        rows = conn.execute('SELECT table_key FROM replica.discovered_tables WHERE engine=%s',(cycle['engine'],)).fetchall()
        for (key,) in rows:
            item = state(conn,key,True)
            if key in observed:
                conn.execute('UPDATE replica.discovered_tables SET missing_cycles=0 WHERE table_key=%s',(key,))
                continue
            if key in known:
                # A visible object with denied SELECT is not evidence of DROP.
                conn.execute("UPDATE replica.discovered_tables SET status='inaccessible',missing_cycles=0,last_error='SELECT indisponível na origem; última publicação preservada' WHERE table_key=%s",(key,))
                continue
            missing = item['missing_cycles'] + 1
            if missing >= 2:
                try:
                    with conn.transaction():
                        retire_view(conn,item)
                    status,error = 'missing_or_inaccessible','Objeto ausente ou sem visibilidade em dois catálogos completos; dados retidos.'
                except Exception as exc:
                    status,error = 'blocked',str(exc)[:1000]
                conn.execute('''UPDATE replica.discovered_tables SET status=%s,last_error=%s,missing_cycles=%s
                    WHERE table_key=%s''',(status,error,missing,key))
                if status == 'missing_or_inaccessible' and item['status'] != status:
                    manifest = item['manifest'] or item['observed_manifest']
                    conn.execute('''INSERT INTO replica.discovered_schema_history
                        (table_key,manifest,schema_hash,classification) VALUES(%s,%s,%s,%s)''',
                        (key,Jsonb(manifest),signature(manifest),status))
            else:
                conn.execute('UPDATE replica.discovered_tables SET missing_cycles=%s WHERE table_key=%s',(missing,key))
        conn.execute("UPDATE replica.discovered_cycles SET status='completed',finished_at=clock_timestamp() WHERE cycle_id=%s",(cycle_id,))
        return
    run_id = str(UUID(event['run_id']))
    run = one(conn,'SELECT * FROM replica.discovered_runs WHERE run_id=%s FOR UPDATE',(run_id,))
    if not run and kind == 'table_start':
        manifest = event['manifest']
        validate_manifest(manifest)
        register(conn,manifest)
        cycle_id = str(UUID(event['cycle_id']))
        conn.execute('INSERT INTO replica.discovered_cycles(cycle_id,engine) VALUES(%s,%s) ON CONFLICT DO NOTHING',
                     (cycle_id,manifest['engine']))
        base = None
        if event.get('base_run_id'):
            base = conn.execute('SELECT generation FROM replica.discovered_generations WHERE run_id=%s',
                                (str(UUID(event['base_run_id'])),)).fetchone()
        conn.execute('''INSERT INTO replica.discovered_runs
            (run_id,cycle_id,table_key,mode,manifest,base_generation,full_snapshot,status,error)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
            (run_id,cycle_id,manifest['key'],event['mode'],Jsonb(manifest),base[0] if base else None,
             event['full_snapshot'],'rejected' if event.get('base_run_id') and not base else 'loading',
             'Missing Kafka replay baseline; perform a fresh source scan' if event.get('base_run_id') and not base else None))
        run = one(conn,'SELECT * FROM replica.discovered_runs WHERE run_id=%s FOR UPDATE',(run_id,))
    if run and kind == 'table_abort' and run['status'] != 'promoted':
        conn.execute("UPDATE replica.discovered_runs SET status='aborted',error=%s,finished_at=clock_timestamp() WHERE run_id=%s AND status='loading'",
                     (event.get('reason','Aborted source read')[:1000],run_id))
        cleanup_staging(conn,run_id)
        return
    if not run or run['status'] != 'loading':
        return
    if kind == 'table_start':
        validate_manifest(run['manifest'])
    elif kind in ('table_seen','table_delta'):
        if kind == 'table_delta':
            payload = event['payload']
            if set(payload) != {c['name'] for c in run['manifest']['columns']}:
                raise ValueError('Unapproved row projection')
            if any(v is not None and not isinstance(v,str) for v in payload.values()):
                raise ValueError('Row values must be lossless text or NULL')
            if signature(payload) != event['row_hash'] or row_key(run['manifest'],payload,event['position']) != event['row_key']:
                raise ValueError('Invalid row/key fingerprint')
            conn.execute('''INSERT INTO replica.discovered_delta(run_id,row_key,seq,payload,row_hash)
                VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
                (run_id,event['row_key'],event['seq'],Jsonb(payload),event['row_hash']))
        else:
            conn.execute('''INSERT INTO replica.discovered_seen(run_id,row_key,seq,row_hash)
                VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
                (run_id,event['row_key'],event['seq'],event['row_hash']))
    elif kind == 'table_end':
        try:
            with conn.transaction():
                promote(conn,run,event)
        except Exception as exc:
            conn.execute("UPDATE replica.discovered_runs SET status='rejected',error=%s,finished_at=clock_timestamp() WHERE run_id=%s",
                         (str(exc)[:1000],run_id))
            conn.execute("UPDATE replica.discovered_tables SET status='error',last_error=%s WHERE table_key=%s",
                         (str(exc)[:1000],run['table_key']))
        cleanup_staging(conn,run_id)
    else:
        raise ValueError('Unknown table event')


def rollback(conn, key):
    with conn.transaction():
        item = state(conn,key,True)
        if not item or item['status'] != 'active':
            raise RuntimeError('No active table to roll back')
        other = 'green' if item['active_slot'] == 'blue' else 'blue'
        generation = item[other + '_generation']
        if not generation or generation == item['active_generation']:
            raise RuntimeError('No previous generation available')
        old = one(conn,'SELECT * FROM replica.discovered_generations WHERE generation=%s',(generation,))
        if old['schema_hash'] != item['schema_hash']:
            raise RuntimeError('Rollback across incompatible schemas is forbidden')
        conn.execute('''UPDATE replica.discovered_tables SET active_slot=%s,active_generation=%s,row_count=%s
            WHERE table_key=%s''',(other,generation,old['row_count'],key))
