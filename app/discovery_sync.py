"""Automatic multi-table reconciliation over the existing Kafka consumer."""
import hashlib
import os
import signal
import threading
import time
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from discovery_catalog import Source, document, signature, validate_manifest, table_key
import discovery_store as store


def wait_for(conn, relation, key, timeout):
    if relation == 'runs':
        statement = 'SELECT status,error FROM replica.discovered_runs WHERE run_id=%s'
        success = 'promoted'
    else:
        statement = 'SELECT status,error FROM replica.discovered_cycles WHERE cycle_id=%s'
        success = 'completed'
    deadline = time.monotonic() + timeout
    while True:
        row = conn.execute(statement,(key,)).fetchone()
        if not row:
            raise RuntimeError('Missing reconciliation state: ' + str(key))
        if row[0] == success:
            return
        if row[0] != 'loading':
            raise RuntimeError(f'{key}: {row[0]}: {row[1]}')
        if time.monotonic() >= deadline:
            raise TimeoutError(f'{key}: consumer promotion timed out; resume this batch, do not republish it')
        time.sleep(1)


def publish_table(lab, source, local, cycle_id, observed, batch_size=250):
    key = observed['key']
    old = store.state(local,key)
    manifest_hash = signature(observed)
    full = old['active_generation'] is None or old['schema_hash'] != manifest_hash or not observed['primary_key']
    run_id = str(uuid4())
    base_run = local.execute('SELECT run_id FROM replica.discovered_generations WHERE generation=%s',
                             (old['active_generation'],)).fetchone() if old['active_generation'] else None
    mode = 'bootstrap' if old['active_generation'] is None else ('schema' if full and old['schema_hash'] != manifest_hash else 'incremental')
    local.execute('''INSERT INTO replica.discovered_runs
        (run_id,cycle_id,table_key,mode,manifest,base_generation,full_snapshot)
        VALUES(%s,%s,%s,%s,%s,%s,%s)''',
        (run_id,cycle_id,key,mode,Jsonb(observed),old['active_generation'],full))
    publisher, errors = lab.producer(), []
    def send(kind, **data):
        event = {'run_id':run_id,'kind':kind,'schema_version':lab.SCHEMA,**data}
        return lab.emit(publisher,lab.TOPIC,event,errors)
    n_seen = n_delta = n_bytes = 0
    seen_hash,delta_hash = hashlib.sha256(),hashlib.sha256()
    try:
        with source.connect() as remote:
            with source.snapshot(remote):
                current = source.inventory(remote).get(key)
                if not current or signature(current) != manifest_hash:
                    raise RuntimeError('Source metadata or SELECT visibility changed before the scan')
                send('table_start',manifest=observed,cycle_id=cycle_id,mode=mode,full_snapshot=full,
                     base_run_id=str(base_run[0]) if base_run else None)
                lab.flush(publisher,errors)
                for batch in source.rows(remote,observed,batch_size):
                    keyed = []
                    for payload in batch:
                        if any(v is not None and '\x00' in v for v in payload.values()):
                            raise ValueError('Source text contains NUL, which PostgreSQL text/JSONB cannot represent')
                        n_seen += 1
                        keyed.append((store.row_key(observed,payload,n_seen),payload,n_seen))
                    existing = {}
                    if not full:
                        existing = dict(local.execute(store.sql.SQL('''SELECT row_key,row_hash FROM {}
                            WHERE table_key=%s AND row_key=ANY(%s)''').format(store.slot_table(old['active_slot'])),
                            (key,[k for k,_,_ in keyed])).fetchall())
                    for row_key,payload,position in keyed:
                        row_hash = signature(payload)
                        seen = {'row_key':row_key,'row_hash':row_hash}
                        seen_hash.update((document(seen) + '\n').encode())
                        n_bytes += send('table_seen',seq=position,**seen)
                        if full or existing.get(row_key) != row_hash:
                            n_delta += 1
                            delta = {**seen,'payload':payload}
                            delta_hash.update((document(delta) + '\n').encode())
                            n_bytes += send('table_delta',seq=n_delta,position=position,**delta)
                    lab.flush(publisher,errors)
                    throttle = int(os.getenv('DISCOVERY_THROTTLE_MS','0'))
                    if throttle:
                        time.sleep(throttle/1000)
                after = source.inventory(remote).get(key)
                if not after or signature(after) != manifest_hash:
                    raise RuntimeError('Source schema changed during the scan; refusing publication')
            # END is published only after the read-only snapshot committed.
        send('table_end',seen_count=n_seen,seen_sha256=seen_hash.hexdigest(),
             delta_count=n_delta,delta_sha256=delta_hash.hexdigest(),logical_bytes=n_bytes)
        lab.flush(publisher,errors)
        lab.log('table_emitted',run_id=run_id,source=observed['schema']+'.'+observed['table'],
                mode=mode,rows=n_seen,changed=n_delta,bytes=n_bytes)
        return run_id
    except Exception as exc:
        try:
            send('table_abort',reason=str(exc)[:1000])
            lab.flush(publisher,errors)
        except Exception:
            pass
        local.execute('''UPDATE replica.discovered_runs SET status='failed',error=%s,
            finished_at=clock_timestamp() WHERE run_id=%s AND status='loading' ''',(str(exc)[:1000],run_id))
        raise


def cycle(lab, timeout=600, strict=False, batch_size=250):
    source = Source(lab.REMOTE_ENGINE,lab.REMOTE)
    if lab.REMOTE_ENGINE == 'sqlserver':
        import mssql_sync
        prepare = mssql_sync.ensure_proxy if os.getenv('MSSQL_HOST') == 'toxiproxy' else lambda: None
    else:
        prepare = lab.ensure_proxy
    deadline = time.monotonic() + 60
    while True:
        try:
            prepare()
            break
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)
    lab.kafka_ready()
    with psycopg.connect(lab.LOCAL,autocommit=True) as local:
        store.ensure_schema(local)
        if not local.execute('SELECT pg_try_advisory_lock(70261021)').fetchone()[0]:
            raise RuntimeError('Another producer is running')
        # Finish previously published batches before sending anything new.
        pending = local.execute('''SELECT r.run_id FROM replica.discovered_runs r
            JOIN replica.discovered_tables t USING(table_key) WHERE t.engine=%s AND r.status='loading'
            ORDER BY r.started_at''',(lab.REMOTE_ENGINE,)).fetchall()
        for (run_id,) in pending:
            lab.log('table_resuming',run_id=str(run_id))
            wait_for(local,'runs',run_id,timeout)
        cycle_id = str(uuid4())
        local.execute('INSERT INTO replica.discovered_cycles(cycle_id,engine) VALUES(%s,%s)',(cycle_id,lab.REMOTE_ENGINE))
        try:
            with source.connect() as remote:
                initial = source.inventory(remote)
        except Exception as exc:
            local.execute("UPDATE replica.discovered_cycles SET status='failed',error=%s,finished_at=clock_timestamp() WHERE cycle_id=%s",
                          (str(exc)[:4000],cycle_id))
            raise
        problems,failures = [],[]
        for manifest in initial.values():
            store.register(local,manifest)
            if not manifest['readable']:
                local.execute("UPDATE replica.discovered_tables SET status='inaccessible',last_error='SELECT indisponível na origem' WHERE table_key=%s AND status!='missing_or_inaccessible'",(manifest['key'],))
                problems.append(manifest['schema']+'.'+manifest['table']+': SELECT indisponível')
                continue
            try:
                validate_manifest(manifest)
                run_id = publish_table(lab,source,local,cycle_id,manifest,batch_size)
                wait_for(local,'runs',run_id,timeout)
            except Exception as exc:
                # A timed-out pending run must retain its state for ordered resume.
                local.execute("UPDATE replica.discovered_tables SET status='error',last_error=%s WHERE table_key=%s",
                              (str(exc)[:1000],manifest['key']))
                problems.append(manifest['schema']+'.'+manifest['table']+': '+str(exc))
                failures.append(str(exc))
                lab.log('table_sync_failed',source=manifest['schema']+'.'+manifest['table'],error=str(exc))
        try:
            with source.connect() as remote:
                final = source.inventory(remote)
            if signature(initial) != signature(final):
                raise RuntimeError('Catalog changed during cycle; no missing-table decisions published')
            publisher,errors = lab.producer(),[]
            visible = [key for key,table in final.items() if table['readable']]
            lab.emit(publisher,lab.TOPIC,{'kind':'table_inventory','schema_version':lab.SCHEMA,
                     'run_id':cycle_id,'cycle_id':cycle_id,'visible_keys':visible,
                     'known_keys':list(final)},errors)
            lab.flush(publisher,errors)
            wait_for(local,'cycles',cycle_id,timeout)
        except Exception as exc:
            local.execute("UPDATE replica.discovered_cycles SET status='failed',error=%s,finished_at=clock_timestamp() WHERE cycle_id=%s AND status='loading'",
                          (str(exc)[:4000],cycle_id))
            raise
        if problems:
            local.execute('UPDATE replica.discovered_cycles SET error=%s WHERE cycle_id=%s',
                          ('; '.join(problems)[:4000],cycle_id))
        lab.log('discovery_cycle_completed',cycle_id=cycle_id,tables=len(initial),problems=problems)
        if strict and failures:
            raise RuntimeError('Reconciliation has blocked tables: ' + '; '.join(problems))


def command(lab, args):
    if args.discovery_command in ('init','once'):
        cycle(lab,timeout=args.promotion_timeout,strict=True,batch_size=args.batch_size)
    elif args.discovery_command == 'schedule':
        stopping = threading.Event()
        def stop(signum,frame):
            lab.log('discovery_scheduler_stopping',signal=signum)
            stopping.set()
        signal.signal(signal.SIGTERM,stop)
        signal.signal(signal.SIGINT,stop)
        while not stopping.is_set():
            try:
                cycle(lab,timeout=args.promotion_timeout,batch_size=args.batch_size)
            except Exception as exc:
                lab.log('discovery_cycle_failed',error=str(exc))
            stopping.wait(args.interval)
    else:
        with psycopg.connect(lab.LOCAL,autocommit=True) as local:
            store.ensure_schema(local)
            if args.discovery_command == 'status':
                rows = local.execute('''SELECT engine,source_schema,source_table,destination_schema,
                    destination_table,status,active_generation,row_count,last_error
                    FROM replica.discovered_tables ORDER BY engine,source_schema,source_table''').fetchall()
                lab.log('discovery_status',tables=[dict(zip(('engine','schema','table','destination_schema',
                        'destination_table','status','generation','rows','error'),r)) for r in rows])
            elif args.discovery_command == 'rollback':
                store.rollback(local,table_key(lab.REMOTE_ENGINE,args.db_schema,args.table))
            elif args.discovery_command == 'abandon':
                if not args.force:
                    raise RuntimeError('Inspect Kafka before confirming --force; END may already be queued')
                publisher,errors = lab.producer(),[]
                lab.emit(publisher,lab.TOPIC,{'kind':'table_abort','schema_version':lab.SCHEMA,
                    'run_id':args.run_id,'reason':'Operator abandoned the pending table scan'},errors)
                lab.flush(publisher,errors)
