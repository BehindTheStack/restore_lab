"""Real PostgreSQL retention checks. Fixtures and deletes are rolled back."""
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb
import retention as r

assert os.getenv('LAB_RETENTION_TEST') == '1', 'Use tests/e2e_retention.sh'
now = datetime.now(timezone.utc)
old = now - timedelta(days=10)
recent = now - timedelta(days=2)
with psycopg.connect(os.environ['LOCAL_DSN'], autocommit=True) as conn:
    r.ensure_schema(conn)
    conn.execute('BEGIN')
    try:
        token = 'retention-' + str(uuid4())
        ids = []
        for stamp in (old, recent, now):
            ids.append(conn.execute("INSERT INTO replica.transitions(action,transitioned_at) VALUES('promote',%s) RETURNING id", (stamp,)).fetchone()[0])
        for stamp in (old, old):
            conn.execute("""INSERT INTO replica.source_catalog_observations
                (source_key,source_engine,object_name,native_manifest,logical_manifest,native_hash,logical_hash,changes,native_changes,classification,observed_at)
                VALUES(%s,'postgresql','fixture','{}','{}',%s,%s,'[]','[]','initial',%s)""",
                (token, 'a'*64, 'b'*64, stamp))
            conn.execute("""INSERT INTO replica.discovered_schema_history(table_key,manifest,schema_hash,classification,observed_at)
                VALUES(%s,'{}','fixture','changed',%s)""", (token, stamp))
        run_ids = []
        for status in ('loading', 'extracting', 'failed', 'aborted', 'promoted', 'rejected'):
            rid = uuid4()
            run_ids.append(rid)
            conn.execute("""INSERT INTO replica.sync_runs(run_id,mode,status,started_at,finished_at)
                VALUES(%s,'incremental',%s,%s,%s)""", (rid, status, old, None if status in ('loading','extracting') else old))
        generation = conn.execute("""INSERT INTO replica.generations(run_id,cutoff,row_count,next_sweep_id,created_at)
            VALUES(%s,%s,1,2,%s) RETURNING version""", (run_ids[4], old, old)).fetchone()[0]
        conn.execute('UPDATE replica.active_state SET version=%s WHERE singleton', (generation,))
        conn.execute('UPDATE replica.slot_state SET version=%s', (generation,))
        conn.execute("INSERT INTO replica.change_journal VALUES(%s,1,NULL,'{}')", (generation,))
        conn.execute("""INSERT INTO replica.range_checkpoints(run_id,sweep_low,sweep_high,source_engine,version,checked_at)
            VALUES(%s,1,10,'postgresql',%s,%s)""", (run_ids[4], generation, old))
        conn.execute("INSERT INTO replica.ingest_keys VALUES(%s,1)", (run_ids[0],))
        conn.execute("INSERT INTO replica.schema_versions(schema_hash,manifest,status) VALUES(%s,'{}','prepared')", (token,))
        conn.execute("INSERT INTO replica.schema_versions(schema_hash,manifest,status) VALUES(%s,'{}','active')", (token+'active',))
        conn.execute('UPDATE replica.schema_state SET active_hash=%s,pending_hash=%s WHERE singleton', (token+'active',token))
        conn.execute("INSERT INTO replica.schema_changes(to_hash,status,diff,created_at) VALUES(%s,'prepared','[]',%s)", (token,old))
        cycles = []
        for status in ('loading','failed','completed'):
            cid = uuid4()
            cycles.append(cid)
            conn.execute("INSERT INTO replica.discovered_cycles(cycle_id,engine,status,started_at) VALUES(%s,'postgresql',%s,%s)", (cid,status,old))
        tracked = ('sync_runs','generations','slot_state','active_state','range_checkpoints','change_journal',
                   'schema_versions','schema_state','schema_changes','ingest_keys','discovered_cycles',
                   'discovered_runs','discovered_generations','discovered_journal','discovered_blue','discovered_green')
        before = {t: conn.execute('SELECT count(*) FROM replica.'+t).fetchone()[0] for t in tracked}
        dry = r.cleanup(conn, r.Settings(), dry_run=True, now=now)
        assert dry['status']=='dry_run', dry
        assert dry['eligible']['transitions'] >= 1 and not dry['removed']
        assert conn.execute('SELECT count(*) FROM replica.transitions WHERE id=ANY(%s)', (ids,)).fetchone()[0] == 3
        long = r.cleanup(conn, r.Settings(audit_retention_days=30), now=now)
        assert long['status']=='ok', long
        assert conn.execute('SELECT count(*) FROM replica.transitions WHERE id=ANY(%s)', (ids,)).fetchone()[0] == 3
        actual = r.cleanup(conn, r.Settings(), now=now)
        assert actual['status']=='ok', actual
        assert conn.execute('SELECT id FROM replica.transitions WHERE id=ANY(%s) ORDER BY id', (ids,)).fetchall()==[(ids[1],),(ids[2],)]
        assert conn.execute('SELECT count(*) FROM replica.source_catalog_observations WHERE source_key=%s', (token,)).fetchone()[0]==1
        assert conn.execute('SELECT count(*) FROM replica.discovered_schema_history WHERE table_key=%s', (token,)).fetchone()[0]==1
        again = r.cleanup(conn, r.Settings(), now=now)
        assert again['status']=='ok' and not any(again['removed'].values()), again
        assert before == {t: conn.execute('SELECT count(*) FROM replica.'+t).fetchone()[0] for t in tracked}
        assert r.pending(conn)
        assert r.cleanup(conn, r.Settings(log_cleanup_enabled=False), automatic=True)['status']=='disabled'
        auto = r.cleanup(conn, r.Settings(), automatic=True)
        # A running consumer may already have an automatic result; either path is safe.
        assert auto['status'] in ('ok','not_due'), auto
        assert r.cleanup(conn, r.Settings(), automatic=True)['status']=='not_due'
        assert r.latest(conn, 'manual')['duration_seconds'] >= 0
        print('PASS: defaults/override, dry-run, recent/latest evidence, expired history, repeated cleanup, pending runs, migration, checkpoints, generations, journals, cycles, schedule and disable')
    finally:
        conn.execute('ROLLBACK')
