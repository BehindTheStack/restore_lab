"""Integration assertions; run only through e2e_monitor_storage.sh's disposable stack."""
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import psycopg
import monitor
import monitor_collect as collector

assert os.getenv('LAB_MONITOR_STORAGE_TEST') == '1', 'Use tests/e2e_monitor_storage.sh'
source = os.environ['LOCAL_DSN']
writer = os.environ['MONITOR_WRITE_DSN']
reader = writer.replace('user=monitor_writer password=monitor-writer', 'user=monitor_reader password=monitor-reader')
admin = source.replace('user=monitor_reader password=monitor-reader', 'user=postgres password=lab-admin')

with psycopg.connect(admin) as conn:
    # Ensure old evidence is copied even after it leaves the UI's latest 40 rows.
    conn.execute('''INSERT INTO replica.discovered_runs
        (run_id,cycle_id,table_key,mode,manifest,base_generation,full_snapshot,status,error,finished_at)
        SELECT gen_random_uuid(),r.cycle_id,r.table_key,'incremental',r.manifest,
        r.base_generation,false,'failed','disposable monitor fixture',clock_timestamp()
        FROM (SELECT * FROM replica.discovered_runs ORDER BY started_at LIMIT 1) r
        CROSS JOIN generate_series(1,45)''')
    old_run, = conn.execute('SELECT run_id::text FROM replica.discovered_runs ORDER BY started_at LIMIT 1').fetchone()

assert collector.collect_once(source, writer, 'catalog')
with psycopg.connect(reader) as conn:
    assert conn.execute("SELECT to_regnamespace('replica')").fetchone()[0] is None
    assert conn.execute("SELECT count(*) FROM monitoring.details WHERE kind='run'").fetchone()[0] >= 46
    assert conn.execute("SELECT has_table_privilege(current_user,'monitoring.details','INSERT')").fetchone() == (False,)
    assert conn.execute("SHOW default_transaction_read_only").fetchone() == ('on',)
with psycopg.connect(source) as conn:
    assert not conn.execute("SELECT has_table_privilege(current_user,'replica.discovered_runs','UPDATE')").fetchone()[0]
    assert not conn.execute("SELECT has_schema_privilege(current_user,'mirror_public','USAGE')").fetchone()[0]

probe = lambda: {'ok': True, 'audit_topic': True, 'group': {'lag': 0, 'members': 1}}
with patch.object(monitor, 'MONITOR_DSN', reader):
    historical = monitor.run_details(old_run)
    assert historical and historical['manifest']['table'] == 'contracts'
    assert monitor.ddl_details({'run_id': [old_run]})['manifest'] == historical['manifest']
    assert monitor.load_snapshot(kafka_probe=probe)['collection']['ok']
    stale = monitor.load_snapshot(now=datetime.now(timezone.utc)+timedelta(seconds=120), kafka_probe=probe)
    assert 'MONITOR_COLLECTION_STALE' in {a['code'] for a in stale['alarms']}

with psycopg.connect(writer) as conn:
    before = conn.execute("SELECT document FROM monitoring.details WHERE kind='run' AND detail_key=%s", (old_run,)).fetchone()[0]
with psycopg.connect(admin) as conn:
    conn.execute("UPDATE replica.discovered_runs SET error='must roll back projection' WHERE run_id=%s", (old_run,))
with patch.object(collector, 'write_state', side_effect=RuntimeError('projection transaction interrupted')):
    try: collector.collect_once(source, writer, 'catalog')
    except RuntimeError: pass
    else: raise AssertionError('Projection write failure was swallowed')
with psycopg.connect(reader) as conn:
    after = conn.execute("SELECT document FROM monitoring.details WHERE kind='run' AND detail_key=%s", (old_run,)).fetchone()[0]
    assert after == before, 'Partial details became visible without their matching snapshot'

assert not collector.collect_once('host=127.0.0.1 port=1 dbname=unavailable', writer, 'catalog')
with patch.object(monitor, 'MONITOR_DSN', reader):
    assert monitor.run_details(old_run) == before, 'Historical detail lost during mirror outage'
    failed = monitor.load_snapshot(kafka_probe=probe)
    assert failed['monitor_db']['ok'] and failed['collection']['ok']
    assert 'LOCAL_DB_DOWN' in {a['code'] for a in failed['alarms']}
assert collector.collect_once(source, writer, 'catalog')
print('PASS: SQL permissions, complete history, atomic projection, freshness and mirror outage recovery')
