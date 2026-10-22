"""Monitor projection expiry and no resurrection, disposable fixtures only."""
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb
import monitor_collect as c
import retention as r

assert os.getenv('LAB_RETENTION_TEST')=='1', 'Use tests/e2e_retention.sh'
source = os.environ['LOCAL_DSN']
writer = os.environ['MONITOR_WRITE_DSN']
admin = source.replace('user=monitor_reader password=monitor-reader', 'user=postgres password=lab-admin')
now = datetime.now(timezone.utc)
old = now-timedelta(days=10)
expired, pending = str(uuid4()),str(uuid4())
try:
    with psycopg.connect(admin) as conn:
        for rid,status in ((expired,'failed'),(pending,'loading')):
            conn.execute("""INSERT INTO replica.discovered_runs
                (run_id,cycle_id,table_key,mode,manifest,full_snapshot,status,started_at,finished_at)
                SELECT %s,cycle_id,table_key,'incremental',manifest,false,%s,%s,%s
                FROM replica.discovered_runs ORDER BY started_at LIMIT 1""", (rid,status,old,old))
    with psycopg.connect(writer) as conn:
        for rid,status in ((expired,'failed'),(pending,'loading')):
            conn.execute("INSERT INTO monitoring.details VALUES('catalog','run',%s,%s)",
                         (rid,Jsonb({'status':status,'finished_at':old.isoformat()})))
        dry = c.cleanup_projection(conn,'catalog',now,r.Settings(),True)
        assert dry['status']=='dry_run' and dry['eligible']>=1 and dry['removed']==0, dry
        actual = c.cleanup_projection(conn,'catalog',now+timedelta(hours=25),r.Settings())
        assert actual['status']=='ok' and actual['removed']>=1, actual
        assert conn.execute("SELECT document FROM monitoring.details WHERE detail_key=%s",(pending,)).fetchone()
        assert not conn.execute("SELECT document FROM monitoring.details WHERE detail_key=%s",(expired,)).fetchone()
    assert c.collect_once(source,writer,'catalog')
    assert c.collect_once(source,writer,'catalog')
    with psycopg.connect(writer) as conn:
        assert conn.execute("SELECT document FROM monitoring.details WHERE detail_key=%s",(pending,)).fetchone()
        assert not conn.execute("SELECT document FROM monitoring.details WHERE detail_key=%s",(expired,)).fetchone(), 'Expired run was recopied'
    with psycopg.connect(admin) as conn:
        assert conn.execute('SELECT count(*) FROM replica.discovered_runs WHERE run_id=ANY(%s::uuid[])',([expired,pending],)).fetchone()[0]==2
    print('PASS: projection dry-run, expired detail removal, pending preservation and no resurrection across repeated collections')
finally:
    with psycopg.connect(admin) as conn:
        conn.execute('DELETE FROM replica.discovered_runs WHERE run_id=ANY(%s::uuid[])',([expired,pending],))
    with psycopg.connect(writer) as conn:
        conn.execute('DELETE FROM monitoring.details WHERE detail_key=ANY(%s)',([expired,pending],))
