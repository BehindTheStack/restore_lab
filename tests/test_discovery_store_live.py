"""Transactional PostgreSQL checks; all fixture objects are rolled back.

Run inside the app image with LAB_DISCOVERY_STORE_TEST=1. Never modifies a source.
"""
import hashlib
import os
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from discovery_catalog import descriptor, document, signature
import discovery_store as store


def run():
    if os.getenv('LAB_DISCOVERY_STORE_TEST') != '1':
        raise SystemExit('Set LAB_DISCOVERY_STORE_TEST=1; requires the initialized local test database')
    with psycopg.connect(os.environ['LOCAL_DSN']) as conn:
        try:
            token=str(uuid4()).replace('-','')[:12]
            manifest=descriptor('postgresql','discovery_test_'+token,'example','1',
                [{'name':'id','native_type':'bigint','target_type':'bigint','nullable':False}],['id'])
            key=manifest['key']
            store.register(conn,manifest)
            cycle_id,run_id=str(uuid4()),str(uuid4())
            conn.execute("INSERT INTO replica.discovered_cycles(cycle_id,engine) VALUES(%s,'postgresql')",(cycle_id,))
            conn.execute('''INSERT INTO replica.discovered_runs(run_id,cycle_id,table_key,mode,manifest,full_snapshot)
                            VALUES(%s,%s,%s,'bootstrap',%s,true)''',(run_id,cycle_id,key,Jsonb(manifest)))
            payload={'id':'1'}
            seen={'row_key':store.row_key(manifest,payload,1),'row_hash':signature(payload)}
            delta={**seen,'payload':payload}
            store.handle(conn,{'kind':'table_seen','run_id':run_id,'seq':1,**seen})
            store.handle(conn,{'kind':'table_delta','run_id':run_id,'seq':1,'position':1,**delta})
            end={'kind':'table_end','run_id':run_id,'seen_count':1,'delta_count':1,'logical_bytes':100,
                 'seen_sha256':hashlib.sha256((document(seen)+'\n').encode()).hexdigest(),
                 'delta_sha256':hashlib.sha256((document(delta)+'\n').encode()).hexdigest()}
            store.handle(conn,end)
            generation=store.state(conn,key)['active_generation']
            view=manifest['destination_schema']+'.example'
            assert conn.execute(store.sql.SQL('SELECT id FROM {}').format(
                store.sql.Identifier(manifest['destination_schema'],'example'))).fetchone()==(1,)
            store.handle(conn,end)
            store.handle(conn,{'kind':'table_abort','run_id':run_id,'reason':'late duplicate'})
            assert store.state(conn,key)['active_generation']==generation

            failed=str(uuid4())
            conn.execute('''INSERT INTO replica.discovered_runs(run_id,cycle_id,table_key,mode,manifest,full_snapshot,status)
                            VALUES(%s,%s,%s,'incremental',%s,false,'failed')''',(failed,cycle_id,key,Jsonb(manifest)))
            conn.execute('INSERT INTO replica.discovered_seen VALUES(%s,%s,1,%s)',(failed,seen['row_key'],seen['row_hash']))
            store.handle(conn,{'kind':'table_abort','run_id':failed,'reason':'failed producer'})
            assert conn.execute('SELECT count(*) FROM replica.discovered_seen WHERE run_id=%s',(failed,)).fetchone()[0]==0

            other_keys=[r[0] for r in conn.execute('SELECT table_key FROM replica.discovered_tables WHERE table_key!=%s',(key,))]
            def inventory(known):
                cid=str(uuid4())
                conn.execute("INSERT INTO replica.discovered_cycles(cycle_id,engine) VALUES(%s,'postgresql')",(cid,))
                event={'kind':'table_inventory','cycle_id':cid,'visible_keys':other_keys,'known_keys':known}
                store.handle(conn,event)
                store.handle(conn,event)  # delivery replay cannot count as a second observation
            inventory(other_keys+[key])
            assert store.state(conn,key)['status']=='inaccessible'
            assert store.state(conn,key)['missing_cycles']==0
            assert conn.execute('SELECT to_regclass(%s)',(view,)).fetchone()[0]
            inventory(other_keys)
            assert store.state(conn,key)['missing_cycles']==1
            assert conn.execute('SELECT to_regclass(%s)',(view,)).fetchone()[0]
            inventory(other_keys)
            assert store.state(conn,key)['status']=='missing_or_inaccessible'
            assert conn.execute('SELECT to_regclass(%s)',(view,)).fetchone()[0] is None
            assert conn.execute('SELECT count(*) FROM replica.discovered_blue WHERE table_key=%s',(key,)).fetchone()[0]==1
        finally:
            conn.rollback()
    print('PASS: promotion replay, abort cleanup, SELECT denial, duplicate inventory and retained DROP archives (rolled back)')


if __name__=='__main__':run()
