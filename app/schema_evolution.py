"""V5 schema gate + nullable TEXT migration and Kafka-mediated backfill.

DDL is issued ONLY against the LOCAL blue/green tables. Remote credentials are
read-only. Kafka events are data/control evidence, NEVER raw executable SQL.
"""
from __future__ import annotations

import hashlib
import json
import re
import time
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

IDENT = re.compile(r'^[a-z_][a-z_0-9]{0,62}$')
BASE = ('id', 'contract_code', 'amount', 'status', 'description')
BASE_TYPES = ('bigint','text','numeric(14,2)','text','text')
MIGRATION_TYPE = 'text'  # Safe, narrowly scoped; other types require manual review.


def stable(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def signature(manifest):
    return hashlib.sha256(stable(manifest)).hexdigest()


def observe(conn):
    """Capture state, not historical DDL commands. pg_catalog, no extension."""
    cols = conn.execute('''
      SELECT a.attname, format_type(a.atttypid, a.atttypmod),
             NOT a.attnotnull, pg_get_expr(d.adbin,d.adrelid),
             a.attidentity, a.attgenerated
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
      JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid
      LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
      WHERE n.nspname='public' AND c.relname='contracts' AND c.relkind IN ('r','p')
        AND a.attnum>0 AND NOT a.attisdropped
      ORDER BY a.attnum
    ''').fetchall()
    if not cols:
        raise RuntimeError('Remote public.contracts missing or inaccessible (DO NOT infer deletes)')
    pk = conn.execute('''SELECT a.attname FROM pg_constraint k
      JOIN pg_class c ON c.oid=k.conrelid
      JOIN pg_namespace n ON n.oid=c.relnamespace
      JOIN unnest(k.conkey) WITH ORDINALITY AS p(attnum,ord) ON TRUE
      JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=p.attnum
      WHERE n.nspname='public' AND c.relname='contracts' AND k.contype='p'
      ORDER BY p.ord''').fetchall()
    constraints=conn.execute('''SELECT k.conname,k.contype,pg_get_constraintdef(k.oid,true)
        FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relname='contracts'
        ORDER BY k.conname,k.contype''').fetchall()
    indexes=conn.execute('''SELECT idx.relname, pg_get_indexdef(idx.oid)
        FROM pg_index i JOIN pg_class t ON t.oid=i.indrelid
        JOIN pg_namespace n ON n.oid=t.relnamespace
        JOIN pg_class idx ON idx.oid=i.indexrelid
        WHERE n.nspname='public' AND t.relname='contracts'
        ORDER BY idx.relname''').fetchall()
    manifest = {'table':'public.contracts', 'primary_key':[p[0] for p in pk],
      'constraints':[{'name':name,'kind':kind,'definition':definition}
                     for name,kind,definition in constraints],
      'indexes':[{'name':name,'definition':definition} for name,definition in indexes],
      'columns':[{'name':n,'type':typ,'nullable':nullable,
                  'default':default,'identity':ident or '',
                  'generated':generated or ''}
                for n,typ,nullable,default,ident,generated in cols]}
    for c in manifest['columns']:
        if not IDENT.fullmatch(c['name']):
            raise RuntimeError('Unsafe/noncanonical remote SQL identifier')
    return manifest


def diff(old, new):
    a, b = old['columns'], new['columns']
    changes = []
    for i, old_col in enumerate(a):
        if i >= len(b) or old_col['name'] != b[i]['name']:
            changes.append({'kind':'remove_or_reorder_or_rename', 'column':old_col['name']})
        elif old_col != b[i]:
            changes.append({'kind':'modify_column', 'before':old_col, 'after':b[i]})
    for col in b[len(a):]:
        changes.append({'kind':'add_column', 'column':col})
    if old['primary_key'] != new['primary_key']:
        changes.append({'kind':'change_primary_key', 'before':old['primary_key'],
                        'after':new['primary_key']})
    for section in ('constraints','indexes'):
        if old.get(section,[])!=new.get(section,[]):
            changes.append({'kind':'change_'+section,
                            'before':old.get(section,[]),'after':new.get(section,[])})
    if old['table'] != new['table']:
        changes.append({'kind':'change_table'})
    return changes


def supported(old, new):
    """Permit ONLY ONE nullable TEXT added at end, with no default/generation."""
    changes=diff(old,new)
    if len(changes)!=1 or changes[0]['kind']!='add_column':
        return False
    c=changes[0]['column']
    return bool(c['type']==MIGRATION_TYPE and c['nullable'] and
                c['default'] is None and c['identity']=='' and c['generated']=='')


def current(local):
    row=local.execute('''SELECT active_hash,pending_hash,backfill_next,backfill_max
      FROM replica.schema_state WHERE singleton''').fetchone()
    if not row: raise RuntimeError('Schema state missing')
    return dict(zip(('active','pending','next','maximum'),row))


def manifest_for(local, sh):
    row=local.execute('SELECT manifest FROM replica.schema_versions WHERE schema_hash=%s',
                      (sh,)).fetchone()
    if row is None: raise RuntimeError('Unknown local schema '+str(sh))
    return row[0]


def fields(local):
    state=current(local)
    if state['active'] is None: return list(BASE)
    return [c['name'] for c in manifest_for(local,state['active'])['columns']]


def ensure_initial(remote, local):
    """First run: seed expected baseline, but ONLY if it matches source schema."""
    remote_manifest=observe(remote)
    st=current(local)
    if st['active'] is not None:
        return remote_manifest
    if ([c['name'] for c in remote_manifest['columns']]!=list(BASE) or
        tuple(c['type'] for c in remote_manifest['columns'])!=BASE_TYPES or
        remote_manifest['primary_key']!=['id']):
        raise RuntimeError('First bootstrap schema incompatible with baseline; manual review')
    h=signature(remote_manifest)
    local.execute('''INSERT INTO replica.schema_versions(schema_hash,manifest,status,activated_at)
      VALUES (%s,%s,'active',clock_timestamp()) ON CONFLICT DO NOTHING''',
      (h,Jsonb(remote_manifest)))
    local.execute('UPDATE replica.schema_state SET active_hash=%s WHERE singleton', (h,))
    return remote_manifest


def gate(remote, local):
    """Fail-closed for unapproved schema. During backfill active projection remains legacy."""
    observed=ensure_initial(remote,local)
    st=current(local)
    got=signature(observed)
    allowed=st['pending'] or st['active']
    if got!=allowed:
        changes=diff(manifest_for(local,st['active']),observed)
        raise RuntimeError('DDL drift: pending approval via schema plan; changes='+json.dumps(changes))
    return st['active']  # Data protocol stays previous until schema activation.


def plan(remote, local, emit_audit=None):
    with local.transaction():
        observed=ensure_initial(remote,local)
        st=current(local)
        h=signature(observed)
        old=manifest_for(local,st['active'])
        modifications=diff(old,observed)
        decision='no_change' if h==st['active'] else ('prepared' if h==st['pending']
             else 'approvable' if supported(old,observed) else 'blocked')
        if h!=st['active']:
            local.execute('''INSERT INTO replica.schema_versions(schema_hash,manifest,status)
                 VALUES (%s,%s,'observed') ON CONFLICT DO NOTHING''',
                 (h,Jsonb(observed)))
            local.execute('''INSERT INTO replica.schema_changes(from_hash,to_hash,status,diff)
              VALUES (%s,%s,%s,%s)''',
              (st['active'],h,decision,Jsonb(modifications)))
    if emit_audit and modifications:
        emit_audit('SCHEMA_CHANGE_DETECTED',old_hash=st['active'],new_hash=h,
                   decision=decision,changes=modifications)
    return {'active_hash':st['active'],'remote_hash':h,'pending_hash':st['pending'],
            'decision':decision,'changes':modifications}


def prepare(remote, local, emit_audit=None):
    """Operator-approved local DDL; ONE narrow shape only; schema stable at read."""
    local.execute('SELECT pg_advisory_xact_lock(70451021)')
    if local.execute("SELECT count(*) FROM replica.sync_runs WHERE status IN ('loading','extracting')").fetchone()[0]:
        raise RuntimeError('Cannot migrate while batch loading')
    observed=observe(remote)
    sh=signature(observed)
    st=current(local)
    if st['pending'] is not None:
        raise RuntimeError('Finish pending schema evolution before new migration')
    old=manifest_for(local,st['active'])
    if not supported(old,observed):
        raise RuntimeError('Unsupported DDL: only append ONE nullable TEXT column, no default')
    new_col=observed['columns'][-1]['name']
    remote_max=remote.execute('SELECT COALESCE(MAX(id),0) FROM public.contracts').fetchone()[0]
    local_max=local.execute('''SELECT COALESCE(MAX(id),0) FROM public.contracts''').fetchone()[0]
    maximum=max(remote_max, local_max)
    local.execute('SET LOCAL lock_timeout = \'3s\'')
    for tbl in ('blue_contracts','green_contracts'):
        local.execute(sql.SQL('ALTER TABLE replica.{} ADD COLUMN {} TEXT').format(
            sql.Identifier(tbl),sql.Identifier(new_col)))
    local.execute('''INSERT INTO replica.schema_versions(schema_hash,manifest,status)
      VALUES(%s,%s,'prepared') ON CONFLICT(schema_hash) DO UPDATE SET status='prepared' ''',
      (sh,Jsonb(observed)))
    local.execute('''UPDATE replica.schema_state SET pending_hash=%s,
      backfill_next=1,backfill_max=%s WHERE singleton''',(sh,maximum))
    local.execute('''INSERT INTO replica.schema_changes(from_hash,to_hash,status,diff,acted_at)
       VALUES(%s,%s,'prepared',%s,clock_timestamp())''',
       (st['active'],sh,Jsonb(diff(old,observed))))
    if emit_audit:
        emit_audit('SCHEMA_PREPARED',new_hash=sh,column=new_col,backfill_max=maximum)
    return {'new_hash':sh,'added_column':new_col,'backfill_next':1,
            'backfill_max':maximum,'public_view_unchanged':True}


def check_backfill_ready(local):
    st=current(local)
    if not st['pending']: raise RuntimeError('No prepared DDL / backfill pending')
    if local.execute("SELECT count(*) FROM replica.schema_backfill_runs WHERE status='loading'").fetchone()[0]:
        raise RuntimeError('Previous backfill still loading; wait for Kafka consumer')
    return st


def backfill_emit(remote, local, width, emit, flush):
    """Publish one complete range's new-field values over the EXISTING Kafka data topic."""
    if width < 1: raise ValueError('sweep-width must be >=1')
    st=check_backfill_ready(local)
    first=st['next']
    if first>st['maximum']:
        return {'complete':True,'next_id':first,'maximum':st['maximum']}
    last=min(first+width-1,st['maximum'])
    pending=manifest_for(local,st['pending'])
    name=pending['columns'][-1]['name']
    if signature(observe(remote))!=st['pending']:
        raise RuntimeError('Source schema changed during backfill; do not publish')
    run_id=str(uuid4())
    remote.execute('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
    remote.execute("SET LOCAL statement_timeout = '45s'")
    remote.execute("SET LOCAL idle_in_transaction_session_timeout = '120s'")
    try:
        if signature(observe(remote))!=st['pending']:
            raise RuntimeError('Remote schema drift during backfill')
        rows=remote.execute(sql.SQL('SELECT id, {} FROM public.contracts WHERE id BETWEEN %s AND %s ORDER BY id').format(
            sql.Identifier(name)),(first,last)).fetchall()
        h=hashlib.sha256()
        emit({'run_id':run_id,'kind':'schema_backfill_start','schema_hash':st['pending'],
              'first_id':first,'last_id':last,'column':name})
        for id_,value in rows:
            item={'id':id_, 'value':value}
            h.update(stable(item)+b'\n')
            emit({'run_id':run_id,'kind':'schema_backfill_row','schema_hash':st['pending'],
                  'id':id_,'column':name,'value':value})
        flush()
        remote.execute('COMMIT')
        emit({'run_id':run_id,'kind':'schema_backfill_end','schema_hash':st['pending'],
              'count':len(rows),'sha256':h.hexdigest()})
        flush()
        return {'run_id':run_id,'range':[first,last],'rows':len(rows),
                'schema_hash':st['pending']}
    except Exception:
        remote.execute('ROLLBACK')
        # No END -> no local DDL activation. Operator can discard a loading run.
        raise


def handle_backfill(local,event):
    """Kafka consumer transaction. Nothing changed until validated END."""
    kind=event['kind']
    sh=event['schema_hash']
    run=event['run_id']
    if kind=='schema_backfill_start':
        st=current(local)
        if st['pending']!=sh: raise RuntimeError('Backfill schema is not prepared')
        local.execute('''INSERT INTO replica.schema_backfill_runs
          (run_id,schema_hash,first_id,last_id,status)
          VALUES(%s,%s,%s,%s,'loading') ON CONFLICT DO NOTHING''',
          (run,sh,event['first_id'],event['last_id']))
        return
    row=local.execute('''SELECT status,first_id,last_id FROM replica.schema_backfill_runs
             WHERE run_id=%s FOR UPDATE''',(run,)).fetchone()
    if row is None: raise RuntimeError('Backfill END/ROW without START')
    status,first,last=row
    if status in ('applied','rejected','aborted'): return
    if kind=='schema_backfill_abort':
        local.execute('''UPDATE replica.schema_backfill_runs SET status='aborted',
          error=%s WHERE run_id=%s''',(event.get('reason','Operator abort'),run))
        local.execute('DELETE FROM replica.schema_backfill_values WHERE run_id=%s',(run,))
        return
    st=current(local)
    if st['pending']!=sh or st['next']!=first or st['maximum']<last:
        raise RuntimeError('Schema/range changed while backfill in-flight')
    if kind=='schema_backfill_row':
        if not first<=event['id']<=last: raise RuntimeError('Row outside range')
        local.execute('''INSERT INTO replica.schema_backfill_values(run_id,id,payload)
            VALUES(%s,%s,%s) ON CONFLICT(run_id,id) DO UPDATE
            SET payload=excluded.payload''',
            (run,event['id'],Jsonb({'id':event['id'],'value':event['value']})))
        return
    if kind!='schema_backfill_end': raise RuntimeError('Unknown backfill event')
    values=local.execute('''SELECT id,payload FROM replica.schema_backfill_values
        WHERE run_id=%s ORDER BY id''',(run,)).fetchall()
    h=hashlib.sha256()
    for id_,item in values: h.update(stable(item)+b'\n')
    # Backfill cannot silently hide missing IDs or infer deletes from an incomplete scan.
    if len(values)!=event['count'] or h.hexdigest()!=event['sha256']:
        local.execute('''UPDATE replica.schema_backfill_runs
          SET status='rejected',error='Kafka backfill count/digest mismatch'
          WHERE run_id=%s''',(run,))
        local.execute('DELETE FROM replica.schema_backfill_values WHERE run_id=%s',(run,))
        return
    live_ids=[r[0] for r in local.execute('''SELECT id FROM public.contracts
       WHERE id BETWEEN %s AND %s ORDER BY id''',(first,last)).fetchall()]
    if live_ids!=[r[0] for r in values]:
        local.execute("UPDATE replica.schema_backfill_runs SET status='rejected',error='Key set mismatch: reconcile data range before retry' WHERE run_id=%s",(run,))
        local.execute('DELETE FROM replica.schema_backfill_values WHERE run_id=%s',(run,))
        return
    col=manifest_for(local,sh)['columns'][-1]['name']
    for tbl in ('blue_contracts','green_contracts'):
        # Set-based patch: no row-by-row UPDATE at 700GB scale.
        cursor=local.execute(sql.SQL('''UPDATE replica.{} AS t SET {}=v.payload->>'value'
           FROM replica.schema_backfill_values AS v
           WHERE v.run_id=%s AND v.id=t.id''').format(
               sql.Identifier(tbl),sql.Identifier(col)),(run,))
        if cursor.rowcount!=len(values):
            raise RuntimeError('Physical slot not aligned before schema backfill')
    local.execute('''INSERT INTO replica.schema_backfill_intervals
      (schema_hash,first_id,last_id,run_id,rows_checked)
      VALUES(%s,%s,%s,%s,%s)''',(sh,first,last,run,len(values)))
    local.execute('''UPDATE replica.schema_backfill_runs SET status='applied',n_rows=%s,
      digest=%s WHERE run_id=%s''',(len(values),event['sha256'],run))
    local.execute('UPDATE replica.schema_state SET backfill_next=%s WHERE singleton',(last+1,))
    local.execute('DELETE FROM replica.schema_backfill_values WHERE run_id=%s',(run,))


def activate(remote,local,emit_audit=None):
    """Transactional append-only VIEW activation; old view stays until completed."""
    local.execute('SELECT pg_advisory_xact_lock(70451021)')
    st=current(local)
    if not st['pending'] or st['next']<=st['maximum']:
        raise RuntimeError('Backfill incomplete or no prepared schema')
    if local.execute("SELECT count(*) FROM replica.sync_runs WHERE status IN ('loading','extracting')").fetchone()[0]:
        raise RuntimeError('Data batch in-flight')
    if local.execute("SELECT count(*) FROM replica.schema_backfill_runs WHERE status='loading'").fetchone()[0]:
        raise RuntimeError('Backfill batch still in-flight')
    if signature(observe(remote))!=st['pending']:
        raise RuntimeError('Remote schema changed after backfill; activation denied')
    # If new key space appeared after prepare, continue backfill to cover it.
    max_remote=remote.execute('SELECT COALESCE(MAX(id),0) FROM public.contracts').fetchone()[0]
    if max_remote>st['maximum']:
        local.execute('UPDATE replica.schema_state SET backfill_max=%s WHERE singleton',(max_remote,))
        return {'extension_required':True,'backfill_max':max_remote,
                'message':'New source IDs detected; continue backfill before activation'}
    new_manifest=manifest_for(local,st['pending'])
    names=[c['name'] for c in new_manifest['columns']]
    columns=sql.SQL(',').join(sql.Identifier(n) for n in names)
    stmt=sql.SQL('''CREATE OR REPLACE VIEW public.contracts AS
      SELECT {} FROM replica.blue_contracts b
      WHERE EXISTS (SELECT 1 FROM replica.active_state s WHERE s.singleton AND s.slot='blue')
      UNION ALL
      SELECT {} FROM replica.green_contracts g
      WHERE EXISTS (SELECT 1 FROM replica.active_state s WHERE s.singleton AND s.slot='green')''').format(columns,columns)
    local.execute('SET LOCAL lock_timeout = \'3s\'')
    local.execute(stmt)
    old=st['active']
    local.execute("UPDATE replica.schema_versions SET status='retired' WHERE schema_hash=%s",(old,))
    local.execute("UPDATE replica.schema_versions SET status='active',activated_at=clock_timestamp() WHERE schema_hash=%s",(st['pending'],))
    local.execute('''UPDATE replica.schema_state SET active_hash=%s,pending_hash=NULL,
      backfill_next=NULL,backfill_max=NULL WHERE singleton''',(st['pending'],))
    if emit_audit: emit_audit('SCHEMA_ACTIVATED',old_hash=old,new_hash=st['pending'])
    return {'active_schema_hash':st['pending'],'old_schema_hash':old,
            'columns':names,'note':'Rollback to prior schema epoch blocked'}
