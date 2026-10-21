"""Portable read-only database catalog observation and versioned dictionary diffs.

This module observes SCHEMA STATE (not SQL DDL log/events). It NEVER executes
DDL at either source. Engine-native manifest and vendor-neutral projection are
both persisted as JSONB locally. SQL Server catalog mode is observability only;
data reconciliation still uses the PostgreSQL-specific V5 range/hash path.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping

IDENT = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,127}$')


def stable_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), default=str)


def sha(value):
    return hashlib.sha256(stable_json(value).encode('utf-8')).hexdigest()


def valid_identifier(value):
    if not value or not IDENT.fullmatch(value):
        raise ValueError(f'Unsafe identifier: {value!r}')
    return value


def mssql_type(name, length, precision, scale):
    """Preserve native distinctions; return a conservative portable type.

    Unicode lengths in sys.columns are expressed in bytes; -1 means MAX.
    Unknown/specialized types deliberately stay prefixed 'unsupported:'.
    """
    name = name.lower()
    if name in ('varchar','char','nvarchar','nchar'):
        chars = 'max' if length == -1 else length // 2 if name.startswith('n') else length
        native = f'{name}({chars})'
        return native, 'text'
    if name in ('text','ntext','sysname'):
        return name, 'text'
    if name in ('numeric','decimal'):
        native = f'{name}({precision},{scale})'
        return native, f'numeric({precision},{scale})'
    if name in ('datetime2','datetimeoffset','time'):
        native = f'{name}({scale})'
        return native, {'datetime2': 'timestamp', 'datetimeoffset': 'timestamptz',
                        'time': 'time'}[name]
    types = {'bigint':'bigint','int':'integer','smallint':'smallint','tinyint':'smallint',
             'bit':'boolean','datetime':'timestamp','smalldatetime':'timestamp',
             'date':'date','uniqueidentifier':'uuid','float':'double precision',
             'real':'real','money':'numeric(19,4)','smallmoney':'numeric(10,4)',
             'binary':'bytea','varbinary':'bytea','image':'bytea','xml':'xml'}
    if name in types:
        return name, types[name]
    return name, 'unsupported:' + name


def pg_type(native):
    """Conservative mapping: only a few common spelling synonyms converge."""
    value = native.lower()
    return {'character varying': 'text', 'character': 'text', 'text':'text',
            'integer':'integer','bigint':'bigint','boolean':'boolean',
            'timestamp without time zone':'timestamp',
            'timestamp with time zone':'timestamptz'}.get(value, value)


def columns_by_name(manifest):
    cols = manifest.get('columns', [])
    if isinstance(cols, Mapping):
        return dict(cols)
    return {c['name']:c for c in cols}


def normalize_postgres(manifest):
    """Postgres native V5 manifest -> portable, source-independent dict."""
    return {
        'object': 'contracts',
        'columns': {
            c['name']: {
                'type':pg_type(c['type']), 'nullable':bool(c['nullable']),
                'has_default':c['default'] is not None,
                'computed':bool(c.get('generated')), 'identity':bool(c.get('identity'))
            } for c in manifest['columns']
        },
        'column_order':[c['name'] for c in manifest['columns']],
        'primary_key':manifest['primary_key'],
        'indexes':{i['name']:i for i in manifest.get('indexes',[])},
        'constraints':{c['name']:c for c in manifest.get('constraints',[])},
    }


def normalize_mssql(manifest):
    return {
        'object':'contracts',
        'columns': {
            c['name']: {
                'type':c['logical_type'],'nullable':bool(c['nullable']),
                'has_default':c['default_present'],
                'computed':bool(c['computed']), 'identity':bool(c['identity'])
            } for c in manifest['columns']
        },
        'column_order':[c['name'] for c in manifest['columns']],
        'primary_key':manifest['primary_key'],
        'indexes':{i['name']:i for i in manifest['indexes']},
        'constraints':{c['name']:c for c in manifest['constraints']},
    }


def native_projection(manifest):
    return {
        'columns':{c['name']:c for c in manifest['columns']},
        'column_order':[c['name'] for c in manifest['columns']],
        'primary_key':manifest['primary_key'],
        'indexes':{c['name']:c for c in manifest.get('indexes',[])},
        'constraints':{c['name']:c for c in manifest.get('constraints',[])},
    }


def diff_dicts(previous, current):
    """Keyed dictionary diff: never guess rename; treat as remove+add."""
    if previous is None:
        return [{'kind':'initial_observation'}]
    changes = []
    left=columns_by_name(previous)
    right=columns_by_name(current)
    for name in sorted(left.keys() - right.keys()):
        changes.append({'kind':'drop_column','column':name,'before':left[name]})
    for name in sorted(right.keys() - left.keys()):
        changes.append({'kind':'add_column','column':name,'after':right[name]})
    for name in sorted(right.keys() & left.keys()):
        if right[name]!=left[name]:
            changes.append({'kind':'alter_column','column':name,
                            'before':left[name],'after':right[name]})
    # A column appended to the end is not a reordering.
    prior_order=previous.get('column_order',[])
    next_order=current.get('column_order',[])
    if next_order[:len(prior_order)]!=prior_order:
        changes.append({'kind':'change_column_order','before':prior_order,'after':next_order})
    for section in ('primary_key','indexes','constraints'):
        if previous.get(section)!=current.get(section):
            changes.append({'kind':'change_'+section, 'before':previous.get(section),
                            'after':current.get(section)})
    return changes


def risk(changes):
    if not changes: return 'unchanged'
    if any(c['kind'] in ('drop_column','alter_column','change_primary_key',
                         'change_column_order','change_constraints') for c in changes):
        return 'blocked'
    if all(c['kind']=='add_column' and
           c['after']['type']=='text' and c['after']['nullable'] and
           c['after']['has_default'] is False and not c['after']['computed'] and
           not c['after']['identity'] for c in changes):
        return 'review_adaptive_migration'
    return 'review_required'


# SQL Server metadata queries are parameterized by schema/table, not interpolated.
# Catalog columns with invisible definitions are represented as UNKNOWN, not as absent.
SQLSERVER_COLUMNS = '''
SELECT c.column_id,c.name,ty.name,c.max_length,c.precision,c.scale,
       c.is_nullable,dc.definition,c.is_identity,cc.definition,
       c.collation_name,c.is_computed,dc.object_id,c.generated_always_type
FROM sys.schemas s
JOIN sys.tables t ON t.schema_id=s.schema_id
JOIN sys.columns c ON c.object_id=t.object_id
JOIN sys.types ty ON ty.user_type_id=c.user_type_id
LEFT JOIN sys.default_constraints dc ON dc.object_id=c.default_object_id
LEFT JOIN sys.computed_columns cc ON cc.object_id=c.object_id AND cc.column_id=c.column_id
WHERE s.name=%s AND t.name=%s
ORDER BY c.column_id
'''
SQLSERVER_PK = '''
SELECT c.name
FROM sys.schemas s JOIN sys.tables t ON t.schema_id=s.schema_id
JOIN sys.key_constraints kc ON kc.parent_object_id=t.object_id AND kc.type='PK'
JOIN sys.index_columns ic ON ic.object_id=t.object_id AND ic.index_id=kc.unique_index_id
JOIN sys.columns c ON c.object_id=t.object_id AND c.column_id=ic.column_id
WHERE s.name=%s AND t.name=%s AND ic.key_ordinal>0
ORDER BY ic.key_ordinal
'''
SQLSERVER_INDEXES = '''
SELECT i.name, i.type_desc,i.is_unique,i.is_primary_key,i.is_disabled,
       i.has_filter,i.filter_definition,ic.key_ordinal,ic.is_included_column,
       ic.is_descending_key,c.name
FROM sys.schemas s JOIN sys.tables t ON t.schema_id=s.schema_id
JOIN sys.indexes i ON i.object_id=t.object_id
LEFT JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
LEFT JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
WHERE s.name=%s AND t.name=%s AND i.index_id>0 AND i.name IS NOT NULL
ORDER BY i.name,ic.key_ordinal,ic.index_column_id
'''
SQLSERVER_CHECKS = '''
SELECT cc.name,cc.definition,cc.is_disabled,cc.is_not_trusted
FROM sys.schemas s JOIN sys.tables t ON t.schema_id=s.schema_id
JOIN sys.check_constraints cc ON cc.parent_object_id=t.object_id
WHERE s.name=%s AND t.name=%s ORDER BY cc.name
'''
SQLSERVER_FKS = '''
SELECT fk.name, OBJECT_SCHEMA_NAME(fk.referenced_object_id),
       OBJECT_NAME(fk.referenced_object_id),fk.is_disabled,fk.is_not_trusted
FROM sys.schemas s JOIN sys.tables t ON t.schema_id=s.schema_id
JOIN sys.foreign_keys fk ON fk.parent_object_id=t.object_id
WHERE s.name=%s AND t.name=%s ORDER BY fk.name
'''


def fetch(conn, statement, params):
    # pymssql implements DB-API with %s placeholders.
    cursor=conn.cursor()
    try:
        cursor.execute(statement, params)
        return cursor.fetchall()
    finally:
        cursor.close()


def observe_mssql(conn, schema='dbo', table='contracts'):
    valid_identifier(schema); valid_identifier(table)
    args=(schema,table)
    rows=fetch(conn,SQLSERVER_COLUMNS,args)
    if not rows:
        raise RuntimeError(f'SQL Server table {schema}.{table} missing or metadata hidden; never infer DROP')
    columns=[]
    for r in rows:
        ordinal,name,typ,length,precision,scale,nullable,default,identity,computed_definition,collation,is_computed,default_id,generated=r
        native,logical=mssql_type(typ,int(length),int(precision),int(scale))
        columns.append({'ordinal':int(ordinal),'name':name,'native_type':native,
                        'logical_type':logical,'nullable':bool(nullable),
                        'default_present':default_id is not None,
                        'default_expression':default if default_id is not None else None,
                        'identity':bool(identity),'computed':bool(is_computed),
                        'computed_expression':computed_definition,
                        'collation':collation,'generated_always_type':int(generated or 0)})
    primary_key=[x[0] for x in fetch(conn,SQLSERVER_PK,args)]
    indexes_by_name={}
    for name,typ,unique,pk,disabled,has_filter,filter_sql,position,included,desc,column in fetch(conn,SQLSERVER_INDEXES,args):
        if name not in indexes_by_name:
            indexes_by_name[name]={'name':name,'type':typ,'unique':bool(unique),
                'primary_key':bool(pk),'disabled':bool(disabled),
                'filter':filter_sql if has_filter else None, 'keys':[], 'include':[]}
        if column:
            if included: indexes_by_name[name]['include'].append(column)
            else: indexes_by_name[name]['keys'].append({'column':column,
                       'ordinal':int(position),'descending':bool(desc)})
    constraints=[]
    for name,definition,disabled,not_trusted in fetch(conn,SQLSERVER_CHECKS,args):
        constraints.append({'name':name,'kind':'check','definition':definition,
                            'disabled':bool(disabled),'not_trusted':bool(not_trusted)})
    for name,ref_schema,ref_table,disabled,not_trusted in fetch(conn,SQLSERVER_FKS,args):
        constraints.append({'name':name,'kind':'foreign_key','references':
                           [ref_schema,ref_table],'disabled':bool(disabled),
                           'not_trusted':bool(not_trusted)})
    native={'engine':'sqlserver','table':f'{schema}.{table}','primary_key':primary_key,
            'columns':columns, 'indexes':sorted(indexes_by_name.values(),key=lambda x:x['name']),
            'constraints':sorted(constraints,key=lambda x:x['name'])}
    return native


def observe_pg(conn):
    import schema_evolution as evo
    manifest=evo.observe(conn)
    return {'engine':'postgresql',**manifest}


def projected(native):
    engine=native['engine']
    if engine=='sqlserver': return normalize_mssql(native)
    if engine=='postgresql': return normalize_postgres(native)
    raise ValueError(f'Unsupported catalog dialect: {engine}')


def persist(local, source_key, native):
    """Persist on every structural transition; repeat observations are idempotent."""
    if not source_key or len(source_key)>200: raise ValueError('Invalid source key')
    logical=projected(native)
    native_hash,logical_hash=sha(native),sha(logical)
    # Serializable per source, but no mutation occurs remotely.
    local.execute('SELECT pg_advisory_xact_lock(hashtext(%s))',(source_key,))
    old=local.execute('''SELECT observation_id,logical_manifest,native_hash,logical_hash,native_manifest
      FROM replica.source_catalog_observations WHERE source_key=%s
      ORDER BY observation_id DESC LIMIT 1''',(source_key,)).fetchone()
    if old and old[2]==native_hash:
        return {'source':source_key,'observation_id':old[0],'changed':False,
                'native_hash':native_hash,'logical_hash':logical_hash,
                'changes':[],'native_changes':[],'classification':'unchanged'}
    delta=diff_dicts(old[1] if old else None,logical)
    native_delta=diff_dicts(native_projection(old[4]) if old else None,
                            native_projection(native))
    classification=risk(delta) if delta else ('native_only_review' if native_delta else 'unchanged')
    from psycopg.types.json import Jsonb
    oid=local.execute('''INSERT INTO replica.source_catalog_observations
      (source_key,source_engine,object_name,native_manifest,logical_manifest,
       native_hash,logical_hash,changes,native_changes,classification)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING observation_id''',
      (source_key,native['engine'],native['table'],Jsonb(native),Jsonb(logical),
       native_hash,logical_hash,Jsonb(delta),Jsonb(native_delta),classification)).fetchone()[0]
    return {'source':source_key,'observation_id':oid,'changed':True,
            'native_hash':native_hash,'logical_hash':logical_hash,
            'changes':delta,'native_changes':native_delta,'classification':classification}


def history(local,source_key,limit=20):
    return [dict(zip(('id','engine','object','hash','logical_hash','classification',
                       'changes','observed_at'),r))
            for r in local.execute('''SELECT observation_id,source_engine,object_name,
                native_hash,logical_hash,classification,changes,observed_at
                FROM replica.source_catalog_observations
                WHERE source_key=%s ORDER BY observation_id DESC LIMIT %s''',
                (source_key,limit)).fetchall()]
