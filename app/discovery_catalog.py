"""SELECT-only discovery and lossless textual projections for both sources."""
from contextlib import contextmanager
import hashlib
import json
import os
import re


def document(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def signature(value):
    return hashlib.sha256(document(value).encode()).hexdigest()


def identifier(name, engine='postgresql'):
    if not isinstance(name, str) or not name or '\x00' in name:
        raise ValueError('Invalid source identifier')
    return ('[' + name.replace(']', ']]') + ']' if engine == 'sqlserver'
            else '"' + name.replace('"', '""') + '"')


def destination_name(name):
    """PostgreSQL identifiers have a 63-byte limit; never truncate to a collision."""
    if len(name.encode()) <= 63:
        return name
    prefix = name.encode()[:44].decode('utf-8', errors='ignore')
    return prefix + '_' + hashlib.sha256(name.encode()).hexdigest()[:16]


def table_key(engine, schema, table):
    return signature([engine, schema, table])


def target_type(native, engine, length=None, precision=None, scale=None):
    """Only fixed/validated type syntax may become local SQL, never source SQL text."""
    typ = native.lower()
    aliases = {'int':'integer', 'tinyint':'smallint', 'bit':'boolean',
               'uniqueidentifier':'uuid', 'float':'double precision', 'real':'real',
               'money':'numeric(19,4)' if engine == 'sqlserver' else 'text',
               'smallmoney':'numeric(10,4)', 'image':'bytea', 'rowversion':'bytea',
               'timestamp':'bytea' if engine == 'sqlserver' else 'timestamp',
               'datetime':'timestamp', 'smalldatetime':'timestamp',
               'timestamp without time zone':'timestamp', 'timestamp with time zone':'timestamptz',
               'time without time zone':'time', 'time with time zone':'timetz'}
    if engine == 'sqlserver':
        if typ in ('varchar','nvarchar','char','nchar'):
            size = length // 2 if typ.startswith('n') and length != -1 else length
            return f'character varying({int(size)})' if size and 0 < size <= 10485760 else 'text'
        if typ in ('decimal','numeric'):
            if not (1 <= int(precision) <= 38 and 0 <= int(scale) <= int(precision)):
                raise ValueError('Invalid SQL Server decimal precision')
            return f'numeric({int(precision)},{int(scale)})'
        if typ in ('datetime2','datetimeoffset','time'):
            # PostgreSQL cannot represent SQL Server's seventh fractional digit.
            return ('text' if int(scale) > 6 else
                    {'datetime2':'timestamp','datetimeoffset':'timestamptz','time':'time'}[typ])
        if typ in ('binary','varbinary'):
            return 'bytea'
    typ = aliases.get(typ, typ)
    if typ in ('smallint','integer','bigint','numeric','boolean','text','uuid','date',
               'timestamp','timestamptz','time','timetz','interval','double precision',
               'real','bytea','json','jsonb','xml'):
        return typ
    if re.fullmatch(r'(?:numeric|decimal)\(\d{1,4},\d{1,4}\)', typ):
        return typ.replace('decimal', 'numeric')
    if re.fullmatch(r'(?:character varying|character|bit varying|bit)\([1-9]\d{0,7}\)', typ):
        return typ
    # Native enums, domains, arrays and specialized values remain lossless text.
    return 'text'


def descriptor(engine, schema, table, native_id, columns, primary_key, readable=True):
    result = {'engine':engine, 'schema':schema, 'table':table, 'native_id':str(native_id),
              'columns':columns, 'primary_key':primary_key, 'readable':bool(readable),
              'destination_schema':destination_name('mirror_' + schema),
              'destination_table':destination_name(table)}
    result['key'] = table_key(engine, schema, table)
    return result


def validate_manifest(manifest):
    if manifest['engine'] not in ('postgresql','sqlserver'):
        raise ValueError('Unsupported source engine')
    if manifest['key'] != table_key(manifest['engine'], manifest['schema'], manifest['table']):
        raise ValueError('Table identity mismatch')
    if manifest['destination_schema'] != destination_name('mirror_' + manifest['schema']):
        raise ValueError('Unmanaged destination schema')
    if manifest['destination_table'] != destination_name(manifest['table']):
        raise ValueError('Destination identity mismatch')
    names = [c['name'] for c in manifest['columns']]
    if not names or len(names) != len(set(names)) or any(len(n.encode()) > 63 for n in names):
        raise ValueError('Columns must have unique PostgreSQL-compatible names')
    if not set(manifest['primary_key']) <= set(names):
        raise ValueError('Primary key references an unknown column')
    for name in names:
        identifier(name)
    for col in manifest['columns']:
        expected = target_type(col['native_type'], manifest['engine'], col.get('length'),
                               col.get('precision'), col.get('scale'))
        if col['target_type'] != expected:
            raise ValueError('Unapproved destination type')


class Source:
    def __init__(self, engine, dsn=None):
        if engine not in ('postgresql','sqlserver'):
            raise ValueError('Unsupported engine')
        self.engine, self.dsn = engine, dsn

    @contextmanager
    def connect(self):
        if self.engine == 'postgresql':
            import psycopg
            conn = psycopg.connect(self.dsn, autocommit=True)
        else:
            import pymssql
            conn = pymssql.connect(server=os.environ['MSSQL_HOST'],
                port=int(os.getenv('MSSQL_PORT','1433')), user=os.environ['MSSQL_USER'],
                password=os.environ['MSSQL_PASSWORD'], database=os.environ['MSSQL_DATABASE'],
                charset='UTF-8', autocommit=True, login_timeout=8, timeout=120)
        try:
            yield conn
        finally:
            conn.close()

    def query(self, conn, statement, args=()):
        # Application queries have a single, audited read-only entry point.
        if not statement.lstrip().upper().startswith('SELECT '):
            raise ValueError('Remote data/catalog queries must be SELECT')
        with conn.cursor() as cur:
            if args:
                cur.execute(statement, args)
            else:
                cur.execute(statement)
            return cur.fetchall()

    @contextmanager
    def snapshot(self, conn):
        with conn.cursor() as cur:
            if self.engine == 'postgresql':
                cur.execute('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
                cur.execute("SET LOCAL statement_timeout = '120s'")
                cur.execute("SET LOCAL TIME ZONE 'UTC'")
                cur.execute("SET LOCAL DateStyle = 'ISO, YMD'")
                cur.execute("SET LOCAL IntervalStyle = 'iso_8601'")
                cur.execute("SET LOCAL extra_float_digits = 3")
            else:
                enabled = self.query(conn, 'SELECT snapshot_isolation_state FROM sys.databases WHERE name=DB_NAME()')
                isolation = 'SNAPSHOT' if enabled and enabled[0][0] == 1 else 'SERIALIZABLE'
                cur.execute('SET TRANSACTION ISOLATION LEVEL ' + isolation)
                cur.execute('SET LOCK_TIMEOUT 8000')
                cur.execute('BEGIN TRANSACTION')
        try:
            yield
            with conn.cursor() as cur:
                cur.execute('COMMIT')
        except BaseException:
            with conn.cursor() as cur:
                cur.execute('ROLLBACK')
            raise

    def inventory(self, conn):
        tables = {}
        if self.engine == 'postgresql':
            rows = self.query(conn, '''SELECT c.oid::text,n.nspname,c.relname,
                has_schema_privilege(n.oid,'USAGE') AND has_table_privilege(c.oid,'SELECT'),
                a.attname,format_type(a.atttypid,a.atttypmod),NOT a.attnotnull,
                pg_get_expr(d.adbin,d.adrelid),a.attidentity,a.attgenerated,co.collname,
                COALESCE((SELECT array_agg(pa.attname ORDER BY p.ord)
                  FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY p(num,ord)
                  JOIN pg_attribute pa ON pa.attrelid=c.oid AND pa.attnum=p.num
                  WHERE i.indrelid=c.oid AND i.indisprimary AND p.ord<=i.indnkeyatts),ARRAY[]::name[])
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
                LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
                LEFT JOIN pg_collation co ON co.oid=a.attcollation
                WHERE c.relkind IN ('r','p') AND NOT c.relispartition
                  AND n.nspname NOT IN ('pg_catalog','information_schema')
                  AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp_%'
                ORDER BY n.nspname,c.relname,a.attnum''')
            for oid, schema, table, readable, name, typ, nullable, default, identity, generated, collation, pk in rows:
                key = table_key(self.engine, schema, table)
                tables.setdefault(key, descriptor(self.engine,schema,table,oid,[],list(pk),readable))
                tables[key]['columns'].append({'name':name,'native_type':typ,
                    'target_type':target_type(typ,self.engine),'nullable':nullable,
                    'default':default,'identity':identity,'generated':generated,'collation':collation})
        else:
            rows = self.query(conn, '''SELECT s.name,t.name,t.object_id,CONVERT(varchar(40),t.create_date,126),
                HAS_PERMS_BY_NAME(QUOTENAME(s.name)+'.'+QUOTENAME(t.name),'OBJECT','SELECT'),
                c.name,ty.name,c.max_length,c.precision,c.scale,c.is_nullable,c.is_identity,c.is_computed,
                c.default_object_id,dc.definition,c.collation_name,
                COALESCE((SELECT ic.key_ordinal FROM sys.indexes i JOIN sys.index_columns ic
                  ON ic.object_id=i.object_id AND ic.index_id=i.index_id
                  WHERE i.object_id=t.object_id AND i.is_primary_key=1 AND ic.column_id=c.column_id),0)
                FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id
                JOIN sys.columns c ON c.object_id=t.object_id
                JOIN sys.types ty ON ty.user_type_id=c.user_type_id
                LEFT JOIN sys.default_constraints dc ON dc.object_id=c.default_object_id
                WHERE t.is_ms_shipped=0 ORDER BY s.name,t.name,c.column_id''')
            for schema,table,oid,created,readable,name,typ,length,precision,scale,nullable,identity,computed,default_id,default,collation,pk_ord in rows:
                key = table_key(self.engine,schema,table)
                tables.setdefault(key,descriptor(self.engine,schema,table,f'{oid}:{created}',[],[],readable))
                tables[key]['columns'].append({'name':name,'native_type':typ,
                    'target_type':target_type(typ,self.engine,length,precision,scale),
                    'length':length,'precision':precision,'scale':scale,'nullable':bool(nullable),
                    'identity':bool(identity),'computed':bool(computed),'default_present':bool(default_id),
                    'default':default,'collation':collation,'pk_ordinal':pk_ord})
            for table in tables.values():
                table['primary_key'] = [c['name'] for c in sorted(table['columns'],key=lambda c:c['pk_ordinal']) if c['pk_ordinal']]
        include = {x.strip() for x in os.getenv('SOURCE_SCHEMAS','').split(',') if x.strip()}
        return {key:table for key,table in tables.items() if not include or table['schema'] in include}

    def rows(self, conn, manifest, batch_size=250):
        projections = []
        for col in manifest['columns']:
            name = identifier(col['name'],self.engine)
            typ = col['native_type'].lower()
            if self.engine == 'postgresql':
                projections.append(name + '::text')
            elif typ in ('binary','varbinary','image','timestamp','rowversion'):
                projections.append("'\\x'+CONVERT(varchar(max)," + name + ',2)')
            elif typ in ('float','real'):
                projections.append('CONVERT(varchar(99),' + name + ',3)')
            elif typ in ('money','smallmoney'):
                projections.append('CONVERT(varchar(99),' + name + ',2)')
            else:
                projections.append('CONVERT(nvarchar(max),' + name + ',127)')
        name = '.'.join(identifier(manifest[k],self.engine) for k in ('schema','table'))
        order = ','.join(identifier(k,self.engine) for k in manifest['primary_key'])
        statement = 'SELECT ' + ','.join(projections) + ' FROM ' + name + (' ORDER BY ' + order if order else '')
        cur = conn.cursor(name='discovery_scan') if self.engine == 'postgresql' else conn.cursor()
        try:
            cur.execute(statement)
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                yield [dict(zip((c['name'] for c in manifest['columns']),row)) for row in rows]
        finally:
            cur.close()
