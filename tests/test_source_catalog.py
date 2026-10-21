"""Offline SQL Server metadata-parser tests; no Docker or real SQL Server needed."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
import source_catalog as sc

class Cursor:
    def __init__(self,conn): self.conn=conn; self.key=None
    def execute(self,sql,args):
        assert args==('dbo','contracts'), 'Catalog SQL must bind parameters'
        if 'sys.default_constraints dc' in sql: key='columns'
        elif 'sys.key_constraints kc' in sql: key='primary_key'
        elif 'sys.indexes i' in sql: key='indexes'
        elif 'sys.check_constraints cc' in sql: key='checks'
        elif 'sys.foreign_keys fk' in sql: key='foreign_keys'
        else: raise AssertionError('Unknown query')
        self.key=key
    def fetchall(self): return self.conn.data[self.key]
    def close(self): pass

class Connection:
    def __init__(self,data): self.data=data
    def cursor(self): return Cursor(self)

base={
    'columns':[
        (1,'id','bigint',8,19,0,0,None,0,None,None,0,None,0),
        (2,'contract_code','nvarchar',200,0,0,0,None,0,None,'Latin1_General_CI_AS',0,None,0),
        (3,'amount','decimal',9,14,2,0,None,0,None,None,0,None,0),
        (4,'status','nvarchar',60,0,0,0,None,0,None,'Latin1_General_CI_AS',0,None,0),
        (5,'description','nvarchar',-1,0,0,1,None,0,None,'Latin1_General_CI_AS',0,None,0),
    ],
    'primary_key':[('id',)],
    'indexes':[('PK_contracts','CLUSTERED',1,1,0,0,None,1,0,0,'id')],
    'checks':[],
    'foreign_keys':[],
}
from copy import deepcopy
native=sc.observe_mssql(Connection(base))
logical=sc.projected(native)
assert native['engine']=='sqlserver'
assert logical['columns']['description']['type']=='text'
assert native['columns'][1]['native_type']=='nvarchar(100)'
assert logical['columns']['amount']['type']=='numeric(14,2)'
assert logical['primary_key']==['id']
assert sc.sha(native)==sc.sha(sc.observe_mssql(Connection(base)))
assert len(sc.diff_dicts(logical,logical))==0
next_data=deepcopy(base)
next_data['columns'].append((6,'currency_code','nvarchar',6,0,0,1,None,0,None,'Latin1_General_CI_AS',0,None,0))
new=sc.projected(sc.observe_mssql(Connection(next_data)))
changes=sc.diff_dicts(logical,new)
assert [c['kind'] for c in changes]==['add_column'], changes
assert sc.risk(changes)=='review_adaptive_migration'
# Changing NVARCHAR(30)->NVARCHAR(100) has the same canonical type but is a
# NATIVE schema change; never silently suppress it.
length_change=deepcopy(base)
row=list(length_change['columns'][3]); row[3]=200; length_change['columns'][3]=tuple(row)
changed=sc.observe_mssql(Connection(length_change))
assert sc.projected(changed)==logical
native_diff=sc.diff_dicts(sc.native_projection(native),sc.native_projection(changed))
assert any(c['kind']=='alter_column' and c['column']=='status' for c in native_diff)
# DROP cannot be interpreted as mere column reorder or a soft warning.
dropped=deepcopy(base); dropped['columns']=dropped['columns'][:-1]
assert sc.risk(sc.diff_dicts(logical,sc.projected(sc.observe_mssql(Connection(dropped)))))=='blocked'
assert sc.mssql_type('nvarchar',-1,0,0)==('nvarchar(max)','text')
assert sc.mssql_type('datetimeoffset',8,0,7)==('datetimeoffset(7)','timestamptz')
assert sc.mssql_type('geography',-1,0,0)[1]=='unsupported:geography'
try:
    sc.observe_mssql(Connection(base),'dbo;drop table x','contracts')
    assert False,'Unsafe SQL identifier accepted'
except ValueError: pass
try:
    sc.observe_mssql(Connection({**base,'columns':[]}), 'dbo','contracts')
    assert False,'Invisible table should not be treated as zero-column table'
except RuntimeError: pass
print('PASS V5: SQL Server sys.* metadata, native vs portable dictionaries, DDL drift')
