"""Stored DDL presentation, escaping, local SELECT access and historical contracts."""
from pathlib import Path
import runpy
from datetime import datetime,timezone
from unittest.mock import patch

ctx=runpy.run_path(str(Path(__file__).with_name('test_monitor.py')),run_name='monitor_fixtures')
m=ctx['m']
d=m.details
stamp='2026-10-22T01:15:00+00:00'
manifest={'engine':'postgresql','schema':'sales','table':'invoices',
          'primary_key':['tenant','id'],'columns':[
              {'name':'tenant','native_type':'integer','nullable':False},
              {'name':'id','native_type':'bigint','nullable':False,'identity':'a'},
              {'name':'note','native_type':'text','nullable':True,'default':"'</code><script>alert(1)</script>'"}]}


def test_native_ddl_and_unavailable_properties():
    sql=d.source_ddl(manifest)
    assert 'CREATE TABLE "sales"."invoices"' in sql
    assert '"id" bigint GENERATED ALWAYS AS IDENTITY NOT NULL' in sql
    assert 'PRIMARY KEY ("tenant", "id")' in sql
    body=d.ddl_body({'manifest':manifest,'observed_at':stamp},m._date_time)
    assert '<script>' not in body and '&lt;script&gt;' in body
    assert '21/10/2026 às 22:15:00' in body
    native={'engine':'sqlserver','schema':'dbo','table':'odd]name','primary_key':['id'],
            'columns':[{'name':'id','native_type':'bigint','nullable':False,'identity':True},
                       {'name':'title','native_type':'nvarchar','length':160,'nullable':True},
                       {'name':'amount','native_type':'decimal','precision':16,'scale':4,'nullable':True},
                       {'name':'at','native_type':'datetime2','scale':7,'nullable':True},
                       {'name':'computed','native_type':'int','computed':True,'default_present':True,'nullable':True}]}
    sql=d.source_ddl(native)
    assert '[dbo].[odd]]name]' in sql and 'nvarchar(80)' in sql
    assert 'decimal(16,4)' in sql and 'datetime2(7)' in sql
    assert 'seed e incremento não registrados' in sql
    assert 'expressão não registrada' in sql and 'expressão sem visibilidade' in sql
    assert d.quote_identifier('a"b','postgresql')=='"a""b"'


class Cursor:
    def __init__(self,db):self.db=db
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def execute(self,sql,params=()):
        assert sql.lstrip().startswith('SELECT'),sql
        self.db.calls.append((sql,params))
    def fetchone(self):return self.db.row


class DB:
    def __init__(self,row):self.row=row;self.calls=[]
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def cursor(self):return Cursor(self)


def test_historical_contract_is_read_from_run_not_current_table():
    db=DB((manifest,datetime(2026,10,22,tzinfo=timezone.utc),'promoted'))
    rid='00000000-0000-0000-0000-000000000001'
    with patch.object(m,'SYNC_MODE','catalog'):
        record=m.ddl_details({'run_id':[rid]},connect=lambda *args,**kwargs:db)
    assert record['manifest']==manifest and record['run_id']==rid
    assert 'FROM replica.discovered_runs' in db.calls[0][0]
    assert db.calls[0][1]==(rid,)
    payload="name'; DROP TABLE replica.sync_runs; --"
    m.ddl_details({'source_key':[payload]},connect=lambda *args,**kwargs:db)
    assert payload not in db.calls[-1][0] and db.calls[-1][1]==(payload,)
    for query in ({},{'table_key':['bad']},{'run_id':['bad']},{'run_id':[rid],'table_key':['a'*64]}, {'table_key':['a'*64,'b'*64]}):
        try:m.ddl_details(query,connect=lambda *a,**kw:(_ for _ in ()).throw(AssertionError('Must reject before DB')))
        except ValueError:pass
        else:raise AssertionError(query)


def test_table_and_execution_references_open_dialogs():
    snap=m.load_snapshot(now=datetime(2026,10,22,tzinfo=timezone.utc),
                        connect=lambda *a,**kw:ctx['DB'](),kafka_probe=lambda:{'ok':True,'audit_topic':True,'group':{}})
    snap['mode']='catalog'
    rid='00000000-0000-0000-0000-000000000001'
    snap['recent_runs']=[{'id':rid,'engine':'postgresql','object':'sales.invoices','table_key':'a'*64,
        'mode':'incremental','low':None,'high':None,'status':'promoted','delta':0,'deleted':0,
        'duration_seconds':1,'started_at':stamp,'finished_at':stamp,'error':None}]
    snap['tables']=[{'key':'a'*64,'engine':'postgresql','schema':'sales','table':'invoices','destination_schema':'mirror_sales',
        'destination_table':'invoices','status':'active','rows':1,'generation':1,'slot':'blue',
        'schema_hash':'hash','checked_at':stamp,'error':None}]
    snap['catalog']=[{'source':'sales.invoices','table_key':'a'*64,'engine':'postgresql','classification':'new','observed_at':stamp}]
    body=m.render_html(snap)
    assert 'href="/run/'+rid+'" data-modal aria-haspopup="dialog"' in body
    assert 'href="/ddl?run_id='+rid+'" data-modal' in body
    assert 'href="/ddl?table_key='+'a'*64+'" data-modal' in body
    assert '<dialog id="details-modal"' in body and 'aria-labelledby="modal-title"' in body
    assert '<script src="/monitor.js" defer>' in body
    run={'run_id':rid,'schema':'sales','table':'invoices','started_at':stamp,'finished_at':None,'manifest':manifest}
    rendered=d.run_body(run,m._date_time)
    assert '21/10/2026 às 22:15:00' in rendered and '<script>' not in rendered
    assert 'Ver DDL registrado para esta execução' in rendered


if __name__=='__main__':
    for test in (test_native_ddl_and_unavailable_properties,test_historical_contract_is_read_from_run_not_current_table,
                 test_table_and_execution_references_open_dialogs):test()
    print('PASS: native DDL, historical contract, escaped content, SELECT-only endpoints and modal links')
