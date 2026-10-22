"""Offline contract tests for observability, without Kafka or Docker."""
import importlib.util
import os
from pathlib import Path
from html.parser import HTMLParser
import sys
import types
from unittest.mock import patch
from datetime import datetime, timedelta, timezone

root=Path(__file__).resolve().parents[1]
os.environ.setdefault('LOCAL_DSN','host=fake user=monitor_reader')
os.environ.setdefault('KAFKA_BOOTSTRAP','fake:9092')
try:import psycopg
except ImportError:
    sys.modules['psycopg']=types.SimpleNamespace(connect=None)
try:import confluent_kafka
except ImportError:
    mod=types.ModuleType('confluent_kafka')
    mod.ConsumerGroupTopicPartitions=lambda x:x
    mod.TopicPartition=lambda x,y:(x,y)
    admin=types.ModuleType('confluent_kafka.admin')
    admin.AdminClient=lambda opts:None
    admin.OffsetSpec=types.SimpleNamespace(latest=lambda:'latest')
    sys.modules['confluent_kafka']=mod
    sys.modules['confluent_kafka.admin']=admin
spec=importlib.util.spec_from_file_location('mon', root/'app/monitor.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def test_coverage():
    cov=m.coverage_from_ranges([(1,100,300),(91,200,300),(250,310,300)])
    assert cov['status']=='partial' and cov['covered_ids']==251 and cov['gaps']==[[201,249]],cov
    full=m.coverage_from_ranges([(1,100,300),(101,200,300),(201,400,300)])
    assert full['status']=='complete' and full['ratio']==100
    assert m.coverage_from_ranges([])['status']=='unknown'
    assert m.coverage_from_ranges([(1,100,None)])['status']=='unknown'
    assert m.coverage_from_ranges([(1,100,100)],truncated=True)['status']=='unknown'
    assert m.coverage_from_ranges([(50,60,100)])['gaps']==[[1,49],[61,100]]


def test_alerts_and_unknown():
    base={'local_db':{'ok':True},'kafka':{'ok':True,'audit_topic':True,
          'group':{'lag':0,'members':1}},'active':{'version':8},
          'metrics':{'stalled':0,'failures_24h':0,'last_success_age_seconds':10},
          'coverage':{'status':'partial','ratio':10},'schema':{'pending_hash':None}}
    assert m.evaluate(base)[0]=='operational'
    base['metrics']['stalled']=1
    assert m.evaluate(base)[0]=='unavailable'
    assert 'RUN_STALLED' in {a['code'] for a in m.evaluate(base)[1]}
    base['metrics']['stalled']=0
    base['kafka']['group']['lag']=m.MAX_LAG+1
    assert 'CONSUMER_LAG' in {a['code'] for a in m.evaluate(base)[1]}
    base['kafka']['group']['lag']=None
    assert 'CONSUMER_LAG' not in {a['code'] for a in m.evaluate(base)[1]}
    base['schema']['pending_hash']='abc'
    assert 'DDL_PENDING' in {a['code'] for a in m.evaluate(base)[1]}


class Cursor:
    def __init__(self): self.sql=''
    def __enter__(self):return self
    def __exit__(self,*a):pass
    def execute(self,sql,params=()):
        self.sql=' '.join(sql.split())
        assert self.sql.startswith('SELECT') or self.sql.startswith('WITH RECURSIVE'),self.sql
    def fetchone(self):
        s=self.sql
        if 'FROM replica.active_state a' in s:return ('blue',7,10000,201,datetime(2026,10,21,tzinfo=timezone.utc),7,6)
        if 'FROM replica.transitions ORDER' in s:return ('promote','green','blue',7,datetime(2026,10,21,tzinfo=timezone.utc))
        if 'COUNT(*) FILTER' in s:return (0,0,0,0,2,4,3,1,5000,datetime(2026,10,21,tzinfo=timezone.utc))
        if 'FROM replica.schema_state' in s:return ('hash-active',None,None,None)
        if 'SELECT finished_at FROM replica.sync_runs' in s:return (datetime(2026,10,21,tzinfo=timezone.utc),)
        if 'FROM replica.schema_changes' in s:return None
        raise AssertionError(s)
    def fetchall(self):
        s=self.sql
        if 'WITH RECURSIVE ancestors' in s:return [(1,100,200),(101,200,200)]
        if 'FROM replica.sync_runs ORDER' in s:return []
        if 'FROM replica.source_catalog_observations' in s:return []
        if 'FROM replica.range_checkpoints' in s:return []
        raise AssertionError(s)
class DB:
    def __enter__(self):return self
    def __exit__(self,*a):pass
    def cursor(self):return Cursor()


def test_end_to_end_snapshot_fake():
    snap=m.load_snapshot(now=datetime(2026,10,21,20,tzinfo=timezone.utc),
      connect=lambda *a,**kw: DB(),
      kafka_probe=lambda:{'ok':True,'audit_topic':True,'brokers':1,'partitions':1,
                          'group':{'lag':0,'members':1,'state':'stable'}})
    assert snap['coverage']['status']=='complete'
    assert snap['local_db']['ok'] and snap['kafka']['ok']
    markup=m.render_html(snap)
    for label in ['Origem remota','Blue-green','Consumer','Cobertura','DDL / schema',
                  'PostgreSQL local','Falhas / rejeições','Faixas efetivamente promovidas']:
        assert label in markup,label
    assert 'Não sondada' in markup
    assert 'Kafbat UI' in markup


def test_database_read_failure_is_distinct_from_connection_failure():
    class PermissionDenied(RuntimeError):
        sqlstate = '42501'

    kafka_probe = lambda: {'ok':True,'audit_topic':True,'group':{}}
    error = PermissionDenied('permission denied for table generations <details>')
    with patch.object(m, '_query_db', side_effect=error):
        snap = m.load_snapshot(connect=lambda *a, **kw: DB(), kafka_probe=kafka_probe)
    assert snap['local_db']['connected'] and not snap['local_db']['ok']
    assert snap['local_db']['error_code'] == '42501'
    codes = {alarm['code'] for alarm in snap['alarms']}
    assert 'LOCAL_DB_READ_ERROR' in codes and 'LOCAL_DB_DOWN' not in codes
    markup = m.render_html(snap)
    assert 'Erro de leitura' in markup
    assert 'permission denied for table generations &lt;details&gt;' in markup
    assert '<details>' not in markup

    def offline(*args, **kwargs):
        raise OSError('connection refused')

    snap = m.load_snapshot(connect=offline, kafka_probe=kafka_probe)
    assert not snap['local_db']['connected'] and not snap['local_db']['ok']
    codes = {alarm['code'] for alarm in snap['alarms']}
    assert 'LOCAL_DB_DOWN' in codes and 'LOCAL_DB_READ_ERROR' not in codes


def test_explanations_have_unique_accessible_targets_and_preserve_navigation():
    snap=m.load_snapshot(connect=lambda *a,**kw: DB(),
                         kafka_probe=lambda:{'ok':True,'audit_topic':True,'group':{}})

    class HelpParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.stack=[]
            self.ids=set()
            self.tooltips=set()
            self.descriptions=[]
            self.stage_links=[]

        def handle_starttag(self,tag,attrs):
            attrs=dict(attrs)
            if 'id' in attrs:
                assert attrs['id'] not in self.ids, attrs['id']
                self.ids.add(attrs['id'])
            if attrs.get('role')=='tooltip':
                self.tooltips.add(attrs['id'])
            if tag=='button':
                assert 'a' not in self.stack, 'Help buttons must not be nested inside navigation links'
                assert attrs.get('type')=='button'
                assert attrs.get('aria-label')
                self.descriptions.append(attrs['aria-describedby'])
            if tag=='a' and attrs.get('class')=='stage-link':
                self.stage_links.append(attrs['href'][1:])
            if tag not in ('meta','br','hr','link','input','img'):
                self.stack.append(tag)

        def handle_endtag(self,tag):
            assert self.stack.pop()==tag, tag

    parser=HelpParser()
    parser.feed(m.render_html(snap))
    assert parser.descriptions and set(parser.descriptions)==parser.tooltips
    assert len(parser.descriptions)==len(parser.tooltips)
    assert len(parser.stage_links)==8 and set(parser.stage_links)<=parser.ids
    assert not parser.stack
    with patch.dict(m.HELP,{'example':('Nome <teste>','Explicação <script>alert(1)</script>')}):
        markup=m._help('example')
    assert '<script>' not in markup and '&lt;script&gt;' in markup
    assert 'Nome &lt;teste&gt;' in markup


def test_permissions_and_protocol():
    src=(root/'app/monitor.py').read_text()
    assert 'REMOTE_DSN' not in src and 'docker.sock' not in src
    assert 'BEGIN TRANSACTION' not in src and 'UPDATE replica.' not in src
    compose=(root/'docker-compose.yml').read_text()
    assert '127.0.0.1:8082:8082' in compose
    core=(root/'app/lab.py').read_text()
    assert "INSERT INTO replica.range_checkpoints" in core
    assert "source_engine':'postgresql'" in core
    assert 'sweep_target_max_id' in (root/'app/mssql_sync.py').read_text()
    assert 'CREATE TABLE replica.range_checkpoints' in (root/'sql/local.sql').read_text()


def test_pagination_is_bounded_and_preserves_other_tables():
    pages=m.page_numbers({'tables_page':['2'],'runs_page':['3'],'schema_page':['<script>'],
                          'ranges_page':['999999999999999999999999']})
    assert pages=={'tables_page':2,'runs_page':3,'schema_page':1,'ranges_page':1}
    rows,nav=m.table_page(list(range(23)),'tables_page','tables',pages,'Tabelas')
    assert rows==list(range(10,20))
    assert '11–20 de 23 registros' in nav and 'Página 2 de 3' in nav
    assert 'runs_page=3&amp;tables_page=3#tables' in nav or 'tables_page=3&amp;runs_page=3#tables' in nav
    rows,nav=m.table_page(list(range(23)),'tables_page','tables',{'tables_page':999},'Tabelas')
    assert rows==[20,21,22] and 'Próxima' not in nav
    assert m.table_page([],'tables_page','tables',{},'Tabelas')==([], '')
    rows,nav=m.table_page([1],'tables_page','tables',{},'Tabelas')
    assert rows==[1] and 'Página 1 de 1' in nav
    assert m.page_numbers({'runs_page':['-1']})['runs_page']==1
    snap=m.load_snapshot(now=datetime(2026,10,21,20,tzinfo=timezone.utc),
        connect=lambda *a,**kw:DB(),kafka_probe=lambda:{'ok':True,'audit_topic':True,'group':{'lag':0,'members':1}})
    snap['mode']='catalog'
    snap['tables']=[{'engine':'postgresql','schema':'public','table':f'only_row_{n:02}',
                     'destination_schema':'mirror_public','destination_table':f'dst_{n}',
                     'status':'active','rows':1,'generation':1,'slot':'blue','schema_hash':'hash',
                     'checked_at':'2026-10-21','error':None} for n in range(23)]
    markup=m.render_html(snap,{'tables_page':2})
    assert 'only_row_09' not in markup and 'only_row_10' in markup and 'only_row_19' in markup
    assert 'only_row_20' not in markup and '11–20 de 23 registros' in markup

if __name__=='__main__':
    for test in (test_coverage,test_alerts_and_unknown,test_end_to_end_snapshot_fake,
                 test_database_read_failure_is_distinct_from_connection_failure,
                 test_explanations_have_unique_accessible_targets_and_preserve_navigation,
                 test_pagination_is_bounded_and_preserves_other_tables,
                 test_permissions_and_protocol):
        test()
    print('PASS full health: coverage, alerts, readonly, SQL, UI')
