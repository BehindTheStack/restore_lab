"""Monitor Kafka restore lab.

No remote credentials, no Docker socket, no Kafka consumer group membership,
no writes and no external telemetry stack. Source reachability is INDIRECT.
"""
from __future__ import annotations

import html
import json
import os
from pathlib import Path
import re
import statistics
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse
from uuid import UUID

import psycopg
from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
import monitor_details as details

LOCAL_DSN = os.environ['LOCAL_DSN']
BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP']
TOPIC = os.getenv('KAFKA_TOPIC', 'lab.sync.events.v6')
AUDIT_TOPIC = os.getenv('KAFKA_AUDIT_TOPIC', 'lab.sync.audit.v6')
GROUP = os.getenv('LAB_CONSUMER_GROUP', 'lab-range-bluegreen-v4')
PORT = int(os.getenv('LAB_MONITOR_PORT', '8082'))
STALLED_MIN = int(os.getenv('LAB_STALLED_MINUTES', '15'))
COVERAGE_HOURS = int(os.getenv('LAB_COVERAGE_WINDOW_HOURS', '24'))
EXPECTED_INTERVAL_MIN = int(os.getenv('LAB_EXPECTED_INTERVAL_MINUTES', '0'))
MAX_LAG = int(os.getenv('LAB_MAX_CONSUMER_LAG', '1000'))
MAX_RANGES = int(os.getenv('LAB_MAX_COVERAGE_INTERVALS', '20000'))
SYNC_MODE = os.getenv('LAB_SYNC_MODE','legacy')


def _iso(v):
    return v.isoformat() if isinstance(v, datetime) else v


def _age(value, now):
    return None if value is None else max(0, round((now - value).total_seconds()))


def _safe(value):
    return html.escape('—' if value is None else str(value), quote=True)


def coverage_from_ranges(ranges, *, truncated=False):
    """Union of audited PK intervals. Never describe as percent of rows/bytes.

    Each interval was checked under a different source transaction; a full
    keyspace sweep does NOT imply simultaneous source-wide consistency.
    """
    if not ranges:
        return {'status': 'unknown', 'ratio': None, 'covered_ids': 0,
                'target_max_id': None, 'gaps': [], 'interval_count': 0,
                'truncated': truncated}
    # If any bound is absent (legacy source) we cannot assert full coverage.
    valid = [(int(lo), int(hi), int(target)) for lo, hi, target in ranges
             if lo is not None and hi is not None and target is not None and int(target) >= 1]
    if not valid or truncated or len(valid) != len(ranges):
        return {'status': 'unknown', 'ratio': None, 'covered_ids': None,
                'target_max_id': max((int(t) for _, _, t in valid), default=None),
                'gaps': [], 'interval_count': len(ranges), 'truncated': truncated}
    target = max(t for _, _, t in valid)
    chunks = sorted((max(1,lo), min(target,hi)) for lo,hi,_ in valid if hi >= 1 and lo <= target)
    chunks = [(lo,hi) for lo,hi in chunks if hi >= lo]
    covered = 0
    gaps = []
    last = 0
    for lo, hi in chunks:
        if lo > last + 1 and len(gaps) < 8:
            gaps.append([last+1,lo-1])
        if hi > last:
            covered += hi - max(last + 1,lo) + 1
            last = hi
    if last < target and len(gaps) < 8:
        gaps.append([last+1,target])
    return {'status': 'complete' if covered == target else 'partial',
            'ratio': round(100*covered/target,2), 'covered_ids': covered,
            'target_max_id': target, 'gaps': gaps,
            'interval_count': len(ranges), 'truncated': False}


def probe_kafka(*, admin_factory=AdminClient):
    """Read Kafka metadata and committed group offsets. Never join consumer group."""
    result = {'ok': False, 'brokers': None, 'data_topic': False,
              'audit_topic': False, 'partitions': None,
              'group': {'name': GROUP, 'state': 'unknown', 'members': None,
                        'lag': None, 'committed_partitions': 0},
              'error': None, 'group_error': None}
    try:
        admin = admin_factory({'bootstrap.servers': BOOTSTRAP,
              'socket.timeout.ms': 2500, 'allow.auto.create.topics': False})
        metadata = admin.list_topics(timeout=3)
        result['brokers'] = len(metadata.brokers)
        for name, field in ((TOPIC,'data_topic'),(AUDIT_TOPIC,'audit_topic')):
            meta = metadata.topics.get(name)
            result[field] = bool(meta and not meta.error)
        result['ok'] = bool(metadata.brokers) and result['data_topic']
        if not result['ok']:
            result['error'] = 'Sem broker ou tópico de dados inacessível'
            return result
        meta = metadata.topics[TOPIC]
        parts = sorted(meta.partitions)
        result['partitions'] = len(parts)
        try:
            detail = admin.describe_consumer_groups([GROUP], request_timeout=3)[GROUP].result(timeout=4)
            result['group']['state'] = str(detail.state).rsplit('.',1)[-1].lower()
            result['group']['members'] = len(detail.members)
            offsets = admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(GROUP)], request_timeout=3)[GROUP].result(timeout=4)
            committed = {(p.topic,p.partition):p.offset for p in offsets.topic_partitions
                         if p.topic == TOPIC and p.offset is not None and p.offset >= 0}
            lag, complete = 0, True
            for part in parts:
                offset = committed.get((TOPIC, part))
                if offset is None:
                    complete = False
                    continue
                tp = TopicPartition(TOPIC,part)
                end = admin.list_offsets({tp:OffsetSpec.latest()}, request_timeout=3)
                last = end[tp].result(timeout=4).offset
                lag += max(0,last-offset)
            result['group']['committed_partitions'] = len(committed)
            result['group']['lag'] = lag if complete and parts else None
        except Exception as exc:
            result['group_error'] = f'{type(exc).__name__}: {str(exc)[:160]}'
    except Exception as exc:
        result['error'] = f'{type(exc).__name__}: {str(exc)[:160]}'
    return result


def _query_catalog_db(conn, now):
    out={'mode':'catalog','tables':[],'active':None,'schema':None,'catalog':[],
         'last_ranges':[],'recent_runs':[],'latest_run':None,'metrics':{},
         'coverage':{'status':'unknown','unit':'tables','ratio':None,'gaps':[]},'table_error_count':0}
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('replica.discovered_tables')")
        if cur.fetchone()[0] is None:
            return out
        cur.execute('''SELECT table_key,engine,source_schema,source_table,destination_schema,destination_table,
            status,active_slot,active_generation,blue_generation,green_generation,row_count,schema_hash,
            checked_at,last_error,EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname=t.destination_schema AND c.relname=t.destination_table AND c.relkind='v'
                AND obj_description(c.oid,'pg_class')='kafka-restore-lab:discovery:'||t.table_key)
            FROM replica.discovered_tables t ORDER BY engine,source_schema,source_table''')
        keys=('key','engine','schema','table','destination_schema','destination_table','status','slot',
              'generation','blue_generation','green_generation','rows','schema_hash','checked_at','error','published')
        tables=[dict(zip(keys,row)) for row in cur.fetchall()]
        for table in tables:
            table['checked_at']=_iso(table['checked_at'])
        out['tables']=tables
        published=[t for t in tables if t['generation'] is not None and t['published']]
        if published:
            out['active']={'version':max(t['generation'] for t in published),'slot':'por tabela',
                           'rows':sum(t['rows'] for t in published),'published_tables':len(published),
                           'blue_tables':sum(t['slot']=='blue' for t in published),
                           'green_tables':sum(t['slot']=='green' for t in published)}
        out['table_error_count']=sum(t['status'] in ('error','blocked','inaccessible') for t in tables)
        cur.execute('''SELECT r.run_id::text,t.engine,t.source_schema,t.source_table,r.mode,r.status,
            r.delta_count,r.seen_count,r.inserted_count,r.updated_count,r.deleted_count,
            r.logical_bytes,r.started_at,r.finished_at,r.error,r.result_generation,t.active_slot,t.table_key
            FROM replica.discovered_runs r JOIN replica.discovered_tables t USING(table_key)
            ORDER BY r.started_at DESC LIMIT 40''')
        for rid,engine,schema,table,mode,status,delta,seen,ins,upd,deleted,nbytes,start,end,error,generation,slot,key in cur.fetchall():
            run={'id':rid,'engine':engine,'object':schema+'.'+table,'mode':mode,'status':status,
                 'low':None,'high':None,'delta':delta,'seen':seen,'inserted':ins,'updated':upd,
                 'deleted':deleted,'bytes':nbytes,'started_at':_iso(start),'finished_at':_iso(end),
                 'duration_seconds':round((end-start).total_seconds(),2) if end else None,
                 'age_seconds':_age(start,now),'error':error,'generation':generation,'slot':slot,'table_key':key}
            out['recent_runs'].append(run)
        if out['recent_runs']:
            out['latest_run']=out['recent_runs'][0]
        promoted=next((r for r in out['recent_runs'] if r['status']=='promoted'),None)
        if promoted:
            out['last_transition']={'action':'promote','to':promoted['slot'],'version':promoted['generation'],
                                    'at':promoted['finished_at']}
        cur.execute('''SELECT count(*) FILTER(WHERE status='loading'),
            count(*) FILTER(WHERE status='loading' AND started_at < %s-make_interval(mins=>%s)),
            count(*) FILTER(WHERE status IN ('failed','rejected','aborted') AND finished_at >= %s-interval '24 hours'),
            count(*) FILTER(WHERE status='rejected' AND finished_at >= %s-interval '24 hours'),
            count(*) FILTER(WHERE status='promoted' AND finished_at >= %s-interval '24 hours'),
            COALESCE(sum(inserted_count) FILTER(WHERE status='promoted' AND finished_at >= %s-interval '24 hours'),0),
            COALESCE(sum(updated_count) FILTER(WHERE status='promoted' AND finished_at >= %s-interval '24 hours'),0),
            COALESCE(sum(deleted_count) FILTER(WHERE status='promoted' AND finished_at >= %s-interval '24 hours'),0),
            COALESCE(sum(logical_bytes) FILTER(WHERE status='promoted' AND finished_at >= %s-interval '24 hours'),0),
            max(finished_at) FILTER(WHERE status='promoted') FROM replica.discovered_runs''',
            (now,STALLED_MIN,now,now,now,now,now,now,now))
        row=cur.fetchone()
        out['metrics']=dict(zip(('pending','stalled','failures_24h','rejected_24h','promotions_24h',
                                'inserted_24h','updated_24h','deleted_24h','kafka_bytes_24h','last_success_at'),row))
        for name in ('inserted_24h','updated_24h','deleted_24h','kafka_bytes_24h'):
            out['metrics'][name]=int(out['metrics'][name])
        out['metrics']['last_success_age_seconds']=_age(row[9],now)
        out['metrics']['last_success_at']=_iso(row[9])
        cur.execute('''SELECT status,error,started_at,finished_at FROM replica.discovered_cycles
                       ORDER BY started_at DESC LIMIT 1''')
        cycle=cur.fetchone()
        if cycle:
            out['latest_cycle']={'status':cycle[0],'error':cycle[1],
                                 'started_at':_iso(cycle[2]),'finished_at':_iso(cycle[3])}
        visible=[t for t in tables if t['status']!='missing_or_inaccessible']
        checked=[t for t in visible if t['checked_at'] and
                 datetime.fromisoformat(t['checked_at']) >= now-timedelta(hours=COVERAGE_HOURS)]
        total=len(visible)
        out['coverage']={'unit':'tables','status':('complete' if len(checked)==total else 'partial') if total else 'unknown',
                         'ratio':round(100*len(checked)/total,2) if total else None,'covered_ids':len(checked),
                         'target_max_id':total,'interval_count':out['metrics']['promotions_24h'],
                         'window_hours':COVERAGE_HOURS,'gaps':[]}
        cur.execute('''SELECT h.table_key,t.engine,h.classification,h.observed_at,h.schema_hash,
            t.source_schema,t.source_table FROM replica.discovered_schema_history h
            JOIN replica.discovered_tables t USING(table_key)
            WHERE h.id IN(SELECT max(id) FROM replica.discovered_schema_history GROUP BY table_key)
            ORDER BY h.observed_at DESC LIMIT 20''')
        out['catalog']=[{'source':r[5]+'.'+r[6],'engine':r[1],'classification':r[2],
                         'observed_at':_iso(r[3]),'native_hash':r[4],'logical_hash':r[4],'table_key':r[0]} for r in cur.fetchall()]
    return out


def _query_db(conn, now):
    if SYNC_MODE=='catalog':
        return _query_catalog_db(conn,now)
    out = {}
    with conn.cursor() as cur:
        cur.execute('''SELECT a.slot,a.version,a.row_count,a.next_sweep_id,a.cutoff,
                      b.version,g.version
                      FROM replica.active_state a
                      LEFT JOIN replica.slot_state b ON b.slot='blue'
                      LEFT JOIN replica.slot_state g ON g.slot='green'
                      WHERE a.singleton''')
        v = cur.fetchone()
        out['active'] = ({'slot': v[0], 'version': v[1], 'rows': v[2],
                          'next_sweep_id': v[3], 'cutoff': _iso(v[4]),
                          'blue_version': v[5], 'green_version': v[6]}
                         if v else None)
        cur.execute('''SELECT action,from_slot,to_slot,to_version,transitioned_at
                       FROM replica.transitions ORDER BY id DESC LIMIT 1''')
        v = cur.fetchone()
        out['last_transition'] = ({'action':v[0], 'from':v[1], 'to':v[2],
                                   'version':v[3], 'at':_iso(v[4]),
                                   'age_seconds':_age(v[4],now)} if v else None)
        cur.execute('''SELECT run_id::text,mode,status,sweep_low,sweep_high,
                             delta_count,sweep_count,inserted_count,updated_count,
                             deleted_count,logical_kafka_bytes,started_at,finished_at,
                             error,source_engine,data_schema_hash
                       FROM replica.sync_runs ORDER BY started_at DESC LIMIT 40''')
        runs=[]
        for r in cur.fetchall():
            (rid,mode,status,lo,hi,delta,seen,ins,upd,deleted,nbytes,start,end,
             error,engine,hash_)=r
            runs.append({'id':rid,'mode':mode,'status':status,'low':lo,'high':hi,
                 'delta':delta,'seen':seen,'inserted':ins,'updated':upd,'deleted':deleted,
                 'bytes':nbytes,'started_at':_iso(start),'finished_at':_iso(end),
                 'duration_seconds':round((end-start).total_seconds(),2) if end else None,
                 'age_seconds':_age(start,now), 'error':error[:500] if error else None,
                 'engine':engine,'schema_hash':hash_})
        out['recent_runs']=runs
        out['latest_run']=runs[0] if runs else None
        cur.execute('''SELECT COUNT(*) FILTER (WHERE status IN ('extracting','loading')),
                      COUNT(*) FILTER (WHERE status IN ('extracting','loading')
                        AND started_at < %s - make_interval(mins => %s)),
                      COUNT(*) FILTER (WHERE status IN ('failed','aborted','rejected')
                        AND COALESCE(finished_at,started_at) >= %s - interval '24 hours'),
                      COUNT(*) FILTER (WHERE status='rejected'
                        AND COALESCE(finished_at,started_at) >= %s - interval '24 hours'),
                      COUNT(*) FILTER (WHERE status='promoted'
                        AND finished_at >= %s - interval '24 hours'),
                      COALESCE(SUM(inserted_count) FILTER (WHERE status='promoted'
                        AND finished_at >= %s - interval '24 hours'),0),
                      COALESCE(SUM(updated_count) FILTER (WHERE status='promoted'
                        AND finished_at >= %s - interval '24 hours'),0),
                      COALESCE(SUM(deleted_count) FILTER (WHERE status='promoted'
                        AND finished_at >= %s - interval '24 hours'),0),
                      COALESCE(SUM(logical_kafka_bytes) FILTER (WHERE status='promoted'
                        AND finished_at >= %s - interval '24 hours'),0),
                      MAX(finished_at) FILTER (WHERE status='promoted')
                      FROM replica.sync_runs
                      WHERE started_at >= %s - interval '24 hours'
                         OR status IN ('extracting','loading')''',
                    (now,STALLED_MIN,now,now,now,now,now,now,now,now))
        a = cur.fetchone()
        # Last success might be older than the 24h aggregate window.
        cur.execute('''SELECT finished_at FROM replica.sync_runs
                       WHERE status='promoted' ORDER BY finished_at DESC LIMIT 1''')
        last_success_row=cur.fetchone()
        last_success=last_success_row[0] if last_success_row else None
        out['metrics']={'pending':a[0], 'stalled':a[1], 'failures_24h':a[2],
                        'rejected_24h':a[3], 'promotions_24h':a[4],
                        'inserted_24h':a[5], 'updated_24h':a[6],
                        'deleted_24h':a[7], 'kafka_bytes_24h':a[8],
                        'last_success_at':_iso(last_success),
                        'last_success_age_seconds':_age(last_success,now)}
        cur.execute('''SELECT active_hash,pending_hash,backfill_next,backfill_max
                      FROM replica.schema_state WHERE singleton''')
        v=cur.fetchone()
        out['schema']={'active_hash':v[0], 'pending_hash':v[1],
                       'backfill_next':v[2], 'backfill_max':v[3]} if v else None
        cur.execute('''SELECT status,diff,created_at FROM replica.schema_changes
                      ORDER BY id DESC LIMIT 1''')
        v=cur.fetchone()
        out['schema_change']={'status':v[0], 'changed_at':_iso(v[2]),
                              'diff_size':len(v[1]) if isinstance(v[1],list) else None} if v else None
        cur.execute('''SELECT source_key,source_engine,classification,observed_at,
                            native_hash,logical_hash,object_name
                      FROM replica.source_catalog_observations
                      WHERE observation_id IN (
                          SELECT MAX(observation_id) FROM replica.source_catalog_observations
                          GROUP BY source_key)
                      ORDER BY source_key LIMIT 20''')
        out['catalog']=[{'source':v[6],'source_key':v[0],'engine':v[1],'classification':v[2],
                         'observed_at':_iso(v[3]),'native_hash':v[4],
                         'logical_hash':v[5]} for v in cur.fetchall()]
        # Filter to branch ancestry: rolled-back/promoted-away generations do not
        # count as evidence for the current dataset.
        cur.execute('''WITH RECURSIVE ancestors AS (
                        SELECT version,parent_version FROM replica.generations
                        WHERE version=(SELECT version FROM replica.active_state WHERE singleton)
                        UNION ALL
                        SELECT g.version,g.parent_version FROM replica.generations g
                        JOIN ancestors a ON a.parent_version=g.version)
                       SELECT c.sweep_low,c.sweep_high,c.target_max_id
                       FROM replica.range_checkpoints c
                       JOIN ancestors a ON a.version=c.version
                       WHERE c.checked_at >= %s - make_interval(hours => %s)
                       ORDER BY c.checked_at DESC LIMIT %s''',
                    (now,COVERAGE_HOURS,MAX_RANGES+1))
        bounds=cur.fetchall()
        out['coverage']=coverage_from_ranges(bounds[:MAX_RANGES],truncated=len(bounds)>MAX_RANGES)
        out['coverage']['window_hours']=COVERAGE_HOURS
        out['coverage']['unit']='PK integer interval, not rows or bytes'
        cur.execute('''SELECT run_id::text,sweep_low,sweep_high,source_engine,
                            target_max_id,version,checked_at
                       FROM replica.range_checkpoints
                       ORDER BY checked_at DESC LIMIT 15''')
        out['last_ranges']=[{'run_id':v[0],'low':v[1],'high':v[2],
                'engine':v[3],'target_max_id':v[4],'version':v[5],
                'at':_iso(v[6])} for v in cur.fetchall()]
    return out


def evaluate(snapshot):
    """Rules with explicit UNKNOWN; service accessibility != data consistency."""
    alarms=[]
    def alert(level, key, msg, action):
        alarms.append({'severity':level,'code':key,'message':msg,'action':action})
    if not snapshot['local_db']['ok']:
        if snapshot['local_db'].get('connected'):
            action = ('Executar a inicialização para recompor a leitura de monitor_reader.'
                      if snapshot['local_db'].get('error_code') == '42501' else
                      'Conferir o schema local e o erro da consulta de auditoria.')
            alert('critical','LOCAL_DB_READ_ERROR','Falha na leitura da auditoria local',
                  action + ' ' + (snapshot['local_db'].get('error') or ''))
        else:
            alert('critical','LOCAL_DB_DOWN','PostgreSQL local indisponível',
                  'Conferir healthcheck, logs e disponibilidade do banco local.')
    if not snapshot['kafka']['ok']:
        alert('critical','KAFKA_DOWN','Kafka ou tópico de dados indisponível',
              'Inspecionar broker e tópico no Kafbat.')
    if not snapshot['local_db']['ok']:
        return 'unavailable',alarms
    a=snapshot.get('active') or {}
    m=snapshot.get('metrics') or {}
    k=(snapshot['kafka'].get('group') or {})
    c=snapshot.get('coverage') or {}
    sc=snapshot.get('schema') or {}
    if not a.get('version'):
        alert('warning','BOOTSTRAP_PENDING','Nenhuma versão local foi promovida',
              'Executar bootstrap e conferir o processamento completo.')
    if m.get('stalled',0):
        alert('critical','RUN_STALLED',f"{m['stalled']} lote(s) sem conclusão há mais de {STALLED_MIN} min",
              'Inspecionar a execução, consumer e tópico antes de reprocessar.')
    if m.get('failures_24h',0):
        alert('warning','RUN_FAILURE',f"{m['failures_24h']} execução(ões) falharam/rejeitadas em 24 h",
              'Examinar erros de execução e verificar os limites de exclusão.')
    if sc.get('pending_hash'):
        alert('warning','DDL_PENDING','Mudança estrutural em preparação bloqueia sincronização',
              'Conferir schema, aprovação e progresso do backfill.')
    if snapshot.get('table_error_count'):
        alert('warning','TABLE_REPLICATION_ERROR',f"{snapshot['table_error_count']} tabela(s) com erro ou acesso bloqueado",
              'Consultar o catálogo de tabelas, conferir SELECT na origem e o erro da última leitura.')
    if (snapshot.get('latest_cycle') or {}).get('status')=='failed':
        alert('warning','CATALOG_CYCLE_FAILED','Última descoberta de tabelas falhou',
              (snapshot['latest_cycle'].get('error') or 'Conferir conexão, catálogo e permissões SELECT da origem.'))
    if k.get('lag') is not None and k['lag'] > MAX_LAG:
        alert('warning','CONSUMER_LAG',f"Atraso Kafka de {k['lag']} mensagens supera {MAX_LAG}",
              'Verificar consumer, throughput e offsets no Kafbat.')
    if snapshot['kafka']['ok'] and a.get('version') and k.get('members') == 0:
        alert('warning','CONSUMER_STOPPED','Grupo Kafka sem consumidores ativos',
              'Conferir container consumer e seu grupo no Kafbat.')
    if (EXPECTED_INTERVAL_MIN>0 and a.get('version') and
        (m.get('last_success_age_seconds') is None or
         m['last_success_age_seconds'] > EXPECTED_INTERVAL_MIN*60)):
        alert('warning','PROMOTION_STALE','Última promoção ultrapassou a janela esperada',
              'Verificar scheduler e disponibilidade da origem; sem consulta direta à fonte.')
    if snapshot['kafka']['ok'] and not snapshot['kafka'].get('audit_topic'):
        alert('warning','AUDIT_TOPIC_MISSING','Tópico Kafka de auditoria não encontrado',
              'Conferir configuração dos tópicos; não criar automaticamente em produção.')
    if c.get('truncated'):
        alert('warning','COVERAGE_INCOMPLETE','Histórico de faixas excedeu limite de leitura do painel',
              'Aumentar janela/limite com cautela ou agregar o histórico.')
    if c.get('status') == 'partial' and c.get('target_max_id'):
        # A partial audit is expected during a continuous sweep. Inform, do not
        # make every in-progress normal operation a false alarm.
        pass
    if any(a['severity']=='critical' for a in alarms):
        return 'unavailable',alarms
    if alarms:
        return 'attention',alarms
    if not a.get('version'):
        return 'not_initialized',alarms
    return 'operational',alarms


def load_snapshot(*, now=None, connect=psycopg.connect, kafka_probe=probe_kafka):
    now=now or datetime.now(timezone.utc)
    state={'checked_at':now.isoformat(),'local_db':{'ok':False,'connected':False,
                                                'error':None,'error_code':None},
           'kafka':{'ok':False},'active':None,'schema':None,
           'metrics':{},'coverage':{'status':'unknown'},'recent_runs':[],
           'catalog':[],'last_ranges':[],'alarms':[], 'overall':'unavailable',
           'source_probe':'not_performed',
           'limitations':[
             'Sem acesso direto ao banco remoto: disponibilidade atual da origem não é aferida.',
             'Cobertura mede intervalos numéricos de PK conferidos, não percentual de linhas ou bytes.',
             'As faixas foram lidas em transações distintas: 100% não garante snapshot global simultâneo.',
             'Sem CDC, mudanças após uma faixa só aparecem na próxima varredura daquela faixa.'
           ]}
    try:
        with connect(LOCAL_DSN,connect_timeout=3) as conn:
            state['local_db']['connected']=True
            state.update(_query_db(conn,now))
        state['local_db']['ok']=True
    except Exception as exc:
        state['local_db']['error']=f'{type(exc).__name__}: {str(exc)[:230]}'
        state['local_db']['error_code']=getattr(exc,'sqlstate',None)
    try:
        state['kafka']=kafka_probe()
    except Exception as exc:
        state['kafka']={'ok':False,'error':f'{type(exc).__name__}: {str(exc)[:230]}',
                        'group':{'state':'unknown','members':None,'lag':None}}
    if state.get('mode')=='catalog':
        state['limitations']=[
            'Sem consulta direta à origem: conectividade atual não aferida pelo painel.',
            'Cobertura mede tabelas com leitura completa validada na janela, não linhas ou bytes.',
            'As tabelas usam transações independentes: 100% não garante snapshot global simultâneo.',
            'Sem CDC, alterações intermediárias podem não ser observadas entre ciclos.',
            'Ausência no catálogo pode significar DROP ou perda de visibilidade/permissão SELECT.'
        ]
    state['overall'],state['alarms']=evaluate(state)
    return state


HELP = {
    'catalog-coverage': ('Cobertura de tabelas', 'Proporção das tabelas visíveis que tiveram uma leitura '
                         'completa validada dentro da janela. Cada tabela é conferida em sua própria transação. '
                         '100% indica leituras concluídas na janela, sem garantir igualdade simultânea de toda a origem.'),
    'published-tables': ('Tabelas publicadas', 'Tabelas que já possuem uma geração validada e uma view no destino. '
                         'As views ficam em mirror_<schema>, por exemplo mirror_public.contratos. '
                         'As gerações e os slots são independentes para cada tabela.'),
    'catalog-schema': ('Estrutura por tabela', 'O catálogo de colunas, tipos e chave primária é relido em cada ciclo. '
                       'Uma mudança prepara uma cópia completa e só publica a nova view depois da validação. '
                       'Tipos sem correspondência segura ficam como texto, preservando a representação original.'),
    'checked-tables': ('Tabelas conferidas', 'Quantidade de tabelas cuja leitura completa foi validada na janela. '
                       'Uma tabela com erro mantém a última geração válida; confira a data e o erro no catálogo.'),
    'visible-tables': ('Tabelas visíveis', 'Tabelas encontradas dentro dos schemas selecionados. A descoberta depende '
                       'da visibilidade e das permissões SELECT da conta remota. O middleware não concede permissões na origem.'),
    'validated-scans': ('Leituras validadas', 'Quantidade de leituras completas promovidas na janela. Uma tabela '
                        'pode ser conferida várias vezes. Sem chave primária, a cópia é exata, mas contagens de UPDATE e DELETE não são inferidas.'),
    'blue-tables': ('Tabelas em BLUE', 'Quantidade de tabelas cuja geração publicada está no slot BLUE. '
                    'Cada tabela alterna seus próprios slots; não existe uma geração global simultânea para toda a origem.'),
    'green-tables': ('Tabelas em GREEN', 'Quantidade de tabelas cuja geração publicada está no slot GREEN. '
                     'Os dados do slot anterior permanecem disponíveis internamente para rollback compatível.'),
    'table-publication': ('Publicação por tabela', 'A view de cada tabela é ativada em uma transação própria. '
                          'Uma tabela com falha não substitui a geração validada. Consulte o catálogo para ver destino, schema e erros.'),
    'source': ('Origem remota', 'Banco PostgreSQL ou SQL Server de onde as tabelas são lidas. '
               'A conta do extrator tem acesso de leitura. “Não sondada” significa que o painel '
               'não testa essa conexão diretamente; ele acompanha as execuções registradas.'),
    'extractor': ('Extração', 'O Python lê a tabela ou a faixa configurada e compara resumos do conteúdo, '
                  'chamados fingerprints. Publica os registros novos ou alterados e os IDs encontrados. '
                  'Esses IDs permitem detectar exclusões dentro da faixa completa.'),
    'kafka': ('Kafka', 'Serviço que armazena e entrega as mensagens do extrator ao consumer. '
              'Os eventos de dados e de auditoria ficam em tópicos separados. “Acessível” confirma '
              'acesso ao broker e ao tópico de dados; o processamento é acompanhado separadamente.'),
    'consumer': ('Consumer', 'Processo que lê as mensagens do Kafka, confere cada lote e aplica as '
                 'diferenças no destino. Só confirma a posição de leitura depois de gravar no banco. '
                 'Durante um rebalanceamento, o grupo redistribui quem lê cada partição.'),
    'local-db': ('PostgreSQL local', 'Banco de destino que guarda os contratos e o histórico de execução. '
                 '“Acessível” significa que o painel conseguiu consultar a auditoria. “Erro de leitura” '
                 'pode indicar permissão ausente ou estrutura incompatível, mesmo com o banco ligado.'),
    'blue-green': ('Blue-green', 'Duas tabelas, BLUE e GREEN, ficam na mesma instância PostgreSQL. '
                   'O consumer prepara a próxima geração na tabela em espera e a publica após validar. '
                   'A aplicação lê public.contracts. O outro slot pode permitir rollback compatível.'),
    'schema': ('DDL e schema', 'Schema é a estrutura da tabela: colunas, tipos e regras. DDL são os comandos '
               'que alteram essa estrutura. Uma migração pendente bloqueia a sincronização normal até '
               'a revisão, o preenchimento dos dados necessários e a ativação.'),
    'last-promotion': ('Última promoção', 'Segundos desde que um lote validado foi publicado no destino. '
                       'Publicar uma geração é uma promoção. Esse tempo mostra o progresso do fluxo; '
                       'alterações da origem só aparecem quando seus IDs forem examinados novamente.'),
    'generation': ('Versão ativa', 'Número que identifica a geração dos dados publicada no destino. '
                   'Cada promoção cria uma geração, inclusive nas faixas sem diferenças. Um rollback '
                   'pode voltar a uma geração anterior. O slot indica qual tabela está publicada; '
                   'não é uma versão do aplicativo.'),
    'lag': ('Lag Kafka', 'Mensagens que o consumer ainda não confirmou. Uma faixa sem alterações pode '
            'gerar 1.002 mensagens: início, 1.000 presenças e fim. Picos podem ocorrer durante lotes '
            f'e rebalanceamentos. Acima de {MAX_LAG} mensagens, o painel alerta; atraso persistente '
            'pede verificar o consumer.'),
    'coverage': ('Cobertura de PK', f'Parte do espaço de IDs conferida por faixas promovidas nas últimas '
                 f'{COVERAGE_HOURS} horas, considerando a linhagem ativa. Com IDs de 1 a 10.000, 70% '
                 'equivale a 7.000 posições conferidas. A carga inicial é feita pelo bootstrap; esta '
                 'porcentagem acompanha a conferência periódica, sem garantir igualdade simultânea de toda a base.'),
    'failures': ('Falhas e rejeições', 'Conta as execuções que falharam, foram abortadas ou rejeitadas '
                 f'nas últimas 24 horas. Um lote pendente por mais de {STALLED_MIN} minutos é considerado '
                 'travado. Consulte o erro e os logs antes de tentar outro lote.'),
    'changes': ('Alterações promovidas', 'Soma das inserções, atualizações e exclusões dos lotes promovidos '
                'nas últimas 24 horas. Inclui a carga inicial do bootstrap. Alterações repetidas no '
                'mesmo contrato podem ser contadas em execuções diferentes.'),
    'overall': ('Estado geral', 'Combina os sinais do banco local, Kafka, consumer e histórico de execução. '
                '“Atenção” pede investigar um alerta; “Falha operacional” indica um sinal crítico. '
                'Sem alertas, os serviços observados estão prontos, mas a igualdade de toda a origem não é certificada.'),
    'last-reading': ('Última leitura', 'Momento em que o monitor reuniu os sinais exibidos nesta página, '
                     'apresentado no horário de Fortaleza. É diferente do horário da última promoção '
                     'e do instante em que um contrato foi alterado na origem.'),
    'refresh': ('Atualização automática', 'A página consulta novamente os indicadores a cada 20 segundos. '
                'Isso atualiza a visualização; não inicia uma extração. O scheduler executa as '
                'varreduras no intervalo configurado separadamente.'),
    'covered-ids': ('Espaço de IDs conferido', 'Quantidade de posições de ID cobertas pela união das faixas '
                    'validadas na janela de cobertura. Faixas sobrepostas contam uma única vez. '
                    'Como pode haver IDs sem registro, esse número pode diferir da quantidade de contratos.'),
    'target-id': ('Limite superior observado', 'Maior ID considerado como meta nas faixas registradas. '
                  'A cobertura é calculada no intervalo de 1 até esse limite. Novos IDs podem ampliar '
                  'a meta. Sem um limite conhecido, a cobertura fica “não aferida”.'),
    'intervals': ('Faixas válidas na janela', 'Quantidade de faixas promovidas usadas no cálculo de cobertura. '
                  'Cada faixa é um intervalo de IDs, como 1–1.000. Lotes rejeitados e gerações fora '
                  'da linhagem ativa não entram no cálculo; sobreposições não aumentam a área conferida.'),
    'brokers': ('Brokers', 'Servidores Kafka encontrados pelo monitor. Um broker recebe e armazena mensagens '
                'para os consumidores. Este laboratório usa um único broker; esse número não mede '
                'quantidade de contratos nem disponibilidade da origem.'),
    'partitions': ('Partições', 'Divisões de um tópico Kafka que mantêm a ordem das mensagens dentro '
                   'de cada divisão. O tópico de dados deste laboratório usa uma partição para '
                   'preservar a ordem dos eventos de cada lote.'),
    'members': ('Membros do grupo', 'Consumidores registrados no grupo Kafka. Com uma partição, apenas '
                'um deles recebe essa partição por vez. Durante um reinício ou rebalanceamento, '
                'o grupo pode mostrar temporariamente membros adicionais ou sem trabalho atribuído.'),
    'blue': ('BLUE', 'Geração dos dados mantida na tabela BLUE. Ela pode estar ativa ou em espera, conforme '
             'o indicador “Ativo”. O número identifica os dados validados que esse slot contém; '
             'um traço indica que não há informação disponível.'),
    'green': ('GREEN', 'Geração dos dados mantida na tabela GREEN. Ela pode estar ativa ou em espera. '
              'Nas promoções, BLUE e GREEN alternam os papéis. O slot em espera é alinhado pelo '
              'histórico de mudanças antes de receber o próximo lote.'),
    'active-slot': ('Slot ativo', 'Tabela que está publicada para leitura pela view public.contracts. '
                    'O consumer troca esse estado na mesma transação que publica a geração validada. '
                    'Um traço pode significar que o bootstrap ainda não terminou ou que a leitura falhou.'),
    'insert': ('INSERT', 'Registros inseridos no destino por lotes promovidos nas últimas 24 horas. '
               'A carga inicial também entra aqui: um bootstrap com 10.000 registros pode mostrar '
               '10.000 inserções mesmo sem novas inclusões depois dele.'),
    'update': ('UPDATE', 'Registros existentes cujo conteúdo mudou e foi atualizado no destino por '
               'lotes promovidos nas últimas 24 horas. Uma mudança só é percebida quando sua tabela '
               'ou faixa é examinada. Sem PK, a cópia usa snapshot e não infere a operação individual.'),
    'delete': ('DELETE', 'Registros removidos do destino porque sua chave estava ausente na leitura completa '
               'e validada da tabela ou faixa. Só conta exclusões promovidas nas últimas 24 horas. '
               'Limites de segurança podem rejeitar um lote com remoções excessivas.'),
    'rejected': ('Lotes rejeitados', 'Lotes que não passaram pelas validações nas últimas 24 horas. '
                 'As causas incluem contagem ou hash divergente, geração base incompatível e excesso '
                 'de exclusões. A rejeição preserva a geração publicada; veja a coluna “Erro”.'),
    'json-bytes': ('Bytes JSON publicados', 'Volume lógico de eventos JSON contabilizado nas execuções '
                   'promovidas nas últimas 24 horas, antes da compressão. Inclui eventos de presença. '
                   'Não corresponde ao espaço ocupado no Kafka nem ao tráfego real de rede.'),
}


def _help(key, instance=None):
    label, explanation = HELP[key]
    tip_id = 'help-' + (instance or key)
    return (f'<span class="help"><button class="help-button" type="button" '
            f'aria-label="{_safe("Entenda: " + label)}" aria-describedby="{_safe(tip_id)}">'
            '<span aria-hidden="true">?</span></button>'
            f'<span class="help-text" id="{_safe(tip_id)}" role="tooltip">'
            f'{_safe(explanation)}</span></span>')


def _summary_card(title, value, desc, help_key):
    return (f'<div class="card has-help">{_help(help_key, "summary-" + help_key)}'
            f'<label>{_safe(title)}</label><strong>{_safe(value)}</strong>'
            f'<small>{_safe(desc)}</small></div>')


def _fact(title, value, help_key):
    return (f'<div class="has-help">{_help(help_key, "fact-" + help_key)}'
            f'<small>{_safe(title)}</small><b>{_safe(value)}</b></div>')


def _pipeline_node(title, help_key, transport=False):
    classes = 'pipeline-node has-help' + (' transport' if transport else '')
    return (f'<span class="{classes}">{_safe(title)}'
            f'{_help(help_key, "pipeline-" + help_key)}</span>')


def _stage(title, value, status, desc, link, help_key):
    classes={'good':'good','warning':'warn','bad':'bad','unknown':'unknown'}
    return (f'<div class="stage has-help {classes.get(status,"unknown")}">'
            f'{_help(help_key, "stage-" + help_key)}'
            f'<a href="#{_safe(link)}" class="stage-link">'
            f'<span class="stage-name">{_safe(title)}</span>'
            f'<b>{_safe(value)}</b><small>{_safe(desc)}</small></a></div>')


PAGE_SIZE = 10
PAGE_KEYS = ('tables_page','runs_page','schema_page','ranges_page')


def page_numbers(query):
    result={}
    for key in PAGE_KEYS:
        raw=query.get(key,['1'])[0]
        try:
            result[key]=max(1,min(1000000,int(raw))) if len(raw)<=7 else 1
        except (TypeError,ValueError):
            result[key]=1
    return result


def table_page(rows, key, anchor, pages, label):
    total=len(rows)
    last=max(1,(total+PAGE_SIZE-1)//PAGE_SIZE)
    current=min(pages.get(key,1),last)
    start=(current-1)*PAGE_SIZE
    if not total:
        return [],''
    def link(number,text):
        query={k:v for k,v in pages.items() if k in PAGE_KEYS and v>1}
        query[key]=number
        href='/?'+urlencode(query)+'#'+anchor
        return f'<a href="{_safe(href)}">{text}</a>'
    controls=[]
    if current>1:
        controls.extend((link(1,'Primeira'),link(current-1,'Anterior')))
    controls.append(f'<span aria-current="page">Página {current} de {last}</span>')
    if current<last:
        controls.extend((link(current+1,'Próxima'),link(last,'Última')))
    nav=(f'<nav class="pagination" aria-label="{_safe(label)}" '
         f'data-page-key="{_safe(key)}" data-page-target="{_safe(anchor)}" data-page-current="{current}">'
         f'<span>{start+1}–{min(start+PAGE_SIZE,total)} de {total} registros</span>'
         '<div>'+''.join(controls)+'</div></nav>')
    return rows[start:start+PAGE_SIZE],nav


def _date_time(value):
    if not value:
        return '—'
    stamp=datetime.fromisoformat(value) if isinstance(value,str) else value
    if stamp.tzinfo is None:
        stamp=stamp.replace(tzinfo=timezone.utc)
    label=stamp.astimezone(timezone(timedelta(hours=-3))).strftime('%d/%m/%Y às %H:%M:%S')
    return f'<time datetime="{_safe(stamp.isoformat())}" title="Horário de Fortaleza (UTC−03)">{label}</time>'


def _transport_activity(kafka):
    group=kafka.get('group') or {}
    if not kafka.get('ok'):
        return 'Kafka indisponível','O monitor não conseguiu acessar o broker ou o tópico de dados. Consulte os alertas.','bad'
    if 'rebalanc' in (group.get('state') or '').lower():
        return 'Reorganizando consumidores','O Kafka está redistribuindo a leitura entre consumers. A confirmação de mensagens pode pausar durante essa transição.','warning'
    if group.get('members')==0:
        return 'Sem consumer ativo','Não há processo no grupo para ler novas mensagens. Verifique o serviço consumer.','bad'
    lag=group.get('lag')
    if lag is None or group.get('members') is None:
        return 'Consumo não aferido','Ainda não há informação suficiente sobre o grupo e seus offsets para confirmar o andamento da fila. Consulte os detalhes técnicos.','unknown'
    if lag==0:
        return 'Fila em dia','Todas as mensagens até os offsets consultados foram confirmadas pelo consumer após a gravação no destino.','good'
    pending=f'{lag:,}'.replace(',','.')
    return 'Aguardando confirmação',f'{pending} mensagens ainda não foram confirmadas. O consumer confirma a leitura após gravar no destino; mensagens incluem dados, presença e controle.',('warning' if lag>MAX_LAG else 'unknown')


def render_html(s, pages=None):
    pages=page_numbers({k:[str(v)] for k,v in (pages or {}).items()})
    a=s.get('active') or {}; k=s.get('kafka') or {}; group=k.get('group') or {}
    m=s.get('metrics') or {}; c=s.get('coverage') or {}; schema=s.get('schema') or {}
    catalog=s.get('catalog') or []; runs=s.get('recent_runs') or []
    last=s.get('last_transition') or {}
    automatic=s.get('mode')=='catalog'
    page_runs,runs_nav=table_page(runs,'runs_page','runs',pages,'Paginação das execuções recentes')
    page_catalog,schema_nav=table_page(catalog,'schema_page','schema',pages,'Paginação dos schemas observados')
    page_tables,tables_nav=table_page(s.get('tables',[]),'tables_page','tables',pages,'Paginação das tabelas e réplicas')
    page_ranges,ranges_nav=table_page(s.get('last_ranges') or [],'ranges_page','ranges',pages,'Paginação das faixas promovidas')
    state_label={'operational':'Serviços prontos · leituras por tabela' if automatic else 'Serviços prontos · integridade por faixas',
                 'attention':'Atenção operacional','not_initialized':'Bootstrap pendente',
                 'unavailable':'Falha operacional'}[s['overall']]
    runtime='good' if s['local_db']['ok'] else 'bad'
    db_label=('Acessível' if s['local_db']['ok'] else
              'Erro de leitura' if s['local_db'].get('connected') else 'Indisponível')
    kafka_state='good' if k.get('ok') else 'bad'
    if group.get('lag') is None: group_state='unknown';lag='Não medido'
    else:
        group_state='warning' if group['lag']>MAX_LAG or group.get('members')==0 else 'good'
        lag=f"{group['lag']:,}".replace(',','.')
    source_state='unknown' if not runs else ('warning' if runs[0]['status'] in ('failed','rejected','aborted') else 'unknown')
    cover_txt=f"{c['ratio']:.2f}%" if c.get('ratio') is not None else 'Não aferida'
    cover_state='good' if c.get('status')=='complete' else 'unknown'
    sc_state=('warning' if s.get('table_error_count') else 'good') if automatic else ('warning' if schema.get('pending_hash') else ('good' if schema.get('active_hash') else 'unknown'))
    latest=runs[0] if runs else {}
    source_label,source_desc,source_level={
        'promoted':('Último lote publicado','A leitura foi concluída e os dados desse lote foram validados e publicados no destino.','good'),
        'extracting':('Leitura ainda sem conclusão','O extrator registrou uma leitura da origem. Confira o horário e as evidências se ela demorar a concluir.','unknown'),
        'loading':('Lote ainda sem conclusão','Há um lote aguardando conclusão da leitura ou validação pelo consumer. Confira o horário e as evidências.','unknown'),
        'failed':('Última tentativa falhou','A leitura ou o envio desse lote falhou. Consulte o erro e as evidências antes de tentar novamente.','bad'),
        'rejected':('Último lote rejeitado','O consumer recusou a publicação desse lote; os dados publicados anteriormente foram preservados.','warning'),
        'aborted':('Último lote interrompido','A execução terminou sem publicar esse lote. Consulte o erro e as evidências.','warning'),
    }.get(latest.get('status'),('Nenhuma leitura registrada','Aguarde a inicialização ou consulte os alertas e o scheduler.','unknown'))
    source_error=latest.get('error')
    source_at=latest.get('finished_at') or latest.get('started_at')
    source_when='Concluído em' if latest.get('finished_at') else 'Iniciado em'
    cycle=s.get('latest_cycle') or {}
    if cycle.get('status')=='failed':
        source_label,source_desc,source_level='Última varredura falhou','O ciclo não foi concluído. Consulte o erro do ciclo e os alertas.','bad'
        source_error=cycle.get('error')
        source_at=cycle.get('finished_at') or cycle.get('started_at')
        source_when='Ciclo registrado em'
    if not s['local_db']['ok']:
        source_label,source_desc,source_level='Histórico indisponível','O monitor não conseguiu consultar as execuções locais. Verifique o alerta do PostgreSQL local.','unknown'
    engine={'postgresql':'PostgreSQL','sqlserver':'SQL Server'}.get(latest.get('engine'),latest.get('engine') or 'Origem não informada')
    source_context=(f'<p>{_safe(engine)}'+(' · '+details.table_link({'id':latest['id']},latest['object']) if latest.get('object') else '')+'</p>') if latest else ''
    source_evidence=('<p>'+details.modal_link('/run/'+latest['id'],'Ver evidências do lote '+latest['id'][:8])+'</p>') if latest else ''
    source_time=(f'<p class="subtle">{source_when}: {_date_time(source_at)}</p>') if source_at else ''
    transport_label,transport_desc,transport_level=_transport_activity(k)
    transport_error=k.get('error') or k.get('group_error')
    technical_open=' open' if (transport_level=='bad' or transport_error or
        (group.get('lag') is not None and group['lag']>MAX_LAG) or not k.get('audit_topic')) else ''
    row_html=''.join(f'''<tr><td>{details.modal_link('/run/'+r['id'],r['id'][:8],code=True)}</td>
        <td class="run-date">{_date_time(r.get('started_at'))}</td>
        <td>{(details.table_link({'id':r['id']},r['object'])+' · ') if r.get('object') else ''}{_safe(r['engine'])}</td><td>{_safe(r['mode'])}</td>
        <td>{_safe(str(r['low'])+'–'+str(r['high']) if r['low'] is not None else 'tabela inteira' if automatic else 'bootstrap')}</td>
        <td><span class="tag">{_safe(r['status'])}</span></td>
        <td>{_safe(r['delta'])}</td><td>{_safe(r['deleted'])}</td>
        <td>{_safe(r['duration_seconds'])} s</td><td title="{_safe(r['error'])}">{_safe((r['error'] or '')[:105])}</td></tr>'''
        for r in page_runs)
    alert_html=''.join(f'''<div class="alert {('critical' if x['severity']=='critical' else 'warning')}">
         <b>{_safe(x['code'])}</b><span>{_safe(x['message'])}</span>
         <small>{_safe(x['action'])}</small></div>''' for x in s['alarms'])
    ranges=''.join(f'''<tr><td>{details.modal_link('/run/'+x['run_id'],x['run_id'][:8])}</td>
        <td>{_safe(x['engine'])}</td><td>{_safe(x['low'])}–{_safe(x['high'])}</td>
        <td>{_safe(x['target_max_id'])}</td><td>{_date_time(x['at'])}</td></tr>'''
        for x in page_ranges)
    catalogs=''.join(f'<tr><td>{details.table_link(x,x["source"])}</td><td>{_safe(x["engine"])}</td>'
        f'<td>{_safe(x["classification"])}</td><td>{_date_time(x["observed_at"])}</td></tr>'
        for x in page_catalog)
    gaps=', '.join(f'{lo}–{hi}' for lo,hi in c.get('gaps',[])) or '—'
    stages=''.join([
        _stage('Origem remota','Não sondada',source_state,'Evidência indireta das extrações','source','source'),
        _stage('Extração / tabelas' if automatic else 'Extração / faixas',f"{m.get('promotions_24h',0)} promoções/24h",
               'warning' if m.get('stalled') else ('good' if a.get('version') else 'unknown'),
               'READ ONLY · fingerprints · deletes','extract','extractor'),
        _stage('Kafka', 'Acessível' if k.get('ok') else 'Indisponível',kafka_state,
               f"{_safe(k.get('partitions'))} partições de dados",'kafka','kafka'),
        _stage('Consumer',f'Lag: {lag}',group_state,
               f"{_safe(group.get('members'))} membros no grupo",'kafka','consumer'),
        _stage('PostgreSQL local',db_label,runtime,
               'Auditoria SQL persistida','destination','local-db'),
        _stage('Blue-green',f"{a.get('published_tables',0)} tabelas publicadas" if automatic else f"{_safe(a.get('slot'))} · v{_safe(a.get('version'))}",
               'good' if a.get('version') else 'unknown','Promoção transacional','destination','blue-green'),
        _stage('DDL / schema',f"{len(s.get('tables',[]))} tabelas observadas" if automatic else 'Pendente' if schema.get('pending_hash') else 'Sem migração pendente',sc_state,
               'Histórico de contrato','schema','catalog-schema' if automatic else 'schema'),
        _stage('Cobertura',cover_txt,cover_state,'Tabelas conferidas na janela' if automatic else 'Faixas de PK na janela móvel',
               'coverage','catalog-coverage' if automatic else 'coverage')])
    age=m.get('last_success_age_seconds')
    rows=a.get('rows')
    summary=''.join([
        _summary_card('Última promoção',f'{age} s' if age is not None else '— s',
                      'Idade da promoção, não da origem','last-promotion'),
        _summary_card('Tabelas publicadas' if automatic else 'Versão ativa',a.get('published_tables',0) if automatic else a.get('version'),
                      f"{rows if rows is not None else '—'} linhas · publicação por tabela" if automatic else f"Slot {a.get('slot') or '—'} · {rows if rows is not None else '—'} linhas",
                      'published-tables' if automatic else 'generation'),
        _summary_card('Lag Kafka',group.get('lag'),'Offsets pendentes; não equivale a linhas','lag'),
        _summary_card('Cobertura de tabelas' if automatic else 'Cobertura de PK',cover_txt,
                      f'Janela de {COVERAGE_HOURS} h · leituras completas' if automatic else f'Janela de {COVERAGE_HOURS} h · não é cobertura de bytes',
                      'catalog-coverage' if automatic else 'coverage'),
        _summary_card('Falhas / rejeições (24h)',m.get('failures_24h'),
                      f"{m.get('stalled') if m.get('stalled') is not None else '—'} lote(s) travado(s)",'failures'),
        _summary_card('Alterações promovidas (24h)',
                      (m.get('inserted_24h') or 0)+(m.get('updated_24h') or 0)+(m.get('deleted_24h') or 0),
                      'INSERT + UPDATE + DELETE','changes')])
    table_rows=''.join(f'''<tr><td>{_safe(t['engine'])} · {details.table_link(t,t['schema']+'.'+t['table'])}</td>
        <td><code>{_safe(t['destination_schema'])}.{_safe(t['destination_table'])}</code></td>
        <td>{_safe(t['status'])}</td><td>{_safe(t['rows'])}</td><td>{_safe(t['generation'])} · {_safe(t['slot'])}</td>
        <td><code>{_safe((t['schema_hash'] or '')[:12])}</code></td><td>{_date_time(t['checked_at'])}</td>
        <td>{_safe(t['error'])}</td></tr>''' for t in page_tables)
    table_section=('''<section class="section" id="tables"><h2>Catálogo de tabelas e réplicas</h2>
        <p>Descoberta por SELECT · leituras consistentes por tabela · views em mirror_&lt;schema&gt;.
        Objetos ausentes em dois catálogos completos são despublicados; os dados internos são preservados.
        Tabelas visíveis sem SELECT mantêm a última publicação e exibem alerta de permissão.</p>
        <div class="data"><table><thead><tr><th>Origem</th><th>Destino</th><th>Estado</th><th>Linhas</th>
        <th>Geração / slot</th><th>Schema</th><th>Conferida em</th><th>Erro</th></tr></thead><tbody>'''+
        (table_rows or '<tr><td colspan="8">Nenhuma tabela descoberta</td></tr>')+'</tbody></table></div>'+tables_nav+'</section>') if automatic else ''
    if automatic:
        destination_facts=(_fact('Tabelas em BLUE',a.get('blue_tables',0),'blue-tables')+
                           _fact('Tabelas em GREEN',a.get('green_tables',0),'green-tables')+
                           _fact('Publicação','Por tabela','table-publication'))
    else:
        destination_facts=(_fact('BLUE v',a.get('blue_version'),'blue')+
                           _fact('GREEN v',a.get('green_version'),'green')+
                           _fact('Ativo',a.get('slot'),'active-slot'))
    ranges_section=('''<section class="section" id="ranges"><h2>Faixas efetivamente promovidas</h2><div class="data"><table>
        <thead><tr><th>Lote</th><th>Motor</th><th>Intervalo</th><th>Limite observado</th><th>Conferida em</th></tr></thead>
        <tbody>'''+(ranges or '<tr><td colspan="5">Nenhuma faixa promovida registrada</td></tr>')+
        '</tbody></table></div>'+ranges_nav+'</section>') if not automatic else ''
    return f'''<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<noscript><meta http-equiv="refresh" content="20"></noscript><title>Reconciliação · Saúde ponta a ponta</title>
<script src="/monitor.js" defer></script>
<style>
:root{{--bg:#f5f7fa;--paper:#fff;--ink:#182638;--muted:#657589;--line:#e1e6ed;--link:#215ca6}}
*{{box-sizing:border-box}}html{{scroll-behavior:smooth}}body{{margin:0;background:var(--bg);font:14px system-ui,Arial,sans-serif;color:var(--ink)}}
a{{color:var(--link)}}main{{max-width:1380px;margin:auto;padding:32px 24px 50px}}
header{{display:flex;align-items:start;justify-content:space-between;gap:20px;flex-wrap:wrap}}
.header-main{{flex:1 1 700px;min-width:0}}
h1{{font-size:27px;margin:3px 0 7px}}h2{{font-size:17px;margin:0 0 13px}}p{{color:var(--muted);line-height:1.55;margin:7px 0}}
.top{{color:var(--muted);font-size:12px;font-weight:700;letter-spacing:.08em}}
.pipeline{{display:flex;align-items:center;flex-wrap:wrap;gap:7px;list-style:none;padding:0;margin:14px 0}}
.pipeline li{{display:flex;align-items:center;gap:7px;font-size:12px}}
.pipeline-node{{border:1px solid var(--line);border-radius:8px;background:var(--paper);padding:6px 9px;white-space:nowrap}}
.pipeline-node.transport{{background:#edf2f8;border-color:#cfdaea;color:var(--link);font-weight:600}}
.pipeline-arrow{{color:#8b9bad;font-size:14px}}
.header-meta{{display:flex;align-items:center;flex-wrap:wrap;gap:8px 20px;color:var(--muted);font-size:12px;line-height:1.6}}
.reading{{display:flex;align-items:baseline;flex-wrap:wrap;gap:4px 8px}}
.reading time{{color:var(--ink);font-weight:600;font-variant-numeric:tabular-nums}}
.reading small{{font-size:11px;color:var(--muted)}}
.refresh{{display:inline-flex;align-items:center;gap:7px}}
.refresh-dot{{width:6px;height:6px;border-radius:50%;background:var(--link)}}
.state{{border:1px solid var(--line);background:#fff;padding:11px 15px;border-radius:24px;font-weight:700}}
.has-help{{position:relative}}
.help{{display:inline-flex;vertical-align:middle;margin-left:6px}}
.help-button{{display:inline-block;width:18px;height:18px;padding:0;border:1px solid #bcc9d8;border-radius:50%;background:var(--paper);color:var(--link);font:700 11px/16px system-ui;cursor:help}}
.help-button:focus-visible,.stage-link:focus-visible{{outline:2px solid var(--link);outline-offset:3px}}
.help-text{{position:absolute;top:calc(100% + 8px);right:0;z-index:40;width:300px;max-width:calc(100vw - 32px);padding:12px 14px;border-radius:10px;background:var(--ink);color:#fff;font:400 12px/1.6 system-ui;text-align:left;white-space:normal;box-shadow:0 8px 24px #18263826;visibility:hidden;opacity:0;pointer-events:none}}
.help-text::before{{content:"";position:absolute;bottom:100%;left:0;right:0;height:10px}}
.has-help:hover>.help>.help-text,.has-help:focus-within>.help>.help-text{{visibility:visible;opacity:1;pointer-events:auto}}
.card>.help,.stage>.help,.facts .has-help>.help{{display:contents}}
.card>.help>.help-button,.stage>.help>.help-button{{position:absolute;top:12px;right:12px}}
.facts .has-help{{padding-right:24px}}
.facts .has-help>.help>.help-button{{position:absolute;top:0;right:0}}
@media(min-width:901px){{.summary .card:first-child .help-text,.pipeline li:first-child .help-text,.facts .has-help:first-child .help-text,.stages .stage:nth-child(4n+1) .help-text,.columns .section:first-child .facts .help-text{{left:0;right:auto}}}}
.summary{{display:grid;grid-template-columns:repeat(auto-fit,minmax(175px,1fr));gap:12px;margin:21px 0}}
.card,.section{{border:1px solid var(--line);background:var(--paper);border-radius:13px;padding:18px}}
.card label{{color:var(--muted);display:block;font-size:12px;padding-right:22px}}.card strong{{display:block;font-size:25px;margin:7px 0}}
.card small{{color:var(--muted)}}.stages{{display:grid;grid-template-columns:repeat(4,1fr);gap:9px;margin:18px 0}}
.stage{{border:1px solid var(--line);border-left:4px solid #a7b4c6;border-radius:10px;background:white;color:var(--ink);min-height:113px}}
.stage-link{{display:block;padding:14px;min-height:113px;color:inherit;text-decoration:none;border-radius:6px}}
.stage-name{{display:block;font-size:12px;color:var(--muted);padding-right:22px}}.stage b{{display:block;margin:9px 0 5px;font-size:16px;overflow-wrap:anywhere}}
.stage small{{display:block;color:var(--muted)}}.stage.good{{border-left-color:#26976a}}.stage.warn{{border-left-color:#ce9026}}.stage.bad{{border-left-color:#cd4451}}
.section{{margin:15px 0;scroll-margin-top:15px}}.columns{{display:grid;grid-template-columns:1fr 1fr;gap:15px}}
.data{{overflow:auto}}table{{width:100%;border-collapse:collapse;min-width:610px;text-align:left}}
.pagination{{display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;margin-top:14px;color:var(--muted);font-size:12px}}
.pagination div{{display:flex;gap:7px;align-items:center;flex-wrap:wrap}}.pagination a,.pagination [aria-current]{{padding:6px 9px;border:1px solid var(--line);border-radius:7px;text-decoration:none;background:var(--paper)}}
.pagination [aria-current]{{background:#edf2f8;color:var(--ink)}}.pagination a:focus-visible{{outline:2px solid var(--link);outline-offset:2px}}
th,td{{border-bottom:1px solid var(--line);padding:11px 9px;vertical-align:top;font-size:12px}}th{{color:var(--muted);font-weight:600}}
.run-date{{white-space:nowrap;font-variant-numeric:tabular-nums}}
.activity-state{{font-size:16px;font-weight:700;line-height:1.5;margin:8px 0}}
.activity-state.good{{color:#207653}}.activity-state.warning{{color:#946318}}.activity-state.bad{{color:#b63743}}.activity-state.unknown{{color:var(--muted)}}
.technical{{border-top:1px solid var(--line);padding-top:12px;margin-top:14px}}
.technical summary{{cursor:pointer;color:var(--link);font-size:12px;font-weight:600}}
.technical summary:focus-visible{{outline:2px solid var(--link);outline-offset:3px}}
.technical .facts{{margin:16px 0}}.technical[open] summary{{margin-bottom:10px}}
.details-modal{{width:min(980px,calc(100vw - 32px));max-height:calc(100dvh - 48px);padding:0;border:1px solid var(--line);border-radius:13px;color:var(--ink);background:var(--paper);box-shadow:0 20px 80px #18263840}}
.details-modal::backdrop{{background:#18263899}}.modal-header{{display:flex;justify-content:space-between;align-items:center;gap:16px;padding:18px 22px;border-bottom:1px solid var(--line)}}
.modal-header h2{{margin:0;overflow-wrap:anywhere}}.modal-actions{{display:flex;gap:8px;flex-shrink:0}}
.modal-actions button{{font:600 12px system-ui;padding:7px 12px;border:1px solid var(--line);border-radius:7px;background:var(--paper);color:var(--link);cursor:pointer}}
.modal-actions button:focus-visible,a[data-modal]:focus-visible{{outline:2px solid var(--link);outline-offset:3px}}
.modal-content{{padding:6px 22px 22px;overflow:auto;max-height:calc(100dvh - 140px)}}
{details.DETAIL_CSS}
.tag{{border-radius:8px;background:#edf2f8;padding:4px 7px}}.alert{{border:1px solid #edcf98;border-radius:9px;padding:10px 12px;margin:8px 0;background:#fffaf0}}
.alert.critical{{border-color:#e9a1a5;background:#fff3f4}}.alert span{{display:block;margin:5px 0}}.alert small{{color:var(--muted)}}
code{{font:12px ui-monospace,monospace}}.meter{{height:10px;background:#e7edf3;border-radius:20px;margin:14px 0;overflow:hidden}}
.meter>div{{height:100%;background:#3e92a5}}.facts{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:14px}}.facts div{{font-size:13px}}
.facts b{{display:block;font-size:17px}}.facts small{{color:var(--muted)}}.subtle{{font-size:12px;color:var(--muted)}}
footer{{font-size:12px;color:var(--muted);line-height:1.7;padding:18px 0}}footer a{{margin-right:18px}}
@media(max-width:900px){{.stages{{grid-template-columns:repeat(2,1fr)}}.columns{{grid-template-columns:1fr}}}}
@media(max-width:900px){{.has-help>.help>.help-text{{position:fixed;top:auto;bottom:16px;left:16px;right:16px;width:auto;max-width:none;max-height:45vh;overflow:auto}}}}
@media(max-width:520px){{main{{padding:19px 12px}}.stages{{grid-template-columns:1fr 1fr}}.stage{{min-height:130px}}}}
</style></head><body><main>
<header><div class="header-main"><div class="top">KAFKA RESTORE LAB / OPERAÇÃO</div>
<h1>Saúde da reconciliação de dados</h1>
<ol class="pipeline" aria-label="Fluxo de reconciliação">
<li>{_pipeline_node('PostgreSQL ou SQL Server','source')}</li>
<li><span class="pipeline-arrow" aria-hidden="true">→</span>{_pipeline_node('Python','extractor')}</li>
<li><span class="pipeline-arrow" aria-hidden="true">→</span>{_pipeline_node('Kafka','kafka',transport=True)}</li>
<li><span class="pipeline-arrow" aria-hidden="true">→</span>{_pipeline_node('Consumer','consumer')}</li>
<li><span class="pipeline-arrow" aria-hidden="true">→</span>{_pipeline_node('PostgreSQL blue-green','blue-green')}</li>
</ol>
<div class="header-meta">
<span class="reading has-help">Última leitura {_date_time(s['checked_at'])}<small>UTC−03 · Fortaleza</small>{_help('last-reading')}</span>
<span class="refresh has-help"><span class="refresh-dot" aria-hidden="true"></span>Atualização automática · a cada 20 segundos{_help('refresh')}</span>
</div></div>
<div class="state has-help">{_safe(state_label)}{_help('overall')}</div></header>
<div class="summary">{summary}</div>
<h2>Fluxo ponta a ponta</h2><div class="stages">{stages}</div>
<div class="columns"><section class="section" id="alerts"><h2>Alertas e próximos passos</h2>
{alert_html or '<p>Sem alertas detectados pelos sinais disponíveis. Isso não certifica igualdade entre as bases.</p>'}
</section><section class="section" id="coverage"><h2>Cobertura da reconciliação</h2>
<div class="facts">{_fact('Tabelas conferidas' if automatic else 'Espaço de IDs conferido',c.get('covered_ids'),'checked-tables' if automatic else 'covered-ids')}
{_fact('Tabelas visíveis' if automatic else 'Limite superior observado',c.get('target_max_id'),'visible-tables' if automatic else 'target-id')}
{_fact('Leituras validadas' if automatic else 'Faixas válidas na janela',c.get('interval_count'),'validated-scans' if automatic else 'intervals')}</div>
<div class="meter"><div style="width:{max(0,min(100,c.get('ratio') or 0))}%"></div></div>
<p>Estado: <b>{_safe(c.get('status'))}</b> · Primeiras lacunas: {_safe(gaps)}.</p>
<p class="subtle">{'Proporção de tabelas com leitura completa validada na janela. Cada tabela usa sua própria transação; 100% não garante igualdade simultânea de toda a origem.' if automatic else 'Usamos união de faixas promovidas pertencentes à linhagem ativa. Isso não mede quantidade de contratos, volume trafegado ou consistência transacional de todo o remoto. Sem meta de cobertura, não há SLO de 100%.'}</p></section></div>
<div class="columns"><section class="section" id="source"><h2>Última atividade da extração</h2>
<div class="activity-state {source_level} has-help">{_safe(source_label)}{_help('source','activity-source')}</div>
<p>{_safe(source_desc)}</p>
{source_context}{source_time}{source_evidence}
{('<p class="alert critical">'+_safe(source_error)+'</p>') if source_error else ''}
</section><section class="section" id="kafka"><h2>Entrega das mensagens</h2>
<div class="activity-state {transport_level} has-help">{_safe(transport_label)}{_help('consumer','activity-transport')}</div>
<p>{_safe(transport_desc)}</p>
{('<p class="alert critical">'+_safe(transport_error)+'</p>') if transport_error else ''}
<details class="technical"{technical_open}><summary>Detalhes técnicos do Kafka</summary>
<div class="facts">{_fact('Servidores Kafka',k.get('brokers'),'brokers')}
{_fact('Partições',k.get('partitions'),'partitions')}
{_fact('Consumers no grupo',group.get('members'),'members')}</div>
<p class="subtle">Tópico de dados: {'acessível' if k.get('data_topic') else 'indisponível'} · Auditoria Kafka: {'acessível' if k.get('audit_topic') else 'não encontrada'}</p>
<p class="subtle">Grupo: <code>{_safe(group.get('name') or GROUP)}</code> · Estado interno: <code>{_safe(group.get('state'))}</code></p>
</details>
</section></div>
<div class="columns"><section class="section" id="destination"><h2>Destino e blue-green</h2>
{('<p class="alert critical">' + _safe(s['local_db'].get('error')) + '</p>') if s['local_db'].get('error') else ''}
<div class="facts">{destination_facts}</div>
<p>Última transição: {_safe(last.get('action'))} · {_date_time(last.get('at'))} · Versão {_safe(last.get('version'))}</p>
<p class="subtle">A versão ativa é transacional; rollback exige compatibilidade do schema e journal disponível.</p>
</section><section class="section" id="schema"><h2>DDL e catálogo de schemas</h2>
<p>{'Contratos independentes por tabela; consulte os hashes e os destinos no catálogo.' if automatic else 'Contrato ativo: <code>'+_safe((schema.get('active_hash') or '')[:20])+'</code>'}</p>
{('<p>Schema alterado exige nova cópia completa e validação antes da publicação.</p>') if automatic else '<p>Pendente: <code>'+_safe((schema.get('pending_hash') or '')[:20] if schema.get('pending_hash') else 'nenhum')+'</code> · Backfill próximo ID: '+_safe(schema.get('backfill_next'))+'</p>'}
<div class="data"><table><thead><tr><th>Fonte</th><th>Motor</th><th>Última classificação</th><th>Observado</th></tr></thead><tbody>{catalogs or '<tr><td colspan="4">Sem observações registradas</td></tr>'}</tbody></table></div>{schema_nav}
</section></div>
<section class="section" id="extract"><h2>Resumo de alterações e guardas (24h)</h2>
<div class="facts">{_fact('INSERT',m.get('inserted_24h'),'insert')}
{_fact('UPDATE',m.get('updated_24h'),'update')}
{_fact('DELETE',m.get('deleted_24h'),'delete')}
{_fact('Lotes rejeitados',m.get('rejected_24h'),'rejected')}
{_fact('Bytes JSON publicados',m.get('kafka_bytes_24h'),'json-bytes')}</div>
<p>{'Deletes são inferidos pela ausência da chave na leitura completa validada. Sem PK, publicamos snapshots e não atribuímos contagens individuais a UPDATE e DELETE. Rejeições preservam a última publicação.' if automatic else 'Deletes são inferidos somente na faixa validada; rejeições não geram checkpoint de cobertura nem promoção.'}</p>
</section>
{table_section}
<section class="section" id="runs"><h2>Últimas execuções · clique no ID para evidências</h2>
<div class="data"><table><thead><tr><th>ID</th><th>Início · Fortaleza</th><th>Origem</th><th>Modo</th><th>Faixa</th><th>Estado</th><th>Deltas</th><th>Deletes</th><th>Duração</th><th>Erro</th></tr></thead><tbody>
{row_html or '<tr><td colspan="10">Nenhuma execução registrada</td></tr>'}</tbody></table></div>{runs_nav}</section>
{ranges_section}
<footer><a href="http://localhost:8080/">Kafbat UI</a><a href="http://localhost:8081/">Adminer</a>
<a href="/status.json">JSON de saúde</a><a href="/readyz">Readiness</a>
<p>Indicadores operacionais calculados sobre evidências locais e metadados Kafka. Sem sonda ativa à origem,
sem instrumentação extra, sem alterações de dados pelo painel e sem promessa de igualdade global entre as bases.</p></footer>
</main>
<dialog id="details-modal" class="details-modal" aria-labelledby="modal-title">
<div class="modal-header"><h2 id="modal-title">Detalhes</h2><div class="modal-actions">
<button id="modal-back" type="button" hidden>Voltar</button>
<button id="modal-close" type="button" autofocus>Fechar</button></div></div>
<div id="modal-content" class="modal-content" aria-busy="false" tabindex="0"></div>
</dialog></body></html>'''


def run_details(rid, connect=psycopg.connect):
    rid=str(UUID(rid))
    with connect(LOCAL_DSN,connect_timeout=3) as conn:
        with conn.cursor() as cur:
            if SYNC_MODE=='catalog':
                cur.execute('''SELECT r.run_id::text,t.engine,t.source_schema,t.source_table,
                    r.mode,r.status,r.manifest,r.base_generation,r.result_generation,
                    r.full_snapshot,r.delta_count,r.seen_count,r.inserted_count,r.updated_count,
                    r.deleted_count,r.logical_bytes,r.error,r.started_at,r.finished_at
                    FROM replica.discovered_runs r JOIN replica.discovered_tables t USING(table_key)
                    WHERE r.run_id=%s''',(rid,))
                row=cur.fetchone()
                if not row:return None
                keys=('run_id','engine','schema','table','mode','status','manifest','base_generation',
                      'result_generation','full_snapshot','delta_count','seen_count','inserted_count',
                      'updated_count','deleted_count','logical_bytes','error','started_at','finished_at')
                return {key:_iso(value) for key,value in zip(keys,row)}
            cur.execute('''SELECT run_id::text,mode,status,sweep_low,sweep_high,cutoff,
                                 data_schema_hash,delta_count,sweep_count,inserted_count,
                                 updated_count,deleted_count,logical_kafka_bytes,error,
                                 started_at,finished_at,result_version,source_engine,
                                 sweep_target_max_id
                          FROM replica.sync_runs WHERE run_id=%s''',(rid,))
            v=cur.fetchone()
            if not v:return None
            keys=('run_id','mode','status','sweep_low','sweep_high','source_cutoff',
                  'data_schema_hash','delta_count','sweep_count','inserted_count',
                  'updated_count','deleted_count','logical_kafka_bytes','error',
                  'started_at','finished_at','result_version','source_engine',
                  'sweep_target_max_id')
            return {key:_iso(value) for key,value in zip(keys,v)}


def ddl_details(query, connect=psycopg.connect):
    selectors=[key for key in ('table_key','source_key','run_id') if key in query]
    if len(selectors)!=1 or len(query[selectors[0]])!=1:
        raise ValueError('Specify one table or execution')
    key=selectors[0]
    value=query[key][0]
    if key=='table_key' and not re.fullmatch('[0-9a-f]{64}',value):
        raise ValueError('Invalid table key')
    if key=='run_id':
        value=str(UUID(value))
    if key=='source_key' and (not value or len(value)>200 or '\x00' in value):
        raise ValueError('Invalid source key')
    with connect(LOCAL_DSN,connect_timeout=3) as conn:
        with conn.cursor() as cur:
            if key=='table_key':
                cur.execute('''SELECT observed_manifest,observed_at,status FROM replica.discovered_tables
                               WHERE table_key=%s''',(value,))
            elif key=='source_key':
                cur.execute('''SELECT native_manifest,observed_at,classification
                               FROM replica.source_catalog_observations WHERE source_key=%s
                               ORDER BY observation_id DESC LIMIT 1''',(value,))
            elif SYNC_MODE=='catalog':
                cur.execute('''SELECT manifest,started_at,status FROM replica.discovered_runs
                               WHERE run_id=%s''',(value,))
            else:
                cur.execute('''SELECT v.manifest,r.started_at,r.status FROM replica.sync_runs r
                               JOIN replica.schema_versions v ON v.schema_hash=r.data_schema_hash
                               WHERE r.run_id=%s AND r.source_engine='postgresql' ''',(value,))
            row=cur.fetchone()
            if not row or not row[0] or not row[0].get('columns'):
                return None
            record={'manifest':row[0],'observed_at':_iso(row[1]),'status':row[2]}
            if key=='run_id':
                record['run_id']=value
            return record


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path=urlparse(self.path).path
        if not (path in ('/','/status.json','/healthz','/readyz','/ddl','/monitor.js') or path.startswith('/run/')):
            self.send_error(404);return
        try:
            try:
                query=parse_qs(urlparse(self.path).query,max_num_fields=20)
            except ValueError:
                self.send_error(400,'Invalid query');return
            if path=='/monitor.js':
                self._send(200,'text/javascript; charset=utf-8',Path(__file__).with_name('monitor_ui.js').read_text());return
            if path=='/healthz':
                self._send(200,'application/json',json.dumps({'monitor_alive':True}));return
            if path.startswith('/run/') or path=='/ddl':
                try:
                    data=ddl_details(query) if path=='/ddl' else run_details(path.split('/')[-1])
                except ValueError:
                    self.send_error(400,'Invalid identifier');return
                if data is None:self.send_error(404,'Details not found');return
                if path=='/ddl':
                    manifest=data['manifest']
                    name=(manifest['schema']+'.' if manifest.get('schema') else '')+manifest['table']
                    title='DDL de '+name
                    body=details.ddl_body(data,_date_time)
                else:
                    title='Execução '+data['run_id'][:8]
                    body=details.run_body(data,_date_time)
                if query.get('format')==['json']:
                    self._send(200,'application/json; charset=utf-8',json.dumps({'title':title,'html':body},ensure_ascii=False))
                else:
                    self._send(200,'text/html; charset=utf-8',details.detail_page(title,body))
                return
            snapshot=load_snapshot()
            if path=='/':
                pages=page_numbers(query)
                self._send(200,'text/html; charset=utf-8',render_html(snapshot,pages))
            elif path=='/status.json':
                self._send(200,'application/json; charset=utf-8',json.dumps(snapshot,ensure_ascii=False,default=str))
            elif path=='/readyz':
                ready=snapshot['local_db']['ok'] and snapshot['kafka']['ok']
                self._send(200 if ready else 503,'application/json',json.dumps({'dependencies_ready':ready}))
        except Exception as exc:
            self._send(503,'application/json',json.dumps({'error':f'{type(exc).__name__}: {str(exc)[:150]}'}))

    def _send(self,code,mime,payload):
        raw=payload.encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type',mime)
        self.send_header('Content-Length',str(len(raw)))
        self.send_header('Cache-Control','no-store')
        self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('Content-Security-Policy',"default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'unsafe-inline'; base-uri 'none'; object-src 'none'; frame-ancestors 'none'")
        self.end_headers();self.wfile.write(raw)
    def log_message(self,fmt,*args):pass


if __name__=='__main__':
    if not (1<=PORT<=65535 and STALLED_MIN>0 and COVERAGE_HOURS>0 and MAX_LAG>=0
            and 0<=EXPECTED_INTERVAL_MIN and 100<=MAX_RANGES<=100000):
        raise ValueError('Invalid monitoring configuration')
    print(f'monitoring http://0.0.0.0:{PORT} (Compose exposes loopback only)',flush=True)
    ThreadingHTTPServer(('0.0.0.0',PORT),Handler).serve_forever()
