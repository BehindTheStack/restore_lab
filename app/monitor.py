"""Monitor Kafka restore lab.

No remote credentials, no Docker socket, no Kafka consumer group membership,
no writes and no external telemetry stack. Source reachability is INDIRECT.
"""
from __future__ import annotations

import html
import json
import os
import statistics
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from uuid import UUID

import psycopg
from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

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


def _query_db(conn, now):
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
                            native_hash,logical_hash
                      FROM replica.source_catalog_observations
                      WHERE observation_id IN (
                          SELECT MAX(observation_id) FROM replica.source_catalog_observations
                          GROUP BY source_key)
                      ORDER BY source_key LIMIT 20''')
        out['catalog']=[{'source':v[0],'engine':v[1],'classification':v[2],
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
    state={'checked_at':now.isoformat(),'local_db':{'ok':False,'error':None},
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
            state.update(_query_db(conn,now))
        state['local_db']['ok']=True
    except Exception as exc:
        state['local_db']['error']=f'{type(exc).__name__}: {str(exc)[:230]}'
    try:
        state['kafka']=kafka_probe()
    except Exception as exc:
        state['kafka']={'ok':False,'error':f'{type(exc).__name__}: {str(exc)[:230]}',
                        'group':{'state':'unknown','members':None,'lag':None}}
    state['overall'],state['alarms']=evaluate(state)
    return state


def _stage(title, value, status, desc, link):
    classes={'good':'good','warning':'warn','bad':'bad','unknown':'unknown'}
    return (f'<a href="#{_safe(link)}" class="stage {classes.get(status,"unknown")}">'
            f'<span class="stage-name">{_safe(title)}</span>'
            f'<b>{_safe(value)}</b><small>{_safe(desc)}</small></a>')


def render_html(s):
    a=s.get('active') or {}; k=s.get('kafka') or {}; group=k.get('group') or {}
    m=s.get('metrics') or {}; c=s.get('coverage') or {}; schema=s.get('schema') or {}
    catalog=s.get('catalog') or []; runs=s.get('recent_runs') or []
    last=s.get('last_transition') or {}
    state_label={'operational':'Serviços prontos · integridade por faixas',
                 'attention':'Atenção operacional','not_initialized':'Bootstrap pendente',
                 'unavailable':'Falha operacional'}[s['overall']]
    runtime='good' if s['local_db']['ok'] else 'bad'
    kafka_state='good' if k.get('ok') else 'bad'
    if group.get('lag') is None: group_state='unknown';lag='Não medido'
    else:
        group_state='warning' if group['lag']>MAX_LAG or group.get('members')==0 else 'good'
        lag=f"{group['lag']:,}".replace(',','.')
    source_state='unknown' if not runs else ('warning' if runs[0]['status'] in ('failed','rejected','aborted') else 'unknown')
    cover_txt=f"{c['ratio']:.2f}%" if c.get('ratio') is not None else 'Não aferida'
    cover_state='good' if c.get('status')=='complete' else 'unknown'
    sc_state='warning' if schema.get('pending_hash') else ('good' if schema.get('active_hash') else 'unknown')
    row_html=''.join(f'''<tr><td><a href="/run/{_safe(r['id'])}"><code>{_safe(r['id'][:8])}</code></a></td>
        <td>{_safe(r['engine'])}</td><td>{_safe(r['mode'])}</td>
        <td>{_safe(str(r['low'])+'–'+str(r['high']) if r['low'] is not None else 'bootstrap')}</td>
        <td><span class="tag">{_safe(r['status'])}</span></td>
        <td>{_safe(r['delta'])}</td><td>{_safe(r['deleted'])}</td>
        <td>{_safe(r['duration_seconds'])} s</td><td title="{_safe(r['error'])}">{_safe((r['error'] or '')[:105])}</td></tr>'''
        for r in runs)
    alert_html=''.join(f'''<div class="alert {('critical' if x['severity']=='critical' else 'warning')}">
         <b>{_safe(x['code'])}</b><span>{_safe(x['message'])}</span>
         <small>{_safe(x['action'])}</small></div>''' for x in s['alarms'])
    ranges=''.join(f'''<tr><td><a href="/run/{_safe(x['run_id'])}">{_safe(x['run_id'][:8])}</a></td>
        <td>{_safe(x['engine'])}</td><td>{_safe(x['low'])}–{_safe(x['high'])}</td>
        <td>{_safe(x['target_max_id'])}</td><td>{_safe(x['at'])}</td></tr>'''
        for x in (s.get('last_ranges') or []))
    catalogs=''.join(f'<tr><td>{_safe(x["source"])}</td><td>{_safe(x["engine"])}</td>'
        f'<td>{_safe(x["classification"])}</td><td>{_safe(x["observed_at"])}</td></tr>'
        for x in catalog)
    gaps=', '.join(f'{lo}–{hi}' for lo,hi in c.get('gaps',[])) or '—'
    stages=''.join([
        _stage('Origem remota','Não sondada',source_state,'Evidência indireta das extrações','source'),
        _stage('Extração / faixas',f"{m.get('promotions_24h',0)} promoções/24h",
               'warning' if m.get('stalled') else ('good' if a.get('version') else 'unknown'),
               'READ ONLY · fingerprints · deletes','extract'),
        _stage('Kafka', 'Acessível' if k.get('ok') else 'Indisponível',kafka_state,
               f"{_safe(k.get('partitions'))} partições de dados",'kafka'),
        _stage('Consumer',f'Lag: {lag}',group_state,
               f"{_safe(group.get('members'))} membros no grupo",'kafka'),
        _stage('PostgreSQL local','Acessível' if s['local_db']['ok'] else 'Indisponível',runtime,
               'Auditoria SQL persistida','destination'),
        _stage('Blue-green',f"{_safe(a.get('slot'))} · v{_safe(a.get('version'))}",
               'good' if a.get('version') else 'unknown','Promoção transacional','destination'),
        _stage('DDL / schema','Pendente' if schema.get('pending_hash') else 'Sem migração pendente',sc_state,
               'Histórico de contrato','schema'),
        _stage('Cobertura',cover_txt,cover_state,'Faixas de PK na janela móvel','coverage')])
    return f'''<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="20"><title>Reconciliação · Saúde ponta a ponta</title>
<style>
:root{{--bg:#f5f7fa;--paper:#fff;--ink:#182638;--muted:#657589;--line:#e1e6ed;--link:#215ca6}}
*{{box-sizing:border-box}}html{{scroll-behavior:smooth}}body{{margin:0;background:var(--bg);font:14px system-ui,Arial,sans-serif;color:var(--ink)}}
a{{color:var(--link)}}main{{max-width:1380px;margin:auto;padding:32px 24px 50px}}
header{{display:flex;align-items:start;justify-content:space-between;gap:20px;flex-wrap:wrap}}
h1{{font-size:27px;margin:3px 0 7px}}h2{{font-size:17px;margin:0 0 13px}}p{{color:var(--muted);line-height:1.55;margin:7px 0}}
.top{{color:var(--muted);font-size:12px;font-weight:700;letter-spacing:.08em}}
.state{{border:1px solid var(--line);background:#fff;padding:11px 15px;border-radius:24px;font-weight:700}}
.summary{{display:grid;grid-template-columns:repeat(auto-fit,minmax(175px,1fr));gap:12px;margin:21px 0}}
.card,.section{{border:1px solid var(--line);background:var(--paper);border-radius:13px;padding:18px}}
.card label{{color:var(--muted);display:block;font-size:12px}}.card strong{{display:block;font-size:25px;margin:7px 0}}
.card small{{color:var(--muted)}}.stages{{display:grid;grid-template-columns:repeat(4,1fr);gap:9px;margin:18px 0}}
.stage{{border:1px solid var(--line);border-left:4px solid #a7b4c6;border-radius:10px;background:white;padding:14px;text-decoration:none;color:var(--ink);min-height:113px}}
.stage-name{{display:block;font-size:12px;color:var(--muted)}}.stage b{{display:block;margin:9px 0 5px;font-size:16px;overflow-wrap:anywhere}}
.stage small{{display:block;color:var(--muted)}}.stage.good{{border-left-color:#26976a}}.stage.warn{{border-left-color:#ce9026}}.stage.bad{{border-left-color:#cd4451}}
.section{{margin:15px 0;scroll-margin-top:15px}}.columns{{display:grid;grid-template-columns:1fr 1fr;gap:15px}}
.data{{overflow:auto}}table{{width:100%;border-collapse:collapse;min-width:610px;text-align:left}}
th,td{{border-bottom:1px solid var(--line);padding:11px 9px;vertical-align:top;font-size:12px}}th{{color:var(--muted);font-weight:600}}
.tag{{border-radius:8px;background:#edf2f8;padding:4px 7px}}.alert{{border:1px solid #edcf98;border-radius:9px;padding:10px 12px;margin:8px 0;background:#fffaf0}}
.alert.critical{{border-color:#e9a1a5;background:#fff3f4}}.alert span{{display:block;margin:5px 0}}.alert small{{color:var(--muted)}}
code{{font:12px ui-monospace,monospace}}.meter{{height:10px;background:#e7edf3;border-radius:20px;margin:14px 0;overflow:hidden}}
.meter>div{{height:100%;background:#3e92a5}}.facts{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:14px}}.facts div{{font-size:13px}}
.facts b{{display:block;font-size:17px}}.facts small{{color:var(--muted)}}.subtle{{font-size:12px;color:var(--muted)}}
footer{{font-size:12px;color:var(--muted);line-height:1.7;padding:18px 0}}footer a{{margin-right:18px}}
@media(max-width:900px){{.stages{{grid-template-columns:repeat(2,1fr)}}.columns{{grid-template-columns:1fr}}}}
@media(max-width:520px){{main{{padding:19px 12px}}.stages{{grid-template-columns:1fr 1fr}}.stage{{min-height:130px}}}}
</style></head><body><main>
<header><div><div class="top">KAFKA RESTORE LAB / OPERAÇÃO</div>
<h1>Saúde da reconciliação de dados</h1>
<p>PostgreSQL ou SQL Server → Python → Kafka → Consumer → PostgreSQL blue-green<br>
Última leitura: {_safe(s['checked_at'])} · Atualização automática a cada 20 segundos</p></div>
<div class="state">{_safe(state_label)}</div></header>
<div class="summary">
<div class="card"><label>Última promoção</label><strong>{_safe(m.get('last_success_age_seconds'))} s</strong><small>Idade da promoção, não da origem</small></div>
<div class="card"><label>Versão ativa</label><strong>{_safe(a.get('version'))}</strong><small>Slot {_safe(a.get('slot'))} · {_safe(a.get('rows'))} linhas</small></div>
<div class="card"><label>Lag Kafka</label><strong>{_safe(group.get('lag'))}</strong><small>Offsets pendentes; não equivale a linhas</small></div>
<div class="card"><label>Cobertura de PK</label><strong>{cover_txt}</strong><small>Janela de {COVERAGE_HOURS} h · não é cobertura de bytes</small></div>
<div class="card"><label>Falhas / rejeições (24h)</label><strong>{_safe(m.get('failures_24h'))}</strong><small>{_safe(m.get('stalled'))} lote(s) travado(s)</small></div>
<div class="card"><label>Alterações promovidas (24h)</label><strong>{_safe((m.get('inserted_24h') or 0)+(m.get('updated_24h') or 0)+(m.get('deleted_24h') or 0))}</strong><small>INSERT + UPDATE + DELETE</small></div>
</div>
<h2>Fluxo ponta a ponta</h2><div class="stages">{stages}</div>
<div class="columns"><section class="section" id="alerts"><h2>Alertas e próximos passos</h2>
{alert_html or '<p>Sem alertas detectados pelos sinais disponíveis. Isso não certifica igualdade entre as bases.</p>'}
</section><section class="section" id="coverage"><h2>Cobertura da reconciliação</h2>
<div class="facts"><div><small>Espaço de IDs conferido</small><b>{_safe(c.get('covered_ids'))}</b></div>
<div><small>Limite superior observado</small><b>{_safe(c.get('target_max_id'))}</b></div>
<div><small>Faixas válidas na janela</small><b>{_safe(c.get('interval_count'))}</b></div></div>
<div class="meter"><div style="width:{max(0,min(100,c.get('ratio') or 0))}%"></div></div>
<p>Estado: <b>{_safe(c.get('status'))}</b> · Primeiras lacunas: {_safe(gaps)}.</p>
<p class="subtle">Usamos união de faixas promovidas pertencentes à linhagem ativa. Isso não mede quantidade de contratos,
volume trafegado ou consistência transacional de todo o remoto. Sem meta de cobertura, não há SLO de 100%.</p></section></div>
<div class="columns"><section class="section" id="source"><h2>Origem e extração</h2>
<p><b>Conectividade atual da origem: não aferida.</b> O painel não usa credenciais remotas nem aumenta carga no servidor.</p>
<p>Último lote: {_safe((runs[0] or {}).get('status') if runs else None)} · Origem: {_safe((runs[0] or {}).get('engine') if runs else None)}</p>
<p>Uma queda antes de criar um lote não aparece como falha da origem. Monitoramos as execuções conhecidas, não uptime real.</p>
</section><section class="section" id="kafka"><h2>Transporte e consumidor Kafka</h2>
<div class="facts"><div><small>Brokers</small><b>{_safe(k.get('brokers'))}</b></div>
<div><small>Partições</small><b>{_safe(k.get('partitions'))}</b></div>
<div><small>Membros do grupo</small><b>{_safe(group.get('members'))}</b></div>
<div><small>Consumer lag</small><b>{_safe(group.get('lag'))}</b></div></div>
<p>Dados: {'acessível' if k.get('data_topic') else 'indisponível'} · Auditoria: {'acessível' if k.get('audit_topic') else 'não encontrada'} · Grupo: {_safe(group.get('state'))}</p>
<p class="subtle">{_safe(k.get('error') or k.get('group_error'))}</p>
</section></div>
<div class="columns"><section class="section" id="destination"><h2>Destino e blue-green</h2>
<div class="facts"><div><small>BLUE v</small><b>{_safe(a.get('blue_version'))}</b></div>
<div><small>GREEN v</small><b>{_safe(a.get('green_version'))}</b></div>
<div><small>Ativo</small><b>{_safe(a.get('slot'))}</b></div></div>
<p>Última transição: {_safe(last.get('action'))} · {_safe(last.get('at'))} · Versão {_safe(last.get('version'))}</p>
<p class="subtle">A versão ativa é transacional; rollback exige compatibilidade do schema e journal disponível.</p>
</section><section class="section" id="schema"><h2>DDL e catálogo de schemas</h2>
<p>Contrato ativo: <code>{_safe((schema.get('active_hash') or '')[:20])}</code></p>
<p>Pendente: <code>{_safe((schema.get('pending_hash') or '')[:20] if schema.get('pending_hash') else 'nenhum')}</code> · Backfill próximo ID: {_safe(schema.get('backfill_next'))}</p>
<div class="data"><table><thead><tr><th>Fonte</th><th>Motor</th><th>Última classificação</th><th>Observado</th></tr></thead><tbody>{catalogs or '<tr><td colspan="4">Sem observações registradas</td></tr>'}</tbody></table></div>
</section></div>
<section class="section" id="extract"><h2>Resumo de alterações e guardas (24h)</h2>
<div class="facts"><div><small>INSERT</small><b>{_safe(m.get('inserted_24h'))}</b></div>
<div><small>UPDATE</small><b>{_safe(m.get('updated_24h'))}</b></div>
<div><small>DELETE</small><b>{_safe(m.get('deleted_24h'))}</b></div>
<div><small>Lotes rejeitados</small><b>{_safe(m.get('rejected_24h'))}</b></div>
<div><small>Bytes JSON publicados</small><b>{_safe(m.get('kafka_bytes_24h'))}</b></div></div>
<p>Deletes são inferidos somente na faixa validada; rejeições não geram checkpoint de cobertura nem promoção.</p>
</section>
<section class="section" id="runs"><h2>Últimas execuções · clique no ID para evidências</h2>
<div class="data"><table><thead><tr><th>ID</th><th>Origem</th><th>Modo</th><th>Faixa</th><th>Estado</th><th>Deltas</th><th>Deletes</th><th>Duração</th><th>Erro</th></tr></thead><tbody>
{row_html or '<tr><td colspan="9">Nenhuma execução registrada</td></tr>'}</tbody></table></div></section>
<section class="section"><h2>Faixas efetivamente promovidas</h2><div class="data"><table>
<thead><tr><th>Lote</th><th>Motor</th><th>Intervalo</th><th>Limite observado</th><th>Conferida em</th></tr></thead>
<tbody>{ranges or '<tr><td colspan="5">Nenhuma faixa promovida registrada</td></tr>'}</tbody></table></div></section>
<footer><a href="http://localhost:8080/">Kafbat UI</a><a href="http://localhost:8081/">Adminer</a>
<a href="/status.json">JSON de saúde</a><a href="/readyz">Readiness</a>
<p>Indicadores operacionais calculados sobre evidências locais e metadados Kafka. Sem sonda ativa à origem,
sem instrumentação extra, sem alterações de dados pelo painel e sem promessa de igualdade global entre as bases.</p></footer>
</main></body></html>'''


def run_details(rid, connect=psycopg.connect):
    rid=str(UUID(rid))
    with connect(LOCAL_DSN,connect_timeout=3) as conn:
        with conn.cursor() as cur:
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


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path=urlparse(self.path).path
        if not (path in ('/','/status.json','/healthz','/readyz') or path.startswith('/run/')):
            self.send_error(404);return
        try:
            if path=='/healthz':
                self._send(200,'application/json',json.dumps({'monitor_alive':True}));return
            if path.startswith('/run/'):
                try:
                    data=run_details(path.split('/')[-1])
                except ValueError:
                    self.send_error(400,'Invalid UUID');return
                if data is None:self.send_error(404,'Run not found');return
                obj='<h1>Execução '+_safe(data['run_id'])+'</h1><p><a href="/">← Painel</a></p>'
                obj+='<table>'+''.join(f'<tr><th>{_safe(key)}</th><td><code>{_safe(val)}</code></td></tr>' for key,val in data.items())+'</table>'
                body='<!doctype html><html lang="pt-BR"><meta charset="utf-8"><title>Execução</title><body style="font:14px system-ui;max-width:900px;margin:30px auto">'+obj+'</body></html>'
                self._send(200,'text/html; charset=utf-8',body);return
            snapshot=load_snapshot()
            if path=='/':
                self._send(200,'text/html; charset=utf-8',render_html(snapshot))
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
        self.send_header('Content-Security-Policy',"default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'")
        self.end_headers();self.wfile.write(raw)
    def log_message(self,fmt,*args):pass


if __name__=='__main__':
    if not (1<=PORT<=65535 and STALLED_MIN>0 and COVERAGE_HOURS>0 and MAX_LAG>=0
            and 0<=EXPECTED_INTERVAL_MIN and 100<=MAX_RANGES<=100000):
        raise ValueError('Invalid monitoring configuration')
    print(f'monitoring http://0.0.0.0:{PORT} (Compose exposes loopback only)',flush=True)
    ThreadingHTTPServer(('0.0.0.0',PORT),Handler).serve_forever()
