"""Offline contracts for isolated monitoring, freshness and failure attribution."""
from pathlib import Path
import importlib
import runpy
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

ctx = runpy.run_path(str(Path(__file__).with_name('test_monitor.py')), run_name='monitor_fixtures')
m = ctx['m']
store = m.monitor_store
collector = importlib.import_module('monitor_collect')
now = datetime(2026, 10, 22, tzinfo=timezone.utc)
rid = '00000000-0000-0000-0000-000000000001'


class Connection:
    def __init__(self, row=None):
        self.row = row
        self.rowcount = 0
        self.calls = []
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def transaction(self): return self
    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        return self
    def fetchone(self): return self.row


def healthy_payload():
    return {'local_db': {'ok': True, 'connected': True}, 'active': {'version': 1},
            'metrics': {'last_success_at': (now-timedelta(seconds=30)).isoformat()},
            'coverage': {'status': 'complete'}, 'recent_runs': []}


def test_monitor_uses_its_own_db_and_reports_collection_age():
    db = Connection((healthy_payload(), now, now, {'ok': True, 'connected': True}))
    called = []
    def connect(dsn, **kwargs):
        called.append(dsn)
        assert dsn == 'monitor-only'
        return db
    probe = lambda: {'ok': True, 'audit_topic': True, 'group': {'lag': 0, 'members': 1}}
    with patch.object(m, 'MONITOR_DSN', 'monitor-only'), patch.object(m, '_query_db', side_effect=AssertionError('No mirror SQL')):
        snap = m.load_snapshot(now=now, connect=connect, kafka_probe=probe)
        assert snap['monitor_db']['ok'] and snap['collection']['ok']
        assert snap['metrics']['last_success_age_seconds'] == 30
        assert snap['overall'] == 'operational'
        assert 'Evidências coletadas' in m.render_html(snap)
        stale = m.load_snapshot(now=now+timedelta(seconds=61), connect=connect, kafka_probe=probe)
        codes = {a['code'] for a in stale['alarms']}
        assert 'MONITOR_COLLECTION_STALE' in codes and 'LOCAL_DB_DOWN' not in codes
        assert stale['overall'] == 'unavailable'
        assert 'Não aferido' in m.render_html(stale)
        def offline(dsn, **kwargs): raise OSError('monitor DB offline')
        down = m.load_snapshot(now=now, connect=offline, kafka_probe=probe)
        assert {a['code'] for a in down['alarms']} == {'MONITOR_DB_DOWN'}
        empty = m.load_snapshot(now=now, connect=lambda *a, **kw: Connection(), kafka_probe=probe)
        assert {a['code'] for a in empty['alarms']} == {'MONITOR_COLLECTION_STALE'}
        db.row = (healthy_payload(), now, now, {'ok': False, 'connected': True,
                                              'error_code': '42501', 'error': 'permission denied'})
        unreadable = m.load_snapshot(now=now, connect=connect, kafka_probe=probe)
        assert {a['code'] for a in unreadable['alarms']} == {'LOCAL_DB_READ_ERROR'}
    assert len(called) == 3
    assert all(sql.lstrip().startswith('SELECT') for sql, _ in db.calls)


def test_modal_reads_projection_and_keeps_historical_contract():
    manifest = {'schema': 'public', 'table': 'old_name', 'columns': [{'name': 'id'}]}
    db = Connection(({'run_id': rid, 'manifest': manifest, 'status': 'promoted', 'started_at': now.isoformat()},))
    with patch.object(m, 'MONITOR_DSN', 'monitor-only'):
        connect = lambda dsn, **kwargs: db if dsn == 'monitor-only' else (_ for _ in ()).throw(AssertionError(dsn))
        assert m.run_details(rid, connect=connect)['manifest'] == manifest
        assert m.ddl_details({'run_id': [rid]}, connect=connect)['manifest'] == manifest
        assert db.calls[-1][1] == (m.SYNC_MODE, 'run', rid)
        assert all('replica.' not in sql for sql, _ in db.calls)


def test_collection_failure_preserves_projection_and_target_failure_propagates():
    source, target = Connection(), Connection()
    def connect(dsn, **kwargs): return source if dsn == 'source' else target
    with patch.object(collector.monitor, '_query_db', side_effect=OSError('mirror unavailable')):
        assert not collector.collect_once('source', 'target', 'catalog', connect=connect, now=now)
    sql, params = target.calls[-1]
    assert 'coalesce(excluded.snapshot' in sql
    assert params[1] is None and not params[3]  # Preserve prior snapshot and collection timestamp.
    assert source.calls[0][0] == 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'
    with patch.object(collector.monitor, '_query_db', return_value={}), \
         patch.object(collector, 'documents', return_value=iter([])), \
         patch.object(collector, 'write_state', side_effect=OSError('target unavailable')):
        try:
            collector.collect_once('source', 'target', 'catalog', connect=connect, now=now)
        except OSError as exc:
            assert str(exc) == 'target unavailable'
        else:
            raise AssertionError('Target failures must propagate, not blame the mirror')


if __name__ == '__main__':
    test_monitor_uses_its_own_db_and_reports_collection_age()
    test_modal_reads_projection_and_keeps_historical_contract()
    test_collection_failure_preserves_projection_and_target_failure_propagates()
    print('PASS: independent DB, stale collection, historical modals and failure attribution')
