"""Static checks for range reconciliation V5 (not integration tests)."""
from pathlib import Path
import ast
import yaml
ROOT=Path(__file__).resolve().parents[1]
source=(ROOT/'app/lab.py').read_text()
module=ast.parse(source)
functions={n.name:n for n in module.body if isinstance(n,ast.FunctionDef)}
assert {'poll','diff_keys','on_end','rollback','align_standby','prune_journal',
        'handle_event','consume','chaos','seed','status','mutate'} <= set(functions)
producer=ast.get_source_segment(source,functions['poll'])
assert 'REPEATABLE READ READ ONLY' in producer
assert 'SELECT id,' in producer and 'BETWEEN %s AND %s' in producer
assert 'changed_ids, missing_ids = diff_keys(observed, local)' in producer
assert "'kind':'delta'" in producer and "'kind':'seen'" in producer
assert 'changed_rows.extend' in producer and 'source.execute(\'COMMIT\')' in producer
on_end=ast.get_source_segment(source,functions['on_end'])
assert "if mode == 'bootstrap'" in on_end and 'TRUNCATE' in on_end
assert 'align_standby(conn, inactive' in on_end
assert 'prune_journal(conn)' in on_end
assert 'local_in_range' in on_end
assert 'sha256' in source and 'jsonb_build_array' in source
assert 'updated_at' not in producer and 'deleted_at' not in producer
compose=yaml.safe_load((ROOT/'docker-compose.yml').read_text())
monitor_services=compose['services']
assert 'MONITOR_DSN' in monitor_services['monitor']['environment']
assert 'LOCAL_DSN' not in monitor_services['monitor']['environment']
assert 'REMOTE_DSN' not in monitor_services['monitor']['environment']
assert 'monitor-db' not in monitor_services['consumer']['depends_on']
assert 'monitor-db' not in monitor_services['scheduler']['depends_on']
assert not monitor_services['monitor-db'].get('ports')
assert 'monitor_data' in compose['volumes']
assert 'user=monitor_reader' in monitor_services['monitor-collector']['environment']['LOCAL_DSN']
assert 'user=monitor_writer' in monitor_services['monitor-collector']['environment']['MONITOR_WRITE_DSN']
assert compose['services']['consumer']['environment']['REMOTE_DSN'].count('user=remote_reader') == 1
assert 'REMOTE_ADMIN_DSN' not in compose['services']['consumer']['environment']
assert 'REMOTE_ADMIN_DSN' not in compose['services']['cli']['environment']
assert 'REMOTE_ADMIN_DSN' in compose['services']['seed-fixture']['environment']
remote=(ROOT/'sql/remote.sql').read_text().split('CREATE ROLE remote_reader')[0]
local=(ROOT/'sql/local.sql').read_text()
assert 'updated_at' not in remote.split('CREATE TABLE public.contracts')[1]
assert 'deleted_at' not in remote.split('CREATE TABLE public.contracts')[1]
assert 'PRIMARY KEY' in remote
assert 'CREATE TABLE replica.change_journal' in local
assert 'CREATE VIEW public.contracts' in local and 'UNION ALL' in local
assert 'updated_at' not in local and 'deleted_at' not in local
schema_src=(ROOT/'app/schema_evolution.py').read_text()
assert 'pg_catalog.pg_attribute' in schema_src
assert 'schema_backfill_end' in schema_src
assert 'CREATE OR REPLACE VIEW public.contracts' in schema_src
assert 'schema_evo.gate' in source
assert 'schema_evo.current' in source
assert 'schema_backfill_runs' in local
assert 'schema_changes' in local
assert 'schema_versions' in local
assert 'schema_state' in local
assert 'data_schema_hash' in local
assert 'COPY lab.py schema_evolution.py' in (ROOT/'app/Dockerfile').read_text()
print('PASS V5: AST/YAML/SQL static invariants')
