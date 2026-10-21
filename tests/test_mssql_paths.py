"""Source-level invariants for SQL Server read-only reconciliation adapter."""
from pathlib import Path
import ast
import yaml
root=Path(__file__).resolve().parents[1]
source=(root/'app/mssql_sync.py').read_text()
lab=(root/'app/lab.py').read_text()
module=ast.parse(source)
functions={n.name:n for n in module.body if isinstance(n,ast.FunctionDef)}
assert {'poll','ensure_snapshot','source_rows','fetch_local_range','observe_and_gate','ensure_proxy'} <= functions.keys()
for token in ('ALLOW_SNAPSHOT_ISOLATION', 'BEGIN TRANSACTION', 'SNAPSHOT',
              'ROLLBACK TRANSACTION', 'COMMIT TRANSACTION', 'source_rows',
              'seen', 'delta', 'source_engine', 'fingerprint(row)',
              'catalog.observe_mssql', 'native_hash', 'data_schema_hash'):
    assert token in source, token
assert 'NOLOCK' not in [line for line in source.splitlines() if line.strip().startswith('SELECT ')]
assert "REMOTE_ENGINE == 'sqlserver'" in lab
assert 'mssql_sync.poll' in lab
compose=yaml.safe_load((root/'docker-compose.yml').read_text())
services=compose['services']
assert services['mssql-cli']['environment']['REMOTE_ENGINE']=='sqlserver'
assert services['mssql-cli']['environment']['MSSQL_USER']=='remote_reader'
assert 'MSSQL_SA_PASSWORD' not in services['mssql-cli']['environment']
assert 'MSSQL_SA_PASSWORD' not in services['consumer']['environment']
assert 'mssql-scheduler' in services
assert 'mssql_sync.py' in (root/'app/Dockerfile').read_text()
assert 'canonical_contract.py' in (root/'app/Dockerfile').read_text()
fixture=(root/'app/sqlserver_fixture.py').read_text()
assert 'ALLOW_SNAPSHOT_ISOLATION ON' in fixture
print('PASS V6: safe SQL Server extraction, Kafka protocol and Compose')
