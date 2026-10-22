#!/usr/bin/env bash
# Isolated, disposable SELECT-only source replication tests. No published ports.
set -euo pipefail
if [[ "${LAB_DISCOVERY_TEST:-0}" != 1 ]]; then
  echo 'Run LAB_DISCOVERY_TEST=1 bash tests/e2e_discovery.sh; only disposable fixtures are used.' >&2
  exit 2
fi
cd "$(dirname "$0")/.."
discovery_base="kafka-discovery-test-$$"
discovery_project="$discovery_base-pg"
discovery_tmp="$(mktemp -d /tmp/kafka-discovery-test-XXXXXX)"
compose() {
  local profiles=()
  if [[ "${discovery_engine:-}" == sqlserver ]]; then profiles=(--profile mssql --profile mssql-tools); fi
  docker compose "${profiles[@]}" -p "$discovery_project" -f docker-compose.yml -f "$discovery_tmp/compose.yml" "$@"
}
cleanup() {
  local result=$?
  if [[ "$result" != 0 ]]; then
    compose logs --tail 50 init consumer scheduler monitor >&2 || true
  fi
  for discovery_project in "$discovery_base-pg" "$discovery_base-ms"; do
    compose --profile mssql --profile mssql-tools down -v --remove-orphans >/dev/null 2>&1 || true
  done
  rm -f "$discovery_tmp/compose.yml" "$discovery_tmp/remote.sql"
  rmdir "$discovery_tmp"
}
trap cleanup EXIT
python3 - "$discovery_tmp/remote.sql" <<'PY'
from pathlib import Path
import sys
Path(sys.argv[1]).write_text(Path('sql/remote.sql').read_text().replace('generate_series(1,10000)','generate_series(1,20)'))
PY
cat > "$discovery_tmp/compose.yml" <<YAML
services:
  remote-db:
    ports: !reset []
    volumes:
      - $discovery_tmp/remote.sql:/docker-entrypoint-initdb.d/01-remote.sql:ro
  local-db:
    ports: !reset []
  remote-sqlserver:
    ports: !reset []
    environment:
      MSSQL_MEMORY_LIMIT_MB: "2048"
  kafka:
    ports: !reset []
  toxiproxy:
    ports: !reset []
  kafbat:
    ports: !reset []
  adminer:
    ports: !reset []
  consumer:
    image: $discovery_base-app
  cli:
    image: $discovery_base-app
  mssql-cli:
    image: $discovery_base-app
  mssql-fixture:
    image: $discovery_base-app
  init:
    image: $discovery_base-app
  scheduler:
    image: $discovery_base-app
  monitor:
    image: $discovery_base-app
    ports: !reset []
YAML

read_local() { compose exec -T local-db psql -X -v ON_ERROR_STOP=1 -U postgres -d lab_local -At -c "$1"; }
expect() {
  local actual
  actual="$(read_local "$1")"
  [[ "$actual" == "$2" ]] || { echo "FAIL: $1 expected [$2], got [$actual]" >&2; exit 1; }
}
source_sql() {
  if [[ "$discovery_engine" == postgresql ]]; then
    compose exec -T remote-db psql -X -v ON_ERROR_STOP=1 -U postgres -d lab_remote
  else
    compose --profile mssql-tools run --rm -T --entrypoint python mssql-fixture -c \
      'import sys; import sqlserver_fixture as f; conn=f.admin("lab_remote"); f.exec_sql(conn,sys.stdin.read()); conn.close()'
  fi
}
once() { compose run --rm "$discovery_cli" discover once --promotion-timeout 120; }

for discovery_engine in ${LAB_DISCOVERY_ENGINE:-postgresql sqlserver}; do
  if [[ "$discovery_engine" == postgresql ]]; then
    discovery_project="$discovery_base-pg"
    discovery_schema=public
    discovery_cli=cli
    echo '[PostgreSQL] Default automatic startup'
    compose up -d --build
    compose stop scheduler
    expect 'SELECT count(*) FROM mirror_public.contracts' 20
    expect "SELECT count(*) FROM replica.discovered_runs WHERE mode='bootstrap' AND status='promoted'" 1
  else
    discovery_project="$discovery_base-ms"
    discovery_schema=dbo
    discovery_cli=mssql-cli
    echo '[SQL Server] Fixture and SELECT-only automatic table initialization'
    compose build consumer
    compose --profile mssql --profile mssql-tools up -d --no-build remote-sqlserver remote-db local-db kafka toxiproxy consumer monitor
    compose --profile mssql-tools run --rm mssql-fixture init
    # Snapshot is deliberately OFF: the reader must use SERIALIZABLE without ALTER DATABASE.
    source_sql <<'SQL'
ALTER DATABASE lab_remote SET ALLOW_SNAPSHOT_ISOLATION OFF;
SQL
    compose run --rm mssql-cli discover init --promotion-timeout 120
    expect 'SELECT count(*) FROM mirror_dbo.contracts' 100
  fi

  echo "[$discovery_engine] Reader has SELECT, no source write privileges"
  compose run --rm -T --entrypoint python "$discovery_cli" -c '
import os
from discovery_catalog import Source
engine=os.getenv("REMOTE_ENGINE","postgresql")
source=Source(engine,os.environ["REMOTE_DSN"])
with source.connect() as conn:
    cur=conn.cursor()
    if engine=="postgresql":cur.execute("SET default_transaction_read_only=off")
    schema="public" if engine=="postgresql" else "dbo"
    try:
        cur.execute("INSERT INTO "+schema+".contracts(id,contract_code,amount,status,description) VALUES(9999999,\x27forbidden\x27,1,\x27x\x27,NULL)")
    except Exception as exc:
        if engine=="postgresql":assert getattr(exc,"sqlstate",None) in ("42501","25006"),exc
        else:assert exc.args[0]==229,exc
        print("Source write refused:",type(exc).__name__)
    else:raise AssertionError("reader unexpectedly has source write privileges")
'

  echo "[$discovery_engine] Unknown CREATE TABLE, INSERT, UPDATE and physical DELETE"
  if [[ "$discovery_engine" == postgresql ]]; then
    source_sql <<'SQL'
CREATE TABLE public.invoices(id BIGINT PRIMARY KEY, description TEXT, amount NUMERIC(14,2));
INSERT INTO public.invoices SELECT g,'item '||g,g FROM generate_series(1,10) g;
SQL
  else
    source_sql <<'SQL'
CREATE TABLE dbo.invoices(id BIGINT PRIMARY KEY, description NVARCHAR(80), amount DECIMAL(14,2));
INSERT INTO dbo.invoices SELECT TOP(10) ROW_NUMBER() OVER(ORDER BY object_id),N'item',12.50 FROM sys.objects;
SQL
  fi
  once
  expect "SELECT count(*) FROM mirror_$discovery_schema.invoices" 10
  source_sql <<SQL
INSERT INTO $discovery_schema.invoices VALUES(11,'new',12.50);
UPDATE $discovery_schema.invoices SET description='changed',amount=42.25 WHERE id=1;
DELETE FROM $discovery_schema.invoices WHERE id=2;
SQL
  once
  expect "SELECT count(*) FROM mirror_$discovery_schema.invoices" 10
  expect "SELECT description||':'||amount::text FROM mirror_$discovery_schema.invoices WHERE id=1" 'changed:42.25'
  expect "SELECT count(*) FROM mirror_$discovery_schema.invoices WHERE id=2" 0
  expect "SELECT inserted_count||','||updated_count||','||deleted_count FROM replica.discovered_runs r JOIN replica.discovered_tables t USING(table_key) WHERE t.source_table='invoices' ORDER BY r.started_at DESC LIMIT 1" '1,1,1'
  compose run --rm "$discovery_cli" discover rollback --db-schema "$discovery_schema" --table invoices
  expect "SELECT count(*) FROM mirror_$discovery_schema.invoices WHERE id=2" 1
  once
  expect "SELECT count(*) FROM mirror_$discovery_schema.invoices WHERE id=2" 0

  echo "[$discovery_engine] Rejected mass deletion preserves the published generation"
  source_sql <<SQL
DELETE FROM $discovery_schema.invoices WHERE id BETWEEN 3 AND 8;
SQL
  if once; then echo 'FAIL: mass deletion was accepted' >&2; exit 1; fi
  expect "SELECT count(*) FROM mirror_$discovery_schema.invoices" 10
  expect "SELECT r.status FROM replica.discovered_runs r JOIN replica.discovered_tables t USING(table_key) WHERE t.source_table='invoices' ORDER BY r.started_at DESC LIMIT 1" rejected
  source_sql <<SQL
INSERT INTO $discovery_schema.invoices VALUES(3,'restored',3),(4,'restored',4),(5,'restored',5),(6,'restored',6),(7,'restored',7),(8,'restored',8);
SQL
  once
  expect "SELECT count(*) FROM mirror_$discovery_schema.invoices" 10

  echo "[$discovery_engine] ADD COLUMN and ALTER COLUMN trigger validated schema publication"
  if [[ "$discovery_engine" == postgresql ]]; then
    source_sql <<'SQL'
ALTER TABLE public.invoices ADD COLUMN currency VARCHAR(3) DEFAULT 'BRL';
SQL
  else
    source_sql <<'SQL'
ALTER TABLE dbo.invoices ADD currency NVARCHAR(3) NULL DEFAULT N'BRL' WITH VALUES;
SQL
  fi
  once
  expect "SELECT count(*) FROM mirror_$discovery_schema.invoices WHERE currency='BRL'" 10
  if [[ "$discovery_engine" == postgresql ]]; then
    source_sql <<'SQL'
ALTER TABLE public.invoices ALTER COLUMN amount TYPE NUMERIC(16,4);
ALTER TABLE public.invoices ALTER COLUMN description TYPE VARCHAR(120);
UPDATE public.invoices SET amount=1.2345 WHERE id=1;
SQL
  else
    source_sql <<'SQL'
ALTER TABLE dbo.invoices ALTER COLUMN amount DECIMAL(16,4);
ALTER TABLE dbo.invoices ALTER COLUMN description NVARCHAR(120);
UPDATE dbo.invoices SET amount=1.2345 WHERE id=1;
SQL
  fi
  once
  expect "SELECT amount::text FROM mirror_$discovery_schema.invoices WHERE id=1" '1.2345'
  expect "SELECT numeric_precision||','||numeric_scale FROM information_schema.columns WHERE table_schema='mirror_$discovery_schema' AND table_name='invoices' AND column_name='amount'" '16,4'
  if compose run --rm "$discovery_cli" discover rollback --db-schema "$discovery_schema" --table invoices; then
    echo 'FAIL: rollback across incompatible schemas was accepted' >&2; exit 1
  fi

  echo "[$discovery_engine] Composite PK and table without PK"
  if [[ "$discovery_engine" == postgresql ]]; then
    source_sql <<'SQL'
CREATE TABLE public.composite_items(parent_id INT, line_no INT, note TEXT, PRIMARY KEY(parent_id,line_no));
INSERT INTO public.composite_items SELECT 1,g,'item' FROM generate_series(1,10) g;
CREATE TABLE public.no_key(note TEXT, amount NUMERIC(16,4));
INSERT INTO public.no_key SELECT 'same',1.2345 FROM generate_series(1,10);
SQL
  else
    source_sql <<'SQL'
CREATE TABLE dbo.composite_items(parent_id INT, line_no INT, note NVARCHAR(50), PRIMARY KEY(parent_id,line_no));
INSERT INTO dbo.composite_items SELECT TOP(10) 1,ROW_NUMBER() OVER(ORDER BY object_id),N'item' FROM sys.objects;
CREATE TABLE dbo.no_key(note NVARCHAR(50),amount DECIMAL(16,4));
INSERT INTO dbo.no_key SELECT TOP(10) N'same',1.2345 FROM sys.objects;
SQL
  fi
  once
  source_sql <<SQL
UPDATE $discovery_schema.composite_items SET note='changed' WHERE parent_id=1 AND line_no=1;
DELETE FROM $discovery_schema.composite_items WHERE parent_id=1 AND line_no=2;
UPDATE $discovery_schema.no_key SET note='changed';
SQL
  once
  expect "SELECT count(*) FROM mirror_$discovery_schema.composite_items" 9
  expect "SELECT count(*) FROM mirror_$discovery_schema.no_key WHERE note='changed' AND amount=1.2345" 10
  expect "SELECT updated_count IS NULL FROM replica.discovered_runs r JOIN replica.discovered_tables t USING(table_key) WHERE t.source_table='no_key' ORDER BY r.started_at DESC LIMIT 1" t

  echo "[$discovery_engine] DROP unpublishes only the managed view after two complete inventories"
  source_sql <<SQL
DROP TABLE $discovery_schema.invoices;
SQL
  once
  expect "SELECT to_regclass('mirror_$discovery_schema.invoices') IS NOT NULL" t
  once
  expect "SELECT to_regclass('mirror_$discovery_schema.invoices') IS NULL" t
  expect "SELECT status FROM replica.discovered_tables WHERE source_table='invoices'" missing_or_inaccessible
  expect "SELECT count(*)>0 FROM replica.discovered_blue b JOIN replica.discovered_tables t USING(table_key) WHERE t.source_table='invoices'" t
  compose exec -T monitor python -c '
import json,urllib.request
s=json.load(urllib.request.urlopen("http://localhost:8082/status.json"))
assert s["mode"]=="catalog",s
assert s["local_db"]["ok"],s["local_db"]
assert any(t["table"]=="invoices" and t["status"]=="missing_or_inaccessible" for t in s["tables"])
assert urllib.request.urlopen("http://localhost:8082/readyz").status==200
rid=s["recent_runs"][0]["id"]
assert urllib.request.urlopen("http://localhost:8082/run/"+rid).status==200
markup=urllib.request.urlopen("http://localhost:8082/?runs_page=2#runs").read().decode()
assert "Página 2 de" in markup and "aria-current=\"page\"" in markup
print("Monitor: dynamic inventory and readonly audit OK")
'
  compose --profile mssql --profile mssql-tools down -v --remove-orphans
done
echo 'PASS: discovery, DML, DDL, physical deletes, composite/no PK, SELECT-only sources and safe DROP propagation'
