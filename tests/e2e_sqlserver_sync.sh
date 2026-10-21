#!/usr/bin/env bash
# Destructive E2E ONLY on isolated disposable Docker Compose volumes.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ "${LAB_RESET:-0}" != "1" ]]; then
  echo 'Only disposable lab! Run LAB_RESET=1 bash tests/e2e_sqlserver_sync.sh' >&2; exit 2
fi
comp(){ docker compose --profile mssql --profile mssql-tools "$@"; }
comp down -v --remove-orphans
comp up -d --build local-db kafka remote-db toxiproxy consumer remote-sqlserver
comp run --rm mssql-fixture init
comp run --rm mssql-cli bootstrap
for i in $(seq 1 60); do
  if [[ "$(comp exec -T local-db psql -U postgres -d lab_local -At -c "SELECT count(*) FROM replica.sync_runs WHERE status='promoted' AND mode='bootstrap'" | tr -d '\r')" == '1' ]]; then break; fi
  sleep 2
  n="$(comp exec -T local-db psql -U postgres -d lab_local -At -c "SELECT count(*) FROM replica.sync_runs WHERE status IN ('failed','rejected','aborted')" | tr -d '\r')"
  [[ "$n" == '0' ]] || { echo 'FAIL bootstrap' >&2; exit 1; }
done
[[ "$(comp exec -T local-db psql -U postgres -d lab_local -At -c "SELECT count(*) FROM replica.sync_runs WHERE status='promoted' AND mode='bootstrap'" | tr -d '\r')" == '1' ]] || { echo 'Timed out waiting for bootstrap' >&2; exit 1; }
comp run --rm mssql-fixture mutate-insert
comp run --rm mssql-fixture mutate-update
comp run --rm mssql-fixture mutate-delete
comp run --rm mssql-cli incremental --from-id 1 --sweep-width 200
for i in $(seq 1 50); do
  promoted="$(comp exec -T local-db psql -U postgres -d lab_local -At -c "SELECT count(*) FROM replica.sync_runs WHERE status='promoted' AND mode='incremental'" | tr -d '\r')"
  [[ "$promoted" == '1' ]] && break
  sleep 2
done
comp exec -T local-db psql -v ON_ERROR_STOP=1 -U postgres -d lab_local <<'SQL'
DO $$ BEGIN
 IF (SELECT count(*) FROM public.contracts)<>100 THEN RAISE EXCEPTION 'wrong row count'; END IF;
 IF EXISTS (SELECT 1 FROM public.contracts WHERE id=2) THEN RAISE EXCEPTION 'hard delete not applied'; END IF;
 IF NOT EXISTS (SELECT 1 FROM public.contracts WHERE id=101 AND contract_code='CT-101'
    AND amount=155.45 AND description='Unicode: ação') THEN RAISE EXCEPTION 'insert not applied'; END IF;
 IF NOT EXISTS (SELECT 1 FROM public.contracts WHERE id=1 AND status='updated'
    AND description='alteração incremental') THEN RAISE EXCEPTION 'update not applied'; END IF;
 IF (SELECT count(*) FROM replica.sync_runs WHERE status='promoted' AND mode='incremental')<>1
    THEN RAISE EXCEPTION 'incremental was not promoted'; END IF;
END $$;
SQL
comp run --rm mssql-fixture add-column
if comp run --rm mssql-cli incremental --from-id 1 --sweep-width 200; then
  echo 'FAIL: drift should prevent new snapshot' >&2; exit 1
fi
comp exec -T local-db psql -v ON_ERROR_STOP=1 -U postgres -d lab_local <<'SQL'
DO $$ BEGIN
 IF (SELECT count(*) FROM public.contracts)<>100 THEN RAISE EXCEPTION 'active data changed on DDL drift'; END IF;
 IF NOT EXISTS (SELECT 1 FROM replica.source_catalog_observations
    WHERE source_key='mssql-dbo-contracts' AND classification<>'unchanged')
    THEN RAISE EXCEPTION 'schema observation not persisted'; END IF;
END $$;
SQL
echo 'PASS V6: SQL Server bootstrap + UPSERT + hard DELETE + blue/green + drift gate'
