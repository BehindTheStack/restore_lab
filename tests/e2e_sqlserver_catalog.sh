#!/usr/bin/env bash
# Disposable SQL Server catalog test. Does not alter an external remote.
set -euo pipefail
cd "$(dirname "$0")/.."
comp(){ docker compose --profile mssql --profile mssql-tools "$@"; }
if [[ "${LAB_SQLSERVER_TEST:-0}" != "1" ]]; then
  echo 'Opt in: LAB_SQLSERVER_TEST=1 bash tests/e2e_sqlserver_catalog.sh' >&2
  exit 2
fi
comp up -d --build local-db kafka remote-sqlserver
comp run --rm mssql-fixture init
comp run --rm mssql-cli catalog observe --engine sqlserver --source-key mssql-dbo-contracts
comp run --rm mssql-fixture add-column
comp run --rm mssql-cli catalog observe --engine sqlserver --source-key mssql-dbo-contracts
comp run --rm mssql-fixture change-length
comp run --rm mssql-cli catalog observe --engine sqlserver --source-key mssql-dbo-contracts
comp run --rm mssql-fixture drop-column
comp run --rm mssql-cli catalog observe --engine sqlserver --source-key mssql-dbo-contracts
comp exec -T local-db psql -v ON_ERROR_STOP=1 -U postgres -d lab_local -c "
 SELECT observation_id,classification,jsonb_array_length(changes) AS logical_changes,
        jsonb_array_length(native_changes) AS native_changes
 FROM replica.source_catalog_observations
 WHERE source_key='mssql-dbo-contracts'
 ORDER BY observation_id;"
N="$(comp exec -T local-db psql -U postgres -d lab_local -At -c "SELECT count(*) FROM replica.source_catalog_observations WHERE source_key='mssql-dbo-contracts'")"
if [[ "$N" -lt 4 ]]; then echo "FAIL: expected four observations; got $N" >&2; exit 1; fi
echo 'PASS: SQL Server metadata observation + JSONB history (data replication not tested)'
