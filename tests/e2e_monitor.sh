#!/usr/bin/env bash
set -euo pipefail
if [ "${LAB_MONITOR_SMOKE:-}" != 1 ]; then
  echo 'Must set LAB_MONITOR_SMOKE=1; this is a read-only, non-destructive smoke test.' >&2
  exit 2
fi
cd "$(dirname "$0")/.."
# Smoke never seeds, resets volumes, applies migrations or promotes a generation.
# An initial bootstrap, monitor-db and collector must exist already.
curl -fsS http://localhost:8082/healthz | grep -q 'monitor_alive'
curl -fsS http://localhost:8082/status.json > /tmp/kafka-restore-status.json
python - <<'PY'
import json
s=json.load(open('/tmp/kafka-restore-status.json'))
for field in ('overall','coverage','local_db','monitor_db','collection','kafka','metrics','schema','alarms','recent_runs'):
    assert field in s,field
assert s['source_probe']=='not_performed'
print('PASS JSON, end-to-end signals and source-probe guard; overall='+s['overall'])
PY
curl -fsS http://localhost:8082/ | grep -q 'Saúde da reconciliação de dados'
# Verify role reads audit and has no write privileges.
docker compose exec -T monitor-db psql -U postgres -d lab_monitor -At -c \
  "SELECT has_table_privilege('monitor_reader','monitoring.collection_state','SELECT'),
          has_table_privilege('monitor_reader','monitoring.collection_state','INSERT');" \
  | grep -q '^t|f$'
echo 'PASS GUI + readonly grant'
