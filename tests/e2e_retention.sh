#!/usr/bin/env bash
# Disposable stack only; existing lab volumes and sources are never reset.
set -euo pipefail
if [[ "${LAB_RETENTION_TEST:-0}" != 1 ]]; then
  echo 'Run LAB_RETENTION_TEST=1 bash tests/e2e_retention.sh; disposable fixtures only.' >&2
  exit 2
fi
cd "$(dirname "$0")/.."
retention_project="kafka-retention-test-$$"
retention_tmp="$(mktemp -d /tmp/kafka-retention-XXXXXX)"
compose() { docker compose -p "$retention_project" -f docker-compose.yml -f "$retention_tmp/compose.yml" "$@"; }
cleanup() {
  local result=$?
  if [[ "$result" != 0 ]]; then compose logs --tail 35 init consumer monitor-collector >&2 || true; fi
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -f "$retention_tmp/compose.yml" "$retention_tmp/remote.sql"
  rmdir "$retention_tmp"
}
trap cleanup EXIT
python3 - "$retention_tmp" "$retention_project" <<'PYCODE'
from pathlib import Path
import sys
import yaml
folder, project = Path(sys.argv[1]), sys.argv[2]
folder.joinpath('remote.sql').write_text(Path('sql/remote.sql').read_text().replace('generate_series(1,10000)','generate_series(1,20)'))
lines = ['services:']
for name, service in yaml.safe_load(Path('docker-compose.yml').read_text())['services'].items():
    lines.append('  '+name+':')
    if 'build' in service:
        lines.append('    image: '+project+'-app')
    if 'ports' in service:
        lines.append('    ports: !reset []')
    if name == 'remote-db':
        lines.extend(['    volumes:', '      - '+str(folder/'remote.sql')+':/docker-entrypoint-initdb.d/01-remote.sql:ro'])
    if 'build' not in service and 'ports' not in service:
        lines.append('    restart: "no"')
folder.joinpath('compose.yml').write_text('\n'.join(lines)+'\n')
PYCODE
await_ready() {
  for attempt in {1..90}; do
    if compose exec -T monitor python -c 'import json,urllib.request; s=json.load(urllib.request.urlopen("http://localhost:8082/status.json",timeout=5)); assert s["local_db"]["ok"] and s.get("maintenance",{}).get("results",{}).get("automatic",{}).get("status")=="ok"; assert s.get("active",{}).get("version"); assert (s.get("latest_cycle") or {}).get("status")=="completed"' >/dev/null 2>&1; then return; fi
    sleep 2
  done
  echo 'Initialization/maintenance did not become ready' >&2
  exit 1
}
await_consumer() {
  for attempt in {1..90}; do
    if compose exec -T consumer python -c 'import monitor; g=monitor.probe_kafka()["group"]; assert g["state"]=="stable" and g["members"]==1' >/dev/null 2>&1; then return; fi
    sleep 2
  done
  echo 'Consumer group did not stabilize' >&2
  exit 1
}
echo '[1/5] New volumes and automatic maintenance'
compose up -d --build init monitor
await_ready
compose run --rm -T --no-deps -e LAB_RETENTION_TEST=1 --entrypoint python cli - < tests/test_retention_live.py
compose run --rm -T --no-deps -e LAB_RETENTION_TEST=1 --entrypoint python cli - < tests/test_retention_kafka_live.py
compose stop monitor-collector
compose run --rm -T --no-deps -e LAB_RETENTION_TEST=1 --entrypoint python monitor-collector - < tests/test_retention_monitor_live.py
compose start monitor-collector

echo '[2/5] Existing Kafka volume receives changed retention'
retention_schedule_before="$(compose exec -T local-db psql -X -U postgres -d lab_local -At -c "SELECT document->>'finished_at' FROM replica.log_cleanup_state WHERE job='automatic'")"
KAFKA_RETENTION_DAYS=14 compose up -d --no-build --force-recreate consumer
await_consumer
compose run --rm -T --no-deps --entrypoint python cli -c '
import os,time
from confluent_kafka.admin import AdminClient,ConfigResource,ResourceType
admin=AdminClient({"bootstrap.servers":os.environ["KAFKA_BOOTSTRAP"]})
resource=ConfigResource(ResourceType.TOPIC,os.environ["KAFKA_AUDIT_TOPIC"])
for attempt in range(30):
    if admin.describe_configs([resource])[resource].result(10)["retention.ms"].value==str(14*86400000): break
    time.sleep(1)
else: raise AssertionError("Existing topic configuration was not updated")
'
echo '[3/5] Restart preserves schedule and state; incremental after maintenance'
compose up -d --no-build --force-recreate consumer
await_consumer
retention_schedule_after="$(compose exec -T local-db psql -X -U postgres -d lab_local -At -c "SELECT document->>'finished_at' FROM replica.log_cleanup_state WHERE job='automatic'")"
[[ "$retention_schedule_before" == "$retention_schedule_after" ]] || { echo 'Restart repeated automatic cleanup before it was due' >&2; exit 1; }
compose run --rm --no-deps cli discover once --promotion-timeout 60
compose run --rm --no-deps cli cleanup --dry-run
compose run --rm --no-deps cli cleanup
compose run --rm --no-deps cli cleanup
compose run --rm --no-deps cli discover rollback --db-schema public --table contracts
compose run --rm --no-deps cli discover once --promotion-timeout 60

echo '[4/5] DDL detection after cleanup'
compose exec -T remote-db psql -X -v ON_ERROR_STOP=1 -U postgres -d lab_remote -c 'ALTER TABLE public.contracts ADD COLUMN retention_fixture TEXT'
compose run --rm --no-deps cli discover once --promotion-timeout 60
compose exec -T local-db psql -X -v ON_ERROR_STOP=1 -U postgres -d lab_local -At -c "SELECT count(*) FROM information_schema.columns WHERE table_schema='mirror_public' AND table_name='contracts' AND column_name='retention_fixture'" | grep -qx 1

echo '[5/5] Monitor exposes maintenance and configured retention'
compose exec -T monitor python -c '
import json,time,urllib.request
for attempt in range(30):
    s=json.load(urllib.request.urlopen("http://localhost:8082/status.json"))
    if s.get("maintenance",{}).get("results",{}).get("manual"): break
    time.sleep(1)
else: raise AssertionError("Maintenance result was not projected")
assert s["maintenance"]["settings"]["audit_retention_days"]==7
assert s["kafka"]["ok"] and s["kafka"]["group"]["lag"]==0
assert "Retenção e manutenção" in urllib.request.urlopen("http://localhost:8082/").read().decode()
print("PASS: new/existing volumes, configured Kafka retention, bootstrap, incremental, rollback, DDL, repeat cleanup and HTTP status")
'
