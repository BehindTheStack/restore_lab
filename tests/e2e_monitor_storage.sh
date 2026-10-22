#!/usr/bin/env bash
set -euo pipefail
if [[ "${LAB_MONITOR_STORAGE_TEST:-0}" != 1 ]]; then
  echo 'Run LAB_MONITOR_STORAGE_TEST=1 bash tests/e2e_monitor_storage.sh; disposable fixtures only.' >&2
  exit 2
fi
cd "$(dirname "$0")/.."
monitor_project="kafka-monitor-storage-test-$$"
monitor_tmp="$(mktemp -d /tmp/kafka-monitor-storage-XXXXXX)"
compose() { docker compose -p "$monitor_project" -f docker-compose.yml -f "$monitor_tmp/compose.yml" "$@"; }
cleanup() {
  local result=$?
  if [[ "$result" != 0 ]]; then
    compose ps -a >&2 || true
    compose exec -T monitor python -c 'import monitor,json; print(json.dumps(monitor.probe_kafka()))' >&2 || true
    compose logs --tail 40 init consumer kafka monitor-collector monitor >&2 || true
  fi
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -f "$monitor_tmp/compose.yml" "$monitor_tmp/remote.sql"
  rmdir "$monitor_tmp"
}
trap cleanup EXIT
python3 - "$monitor_tmp" "$monitor_project" <<'PY'
from pathlib import Path
import sys
import yaml
folder, project = Path(sys.argv[1]), sys.argv[2]
folder.joinpath('remote.sql').write_text(Path('sql/remote.sql').read_text().replace('generate_series(1,10000)','generate_series(1,20)'))
base = yaml.safe_load(Path('docker-compose.yml').read_text())
lines = ['services:']
for name, service in base['services'].items():
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
PY
compose up -d --build init monitor
for attempt in {1..90}; do
  if compose exec -T monitor python -c 'import json,urllib.request; urllib.request.urlopen("http://localhost:8082/readyz",timeout=5).read(); s=json.load(urllib.request.urlopen("http://localhost:8082/status.json",timeout=5)); assert (s.get("active") or {}).get("version")' >/dev/null 2>&1; then break; fi
  if [[ "$attempt" == 90 ]]; then echo 'Monitor did not become ready' >&2; exit 1; fi
  sleep 2
done
compose stop monitor-collector
compose run --rm -T --no-deps -e LAB_MONITOR_STORAGE_TEST=1 --entrypoint python monitor-collector - < tests/test_monitor_storage_live.py

echo 'Replication continues with the monitor database stopped'
compose stop monitor-db
compose exec -T remote-db psql -X -v ON_ERROR_STOP=1 -U postgres -d lab_remote -c "UPDATE public.contracts SET description='monitor-isolated' WHERE id=1"
compose run --rm --no-deps cli discover once --promotion-timeout 30
compose exec -T local-db psql -X -v ON_ERROR_STOP=1 -U postgres -d lab_local -At -c "SELECT description FROM mirror_public.contracts WHERE id=1" | grep -qx 'monitor-isolated'
compose exec -T monitor python -c '
import json, urllib.request
s=json.load(urllib.request.urlopen("http://localhost:8082/status.json"))
assert not s["monitor_db"]["ok"]
assert "MONITOR_DB_DOWN" in {a["code"] for a in s["alarms"]}
'
compose start monitor-db monitor-collector
for attempt in {1..60}; do
  if compose exec -T monitor python -c 'import urllib.request; urllib.request.urlopen("http://localhost:8082/readyz",timeout=5).read()' >/dev/null 2>&1; then break; fi
  if [[ "$attempt" == 60 ]]; then echo 'Monitor did not recover' >&2; exit 1; fi
  sleep 2
done
compose exec -T monitor python -c '
import json,urllib.request
s=json.load(urllib.request.urlopen("http://localhost:8082/status.json"))
assert s["monitor_db"]["ok"] and s["collection"]["ok"]
rid=s["recent_runs"][0]["id"]
urllib.request.urlopen("http://localhost:8082/run/"+rid+"?format=json").read()
urllib.request.urlopen("http://localhost:8082/ddl?run_id="+rid+"&format=json").read()
print("PASS: independent services, replication during monitor DB outage, HTTP modals and recovery")
'
