#!/usr/bin/env bash
# Creates and removes only its own disposable projects; publishes no host ports.
set -euo pipefail
if [[ "${LAB_STARTUP_TEST:-0}" != "1" ]]; then
  echo 'Run LAB_STARTUP_TEST=1 bash tests/e2e_startup.sh for isolated startup tests.' >&2
  exit 2
fi
cd "$(dirname "$0")/.."
startup_base="kafka-startup-test-$$"
startup_project="$startup_base"
startup_override="$(mktemp /tmp/kafka-startup-compose-XXXXXX.yml)"
compose() { docker compose -p "$startup_project" -f docker-compose.yml -f "$startup_override" "$@"; }
cleanup() {
  for startup_project in "$startup_base" "$startup_base-pending"; do
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  done
  rm -f "$startup_override"
}
trap cleanup EXIT

cat > "$startup_override" <<YAML
services:
  remote-db:
    ports: !reset []
  local-db:
    ports: !reset []
  kafka:
    ports: !reset []
  toxiproxy:
    ports: !reset []
  kafbat:
    ports: !reset []
  adminer:
    ports: !reset []
  consumer:
    image: $startup_base-app
  cli:
    image: $startup_base-app
  init:
    image: $startup_base-app
  scheduler:
    image: $startup_base-app
    command: ["schedule", "--interval", "2", "--sweep-width", "1000"]
  monitor:
    image: $startup_base-app
    ports: !reset []
YAML

read_local() {
  compose exec -T local-db psql -v ON_ERROR_STOP=1 -U postgres -d lab_local -At -c "$1"
}
await_query() {
  local query="$1" expected="$2" actual=""
  for _ in {1..90}; do
    actual="$(read_local "$query")"
    [[ "$actual" == "$expected" ]] && return 0
    sleep 1
  done
  compose logs --tail 50 init consumer scheduler >&2
  echo "FAIL: $query expected [$expected], actual [$actual]" >&2
  exit 1
}

echo '[1/4] Default up bootstraps, promotes, starts scheduler and includes monitor'
compose up -d --build
await_query "SELECT count(*) FROM replica.sync_runs WHERE mode='bootstrap' AND status='promoted'" 1
await_query "SELECT count(*) > 0 FROM replica.sync_runs WHERE mode='incremental' AND status='promoted'" t
await_query "SELECT count(*) FROM replica.sync_runs WHERE mode='incremental' AND started_at < (SELECT finished_at FROM replica.sync_runs WHERE mode='bootstrap')" 0
await_query 'SELECT count(*) FROM public.contracts' 10000
compose exec -T monitor python -c \
  "import json,urllib.request; assert json.load(urllib.request.urlopen('http://localhost:8082/readyz'))['dependencies_ready']"

echo '[2/4] Restart reuses persisted data without a second bootstrap'
compose stop scheduler
await_query "SELECT count(*) FROM replica.sync_runs WHERE status IN ('extracting','loading')" 0
startup_incrementals="$(read_local "SELECT count(*) FROM replica.sync_runs WHERE mode='incremental' AND status='promoted'")"
compose stop
compose up -d --no-build
compose run --rm --no-deps init
await_query "SELECT count(*) FROM replica.sync_runs WHERE mode='bootstrap'" 1
await_query "SELECT count(*) > $startup_incrementals FROM replica.sync_runs WHERE mode='incremental' AND status='promoted'" t
compose stop

echo '[3/4] Pending bootstrap times out without republishing or enabling scheduler'
startup_project="$startup_base-pending"
compose up -d --no-build remote-db local-db kafka toxiproxy consumer monitor
compose stop consumer
compose run --rm cli bootstrap
if compose run --rm --no-deps init initialize --promotion-timeout 2; then
  echo 'FAIL: initialization should time out while consumer is stopped' >&2
  exit 1
fi
await_query "SELECT count(*) FROM replica.sync_runs WHERE mode='bootstrap'" 1
await_query "SELECT count(*) FROM replica.sync_runs WHERE mode='incremental'" 0
await_query 'SELECT version IS NULL FROM replica.active_state' t

echo '[4/4] Consumer resumes the queued bootstrap; initialization waits for promotion'
compose start consumer
compose run --rm --no-deps init
await_query "SELECT count(*) FROM replica.sync_runs WHERE mode='bootstrap' AND status='promoted'" 1
await_query 'SELECT count(*) FROM public.contracts' 10000
echo 'PASS: automatic startup, promotion gate, monitor, restart and pending bootstrap recovery'
