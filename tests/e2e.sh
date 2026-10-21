#!/usr/bin/env bash
# ONLY destructive to the dedicated synthetic lab databases. Run with Docker Compose.
set -euo pipefail
cd "$(dirname "$0")/.."
cli(){ docker compose run --rm cli "$@"; }
fixture(){ docker compose run --rm seed-fixture "$@"; }
read_local(){ docker compose exec -T local-db psql -v ON_ERROR_STOP=1 -U postgres -d lab_local -At -c "$1"; }
await_result(){
 local query="$1" expected="$2" got=""
 for _ in {1..100}; do
   got="$(read_local "$query")"
   [[ "$got" == "$expected" ]] && return 0
   sleep 1
 done
 echo "FAIL: $query expected [$expected] actual [$got]" >&2; exit 1
}
run_and_wait(){
 cli "$@"
 await_result "SELECT status FROM replica.sync_runs ORDER BY started_at DESC LIMIT 1;" promoted
}
echo '[1/9] Bootstrap without remote timestamps'
docker compose up -d --build
cli chaos init
fixture seed --rows 1000 --description-bytes 128
run_and_wait bootstrap
await_result 'SELECT COUNT(*) FROM public.contracts;' 1000

echo '[2/9] INSERT, UPDATE and physical DELETE in complete PK range'
fixture mutate update 1
fixture mutate hard-delete 2
fixture mutate insert 1001
run_and_wait incremental --from-id 1 --sweep-width 2000
await_result 'SELECT COUNT(*) FROM public.contracts;' 1000
await_result 'SELECT amount FROM public.contracts WHERE id=1;' 59.31
await_result 'SELECT COUNT(*) FROM public.contracts WHERE id=2;' 0
await_result 'SELECT COUNT(*) FROM public.contracts WHERE id=1001;' 1
await_result "SELECT inserted_count::text||','||updated_count::text||','||deleted_count::text FROM replica.sync_runs WHERE status='promoted' ORDER BY started_at DESC LIMIT 1;" '1,1,1'

echo '[3/9] Unscanned range does not see change'
fixture mutate update 700
run_and_wait incremental --from-id 2001 --sweep-width 2000
await_result 'SELECT amount FROM public.contracts WHERE id=700;' 12117.00
run_and_wait incremental --from-id 1 --sweep-width 2000
await_result 'SELECT amount FROM public.contracts WHERE id=700;' 12159.00

echo '[4/9] Rollback of blue/green'
prev="$(read_local 'SELECT version FROM replica.active_state;')"
cli rollback
now="$(read_local 'SELECT version FROM replica.active_state;')"
[[ "$now" != "$prev" ]] || { echo 'FAIL: rollback did not switch version'; exit 1; }
await_result 'SELECT amount FROM public.contracts WHERE id=700;' 12117.00
run_and_wait incremental --from-id 1 --sweep-width 2000
await_result 'SELECT amount FROM public.contracts WHERE id=700;' 12159.00

echo '[5/9] Single delete can be guard-rejected'
fixture mutate hard-delete 3
prev="$(read_local 'SELECT version FROM replica.active_state;')"
# Guardrail is enforced by CONSUMER env (not the CLI).
docker compose stop consumer
docker compose run -d --name kafka-lab-guard-consumer -e MAX_REMOVE_FRACTION=0 cli consume >/dev/null
cli incremental --from-id 1 --sweep-width 2000
await_result "SELECT status FROM replica.sync_runs ORDER BY started_at DESC LIMIT 1;" rejected
await_result 'SELECT version FROM replica.active_state;' "$prev"
docker rm -f kafka-lab-guard-consumer >/dev/null
docker compose start consumer
run_and_wait incremental --from-id 1 --sweep-width 2000
await_result 'SELECT COUNT(*) FROM public.contracts WHERE id=3;' 0

echo '[6/9] Remote unavailable does not promote'
prev="$(read_local 'SELECT version FROM replica.active_state;')"
cli chaos down
if cli incremental --from-id 1 --sweep-width 2000; then echo 'FAIL: expected remote error'; exit 1; fi
await_result 'SELECT version FROM replica.active_state;' "$prev"
cli chaos up
run_and_wait incremental --from-id 1 --sweep-width 2000

echo '[7/9] Schema drift prohibited'
docker compose exec -T remote-db psql -v ON_ERROR_STOP=1 -U postgres -d lab_remote -c 'ALTER TABLE public.contracts ADD COLUMN extraneous TEXT;'
prev="$(read_local 'SELECT version FROM replica.active_state;')"
if cli incremental --from-id 1 --sweep-width 2000; then echo 'FAIL: schema drift not detected'; exit 1; fi
await_result 'SELECT version FROM replica.active_state;' "$prev"
docker compose exec -T remote-db psql -v ON_ERROR_STOP=1 -U postgres -d lab_remote -c 'ALTER TABLE public.contracts DROP COLUMN extraneous;'

echo '[8/9] Consumer outage and Kafka replay'
docker compose stop consumer
fixture mutate update 1
prev="$(read_local 'SELECT version FROM replica.active_state;')"
cli incremental --from-id 1 --sweep-width 2000
await_result 'SELECT version FROM replica.active_state;' "$prev"
docker compose start consumer
for _ in {1..100}; do
 now="$(read_local 'SELECT version FROM replica.active_state;')"
 [[ "$now" != "$prev" ]] && break
 sleep 1
done
[[ "$now" != "$prev" ]] || { echo 'FAIL: no Kafka replay'; exit 1; }

echo '[9/9] Report status'
cli status
read_local 'SELECT action,from_slot,to_slot,from_version,to_version FROM replica.transitions ORDER BY id;'
echo 'PASS V4: reconciliation, deletes, blue-green, rollback, chaos and replay'
