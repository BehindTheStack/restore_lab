#!/usr/bin/env bash
# DANGEROUS: with LAB_RESET=1, destroys named V5 Docker lab volumes ONLY.
set -euo pipefail
cd "$(dirname "$0")/.."
cli(){ docker compose run --rm cli "$@"; }
fixture(){ docker compose run --rm seed-fixture "$@"; }
read_local(){ docker compose exec -T local-db psql -v ON_ERROR_STOP=1 -U postgres -d lab_local -At -c "$1"; }
read_remote(){ docker compose exec -T remote-db psql -v ON_ERROR_STOP=1 -U postgres -d lab_remote -At -c "$1"; }
wait_value(){
 local query="$1" expected="$2" got=""
 for _ in {1..90}; do
   got="$(read_local "$query")"
   [[ "$got" == "$expected" ]] && return 0
   sleep 1
 done
 echo "FAIL: [$query] expected '$expected', got '$got'" >&2
 exit 1
}
if [[ "${LAB_RESET:-0}" != "1" ]]; then
 echo 'Run only on disposable V5 lab: LAB_RESET=1 bash tests/e2e_schema.sh' >&2
 exit 2
fi

echo '[1/10] Bootstrap isolated lab'
docker compose down -v --remove-orphans
docker compose up -d --build
cli chaos init
fixture seed --rows 120 --description-bytes 128
cli bootstrap
wait_value 'SELECT COUNT(*) FROM public.contracts' 120

# Source DDL is administrative ONLY for this synthetic fixture. Reader stays read-only.
echo '[2/10] New additive nullable TEXT; old public contract unchanged'
read_remote 'ALTER TABLE public.contracts ADD COLUMN currency_code TEXT;'
read_remote "UPDATE public.contracts SET currency_code=CASE WHEN id % 2=0 THEN 'BRL' ELSE 'USD' END;"
cli schema plan | tee /tmp/schema_plan_v5.log
grep -q 'approvable' /tmp/schema_plan_v5.log
if cli incremental --from-id 1 --sweep-width 40; then
 echo 'FAIL: unapproved schema was accepted' >&2; exit 1
fi
wait_value "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='contracts' AND column_name='currency_code'" 0

echo '[3/10] Explicit approval, alignment barrier, physical DDL only'
cli schema apply
wait_value "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='replica' AND table_name IN ('blue_contracts','green_contracts') AND column_name='currency_code'" 2
wait_value "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='contracts' AND column_name='currency_code'" 0
wait_value "SELECT backfill_next FROM replica.schema_state" 1

echo '[4/10] Blue/green frozen during backfill'
if cli rollback; then echo 'FAIL: rollback during schema backfill' >&2; exit 1; fi
if cli incremental --from-id 1 --sweep-width 40; then echo 'FAIL: incremental during migration' >&2; exit 1; fi

echo '[5/10] Kafka backfill of four independently validated ranges'
for first in 1 31 61 91; do
 cli schema backfill --sweep-width 30
 next=$((first+30))
 wait_value 'SELECT backfill_next FROM replica.schema_state' "$next"
done
wait_value "SELECT COUNT(*) FROM replica.schema_backfill_intervals" 4
wait_value "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='contracts' AND column_name='currency_code'" 0

echo '[6/10] Atomic schema VIEW activation'
cli schema activate
wait_value "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='contracts' AND column_name='currency_code'" 1
wait_value "SELECT COUNT(*) FROM public.contracts WHERE currency_code IN ('BRL','USD')" 120
wait_value "SELECT COUNT(*) FROM public.contracts WHERE id=2 AND currency_code='BRL'" 1
wait_value "SELECT pending_hash IS NULL FROM replica.schema_state" t

echo '[7/10] Detect and apply updated NEW column via range/hash Kafka pipeline'
read_remote "UPDATE public.contracts SET currency_code='EUR' WHERE id=1"
cli incremental --from-id 1 --sweep-width 30
wait_value "SELECT currency_code FROM public.contracts WHERE id=1" EUR

echo '[8/10] INSERT and hard DELETE still supported after DDL'
fixture mutate hard-delete 5
fixture mutate insert 121
cli incremental --from-id 1 --sweep-width 150
wait_value "SELECT COUNT(*) FROM public.contracts WHERE id=5" 0
wait_value "SELECT COUNT(*) FROM public.contracts WHERE id=121" 1

echo '[9/10] Incompatible DDL: fail closed'
read_remote 'ALTER TABLE public.contracts RENAME COLUMN currency_code TO currency_new;'
cli schema plan | tee /tmp/schema_bad_v5.log
grep -q 'blocked' /tmp/schema_bad_v5.log
before="$(read_local 'SELECT version FROM replica.active_state')"
if cli incremental --from-id 1 --sweep-width 30; then
 echo 'FAIL: schema rename accepted' >&2;exit 1
fi
wait_value 'SELECT version FROM replica.active_state' "$before"

 echo '[10/10] Audit and summary'
cli status
cli schema status
read_local "SELECT status, COUNT(*) FROM replica.schema_backfill_runs GROUP BY status"
read_local "SELECT status, COUNT(*) FROM replica.schema_changes GROUP BY status"
echo 'PASS V5 schema: detect/approve/backfill/activate/validate/fail-closed'
