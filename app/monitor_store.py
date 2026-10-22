"""Read-only access to the monitor's independent metadata projection."""
from datetime import datetime, timezone


def read_snapshot(conn, mode, now, max_age):
    row = conn.execute('''SELECT snapshot,attempted_at,collected_at,source_status
        FROM monitoring.collection_state WHERE mode=%s''', (mode,)).fetchone()
    if row is None:
        return {}, {'ok': False, 'error': 'Aguardando a primeira coleta de evidências.'}
    payload, attempted, collected, source_status = row
    age = max(0, (now - attempted).total_seconds())
    collection = {'ok': age <= max_age, 'attempted_at': attempted.isoformat(),
                  'collected_at': collected.isoformat() if collected else None,
                  'age_seconds': round(age), 'error': None}
    if not collection['ok']:
        collection['error'] = f'Coletor sem atualização há {round(age)} s (limite {max_age} s).'
    payload = dict(payload or {})
    payload['local_db'] = source_status
    # Ages grow even when the collector cannot read the mirror database.
    for record, field, target in (
        (payload.get('metrics') or {}, 'last_success_at', 'last_success_age_seconds'),
        (payload.get('last_transition') or {}, 'at', 'age_seconds'),
    ):
        if record.get(field):
            stamp = datetime.fromisoformat(record[field])
            record[target] = max(0, round((now - stamp.astimezone(timezone.utc)).total_seconds()))
    return payload, collection


def read_detail(conn, mode, kind, key):
    row = conn.execute('''SELECT document FROM monitoring.details
        WHERE mode=%s AND kind=%s AND detail_key=%s''', (mode, kind, key)).fetchone()
    return row[0] if row else None


def ddl_from_run(record):
    if not record or not (record.get('manifest') or {}).get('columns'):
        return None
    return {'manifest': record['manifest'], 'observed_at': record['started_at'],
            'status': record['status'], 'run_id': record['run_id']}
