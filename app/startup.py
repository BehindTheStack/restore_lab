"""Prepare the proxy and bootstrap an empty destination before scheduling scans."""
import time


def initialize(connect, prepare_proxy, publish_bootstrap, log, *,
               ready_timeout=60, promotion_timeout=600, poll_interval=2):
    """Reuse durable bootstrap evidence; never re-publish a recorded attempt."""
    deadline = time.monotonic() + ready_timeout
    while True:
        try:
            prepare_proxy()
            break
        except OSError as exc:
            if time.monotonic() >= deadline:
                raise TimeoutError('Toxiproxy did not become ready') from exc
            log('startup_proxy_waiting', error=str(exc))
            time.sleep(poll_interval)
    log('startup_proxy_ready')

    # Separate from the producer lock: publishing uses its own DB connection.
    # A session lock serializes initializers while the consumer remains free.
    with connect() as conn:
        if not conn.execute('SELECT pg_try_advisory_lock(70261022)').fetchone()[0]:
            raise RuntimeError('Another initializer is running')
        # Init SQL does not run again for existing PostgreSQL volumes.
        # Repair the SELECT required by the monitor's ancestry query here too.
        conn.execute('GRANT SELECT ON replica.generations TO monitor_reader')
        log('startup_monitor_permissions_ready')
        active = conn.execute('''SELECT version FROM replica.active_state
                                 WHERE singleton''').fetchone()
        if active is None:
            raise RuntimeError('Missing active_state; inspect local database initialization')
        if active[0] is not None:
            log('startup_already_initialized', version=active[0])
            return

        previous = conn.execute('''SELECT run_id FROM replica.sync_runs
                                   WHERE mode='bootstrap'
                                   ORDER BY started_at DESC LIMIT 1''').fetchone()
        if previous is None:
            if conn.execute('SELECT EXISTS(SELECT 1 FROM replica.generations)').fetchone()[0]:
                raise RuntimeError('Existing generations without active state; refusing bootstrap')
            log('startup_bootstrap_publishing')
            run_id = publish_bootstrap()
            if run_id is None:
                raise RuntimeError('Bootstrap publisher did not return a run ID')
        else:
            run_id = str(previous[0])
            log('startup_bootstrap_resuming', run_id=run_id)

        deadline = time.monotonic() + promotion_timeout
        last_status = None
        while True:
            run = conn.execute('''SELECT r.status,r.error,r.result_version,a.version
                                  FROM replica.sync_runs r
                                  CROSS JOIN replica.active_state a
                                  WHERE r.run_id=%s AND a.singleton''', (run_id,)).fetchone()
            if run is None:
                raise RuntimeError(f'Bootstrap {run_id} or active state missing')
            status, error, result_version, active_version = run
            if status == 'promoted':
                if result_version is None or active_version is None:
                    raise RuntimeError(f'Bootstrap {run_id} promoted without a valid generation')
                log('startup_bootstrap_promoted', run_id=run_id, version=result_version)
                return
            if status not in ('extracting', 'loading'):
                raise RuntimeError(f'Bootstrap {run_id} ended as {status}: {error or "inspect consumer logs"}')
            if status != last_status:
                log('startup_bootstrap_waiting', run_id=run_id, status=status)
                last_status = status
            if time.monotonic() >= deadline:
                raise TimeoutError(f'Bootstrap {run_id} was not promoted within {promotion_timeout}s; '
                                   'inspect consumer and Kafka before retrying initialization')
            time.sleep(poll_interval)
