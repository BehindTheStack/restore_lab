"""Offline startup lifecycle tests, including interrupted first installations."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

import yaml


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('startup', ROOT / 'app/startup.py')
startup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(startup)


class Clock:
    def __init__(self):
        self.now = 0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class Result:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class Destination:
    def __init__(self, *, active=None, history=None, states=None):
        self.active = (active,)
        self.history = history
        self.states = states or [('promoted', None, 1, 1)]
        self.lock_available = True
        self.has_generations = False
        self.closed = False
        self.can_read_generations = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def execute(self, sql, params=()):
        sql = ' '.join(sql.split())
        if 'pg_try_advisory_lock' in sql:
            return Result((self.lock_available,))
        if sql == 'GRANT SELECT ON replica.generations TO monitor_reader':
            self.can_read_generations = True
            return Result(None)
        if sql.startswith('SELECT version FROM replica.active_state'):
            return Result(self.active)
        if sql.startswith('SELECT run_id FROM replica.sync_runs'):
            return Result((self.history,) if self.history else None)
        if sql.startswith('SELECT EXISTS'):
            return Result((self.has_generations,))
        if 'CROSS JOIN replica.active_state' in sql:
            assert params == ('bootstrap-id',), params
            state = self.states[0]
            if len(self.states) > 1:
                self.states.pop(0)
            if state is not None and state[0] == 'promoted':
                self.active = (state[3],)
            return Result(state)
        raise AssertionError(sql)


class StartupTest(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.clock_patch = patch.object(startup, 'time', self.clock)
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)
        self.destination = Destination()
        self.connect = Mock(side_effect=lambda: self.destination)
        self.proxy = Mock()
        self.log = Mock()
        self.publish = Mock(side_effect=self.publish_bootstrap)

    def publish_bootstrap(self):
        self.destination.history = 'bootstrap-id'
        return 'bootstrap-id'

    def initialize(self, **kwargs):
        startup.initialize(self.connect, self.proxy, self.publish, self.log,
                           poll_interval=1, **kwargs)

    def test_first_installation_waits_for_promotion_and_restart_skips_bootstrap(self):
        self.destination.states = [
            ('extracting', None, None, None),
            ('loading', None, None, None),
            ('promoted', None, 1, 1),
        ]
        self.initialize()
        self.publish.assert_called_once_with()
        self.assertEqual(self.clock.now, 2)
        self.log.assert_called_with('startup_bootstrap_promoted', run_id='bootstrap-id', version=1)
        self.assertTrue(self.destination.closed)
        self.initialize()
        self.publish.assert_called_once_with()
        self.log.assert_called_with('startup_already_initialized', version=1)

    def test_existing_destination_repairs_monitor_permission_without_bootstrap(self):
        self.destination.active = (42,)
        self.initialize()
        self.proxy.assert_called_once_with()
        self.publish.assert_not_called()
        self.assertTrue(self.destination.can_read_generations)
        self.assertEqual(self.clock.now, 0)

    def test_pending_bootstrap_is_resumed_without_publication(self):
        self.destination.history = 'bootstrap-id'
        self.destination.states = [('loading', None, None, None), ('promoted', None, 1, 1)]
        self.initialize()
        self.publish.assert_not_called()
        self.assertEqual(self.clock.now, 1)

    def test_terminal_bootstrap_failure_never_reports_success_or_republishes(self):
        for status in ('failed', 'rejected', 'aborted'):
            with self.subTest(status=status):
                self.destination.history = 'bootstrap-id'
                self.destination.states = [(status, 'test failure', None, None)]
                with self.assertRaisesRegex(RuntimeError, f'{status}: test failure'):
                    self.initialize()
        self.publish.assert_not_called()
        self.assertNotIn('startup_bootstrap_promoted', [call.args[0] for call in self.log.call_args_list])

    def test_timeout_retains_batch_and_next_attempt_waits_for_the_same_run(self):
        self.destination.states = [('loading', None, None, None)]
        with self.assertRaisesRegex(TimeoutError, 'not promoted'):
            self.initialize(promotion_timeout=2)
        self.publish.assert_called_once_with()
        self.assertEqual(self.clock.now, 2)
        self.destination.states = [('promoted', None, 1, 1)]
        self.initialize()
        self.publish.assert_called_once_with()

    def test_proxy_readiness_precedes_bootstrap(self):
        self.proxy.side_effect = [ConnectionRefusedError(), OSError(), None]
        self.publish.side_effect = lambda: (
            self.assertEqual(self.proxy.call_count, 3), self.publish_bootstrap())[1]
        self.initialize()
        self.assertEqual(self.clock.now, 2)
        self.publish.assert_called_once_with()

    def test_proxy_timeout_never_publishes_bootstrap(self):
        self.proxy.side_effect = ConnectionRefusedError()
        with self.assertRaisesRegex(TimeoutError, 'Toxiproxy'):
            self.initialize(ready_timeout=2)
        self.connect.assert_not_called()
        self.publish.assert_not_called()

    def test_publisher_failure_propagates_without_waiting_or_retrying(self):
        self.publish.side_effect = RuntimeError('publisher failed')
        with self.assertRaisesRegex(RuntimeError, 'publisher failed'):
            self.initialize()
        self.publish.assert_called_once_with()
        self.assertEqual(self.clock.now, 0)

    def test_missing_publisher_run_id_is_not_accepted(self):
        self.publish.side_effect = None
        self.publish.return_value = None
        with self.assertRaisesRegex(RuntimeError, 'return a run ID'):
            self.initialize()

    def test_missing_or_inconsistent_database_state_is_not_reinitialized(self):
        self.destination.active = None
        with self.assertRaisesRegex(RuntimeError, 'Missing active_state'):
            self.initialize()
        self.destination.active = (None,)
        self.destination.has_generations = True
        with self.assertRaisesRegex(RuntimeError, 'Existing generations'):
            self.initialize()
        self.publish.assert_not_called()

    def test_missing_batch_or_invalid_promoted_state_is_not_success(self):
        self.destination.history = 'bootstrap-id'
        for state in (None, ('promoted', None, None, None)):
            with self.subTest(state=state):
                self.destination.states = [state]
                with self.assertRaises(RuntimeError):
                    self.initialize()
        self.publish.assert_not_called()

    def test_concurrent_initializer_does_not_publish(self):
        self.destination.lock_available = False
        with self.assertRaisesRegex(RuntimeError, 'Another initializer'):
            self.initialize()
        self.publish.assert_not_called()
        self.assertTrue(self.destination.closed)

    def test_compose_blocks_default_scheduler_until_initializer_succeeds(self):
        services = yaml.safe_load((ROOT / 'docker-compose.yml').read_text())['services']
        for name in ('init', 'scheduler', 'monitor'):
            self.assertNotIn('profiles', services[name])
        self.assertEqual(services['init']['command'][0], 'initialize')
        self.assertEqual(services['init']['restart'], 'no')
        self.assertEqual(services['scheduler']['depends_on']['init']['condition'],
                         'service_completed_successfully')
        self.assertEqual(services['init']['depends_on']['consumer']['condition'], 'service_started')
        self.assertNotIn('REMOTE_ADMIN_DSN', services['init']['environment'])
        self.assertNotIn('MSSQL_SA_PASSWORD', services['init']['environment'])
        self.assertIn('startup.py', (ROOT / 'app/Dockerfile').read_text())


if __name__ == '__main__':
    unittest.main()
