"""Offline settings and Kafka frontier tests. Run with the app dependencies."""
import os
import sys
import unittest
from concurrent.futures import Future
from contextlib import nullcontext, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'app'))
import retention as r


def done(value):
    future = Future()
    future.set_result(value)
    return future


class Admin:
    def __init__(self, committed=80, age=60):
        self.committed, self.age = committed, age
        self.deleted = []
    def list_topics(self, **kwargs):
        return SimpleNamespace(topics={'events': SimpleNamespace(error=None, partitions={0: None})})
    def list_consumer_group_offsets(self, *args, **kwargs):
        return {r.GROUP: done(SimpleNamespace(topic_partitions=[SimpleNamespace(
            topic='events', partition=0, offset=self.committed, error=None)]))}
    def list_offsets(self, requests, **kwargs):
        partition, spec = next(iter(requests.items()))
        from confluent_kafka.admin import OffsetSpec
        value = 10 if spec._value == OffsetSpec.earliest()._value else (
            100 if spec._value == OffsetSpec.latest()._value else self.age)
        return {partition: done(SimpleNamespace(offset=value))}
    def delete_records(self, partitions, **kwargs):
        self.deleted += [p.offset for p in partitions]
        return {partitions[0]: done(SimpleNamespace(low_watermark=partitions[0].offset))}


class RetentionTests(unittest.TestCase):
    def test_partial_failure_keeps_count_of_committed_batches(self):
        class Database:
            batches = 0
            def transaction(self): return nullcontext()
            def execute(self, query, params=()):
                if isinstance(query, str): return None  # SET LOCAL
                self.batches += 1
                if self.batches == 2:
                    raise RuntimeError('statement timeout in second batch')
                return SimpleNamespace(rowcount=r.BATCH_SIZE)
        with self.assertRaises(r.PruneFailure) as error:
            r.prune(Database(), 'replica', 'transitions', 'TRUE', ())
        self.assertEqual(error.exception.removed, r.BATCH_SIZE)
        class CommitFailure(Database):
            @contextmanager
            def transaction(self):
                yield
                raise RuntimeError('transaction did not commit')
        with self.assertRaises(r.PruneFailure) as error:
            r.prune(CommitFailure(), 'replica', 'transitions', 'TRUE', ())
        self.assertEqual(error.exception.removed, 0)

    def test_defaults_and_overrides(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(r.Settings.from_env(), r.Settings(7, 7, 7, True, 24))
        with patch.dict(os.environ, {'LOG_RETENTION_DAYS': '3', 'AUDIT_RETENTION_DAYS': '30',
                'KAFKA_RETENTION_DAYS': '14', 'LOG_CLEANUP_ENABLED': 'false',
                'LOG_CLEANUP_INTERVAL_HOURS': '2'}, clear=True):
            self.assertEqual(r.Settings.from_env(), r.Settings(3, 30, 14, False, 2))
        for name, value in [('AUDIT_RETENTION_DAYS', '0'), ('KAFKA_RETENTION_DAYS', '-1'),
                            ('LOG_RETENTION_DAYS', 'x'), ('LOG_CLEANUP_ENABLED', 'yes')]:
            with patch.dict(os.environ, {name: value}, clear=True), self.assertRaises(ValueError):
                r.Settings.from_env()

    def test_topic_config_and_existing_volume_update(self):
        configs = r.topic_configs(r.Settings(kafka_retention_days=14), 'events', 'audit')
        self.assertEqual(configs['audit']['retention.ms'], str(14 * 86400000))
        self.assertEqual(configs['events']['retention.ms'], '-1')
        self.assertEqual(configs['events']['retention.bytes'], '-1')
        with self.assertRaises(ValueError):
            r.topic_configs(r.Settings(), 'events', 'events')
        from unittest.mock import Mock
        admin = Mock()
        admin.incremental_alter_configs.return_value = {'topic': done(None)}
        r.configure_topics(admin, configs)
        resources = admin.incremental_alter_configs.call_args.args[0]
        self.assertEqual({resource.name for resource in resources}, {'events', 'audit'})

    def test_never_delete_uncommitted_or_recent_events(self):
        for committed, age, expected in [(80, 60, 60), (40, 60, 40), (80, 10, None),
                                         (80, -1, 80), (-1001, 60, None)]:
            with self.subTest(committed=committed, age=age), patch.object(r, 'pending', return_value=False):
                admin = Admin(committed, age)
                r.expire_events(None, admin, 'events', datetime.now(timezone.utc))
                self.assertEqual(admin.deleted, [] if expected is None else [expected])

    def test_pending_and_dry_run_never_delete_events(self):
        admin = Admin()
        with patch.object(r, 'pending', return_value=True):
            report = r.expire_events(None, admin, 'events', datetime.now(timezone.utc))
            self.assertEqual(report['status'], 'preserved_pending')
        with patch.object(r, 'pending', return_value=False):
            report = r.expire_events(None, admin, 'events', datetime.now(timezone.utc), True)
            self.assertEqual(report['eligible_offsets'], 50)
        self.assertEqual(admin.deleted, [])


if __name__ == '__main__':
    unittest.main()
