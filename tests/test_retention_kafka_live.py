"""Real Kafka expiration, isolated topic/group, never business data."""
import os
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from unittest.mock import patch

import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType
import retention as r

assert os.getenv('LAB_RETENTION_TEST') == '1', 'Use tests/e2e_retention.sh'
topic = 'retention-fixture-' + str(uuid4())
group = topic + '-reader'
admin = AdminClient({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP']})
configs = r.topic_configs(r.Settings(), topic, topic+'-audit')
# Synthetic dated records exercise the time frontier without waiting seven days.
# Managed production events use LogAppendTime, verified separately below.
configs[topic]['message.timestamp.type'] = 'CreateTime'
try:
    for f in admin.create_topics([NewTopic(t, 1, 1, config=c) for t,c in configs.items()]).values():
        f.result(10)
    producer = Producer({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP']})
    now = datetime.now(timezone.utc)
    failures = []
    for i in range(12):
        stamp = now-timedelta(days=10 if i<10 else 2)
        producer.produce(topic, value=str(i).encode(), timestamp=int(stamp.timestamp()*1000),
                         on_delivery=lambda err,msg: failures.append(err) if err else None)
    assert producer.flush(15)==0 and not failures, failures
    consumer = Consumer({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP'], 'group.id': group,
                         'enable.auto.commit': False})
    try:
        consumer.assign([TopicPartition(topic,0,0)])
        messages = []
        deadline = time.monotonic()+20
        while len(messages)<12 and time.monotonic()<deadline:
            messages.extend(consumer.consume(12-len(messages), timeout=1))
        assert len(messages)==12 and not any(m.error() for m in messages)
        consumer.commit(offsets=[TopicPartition(topic,0,8)], asynchronous=False)
        with psycopg.connect(os.environ['LOCAL_DSN'], autocommit=True) as conn, patch.object(r, 'GROUP', group):
            cutoff = now-timedelta(days=7)
            assert not r.pending(conn), 'Finish automatic initialization before this test'
            dry = r.expire_events(conn, admin, topic, cutoff, True)
            assert dry['eligible_offsets']==8 and dry['removed_offsets']==0, dry
            with patch.object(r, 'pending', return_value=True):
                assert r.expire_events(conn,admin,topic,cutoff)['status']=='preserved_pending'
            actual = r.expire_events(conn,admin,topic,cutoff)
            assert actual['removed_offsets']==8, actual
            consumer.commit(offsets=[TopicPartition(topic,0,12)], asynchronous=False)
            actual = r.expire_events(conn,admin,topic,cutoff)
            assert actual['removed_offsets']==2, actual
            assert r.expire_events(conn,admin,topic,cutoff)['removed_offsets']==0
            consumer.assign([TopicPartition(topic,0,10)])
            recent = consumer.consume(2,timeout=10)
            assert [m.value() for m in recent]==[b'10',b'11'], recent
        production = ConfigResource(ResourceType.TOPIC, os.environ['KAFKA_TOPIC'])
        effective = admin.describe_configs([production])[production].result(10)
        assert effective['retention.ms'].value=='-1'
        assert effective['message.timestamp.type'].value=='LogAppendTime'
        print('PASS: real Kafka dry-run, age and committed frontiers, pending protection, idempotence and consumer reads after deletion')
    finally:
        consumer.close()
finally:
    for future in admin.delete_topics(list(configs)).values():
        future.result(10)
