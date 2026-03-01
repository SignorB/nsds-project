import json
import random
import time

from confluent_kafka import Consumer, OFFSET_BEGINNING, OFFSET_END, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from .config import DEFAULT_RESPONSE_TIMEOUT_S, KAFKA_BOOTSTRAP


def ensure_topic_exists(topic: str, num_partitions: int = 1, replication_factor: int = 1) -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)]
    )
    try:
        futures[topic].result()
    except Exception as exc:
        msg = str(exc).lower()
        if "topic_already_exists" not in msg and "already exists" not in msg:
            raise


def list_partitions(topic: str) -> list[int]:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    meta = admin.list_topics(topic=topic, timeout=10)
    if topic in meta.topics:
        return list(meta.topics[topic].partitions.keys())
    return [0]


def _delivery_report(err, msg):
    if err is not None:
        print(f"[KafkaService] Delivery failed: {err}")


def send_event(topic: str, event_id: str, payload: dict) -> None:
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    p.produce(
        topic,
        key=event_id.encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
        callback=_delivery_report,
    )
    p.flush()


def _make_consumer(group_suffix: str = "") -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"frontend-{group_suffix}-{random.randint(0, 999_999)}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })


def poll_for_response(
        topic: str,
        event_id: str,
        timeout_s: float = DEFAULT_RESPONSE_TIMEOUT_S,
) -> dict | None:
    partitions = list_partitions(topic)
    tps = [TopicPartition(topic, p, OFFSET_END) for p in partitions]

    c = _make_consumer("rpc")
    c.assign(tps)

    deadline = time.time() + timeout_s
    try:
        while time.time() < deadline:
            msg = c.poll(timeout=0.5)
            if msg is None or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
                if data.get("eventId") == event_id:
                    return data
            except Exception:
                continue
    finally:
        c.close()

    return None


def drain_topic(topic: str, max_messages: int = 500) -> list[dict]:
    partitions = list_partitions(topic)
    tps = [TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions]

    c = _make_consumer("drain")
    c.assign(tps)

    results: list[dict] = []
    empty = 0
    try:
        while empty < 3 and len(results) < max_messages:
            msg = c.poll(timeout=0.3)
            if msg is None:
                empty += 1
                continue
            if msg.error():
                break
            empty = 0
            try:
                data = json.loads(msg.value().decode("utf-8"))
                results.append(data)
            except Exception:
                pass
    finally:
        c.close()

    return results


def get_live_consumer(topic: str) -> Consumer:
    partitions = list_partitions(topic)
    tps = [TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions]
    c = _make_consumer("live")
    c.assign(tps)
    return c


def poll_new_messages(consumer: Consumer, max_empty: int = 3) -> list[dict]:
    import pandas as pd

    results = []
    empty = 0
    while empty < max_empty:
        msg = consumer.poll(timeout=0.3)
        if msg is None:
            empty += 1
            continue
        if msg.error():
            break
        empty = 0
        try:
            value = json.loads(msg.value().decode("utf-8"))
        except Exception:
            value = msg.value().decode("utf-8") if msg.value() else ""
        results.append({
            "partition": msg.partition(),
            "offset": msg.offset(),
            "key": msg.key().decode("utf-8") if msg.key() else None,
            "value": value if isinstance(value, str) else json.dumps(value),
            "timestamp": pd.to_datetime(msg.timestamp()[1], unit="ms"),
        })
    return results
