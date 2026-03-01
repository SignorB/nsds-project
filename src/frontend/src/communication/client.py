import json
import time
import uuid

from confluent_kafka import TopicPartition

from .config import DEFAULT_RESPONSE_TIMEOUT_S

from .kafka_service import (
    drain_topic,
    ensure_topic_exists,
    send_event,
)

TOPIC_USER_REQUESTS = "user-requests"
TOPIC_USER_LIFECYCLE = "user-lifecycle-events"
TOPIC_NODE_REQUESTS = "node-management-requests"
TOPIC_NODE_LIFECYCLE = "node-lifecycle-events"
TOPIC_MEASUREMENTS_RAW = "measurements-raw"
TOPIC_MEASUREMENT_EVENTS = "measurement-events"
TOPIC_USAGE_RECORDS = "usage-records"


def _new_event_id(prefix: str = "fe") -> str:
    return f"{prefix}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"


def _request(event_type: str, payload: dict, event_id: str) -> dict:
    return {
        "eventId": event_id,
        "eventType": event_type,
        "timestamp": str(int(time.time() * 1000)),
        "serviceId": "frontend",
        "payload": payload,
    }


def _send_and_await(
    request_topic: str,
    response_topic: str,
    event_type: str,
    payload: dict,
    id_prefix: str = "fe",
) -> dict | None:
    ensure_topic_exists(request_topic)
    ensure_topic_exists(response_topic)

    event_id = _new_event_id(id_prefix)
    msg = _request(event_type, payload, event_id)

    from .kafka_service import _make_consumer, list_partitions
    partitions = list_partitions(response_topic)

    c = _make_consumer("rpc")
    c.assign([TopicPartition(response_topic, p) for p in partitions])

    ready_deadline = time.time() + 5.0
    while time.time() < ready_deadline:
        c.poll(timeout=0.05)
        assigned = c.assignment()
        if assigned and all(tp.offset != -1001 for tp in c.position(assigned)):
            break

    for p in partitions:
        _, high = c.get_watermark_offsets(TopicPartition(response_topic, p), timeout=5.0)
        c.seek(TopicPartition(response_topic, p, high))

    send_event(request_topic, event_id, msg)

    deadline = time.time() + DEFAULT_RESPONSE_TIMEOUT_S
    try:
        while time.time() < deadline:
            m = c.poll(timeout=0.5)
            if m is None or m.error():
                continue
            try:
                data = json.loads(m.value().decode("utf-8"))
                if data.get("eventId") == event_id:
                    return data
                else:
                    print(f"Ignoring unmatched eventId: {data.get('eventId')}, data: {data}")
            except Exception:
                continue
    finally:
        c.close()

    return None


def register_user(email: str, full_name: str) -> dict | None:
    return _send_and_await(
        TOPIC_USER_REQUESTS,
        TOPIC_USER_LIFECYCLE,
        "UserRegisterRequest",
        {"email": email, "fullName": full_name},
        id_prefix="reg",
    )


def update_user(user_id: str, email: str, full_name: str) -> dict | None:
    return _send_and_await(
        TOPIC_USER_REQUESTS,
        TOPIC_USER_LIFECYCLE,
        "UserUpdateRequest",
        {"userId": user_id, "email": email, "fullName": full_name},
        id_prefix="upd",
    )


def remove_user(user_id: str) -> dict | None:
    return _send_and_await(
        TOPIC_USER_REQUESTS,
        TOPIC_USER_LIFECYCLE,
        "UserRemoveRequest",
        {"userId": user_id},
        id_prefix="rem",
    )


def create_node(user_id: str, district_id: str, node_type: str) -> dict | None:
    return _send_and_await(
        TOPIC_NODE_REQUESTS,
        TOPIC_NODE_LIFECYCLE,
        "NodeCreateRequest",
        {"userId": user_id, "districtId": district_id, "nodeType": node_type},
        id_prefix="ncr",
    )


def update_node(
    node_id: str, user_id: str, district_id: str, node_type: str
) -> dict | None:
    return _send_and_await(
        TOPIC_NODE_REQUESTS,
        TOPIC_NODE_LIFECYCLE,
        "NodeUpdateRequest",
        {"nodeId": node_id, "userId": user_id, "districtId": district_id, "nodeType": node_type},
        id_prefix="nup",
    )


def delete_node(node_id: str, user_id: str) -> dict | None:
    return _send_and_await(
        TOPIC_NODE_REQUESTS,
        TOPIC_NODE_LIFECYCLE,
        "NodeDeleteRequest",
        {"nodeId": node_id, "userId": user_id},
        id_prefix="ndl",
    )


def get_measurement_events(max_messages: int = 200) -> list[dict]:
    ensure_topic_exists(TOPIC_MEASUREMENT_EVENTS)
    return drain_topic(TOPIC_MEASUREMENT_EVENTS, max_messages)


def get_usage_records(max_messages: int = 200) -> list[dict]:
    ensure_topic_exists(TOPIC_USAGE_RECORDS)
    return drain_topic(TOPIC_USAGE_RECORDS, max_messages)
