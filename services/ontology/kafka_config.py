from __future__ import annotations
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient
from common.env import Settings

def admin_client(settings: Settings) -> AdminClient:
    return AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

def json_producer(settings: Settings) -> Producer:
    return Producer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "linger.ms": 5,
        "acks": "all",
        "retries": 3,
    })

def json_consumer(settings: Settings, group_id: str = "ontology-consumer") -> Consumer:
    return Consumer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
    })
