import json
import os
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC_ONTOLOGY", "legal-ontology-updates")

events = [
    {
        "event_id": "e1",
        "action": "CREATE_EDGE",
        "source_id": "K001",
        "target_id": "K002",
        "relation": "RELATED_TO",
        "properties": {"score": 0.87, "note": "Hợp đồng tín dụng ↔ Lãi suất phạt"}
    },
    {
        "event_id": "e2",
        "action": "CREATE_NODE",
        "source_id": "K003",
        "properties": {"name": "Điều khoản phạt chậm trả"}
    },
    {
        "event_id": "e3",
        "action": "LINK_NODE",
        "source_id": "K003",
        "target_id": "K001",
        "relation": "REFERENCES",
        "properties": {"weight": 0.66}
    }
]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for e in events:
    producer.send(TOPIC, e)
    print(f"sent: {e['event_id']}")

producer.flush()
print("done.")
