# ~/legal_ai_infrastructure/legal-ai-implementation/legal-ai-platform/tools/kafka/producer_avro.py
from __future__ import annotations
import time, uuid
from pathlib import Path
from typing import Optional

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from tools.kafka.kafka_config import producer_conf, schema_registry_conf, topic_names

# xác định đường dẫn repo gốc để tìm schemas
ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCHEMA = ROOT / "schemas" / "document_analysis_completed.avsc"

def _load_text(p: Path) -> str:
    with open(p, "r", encoding="utf-8") as f:
        return f.read()

def make_producer(schema_path: Optional[str] = None) -> SerializingProducer:
    """
    Tạo SerializingProducer kèm AvroSerializer theo schema cung cấp (mặc định dùng document_analysis_completed.avsc).
    Yêu cầu .env.dev có SCHEMA_REGISTRY_URL (ví dụ http://localhost:8081) và Kafka bootstrap servers hợp lệ.
    """
    # Schema Registry
    sr_conf = schema_registry_conf()
    sr = SchemaRegistryClient(sr_conf)

    # Schema
    schema_file = Path(schema_path) if schema_path else DEFAULT_SCHEMA
    schema_str = _load_text(schema_file)

    # Avro serializer
    avro_serializer = AvroSerializer(sr, schema_str)

    # Kafka conf + serializer
    conf = producer_conf()
    conf.update({
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
    })
    return SerializingProducer(conf)

def send_sample_document_event() -> None:
    """
    Gửi 1 bản ghi mẫu theo schema DocumentAnalysisCompleted lên topic documents.
    Dùng để smoke-test đường Avro khi đã có Schema Registry.
    """
    tn = topic_names()
    topic = tn["documents"]

    p = make_producer()  # dùng default schema
    record = {
        "eventId": str(uuid.uuid4()),
        "documentId": "DOC-999",
        "analysisVersion": "v1.0",
        "totalFacts": 5,
        "issuesDetected": ["missing signature"],
        "qualityScore": 0.97,
        "occurredAt": int(time.time() * 1000)
    }
    p.produce(topic=topic, key=record["documentId"], value=record)
    p.flush()
    print(f"[OK] sent sample event to topic: {topic}")

if __name__ == "__main__":
    # Chạy trực tiếp file để gửi 1 event mẫu (khi Schema Registry đã chạy)
    send_sample_document_event()
