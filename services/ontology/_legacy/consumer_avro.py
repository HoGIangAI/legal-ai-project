# ruff: noqa
# ruff: noqa
import time
import uuid
from confluent_kafka.schema_registry import SchemaRegistryClient
from tools.kafka.kafka_config import producer_conf, schema_registry_conf, topic_names


def load_schema(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def make_producer():
    return SerializingProducer(
        {
            **producer_conf(),
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": None,  # set per-message with AvroSerializer
        }
    )


def get_avro_serializer(sr, schema_str):
    return AvroSerializer(sr, schema_str)


def delivery_cb(err, msg):
    if err:
        print(f"[DELIVERY-ERROR] {err}")
    else:
        print(f"[DELIVERED] {msg.topic()}[{msg.partition()}]@{msg.offset()} key={msg.key()}")


def main():
    tn = topic_names()
    sr = SchemaRegistryClient(schema_registry_conf())
    doc_schema = get_avro_serializer(sr, load_schema("schemas/document_analysis_completed.avsc"))
    intent_schema = get_avro_serializer(sr, load_schema("schemas/intent_classified.avsc"))

    p = make_producer()
    tx_id = os.getenv("KAFKA_TRANSACTIONAL_ID", "")
    if tx_id:
        p.init_transactions()
        p.begin_transaction()

    # Ví dụ gửi 1 event documents (key=documentId)
    doc_event = {
        "eventId": str(uuid.uuid4()),
        "documentId": "doc-123",
        "analysisVersion": "v1.0.0",
        "totalFacts": 42,
        "issuesDetected": ["missing_signature", "date_conflict"],
        "qualityScore": 0.91,
        "occurredAt": int(time.time() * 1000),
    }
    p.produce(
        topic=tn["documents"],
        key=doc_event["documentId"],
        value=doc_event,
        value_serializer=doc_schema,
        on_delivery=delivery_cb,
        headers=[("content-type", "avro")],
    )

    # Ví dụ gửi 1 event intents (key=sessionId)
    intent_event = {
        "eventId": str(uuid.uuid4()),
        "sessionId": "sess-789",
        "text": "Hỏi về điều kiện thành lập công ty",
        "domain": "doanh_nghiep",
        "confidence": 0.88,
        "occurredAt": int(time.time() * 1000),
    }
    p.produce(
        topic=tn["intents"],
        key=intent_event["sessionId"],
        value=intent_event,
        value_serializer=intent_schema,
        on_delivery=delivery_cb,
        headers=[("content-type", "avro")],
    )

    p.flush()
    if tx_id:
        p.commit_transaction()


if __name__ == "__main__":
    main()
