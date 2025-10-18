import os, json, time, uuid
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

def main():
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    topic = os.getenv("TOPIC_ONTOLOGY", "legal-ontology-updates")
    input_file = os.getenv("ONTOLOGY_FILE", "data/ontology/ontology_thuytinh_dev.jsonl")

    print(f"[START] Emitting events from: {input_file}")
    schema_str = r'''
    {
      "type":"record",
      "name":"OntologyUpdated",
      "namespace":"ai.legal.events",
      "fields":[
        {"name":"eventId","type":"string"},
        {"name":"legalDocId","type":"string"},
        {"name":"changeType", "type":{"type":"enum","name":"ChangeType","symbols":["MINOR","MAJOR","AMENDMENT","REPLACEMENT"]}},
        {"name":"version","type":"string"},
        {"name":"occurredAt", "type":{"type":"long","logicalType":"timestamp-millis"}}
      ]
    }
    '''

    sr = SchemaRegistryClient({"url": schema_registry_url})
    avro_ser = AvroSerializer(sr, schema_str)
    producer = SerializingProducer({
        "bootstrap.servers": kafka_bootstrap,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_ser
    })

    count = 0
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception as e:
                print(f"[WARN] Skipped invalid JSON: {e}")
                continue

            event = {
                "eventId": str(uuid.uuid4()),
                "legalDocId": str(payload.get("id", f"auto-{count}")),
                "changeType": "MAJOR",
                "version": "v3.0.0",
                "occurredAt": int(time.time() * 1000)
            }

            try:
                producer.produce(topic=topic, key=event["legalDocId"], value=event)
                count += 1
            except Exception as e:
                print(f"[ERR] produce(): {e}")
                continue

    producer.flush(10)
    print(f"[DONE] Emitted {count} OntologyUpdated events to topic '{topic}'")

if __name__ == "__main__":
    main()

