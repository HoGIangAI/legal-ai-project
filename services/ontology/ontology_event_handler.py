import os, json, time
from kafka import KafkaConsumer
from adapter_neo4j_v3rev2 import OntologyAdapter

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","localhost:9092")
TOPICS = [os.getenv("TOPIC_KNOWLEDGE_UPDATED","v3.knowledge.updated"),
          os.getenv("TOPIC_ONTOLOGY_UPDATED","v3.ontology.updated")]
GROUP_ID = os.getenv("GROUP_ID","ontology-sync-neo4j-v3")

def main():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP.split(","),
        group_id=GROUP_ID,
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        max_poll_records=32,
    )
    print(f"[Consumer] Listening {TOPICS} on {BOOTSTRAP} group={GROUP_ID}")
    adapter = OntologyAdapter()
    try:
        for msg in consumer:
            evt = msg.value
            try:
                adapter.ingest_event(evt)
                print("[OK] upsert", evt.get("rel",{}).get("type"), evt.get("rel",{}).get("edge_id"))
            except Exception as e:
                print("[ERR] ingest failed:", e, "event=", evt)
            time.sleep(0.02)
    except KeyboardInterrupt:
        print("[STOP] by user")
    finally:
        adapter.close()

if __name__ == "__main__":
    main()
