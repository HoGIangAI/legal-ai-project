import sys; print(sys.executable); print(sys.path)
import os, json, asyncio
from aiokafka import AIOKafkaConsumer
from services.ontology.adapter_neo4j_v3rev2 import Neo4jAdapter

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "legalai123")
NEO4J_DB = os.getenv("NEO4J_DB", "neo4j")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC_ONTOLOGY", "legal-ontology-updates")

async def handle_event(adapter: Neo4jAdapter, evt: dict):
    op = evt.get("op", "upsert")
    if "node" in evt:
        n = evt["node"]
        if op == "delete":
            adapter.delete_node(n["label"], n["id"])
        else:
            adapter.upsert_node(n["label"], n["id"], n.get("props", {}))
    elif "edge" in evt:
        e = evt["edge"]
        if op == "delete":
            # Bản rút gọn: không xóa edge ở bước mẫu
            pass
        else:
            adapter.upsert_edge(e["from"]["label"], e["from"]["id"],
                                e["type"],
                                e["to"]["label"], e["to"]["id"],
                                e.get("props", {}))

async def main():
    adapter = Neo4jAdapter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DB)
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        group_id="ontology_v3rev2"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await handle_event(adapter, msg.value)
    finally:
        await consumer.stop()
        adapter.close()

if __name__ == "__main__":
    asyncio.run(main())

