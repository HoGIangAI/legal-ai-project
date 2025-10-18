import os, json, asyncio, signal
from aiokafka import AIOKafkaConsumer
from services.ontology.adapter_neo4j_v3rev2 import Neo4jAdapter

TOPIC = os.getenv("TOPIC_GRAPHOPS", "legal-ontology-graphops")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID = os.getenv("GRAPHOPS_GROUP_ID", "ontology_graphops_v1")

adapter = None
consumer = None

async def handle_record(msg):
    # value là bytes -> str -> json
    try:
        payload = json.loads(msg.value.decode("utf-8"))
    except Exception as e:
        print(f"[SKIP] invalid JSON: {e}")
        return

    op = payload.get("op")
    if op == "upsert" and "node" in payload:
        try:
            adapter.upsert_node(payload["node"])
            print(f"[OK] node {payload['node'].get('id')}")
        except Exception as e:
            print(f"[ERR] upsert_node: {e}")
    elif op == "upsert" and "edge" in payload:
        try:
            adapter.upsert_edge(payload["edge"])
            print(f"[OK] edge {payload['edge'].get('type')}")
        except Exception as e:
            print(f"[ERR] upsert_edge: {e}")
    else:
        print(f"[WARN] unsupported payload keys: {list(payload.keys())}")

async def run():
    global adapter, consumer
    adapter = Neo4jAdapter()
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v,  # raw bytes
    )
    await consumer.start()
    print(f"[START] Consuming JSON graph-ops from topic '{TOPIC}'")
    try:
        async for msg in consumer:
            await handle_record(msg)
    finally:
        await consumer.stop()
        adapter.close()

def main():
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, loop.stop)
    loop.run_until_complete(run())

if __name__ == "__main__":
    main()

