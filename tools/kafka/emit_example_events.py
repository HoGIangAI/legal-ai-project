import os, json, asyncio
from aiokafka import AIOKafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","localhost:9092")
TOPIC = os.getenv("TOPIC_ONTOLOGY_UPDATED","v3.ontology.updated")

async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    try:
        events = [
            {
                "src":{"concept_id":"K001","name":"Clause K1","domain":"legal"},
                "dst":{"concept_id":"K002","name":"Clause K2","domain":"legal"},
                "rel":{"type":"CROSSES_DOMAIN","edge_id":"edge-K001-K002","metadata":{"source":"bf"},"version":"v1","kctx":"ctx-bf-1"}
            },
            {
                "src":{"concept_id":"K002","name":"Clause K2","domain":"legal"},
                "dst":{"concept_id":"K003","name":"Clause K3","domain":"legal"},
                "rel":{"type":"SUPERSEDED_BY","edge_id":"edge-K002-K003","metadata":{"note":"sup"},"version":"v2","kctx":"ctx-bf-2"}
            },
            {
                "src":{"concept_id":"K004","name":"Clause K4","domain":"legal"},
                "dst":{"concept_id":"K003","name":"Clause K3","domain":"legal"},
                "rel":{"type":"CONFLICTS_WITH","edge_id":"edge-K004-K003","metadata":{"conf":"dup"},"version":"v1","kctx":"ctx-bf-3"}
            }
        ]
        for e in events:
            await producer.send_and_wait(TOPIC, e)
            print("sent:", e["rel"]["edge_id"])
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
