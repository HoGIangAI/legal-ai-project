import os, json
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","localhost:9092")
TOPIC = os.getenv("TOPIC_ONTOLOGY_UPDATED","v3.ontology.updated")

p = KafkaProducer(bootstrap_servers=BOOTSTRAP.split(","), value_serializer=lambda v: json.dumps(v).encode("utf-8"))
events = [
    {"src":{"concept_id":"K001","name":"Clause K1","domain":"legal"},"dst":{"concept_id":"K002","name":"Clause K2","domain":"legal"},"rel":{"type":"CROSSES_DOMAIN","edge_id":"edge-K001-K002","metadata":{"source":"bf"},"version":"v1","kctx":"ctx-bf-1"}},
    {"src":{"concept_id":"K002","name":"Clause K2","domain":"legal"},"dst":{"concept_id":"K003","name":"Clause K3","domain":"legal"},"rel":{"type":"SUPERSEDED_BY","edge_id":"edge-K002-K003","metadata":{"note":"sup"},"version":"v2","kctx":"ctx-bf-2"}},
    {"src":{"concept_id":"K004","name":"Clause K4","domain":"legal"},"dst":{"concept_id":"K003","name":"Clause K3","domain":"legal"},"rel":{"type":"CONFLICTS_WITH","edge_id":"edge-K004-K003","metadata":{"conf":"dup"},"version":"v1","kctx":"ctx-bf-3"}}
]
for e in events:
    p.send(TOPIC, e).get(timeout=10)
    print("sent:", e["rel"]["edge_id"])
p.flush()
