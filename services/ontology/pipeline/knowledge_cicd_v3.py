import os, json, time, logging
from services.graph.graph_client import GraphClient
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","localhost:9092")
TOPIC_IN = os.getenv("TOPIC_ONTOLOGY_UPDATED","v3.ontology.updated")
TOPIC_OUT = os.getenv("TOPIC_KNOWLEDGE_VALIDATED","v3.knowledge.validated")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("knowledge_cicd")

def validate_relation(event):
    """Kiểm tra logic quan hệ cơ bản theo ontology_thuytinh_*"""
    rel_type = event["rel"]["type"]
    src, dst = event["src"], event["dst"]
    # Ràng buộc “thủy tinh”: chỉ chấp nhận 3 loại edge, không vòng lặp, có domain rõ ràng
    if rel_type not in ("CROSSES_DOMAIN", "SUPERSEDED_BY", "CONFLICTS_WITH"):
        return False, f"Invalid relation type {rel_type}"
    if src["concept_id"] == dst["concept_id"]:
        return False, "Self-loop not allowed"
    if not src.get("domain") or not dst.get("domain"):
        return False, "Missing domain info"
    return True, "OK"

def main():
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=BOOTSTRAP.split(","),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="knowledge_cicd_v3",
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    graph = GraphClient()

    logger.info(f"[CI/CD] Listening {TOPIC_IN} ...")
    for msg in consumer:
        evt = msg.value
        ok, note = validate_relation(evt)
        evt["validation"] = {"ok": ok, "note": note, "ts": time.time()}
        if ok:
            graph.neighbors(evt["src"]["concept_id"])  # warm-up query
            producer.send(TOPIC_OUT, evt)
            logger.info(f"✅ validated & published: {evt['rel']['type']}")
        else:
            logger.warning(f"❌ invalid event: {note}")
    graph.close()

if __name__ == "__main__":
    main()
