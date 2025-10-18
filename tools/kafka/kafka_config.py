import os
from dotenv import load_dotenv
load_dotenv()  # đọc .env.dev/.env nếu có

# ---- Topic names ----
def topic_names():
    return {
        "intents":   os.getenv("TOPIC_INTENTS", "legal-ai-intents"),
        "documents": os.getenv("TOPIC_DOCUMENTS", "legal-documents"),
        "ontology":  os.getenv("TOPIC_ONTOLOGY", "legal-ontology-updates"),
        "tasks":     os.getenv("TOPIC_AGENT_TASKS", "agent-coordination-tasks"),
        "dlq_suffix": os.getenv("DLQ_SUFFIX", ".dlq.v1"),
    }

# ---- Base Kafka conf for AdminClient/Producer/Consumer ----
def base_kafka_conf():
    return {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        # idempotent producer options (admin may ignore some)
        "compression.type": os.getenv("PRODUCER_COMPRESSION", "lz4"),
        "enable.idempotence": True,
        "request.timeout.ms": int(os.getenv("REQUEST_TIMEOUT_MS", "30000")),
    }

def producer_conf():
    conf = base_kafka_conf().copy()
    # tune thêm nếu muốn: linger.ms, batch.size, acks
    conf["linger.ms"] = int(os.getenv("PRODUCER_LINGER_MS", "10"))
    conf["batch.size"] = int(os.getenv("PRODUCER_BATCH_SIZE", "131072"))
    conf["acks"] = os.getenv("PRODUCER_ACKS", "all")
    # SASL (nếu cần)
    if conf.get("security.protocol") in ("SASL_SSL", "SASL_PLAINTEXT"):
        conf.update({
            "sasl.mechanisms": os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
            "sasl.username": os.getenv("KAFKA_SASL_USERNAME"),
            "sasl.password": os.getenv("KAFKA_SASL_PASSWORD"),
        })
    tx_id = os.getenv("KAFKA_TRANSACTIONAL_ID")
    if tx_id:
        conf["transactional.id"] = tx_id
    return conf

def consumer_conf(group_id: str):
    conf = base_kafka_conf().copy()
    conf.update({
        "group.id": group_id,
        "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET", "earliest"),
        "enable.auto.commit": False,
        "max.poll.records": int(os.getenv("CONSUMER_MAX_POLL_RECORDS", "200")),
        "max.poll.interval.ms": int(os.getenv("CONSUMER_MAX_POLL_INTERVAL_MS", "300000")),
    })
    return conf

# ---- Schema Registry (for Avro path) ----
def schema_registry_conf():
    url = os.getenv("SCHEMA_REGISTRY_URL")
    basic_auth = os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO")
    c = {}
    if url:
        c["url"] = url
    if basic_auth:
        c["basic.auth.user.info"] = basic_auth
    return c
