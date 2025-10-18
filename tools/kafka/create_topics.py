# tools/kafka/create_topics.py
import os
from confluent_kafka.admin import AdminClient, NewTopic
from tools.kafka.kafka_config import base_kafka_conf, topic_names, default_sizing

def main():
    single_broker = os.getenv("SINGLE_BROKER", "true").lower() in ("1", "true", "yes")
    conf = base_kafka_conf()
    admin = AdminClient(conf)
    tn = topic_names()
    sizing = default_sizing(single_broker=single_broker)

    topics_to_create = {
        tn["intents"]:   sizing["legal-ai-intents"],
        tn["documents"]: sizing["legal-documents"],
        tn["ontology"]:  sizing["legal-ontology-updates"],
        tn["tasks"]:     sizing["agent-coordination-tasks"],
    }

    dlq_suffix = tn["dlq_suffix"]
    for base in list(topics_to_create.keys()):
        topics_to_create[f"{base}{dlq_suffix}"] = {
            "partitions": 6,
            "replication_factor": sizing["legal-ai-intents"]["replication_factor"],
            "configs": {"retention.ms": str(30*24*3600*1000)}
        }

        new_topics = [
        NewTopic(
            topic=name,  # ⬅⬅⬅ đổi 'name=' thành 'topic='
            num_partitions=cfg["partitions"],
            replication_factor=cfg["replication_factor"],
            config=cfg["configs"]
        )
        for name, cfg in topics_to_create.items()
    ]


    print("[INFO] Creating topics...")
    futures = admin.create_topics(new_topics, request_timeout=30)
    for t, f in futures.items():
        try:
            f.result()
            print(f"[OK] created {t}")
        except Exception as e:
            print(f"[WARN] {t}: {e}")

    md = admin.list_topics(timeout=10)
    print("\n[INFO] Current topics:")
    for t in sorted(md.topics.keys()):
        print(" -", t)

if __name__ == "__main__":
    main()

