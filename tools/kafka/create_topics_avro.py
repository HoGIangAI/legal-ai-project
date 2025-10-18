import os
from confluent_kafka.admin import AdminClient, NewTopic
from tools.kafka.kafka_config import base_kafka_conf, topic_names

def main():
    conf = base_kafka_conf()
    admin = AdminClient(conf)
    tn = topic_names()

    # sizing mặc định (DEV: single broker => RF=1)
    sizing = {
        tn["intents"]:   {"partitions": 6, "replication_factor": 1, "configs": {"retention.ms": str(7*24*3600*1000)}},
        tn["documents"]: {"partitions": 6, "replication_factor": 1, "configs": {"retention.ms": str(7*24*3600*1000)}},
        tn["ontology"]:  {"partitions": 6, "replication_factor": 1, "configs": {"cleanup.policy": "compact,delete", "min.cleanable.dirty.ratio": "0.1"}},
        tn["tasks"]:     {"partitions": 6, "replication_factor": 1, "configs": {"retention.ms": str(7*24*3600*1000)}},
    }
    # DLQ
    dlqs = { f"{t}{tn['dlq_suffix']}": {"partitions": 3, "replication_factor": 1, "configs":{"retention.ms": str(30*24*3600*1000)}}
             for t in [tn["intents"], tn["documents"], tn["ontology"], tn["tasks"]]}

    # hợp nhất
    to_create = {**sizing, **dlqs}

    new_topics = []
    for name, spec in to_create.items():
        new_topics.append(NewTopic(topic=name,
                                   num_partitions=spec["partitions"],
                                   replication_factor=spec["replication_factor"],
                                   config=spec.get("configs", {})))

    fs = admin.create_topics(new_topics, request_timeout=30)
    for t, f in fs.items():
        try:
            f.result()
            print(f"[OK] created: {t}")
        except Exception as e:
            print(f"[INFO] {t}: {e}")  # đã tồn tại, hoặc lỗi nhẹ -> in thông tin

if __name__ == "__main__":
    main()
