#!/usr/bin/env python3
"""
Diagnostics cho Legal AI GraphOps:
- Kafka: kết nối, topic chính & DLQ (tự tạo nếu thiếu).
- Neo4j: kết nối Bolt.
- Xuất JSON report vào build/reports/diagnostics_*.json
"""

import os, sys, json, time, pathlib, socket
from typing import Dict, Any
from confluent_kafka.admin import AdminClient, NewTopic
from neo4j import GraphDatabase
from common.env import Settings

EXIT_OK = 0
EXIT_ENV = 2
EXIT_CONN = 3

MODULE = "diagnostics"

def jlog(level, msg, error_code=0, hint=None, extra=None):
    print(json.dumps({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "level": level, "module": MODULE, "msg": msg,
        "error_code": error_code, "hint": hint, "extra": extra or {}
    }))

def kafka_admin(bootstrap: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap})

def ensure_topic(admin: AdminClient, name: str, partitions: int = 1, rf: int = 1) -> Dict[str, Any]:
    md = admin.list_topics(timeout=5)
    if name in md.topics:
        return {"name": name, "exists": True, "created": False}
    fs = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=rf)])
    try:
        fs[name].result(5)
        return {"name": name, "exists": True, "created": True}
    except Exception as e:
        return {"name": name, "exists": False, "created": False, "error": str(e)}

def check_neo4j(uri: str, user: str, pwd: str) -> Dict[str, Any]:
    try:
        driver = GraphDatabase.driver(uri, auth=(user, pwd))
        with driver.session() as s:
            val = s.run("RETURN 1 AS ok").single()["ok"]
        driver.close()
        return {"ok": bool(val)}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def main():
    try:
        settings = Settings.load()
    except Exception as e:
        jlog("error", "settings_invalid", EXIT_ENV, "Check environment variables", {"exc": str(e)})
        sys.exit(EXIT_ENV)

    pathlib.Path("build/reports").mkdir(parents=True, exist_ok=True)
    report = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S"), "checks": {}, "ok": True}

    # Kafka
    try:
        admin = kafka_admin(settings.KAFKA_BOOTSTRAP_SERVERS)
        md = admin.list_topics(timeout=5)
        report["checks"]["kafka"] = {"ok": True, "brokers": len(md.brokers)}
        jlog("info", "kafka_ok", extra={"brokers": len(md.brokers)})
    except Exception as e:
        report["checks"]["kafka"] = {"ok": False, "error": str(e)}
        report["ok"] = False
        jlog("error", "kafka_connect_failed", EXIT_CONN, "Check docker-compose.kafka.yml", {"exc": str(e)})

    # Topic & DLQ
    if report["checks"].get("kafka", {}).get("ok"):
        topic = settings.TOPIC_GRAPHOPS
        dlq = f"{topic}.dlq.v1"
        topic_res = ensure_topic(admin, topic)
        dlq_res = ensure_topic(admin, dlq)
        report["checks"]["topics"] = {"topic": topic_res, "dlq": dlq_res}
        if not topic_res.get("exists") or not dlq_res.get("exists"):
            report["ok"] = False
        jlog("info", "topics_checked", extra={"topic": topic_res, "dlq": dlq_res})

    # Neo4j
    n4j = check_neo4j(settings.NEO4J_URI, settings.NEO4J_USER, settings.NEO4J_PASSWORD)
    report["checks"]["neo4j"] = n4j
    if not n4j["ok"]:
        report["ok"] = False
        jlog("error", "neo4j_connect_failed", EXIT_CONN, "Check Neo4j docker-compose & creds", {"exc": n4j.get("error")})
    else:
        jlog("info", "neo4j_ok")

    out = f"build/reports/diagnostics_{int(time.time())}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    jlog("info", "diagnostics_done", extra={"output": out, "ok": report["ok"]})
    sys.exit(EXIT_OK if report["ok"] else EXIT_CONN)

if __name__ == "__main__":
    main()

