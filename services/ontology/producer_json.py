#!/usr/bin/env python3
"""
Emit GraphOps JSONL → Kafka (JSON, not Avro).
- Safe logging JSON, retries, quarantine on parse errors.
- Writes build/reports/emit_stats.json for later verify.
"""
import argparse, json, os, sys, time, uuid, signal, pathlib
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_random_exponential
from confluent_kafka import Producer

MODULE = "producer_json"

def jlog(level: str, msg: str, error_code: int = 0, hint: Optional[str] = None, extra: Optional[dict] = None):
    print(json.dumps({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "level": level, "module": MODULE, "msg": msg,
        "error_code": error_code, "hint": hint, "extra": extra or {}
    }))

def load_env():
    return {
        "kafka_bootstrap": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "topic": os.getenv("TOPIC_GRAPHOPS", "legal-ontology-graphops"),
        "src_file": os.getenv("ONTOLOGY_FILE", "./data/ontology/ontology_thuytinh_dev.jsonl"),
        "report_dir": "build/reports",
        "quarantine_dir": "build/quarantine",
    }

stop_flag = False
def _graceful(*_):
    global stop_flag
    stop_flag = True
    jlog("warning", "graceful shutdown requested")

def ensure_dirs(*paths):
    for p in paths:
        pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def delivery_cb(err, msg):
    if err is not None:
        jlog("error", "delivery failed", error_code=3, hint="Kafka unreachable?", extra={"err": str(err)})
    else:
        if os.getenv("VERBOSE", "") or os.getenv("LOG_LEVEL", "INFO").upper() == "DEBUG":
            jlog("info", "delivered", extra={"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()})

@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(exp_base=2, max=5))
def _build_producer(bootstrap: str) -> Producer:
    conf = {
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "retries": 3,
        "linger.ms": 10,
        "request.timeout.ms": 10000,
        "message.timeout.ms": 120000,
    }
    return Producer(conf)

def main():
    global stop_flag
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--json", action="store_true")  # not used for input format (always JSONL); kept for CLI contract
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _graceful)

    env = load_env()
    ensure_dirs(env["report_dir"], env["quarantine_dir"])

    if args.verbose:
        os.environ["VERBOSE"] = "1"

    src = env["src_file"]
    if not os.path.exists(src):
        jlog("error", "ONTOLOGY_FILE not found", error_code=2, hint=f"Missing file: {src}")
        sys.exit(2)

    sent, bad = 0, 0
    topic = env["topic"]
    correlation = str(uuid.uuid4())

    jlog("info", "starting emit", extra={"file": src, "topic": topic, "correlation_id": correlation})

    producer = None
    if not args.dry_run:
        try:
            producer = _build_producer(env["kafka_bootstrap"])
        except Exception as e:
            jlog("error", "producer init failed", error_code=3, hint="Check Kafka docker-compose", extra={"exc": str(e)})
            sys.exit(3)

    quarantine_path = os.path.join(env["quarantine_dir"], f"emit_quarantine_{int(time.time())}.jsonl")
    with open(src, "r", encoding="utf-8") as fh, open(quarantine_path, "a", encoding="utf-8") as qh:
        for line in fh:
            if stop_flag: break
            line = line.strip()
            if not line: continue
            try:
                record = json.loads(line)
                # minimal validation
                if "op" not in record or not isinstance(record["op"], str):
                    raise ValueError("missing op")
                key = None
                if "node" in record and isinstance(record["node"], dict):
                    key = str(record["node"].get("id") or record["node"].get("props", {}).get("id") or "")
                elif "edge" in record and isinstance(record["edge"], dict):
                    # prefer deterministic key: src_id|type|dst_id
                    e = record["edge"]
                    key = f'{e.get("src_id") or ""}|{e.get("type") or ""}|{e.get("dst_id") or ""}'
                payload = json.dumps({
                    "correlation_id": correlation,
                    "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "data": record
                }, ensure_ascii=False)
            except Exception as e:
                bad += 1
                qh.write(line + "\n")
                jlog("error", "bad JSON line quarantined", error_code=5, hint="See quarantine file", extra={"exc": str(e)})
                continue

            if args.dry_run:
                sent += 1
                if args.verbose:
                    jlog("info", "dry-run enqueue", extra={"key": key})
                continue

            try:
                producer.produce(topic=topic, key=key, value=payload, callback=delivery_cb)
                sent += 1
                producer.poll(0)
            except BufferError:
                producer.flush(2)
                producer.produce(topic=topic, key=key, value=payload, callback=delivery_cb)
                sent += 1
            except Exception as e:
                jlog("error", "produce failed", error_code=3, hint="Kafka backpressure?", extra={"exc": str(e)})
                bad += 1
                qh.write(line + "\n")

    if producer:
        producer.flush()

    stats_path = os.path.join(env["report_dir"], "emit_stats.json")
    with open(stats_path, "w", encoding="utf-8") as sf:
        json.dump({"ts": time.time(), "file": src, "topic": topic, "sent": sent, "bad": bad, "correlation_id": correlation}, sf, indent=2)
    jlog("info", "emit finished", extra={"sent": sent, "bad": bad, "stats": stats_path, "quarantine": quarantine_path})
    sys.exit(0 if bad == 0 else 5)

if __name__ == "__main__":
    main()

