#!/usr/bin/env python3
import argparse, json, os, sys, time, signal, pathlib, uuid
from tenacity import retry, stop_after_attempt, wait_random_exponential
from confluent_kafka import Consumer, Producer, KafkaError
from neo4j import GraphDatabase

MODULE, stop_flag = "ontology_consumer", False

def jlog(level, msg, error_code=0, hint=None, extra=None):
    print(json.dumps({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "level": level, "module": MODULE, "msg": msg,
        "error_code": error_code, "hint": hint, "extra": extra or {}
    }))

def _graceful(*_):
    global stop_flag
    stop_flag = True
    jlog("warning", "graceful_shutdown_requested")

def ensure_dirs(*paths):
    for p in paths:
        pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def envs():
    return {
        "bootstrap": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group_id": os.getenv("CONSUMER_GROUP", "ontology-consumer-v1"),
        "topic": os.getenv("TOPIC_GRAPHOPS", "legal-ontology-graphops"),
        "dlq": os.getenv("DLQ_TOPIC", None),
        "neo4j_uri": os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        "neo4j_user": os.getenv("NEO4J_USER", "neo4j"),
        "neo4j_pass": os.getenv("NEO4J_PASSWORD", "legalai123"),
        "quarantine_dir": "build/quarantine",
        "auto_offset_reset": os.getenv("AUTO_OFFSET_RESET", "earliest"),
    }

@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(exp_base=2, max=5))
def _build_consumer(bootstrap: str, group_id: str, aor: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": aor,             # earliest | latest
        "enable.auto.commit": False,
        "session.timeout.ms": 10000,
        "max.poll.interval.ms": 300000,
        "isolation.level": "read_committed",
        "client.id": f"{MODULE}-{os.getpid()}",
        "enable.partition.eof": True,         # hỗ trợ one-shot/EOF
    })

@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(exp_base=2, max=5))
def _build_producer(bootstrap: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap, "acks": "all", "retries": 3})

def neo4j_driver(uri, user, pwd):
    return GraphDatabase.driver(uri, auth=(user, pwd))

# --- Cypher (Neo4j 5-safe) ---
CYP_NODE_UPSERT = """MERGE (n:`$LABEL` {id: $id}) SET n += $props RETURN n"""
CYP_EDGE_UPSERT = """MERGE (s:`$SRC_LABEL` {id: $src_id})
MERGE (d:`$DST_LABEL` {id: $dst_id})
MERGE (s)-[r:`$TYPE`]->(d) SET r += $props RETURN type(r) AS type"""

def upsert_node(tx, label: str, nid: str, props: dict):
    q = CYP_NODE_UPSERT.replace("$LABEL", label.replace("`",""))
    return tx.run(q, id=nid, props=props).consume()

def upsert_edge(tx, sl: str, sid: str, dl: str, did: str, et: str, props: dict):
    q = (CYP_EDGE_UPSERT
         .replace("$SRC_LABEL", sl.replace("`",""))
         .replace("$DST_LABEL", dl.replace("`",""))
         .replace("$TYPE", et.replace("`","")))
    return tx.run(q, src_id=sid, dst_id=did, props=props).consume()

def _wrap_if_top_level(obj: dict) -> dict:
    # Cho phép cả payload top-level {'op':...} và {'data': {'op':...}}
    if "data" in obj:
        return obj
    return {"correlation_id": obj.get("correlation_id", str(uuid.uuid4())), "data": obj}

def normalize_record(payload: dict) -> dict:
    d = payload.get("data", {})
    if "node" in d:
        n = d["node"]
        label = n.get("label") or (n.get("labels") or [None])[0]
        nid = n.get("id") or (n.get("props") or {}).get("id")
        if not label or not nid:
            raise ValueError("node missing label/id")
        return {"kind": "node", "label": str(label), "id": str(nid), "props": n.get("props") or {}}
    if "edge" in d:
        e = d["edge"]
        et = e.get("type")
        sl, dl, sid, did = e.get("src_label"), e.get("dst_label"), e.get("src_id"), e.get("dst_id")
        if not all([sl, dl, sid, did, et]):
            fin, to = e.get("from", {}), e.get("to", {})
            sl = (fin.get("labels") or [None])[0] or sl
            dl = (to.get("labels") or [None])[0] or dl
            sid = fin.get("id") or sid
            did = to.get("id") or did
        if not all([sl, dl, sid, did, et]):
            raise ValueError("edge missing normalized endpoints/type")
        return {"kind": "edge",
                "src_label": str(sl), "src_id": str(sid),
                "dst_label": str(dl), "dst_id": str(did),
                "type": str(et), "props": e.get("props") or {}}
    raise ValueError("unknown record kind")

def _send_dlq(prod: Producer, topic: str, raw: bytes, reason: str):
    try:
        prod.produce(topic, value=raw, headers={"x-reason": reason})
    except Exception as e:
        jlog("error", "dlq_produce_failed", 3, "Check Kafka", {"exc": str(e)})

def _quarantine(qdir: str, raw: bytes, reason: str):
    ensure_dirs(qdir)
    fn = pathlib.Path(qdir) / f"quarantine_{int(time.time())}_{uuid.uuid4().hex}.jsonl"
    with open(fn, "wb") as f:
        f.write(raw + b"\n")
    jlog("warning", "quarantined_record", 5, reason, {"file": str(fn)})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--group-id", default=None)
    ap.add_argument("--auto-offset-reset", choices=["earliest", "latest"], default=None)
    # --- thêm QoL flags ---
    ap.add_argument("--once", action="store_true", help="Exit when idle for given seconds")
    ap.add_argument("--idle-timeout-sec", type=int, default=3)
    args = ap.parse_args()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _graceful)

    cfg = envs()
    ensure_dirs(cfg["quarantine_dir"])

    topic = cfg["topic"]
    dlq = cfg["dlq"] or f"{topic}.dlq.v1"
    group_id = args.group_id or cfg["group_id"]
    aor = args.auto_offset_reset or cfg["auto_offset_reset"]

    jlog("info", "starting consumer", extra={
        "topic": topic, "dlq": dlq, "group_id": group_id, "auto_offset_reset": aor
    })

    try:
        cons = _build_consumer(cfg["bootstrap"], group_id, aor)
    except Exception as e:
        jlog("error", "consumer_init_failed", 3, "Check Kafka docker-compose", {"exc": str(e)})
        sys.exit(3)

    prod = None
    if not args.dry_run:
        try:
            prod = _build_producer(cfg["bootstrap"])
        except Exception as e:
            jlog("error", "producer_init_failed", 3, "Check Kafka", {"exc": str(e)})
            sys.exit(3)

    try:
        driver = neo4j_driver(cfg["neo4j_uri"], cfg["neo4j_user"], cfg["neo4j_pass"])
    except Exception as e:
        jlog("error", "neo4j_connect_failed", 3, "Check Neo4j bolt & creds", {"exc": str(e)})
        sys.exit(3)

    cons.subscribe([topic])
    jlog("info", "consume_start", extra={"subscribed": [topic]})

    # --- idle timeout for --once
    idle_since = None

    try:
        while not stop_flag:
            msg = cons.poll(1.0)
            now = time.time()

            if msg is None:
                if args.once:
                    if idle_since is None:
                        idle_since = now
                    if now - idle_since >= args.idle_timeout_sec:
                        jlog("info", "idle_timeout_reached", extra={"idle_sec": args.idle_timeout_sec})
                        break
                continue

            # reset idle timer if a message arrived
            idle_since = None

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Partition EOF: an toàn bỏ qua; --once sẽ thoát theo idle timer
                    continue
                jlog("error", "kafka_consume_error", 3, "Broker/partition error", {"err": str(msg.error())})
                continue

            raw = msg.value()
            if raw is None:
                jlog("warning", "skip_null_message")
                cons.commit(message=msg, asynchronous=False)
                continue

            try:
                obj = json.loads(raw.decode("utf-8"))
                rec = normalize_record(_wrap_if_top_level(obj))
                with driver.session() as s:
                    if rec["kind"] == "node":
                        s.execute_write(upsert_node, rec["label"], rec["id"], rec["props"])
                    else:
                        s.execute_write(upsert_edge, rec["src_label"], rec["src_id"],
                                        rec["dst_label"], rec["dst_id"], rec["type"], rec["props"])
                jlog("info", "upsert_ok", extra={"kind": rec["kind"]})
                cons.commit(message=msg, asynchronous=False)
                jlog("debug", "commit_ok")
            except Exception as e:
                reason = str(e)
                jlog("error", "process_failed", 5, "Record moved to DLQ & quarantine", {"exc": reason})
                if prod is not None:
                    _send_dlq(prod, dlq, raw, reason)
                _quarantine(cfg["quarantine_dir"], raw, reason)
                try:
                    cons.commit(message=msg, asynchronous=False)
                except Exception as ce:
                    jlog("warning", "commit_after_error_failed", extra={"exc": str(ce)})

        jlog("warning", "stopping_consumer")
    finally:
        try:
            cons.close()
        except Exception:
            pass
        if prod is not None:
            try:
                prod.flush()
            except Exception:
                pass
        try:
            driver.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()

