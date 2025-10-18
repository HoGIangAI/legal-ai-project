#!/usr/bin/env python3
import asyncio
from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import os, json, time
from pathlib import Path
from typing import Any, Dict
from confluent_kafka import Consumer
from common.env import Settings
from common.diagnostics import kafka_admin, ensure_topic, check_neo4j
from tools.ontology.audit_integrity import audit as run_audit
from neo4j import GraphDatabase
from confluent_kafka import Producer

APP_NAME = "Ontology GraphOps Monitor Pro"
app = FastAPI(title=APP_NAME, version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# === utility log helper ===
def jlog(level: str, msg: str, **extra: Any):
    print(json.dumps({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "level": level, "module": "monitor_api", "msg": msg, "extra": extra
    }))

# === core helpers ===
def _neo4j_counts(uri: str, user: str, pwd: str) -> Dict[str,int]:
    drv = GraphDatabase.driver(uri, auth=(user, pwd))
    def scalar(q): 
        with drv.session() as s:
            rec = s.run(q).single()
            return 0 if rec is None else list(rec.values())[0]
    data = {
        "LawArticle": scalar("MATCH (n:LawArticle) RETURN count(n) AS c"),
        "LawConcept": scalar("MATCH (n:LawConcept) RETURN count(n) AS c"),
        "LawEntity" : scalar("MATCH (n:LawEntity) RETURN count(n) AS c"),
        "REFERS_TO" : scalar("MATCH ()-[r:REFERS_TO]->() RETURN count(r) AS c"),
    }
    drv.close()
    return data

# === endpoints ===
@app.get("/health")
def health():
    s = Settings.load()
    rep = {"ok": True}
    admin = None  # 👈 đảm bảo biến tồn tại

    # Kafka check
    try:
        admin = kafka_admin(s.KAFKA_BOOTSTRAP_SERVERS)
        md = admin.list_topics(timeout=5)
        rep["kafka"] = {"ok": True, "brokers": len(md.brokers)}
    except Exception as e:
        rep["kafka"] = {"ok": False, "error": str(e)}
        rep["ok"] = False

    # Neo4j check
    rep["neo4j"] = check_neo4j(s.NEO4J_URI, s.NEO4J_USER, s.NEO4J_PASSWORD)

    # Topic & DLQ check (chỉ khi Kafka OK)
    topic = s.TOPIC_GRAPHOPS
    dlq   = f"{topic}.dlq.v1"
    if rep["kafka"].get("ok") and admin is not None:
        rep["topics"] = {
            "topic": ensure_topic(admin, topic),
            "dlq":   ensure_topic(admin, dlq)
        }
    else:
        rep["topics"] = {"topic": {"exists": False}, "dlq": {"exists": False}}
        rep["ok"] = False

    return rep

@app.post("/emit/sample")
def emit_sample():
    s = Settings.load()
    try:
        prod = Producer({"bootstrap.servers": s.KAFKA_BOOTSTRAP_SERVERS, "acks": "all"})
        sample = {
            "op": "upsert",
            "edge": {
                "type": "REFERS_TO",
                "src_label": "LawArticle", "src_id": "A001",
                "dst_label": "LawConcept", "dst_id": "K001",
                "props": {"note": "ui-sample"}
            }
        }
        payload = json.dumps(sample, ensure_ascii=False).encode("utf-8")
        prod.produce(s.TOPIC_GRAPHOPS, value=payload)
        prod.flush(3)
        return {"ok": True, "topic": s.TOPIC_GRAPHOPS, "sent": sample}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/counts")
def counts():
    s = Settings.load()
    c = _neo4j_counts(s.NEO4J_URI, s.NEO4J_USER, s.NEO4J_PASSWORD)
    return {"ok": True, "counts": c}

@app.post("/audit")
def audit():
    s = Settings.load()
    Path("build/reports").mkdir(parents=True, exist_ok=True)
    rep = run_audit(s.NEO4J_URI, s.NEO4J_USER, s.NEO4J_PASSWORD)
    dst = f"build/reports/ontology_audit_{int(time.time())}.json"
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2)
    jlog("info", "audit_done", path=dst)
    return {"ok": True, "saved": dst, "report": rep}

@app.get("/repo")
def repo_info():
    report_dir = Path("build/reports")
    reports = sorted(report_dir.glob("ontology_audit_*.json"),
                     key=os.path.getmtime, reverse=True)
    if not reports: 
        return {"ok": False, "message": "no reports yet"}
    latest = reports[0]
    with open(latest,"r",encoding="utf-8") as f: data = json.load(f)
    return {"ok": True, "latest": str(latest), "summary": data.get("summary",{})}

@app.get("/dlq/tail")
def tail_dlq(n: int = 10):
    s = Settings.load()
    topic = f"{s.TOPIC_GRAPHOPS}.dlq.v1"
    c = None
    msgs = []
    try:
        c = Consumer({
            "bootstrap.servers": s.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"dlq-tail-{int(time.time())}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        c.subscribe([topic])
        start = time.time()
        while len(msgs) < n and time.time() - start < 5:
            m = c.poll(0.5)
            if not m:
                continue
            if m.error():
                # Bỏ qua lỗi lặt vặt; có thể điền m.error().str() nếu muốn
                continue
            msgs.append({
                "ts": int((m.timestamp() or (0,0))[1] or 0),
                "headers": dict(m.headers() or []),
                "value": m.value().decode("utf-8", "replace"),
            })
        return {"ok": True, "topic": topic, "count": len(msgs), "messages": msgs}
    except Exception as e:
        return {"ok": False, "topic": topic, "error": str(e), "messages": []}
    finally:
        if c is not None:
            try: c.close()
            except Exception: pass

@app.get("/stream/logs")
async def stream_logs():
    """Simple SSE streamer reading tail -f from loop.log."""
    async def gen():
        log_path = Path("loop.log")
        pos = 0
        while True:
            if not log_path.exists():
                yield f"data: waiting log...\n\n"; await asyncio.sleep(2); continue
            with open(log_path) as f:
                f.seek(pos)
                lines=f.readlines()
                pos=f.tell()
            for ln in lines[-5:]:
                yield f"data: {ln.strip()}\n\n"
            await asyncio.sleep(2)
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    p = Path("tools/monitor/dashboard.html")
    if p.exists():
        return HTMLResponse(p.read_text(encoding="utf-8"))
    # Fallback tối thiểu
    return HTMLResponse("<h1>Ontology GraphOps Monitor</h1><p>dashboard.html not found.</p>")

