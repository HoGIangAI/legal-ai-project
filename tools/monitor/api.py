#!/usr/bin/env python3
from __future__ import annotations
import os, json, time, pathlib
from pathlib import Path
from typing import Any, Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse

from neo4j import GraphDatabase
from common.env import Settings
from common.diagnostics import kafka_admin, ensure_topic, check_neo4j
from tools.ontology.audit_integrity import audit as run_audit

"""
Ontology GraphOps Monitor API
Endpoints:
- /health: kiểm tra Kafka, Neo4j, Topic, DLQ
- /counts: đếm node/edge
- /audit : kiểm tra toàn vẹn graph
- /repo  : lấy thông tin report gần nhất
- /dashboard: giao diện HTML
"""

# ============================================================
APP_NAME = "Ontology GraphOps Monitor"
app = FastAPI(title=APP_NAME, version="0.1.0")
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------
# Logging helper
# ------------------------------------------------------------
def jlog(level: str, msg: str, **extra: Any) -> None:
    print(json.dumps({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "level": level,
        "module": "monitor_api",
        "msg": msg,
        "extra": extra
    }))

# ------------------------------------------------------------
# Neo4j counts helper
# ------------------------------------------------------------
def _neo4j_counts(uri: str, user: str, pwd: str) -> Dict[str, int]:
    drv = GraphDatabase.driver(uri, auth=(user, pwd))
    def scalar(q: str) -> int:
        with drv.session() as s:
            rec = s.run(q).single()
            return 0 if rec is None else list(rec.values())[0]
    data = {
        "LawArticle": scalar("MATCH (n:LawArticle) RETURN count(n) AS c"),
        "LawConcept": scalar("MATCH (n:LawConcept) RETURN count(n) AS c"),
        "LawEntity":  scalar("MATCH (n:LawEntity) RETURN count(n) AS c"),
        "REFERS_TO":  scalar("MATCH ()-[r:REFERS_TO]->() RETURN count(r) AS c"),
    }
    drv.close()
    return data

# ------------------------------------------------------------
# API ROUTES
# ------------------------------------------------------------

@app.get("/health")
def health():
    settings = Settings.load()
    k_report, topics = {}, {}

    try:
        admin = kafka_admin(settings.KAFKA_BOOTSTRAP_SERVERS)
        md = admin.list_topics(timeout=5)
        k_report = {"ok": True, "brokers": len(md.brokers)}
        topic = settings.TOPIC_GRAPHOPS
        dlq = f"{topic}.dlq.v1"
        topics = {
            "topic": ensure_topic(admin, topic),
            "dlq": ensure_topic(admin, dlq),
        }
    except Exception as e:
        k_report = {"ok": False, "error": str(e)}

    n_report = check_neo4j(settings.NEO4J_URI, settings.NEO4J_USER, settings.NEO4J_PASSWORD)

    ok = all([
        k_report.get("ok"),
        n_report.get("ok"),
        topics.get("topic", {}).get("exists", True),
        topics.get("dlq", {}).get("exists", True),
    ])

    out = {"ok": ok, "kafka": k_report, "topics": topics, "neo4j": n_report}
    jlog("info", "health_checked", **out)
    return out

@app.get("/counts")
def counts():
    settings = Settings.load()
    c = _neo4j_counts(settings.NEO4J_URI, settings.NEO4J_USER, settings.NEO4J_PASSWORD)
    jlog("info", "counts", **c)
    return {"ok": True, "counts": c}

@app.post("/audit")
def audit():
    settings = Settings.load()
    pathlib.Path("build/reports").mkdir(parents=True, exist_ok=True)
    report = run_audit(settings.NEO4J_URI, settings.NEO4J_USER, settings.NEO4J_PASSWORD)
    dst = f"build/reports/ontology_audit_{int(time.time())}.json"
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    jlog("info", "audit_done", path=dst, ok=report.get("ok"))
    return {"ok": True, "saved": dst, "report": report}

@app.get("/repo", response_class=JSONResponse)
def repo_info():
    report_dir = Path("build/reports")
    reports = sorted(report_dir.glob("ontology_audit_*.json"), key=os.path.getmtime, reverse=True)
    latest = reports[0] if reports else None
    data = {}
    if latest:
        with open(latest, "r", encoding="utf-8") as f:
            data = json.load(f)
    return {"ok": bool(latest), "latest_report": str(latest), "summary": data.get("report", {}).get("summary", {})}

# ------------------------------------------------------------
# DASHBOARD HTML
# ------------------------------------------------------------
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return Path(__file__).with_name("dashboard.html").read_text(encoding="utf-8")

@app.get("/")
def root():
    return {
        "app": APP_NAME,
        "endpoints": ["/health", "/counts", "/audit", "/repo", "/dashboard"],
        "env": {
            "topic": os.getenv("TOPIC_GRAPHOPS", "legal-ontology-graphops"),
            "neo4j_uri": os.getenv("NEO4J_URI", "bolt://localhost:7687")
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("tools.monitor.api:app", host="0.0.0.0", port=8088, reload=True)

