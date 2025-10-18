#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ontology GraphOps Monitor API (v0.2.0)
Endpoints:
- GET  /health         : Kiểm tra Kafka, topic/DLQ, Neo4j
- GET  /counts         : Đếm node/edge chính
- POST /audit          : Chạy audit và lưu báo cáo JSON vào build/reports/
- GET  /repo           : Thông tin báo cáo audit mới nhất
- GET  /dlq/tail       : Đọc nhanh các bản ghi mới nhất từ DLQ (mặc định n=10)
- GET  /stream/logs    : SSE stream đọc file loop.log (Realtime logs)
- GET  /dashboard      : UI HTML
- GET  /               : Info
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from confluent_kafka import Consumer
from neo4j import GraphDatabase

# các util trong repo
from common.env import Settings
from common.diagnostics import kafka_admin, ensure_topic, check_neo4j
from tools.ontology.audit_integrity import audit as run_audit


APP_NAME = "Ontology GraphOps Monitor Pro"
VERSION = "0.2.0"

app = FastAPI(title=APP_NAME, version=VERSION)

# CORS mở cho tiện quan sát cục bộ
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def jlog(level: str, msg: str, **extra: Any) -> None:
    """Structured log đơn giản cho API monitor."""
    print(
        json.dumps(
            {
                "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "level": level,
                "module": "monitor_api",
                "msg": msg,
                "extra": extra,
            },
            ensure_ascii=False,
        )
    )


def _neo4j_counts(uri: str, user: str, pwd: str) -> Dict[str, int]:
    """Đếm một số label/relationship thường dùng."""
    drv = GraphDatabase.driver(uri, auth=(user, pwd))

    def scalar(q: str) -> int:
        with drv.session() as s:
            rec = s.run(q).single()
            return 0 if rec is None else list(rec.values())[0]

    data = {
        "LawArticle": scalar("MATCH (n:LawArticle) RETURN count(n) AS c"),
        "LawConcept": scalar("MATCH (n:LawConcept) RETURN count(n) AS c"),
        "LawEntity": scalar("MATCH (n:LawEntity) RETURN count(n) AS c"),
        "REFERS_TO": scalar("MATCH ()-[r:REFERS_TO]->() RETURN count(r) AS c"),
    }
    drv.close()
    return data


@app.get("/health")
def health() -> Dict[str, Any]:
    """Preflight: Kafka/Topic/DLQ/Neo4j."""
    s = Settings.load()

    # Kafka
    k_report: Dict[str, Any]
    admin = None
    try:
        admin = kafka_admin(s.KAFKA_BOOTSTRAP_SERVERS)
        md = admin.list_topics(timeout=5)
        k_report = {"ok": True, "brokers": len(md.brokers)}
    except Exception as e:
        k_report = {"ok": False, "error": str(e)}

    # Topic/DLQ
    topics: Dict[str, Any] = {}
    if k_report.get("ok") and admin is not None:
        topic = s.TOPIC_GRAPHOPS
        dlq = f"{topic}.dlq.v1"
        topics = {
            "topic": ensure_topic(admin, topic),
            "dlq": ensure_topic(admin, dlq),
        }

    # Neo4j
    n_report = check_neo4j(s.NEO4J_URI, s.NEO4J_USER, s.NEO4J_PASSWORD)

    ok = (
        k_report.get("ok")
        and n_report.get("ok")
        and topics.get("topic", {}).get("exists", True)
        and topics.get("dlq", {}).get("exists", True)
    )

    out = {"ok": bool(ok), "kafka": k_report, "neo4j": n_report, "topics": topics}
    jlog("info", "health_checked", **out)
    return out


@app.get("/counts")
def counts() -> Dict[str, Any]:
    """Đếm số lượng node/edge hiện có trong Neo4j."""
    s = Settings.load()
    c = _neo4j_counts(s.NEO4J_URI, s.NEO4J_USER, s.NEO4J_PASSWORD)
    jlog("info", "counts", **c)
    return {"ok": True, "counts": c}


@app.post("/audit")
def audit() -> Dict[str, Any]:
    """Chạy audit & lưu báo cáo."""
    s = Settings.load()
    Path("build/reports").mkdir(parents=True, exist_ok=True)
    report = run_audit(s.NEO4J_URI, s.NEO4J_USER, s.NEO4J_PASSWORD)
    dst = f"build/reports/ontology_audit_{int(time.time())}.json"
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    jlog("info", "audit_done", path=dst, ok=report.get("ok"))
    return {"ok": True, "saved": dst, "report": report}


@app.get("/repo", response_class=JSONResponse)
def repo_info() -> Dict[str, Any]:
    """Thông tin báo cáo gần nhất."""
    report_dir = Path("build/reports")
    reports = sorted(
        report_dir.glob("ontology_audit_*.json"),
        key=os.path.getmtime,
        reverse=True,
    )
    if not reports:
        return {"ok": False, "message": "no reports yet"}
    latest = reports[0]
    data: Dict[str, Any] = {}
    try:
        with open(latest, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return {"ok": False, "latest": str(latest), "error": str(e)}
    return {"ok": True, "latest": str(latest), "summary": data.get("summary") or data}


@app.get("/dlq/tail")
def tail_dlq(n: int = 10) -> Dict[str, Any]:
    """Đọc N bản ghi mới nhất từ DLQ (best-effort)."""
    s = Settings.load()
    topic = f"{s.TOPIC_GRAPHOPS}.dlq.v1"

    c = Consumer(
        {
            "bootstrap.servers": s.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"dlq-tail-{int(time.time())}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([topic])

    msgs = []
    start = time.time()
    while len(msgs) < n and time.time() - start < 5:
        m = c.poll(0.5)
        if not m:
            continue
        if m.error():
            continue
        msgs.append(
            {
                "ts": int((m.timestamp() or (0, 0))[1] or 0),
                "headers": dict(m.headers() or []),
                "value": m.value().decode("utf-8", "replace"),
            }
        )
    c.close()
    return {"ok": True, "topic": topic, "count": len(msgs), "messages": msgs}


@app.get("/stream/logs")
async def stream_logs() -> StreamingResponse:
    """SSE stream tail file loop.log (thích hợp gắn vào UI)."""

    async def gen():
        log_path = Path("loop.log")
        pos = 0
        while True:
            if not log_path.exists():
                yield "data: waiting log...\n\n"
                await asyncio.sleep(2)
                continue
            with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                f.seek(pos)
                lines = f.readlines()
                pos = f.tell()
            for ln in lines[-50:]:
                yield f"data: {ln.rstrip()}\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard() -> HTMLResponse:
    """Render giao diện HTML tách riêng file."""
    html = Path("tools/monitor/dashboard.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


@app.get("/")
def root() -> Dict[str, Any]:
    """Info root."""
    return {
        "app": APP_NAME,
        "version": VERSION,
        "endpoints": [
            "/health",
            "/counts",
            "/audit",
            "/repo",
            "/dlq/tail",
            "/stream/logs",
            "/dashboard",
        ],
        "env": {
            "topic": os.getenv("TOPIC_GRAPHOPS", "legal-ontology-graphops"),
            "neo4j_uri": os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        },
    }


# (tuỳ chọn) Banner khi startup để dễ nhận biết đúng bản
@app.on_event("startup")
async def _banner() -> None:
    jlog("info", "startup", app=APP_NAME, version=VERSION)

# --- OPS: Emit / Consume Once / Repo Sync ---
import subprocess
from typing import Optional

def _run(cmd: list[str], cwd: Optional[str] = None, timeout: int = 600) -> dict:
    try:
        p = subprocess.run(
            cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout
        )
        return {
            "ok": p.returncode == 0,
            "returncode": p.returncode,
            "stdout": p.stdout[-8000:],
            "stderr": p.stderr[-8000:],
            "cmd": " ".join(cmd),
        }
    except subprocess.TimeoutExpired as e:
        return {"ok": False, "error": "timeout", "stdout": e.stdout, "stderr": e.stderr}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/ops/emit")
def ops_emit(payload: dict = None):
    """
    Body (optional):
    { "input": "/path/to/file.jsonl", "dry_run": false }
    """
    s = Settings.load()
    input_path = (payload or {}).get("input") or s.ONTOLOGY_FILE
    dry_run = bool((payload or {}).get("dry_run", False))
    cmd = [
        "python", "-m", "tools.ontology.emit_graphops_jsonl",
        "--input", input_path, "--json"
    ]
    if dry_run:
        cmd.append("--dry-run")
    res = _run(cmd)
    jlog("info", "ops_emit", input=input_path, dry_run=dry_run, **res)
    return res

@app.post("/ops/consume-once")
def ops_consume_once(payload: dict = None):
    """
    Body (optional):
    {
      "group_id": "ontology-consumer-once-<ts>",
      "auto_offset_reset": "earliest",
      "idle_timeout_sec": 3
    }
    """
    group_id = (payload or {}).get("group_id") or f"ontology-consumer-once-{int(time.time())}"
    aor = (payload or {}).get("auto_offset_reset", "earliest")
    idle = int((payload or {}).get("idle_timeout_sec", 3))
    cmd = [
        "python", "-m", "services.ontology.ontology_consumer",
        "--json", "--once", "--idle-timeout-sec", str(idle),
        "--auto-offset-reset", aor, "--group-id", group_id
    ]
    res = _run(cmd, timeout=900)
    jlog("info", "ops_consume_once", group_id=group_id, **res)
    return res

@app.post("/ops/repo-sync")
def ops_repo_sync(payload: dict = None):
    """
    Body (optional):
    { "message": "chore: sync from dashboard" }
    - Safe add: chỉ add các file code/tài nguyên dự án; KHÔNG add .env, .venv, build/
    """
    msg = (payload or {}).get("message") or "chore: sync from dashboard"
    # 1) git add (white-list)
    add_paths = [
        "tools/monitor/api.py",
        "tools/monitor/dashboard.html",
        "tools/ontology/audit_integrity.py",
        "tools/ontology/emit_graphops_jsonl.py",
        "services/ontology/ontology_consumer.py",
        "common/",
        "models/",
        "data/ontology/",
        "docker-compose.kafka.yml",
        "docker-compose.neo4j.yml",
        "Makefile",
        "pyproject.toml",
        ".gitignore",
        ".env.example",
        "README.md",
    ]
    res_add = _run(["git", "add"] + add_paths)
    if not res_add.get("ok"):
        return {"ok": False, "step": "git add", **res_add}

    # 2) commit (cho phép no-op nếu không có thay đổi)
    res_commit = _run(["git", "commit", "-m", msg])
    # if nothing to commit, git returns non-zero; ta vẫn tiếp tục push
    # 3) push
    res_push = _run(["git", "push", "-u", "origin", "main"])
    ok = res_push.get("ok", False)
    jlog("info", "ops_repo_sync", message=msg, push_ok=ok, add=res_add, commit=res_commit, push=res_push)
    return {"ok": ok, "add": res_add, "commit": res_commit, "push": res_push}

