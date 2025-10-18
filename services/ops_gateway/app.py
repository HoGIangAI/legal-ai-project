import os, time, json, subprocess, uuid
from typing import List, Deque
from collections import deque
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from neo4j import GraphDatabase
from kafka import KafkaProducer

APP_NAME = "ops-gateway"
NEO4J_URI = os.getenv("NEO4J_URI","bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER","neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD","neo4j")
NEO4J_DB = os.getenv("NEO4J_DB","neo4j")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","localhost:9092")
TOPIC_ONTOLOGY_UPDATED = os.getenv("TOPIC_ONTOLOGY_UPDATED","v3.ontology.updated")
TOPIC_KNOWLEDGE_VALIDATED = os.getenv("TOPIC_KNOWLEDGE_VALIDATED","v3.knowledge.validated")
DLQ_TOPIC = os.getenv("TOPIC_DLQ","v3.ontology.updated.dlq.v1")

app = FastAPI(title=APP_NAME)

# --- Clients ---
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# --- Models ---
class LogItem(BaseModel):
    ts: float = Field(default_factory=lambda: time.time())
    level: str = "INFO"
    source: str = APP_NAME
    message: str

class LogBatch(BaseModel):
    items: List[LogItem]

class OntologyNode(BaseModel):
    concept_id: str
    name: str | None = None
    domain: str | None = None

class OntologyRel(BaseModel):
    type: str
    edge_id: str
    version: str | None = None
    kctx: str | None = None
    metadata: dict | None = None

class OntologyEvent(BaseModel):
    src: OntologyNode
    dst: OntologyNode
    rel: OntologyRel

# ---- Incident (để gom log nhiều vòng) ----
class IncidentItem(BaseModel):
    message: str
    level: str = "INFO"
    source: str = "cli"
    ts: float = Field(default_factory=lambda: time.time())

class IncidentCreate(BaseModel):
    title: str
    items: List[IncidentItem]

class IncidentAppend(BaseModel):
    incident_id: str
    items: List[IncidentItem]

INCIDENTS: dict[str, Deque[IncidentItem]] = {}
INCIDENT_TITLES: dict[str, str] = {}

def summarize_errors(lines: List[str], max_out: int = 20):
    keys = ("error", "exception", "traceback", "failed", "neo4j.exceptions", "module not found")
    matched = [l for l in lines if any(k in l.lower() for k in keys)]
    return matched[:max_out]

def _summarize(items: List[IncidentItem], max_lines=20):
    lines = [f"[{i.level}] {i.source}: {i.message}" for i in items]
    errs = summarize_errors(lines, max_lines)
    tail = lines[-max_lines:]
    snippet = "INCIDENT SUMMARY\n" \
              f"- errors: {len(errs)}\n" \
              f"- total_lines: {len(lines)}\n" \
              "Top errors:\n" + "\n".join(f"  - {e}" for e in errs[:5])
    return {"error_snippets": errs, "tail": tail, "count": len(lines)}, snippet

# ---- Core endpoints ----
@app.get("/health")
def health():
    try:
        driver.verify_connectivity()
        ok = True
    except Exception:
        ok = False
    return {"service": APP_NAME, "ok": ok, "ts": time.time()}

@app.get("/config")
def config():
    return {
        "NEO4J_URI": NEO4J_URI, "NEO4J_DB": NEO4J_DB,
        "KAFKA_BOOTSTRAP": KAFKA_BOOTSTRAP,
        "topics": {"ontology_updated": TOPIC_ONTOLOGY_UPDATED, "knowledge_validated": TOPIC_KNOWLEDGE_VALIDATED, "dlq": DLQ_TOPIC}
    }

@app.get("/observability/neo4j")
def observability_neo4j():
    q1 = "MATCH (n:Concept) RETURN count(n) AS nodes"
    q2 = "MATCH ()-[r:CONFLICTS_WITH|SUPERSEDED_BY|CROSSES_DOMAIN]->() RETURN count(r) AS edges"
    t0 = time.time()
    with driver.session(database=NEO4J_DB) as s:
        nodes = s.run(q1).data()[0]["nodes"]
        edges = s.run(q2).data()[0]["edges"]
    return {"nodes": nodes, "edges": edges, "elapsed_sec": round(time.time()-t0,3)}

@app.post("/ops/logs")
def ingest_logs(batch: LogBatch):
    lines = [f"[{i.level}] {i.source}: {i.message}" for i in batch.items]
    errs = summarize_errors(lines)
    snippet = "INCIDENT SUMMARY\n- errors: {}\n- total_lines: {}\nTop errors:\n{}".format(
        len(errs), len(lines), "\n".join("  - "+e for e in errs[:5])
    )
    return {"received": len(batch.items), "error_snippets": errs, "chat_snippet": snippet}

@app.post("/ontology/event")
def publish_ontology_event(evt: OntologyEvent):
    if evt.rel.type not in ("CROSSES_DOMAIN","SUPERSEDED_BY","CONFLICTS_WITH"):
        raise HTTPException(400, f"Invalid relation type {evt.rel.type}")
    payload = json.loads(evt.model_dump_json())
    producer.send(TOPIC_ONTOLOGY_UPDATED, payload).get(timeout=10)
    return {"status":"sent","topic":TOPIC_ONTOLOGY_UPDATED,"edge_id":evt.rel.edge_id}

# ---- Incident endpoints ----
@app.post("/ops/incident")
def create_incident(body: IncidentCreate):
    iid = uuid.uuid4().hex[:12]
    dq: Deque[IncidentItem] = deque(maxlen=2000)
    for it in body.items:
        dq.append(it)
    INCIDENTS[iid] = dq
    INCIDENT_TITLES[iid] = body.title
    summary, snippet = _summarize(list(dq))
    return {"incident_id": iid, "title": body.title, "summary": summary,
            "chat_snippet": f"[incident:{iid}] {body.title}\n" + snippet}

@app.post("/ops/incident/append")
def append_incident(body: IncidentAppend):
    iid = body.incident_id
    if iid not in INCIDENTS:
        raise HTTPException(404, "incident not found")
    dq = INCIDENTS[iid]
    for it in body.items:
        dq.append(it)
    summary, snippet = _summarize(list(dq))
    return {"incident_id": iid, "title": INCIDENT_TITLES.get(iid,""), "summary": summary,
            "chat_snippet": f"[incident:{iid}] {INCIDENT_TITLES.get(iid,'')}\n" + snippet}

@app.get("/ops/incident/{incident_id}")
def get_incident(incident_id: str):
    if incident_id not in INCIDENTS:
        raise HTTPException(404, "incident not found")
    dq = INCIDENTS[incident_id]
    summary, snippet = _summarize(list(dq))
    return {"incident_id": incident_id, "title": INCIDENT_TITLES.get(incident_id,""),
            "summary": summary, "chat_snippet": f"[incident:{incident_id}] {INCIDENT_TITLES.get(incident_id,'')}\n" + snippet}

# ---- Simple UI ----
@app.get("/ui", response_class=HTMLResponse)
def ui():
    return """
<!doctype html><html><head>
<meta charset="utf-8"/><title>Ops Gateway UI</title>
<style>
body{font-family:system-ui,Arial;margin:16px;max-width:1100px}
h2{margin-top:18px} textarea,button{font-size:14px}
pre{background:#f6f8fa;padding:10px;border-radius:8px;max-height:260px;overflow:auto}
.row{display:flex;gap:16px;flex-wrap:wrap}
.card{border:1px solid #e5e7eb;border-radius:10px;padding:12px;flex:1;min-width:320px}
</style></head><body>
<h1>Ops Gateway — Control Panel</h1>
<div class="row">
  <div class="card">
    <h2>Observability (Neo4j)</h2>
    <button onclick="refreshObs()">Refresh</button>
    <pre id="obs">...</pre>
  </div>
  <div class="card">
    <h2>Post Ontology Event</h2>
    <textarea id="evt" rows="10" style="width:100%">{ 
  "src": {"concept_id":"K010","domain":"legal"},
  "dst": {"concept_id":"K011","domain":"finance"},
  "rel": {"type":"CROSSES_DOMAIN","edge_id":"edge-K010-K011","version":"v1","kctx":"ctx-ui"}
}</textarea>
    <div style="margin-top:8px"><button onclick="sendEvt()">Send</button></div>
    <pre id="evtOut">...</pre>
  </div>
</div>
<div class="row">
  <div class="card">
    <h2>Send Logs (summarize errors)</h2>
    <textarea id="logs" rows="8" style="width:100%">{"items":[
  {"level":"INFO","source":"dev","message":"start"},
  {"level":"ERROR","source":"worker","message":"neo4j.exceptions.ServiceUnavailable: cannot connect"}
]}</textarea>
    <div style="margin-top:8px"><button onclick="sendLogs()">Send</button></div>
    <pre id="logsOut">...</pre>
  </div>
  <div class="card">
    <h2>Incident ID → fetch snippet</h2>
    <input id="iid" placeholder="incident id" style="width:60%"/>
    <button onclick="getIncident()">Get</button>
    <pre id="iidOut">...</pre>
  </div>
</div>
<script>
async function refreshObs(){
  const r = await fetch('/observability/neo4j');
  document.getElementById('obs').textContent = JSON.stringify(await r.json(),null,2);
}
async function sendEvt(){
  try{
    const body = JSON.parse(document.getElementById('evt').value);
    const r = await fetch('/ontology/event',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    document.getElementById('evtOut').textContent = JSON.stringify(await r.json(),null,2);
  }catch(e){document.getElementById('evtOut').textContent = e.toString();}
}
async function sendLogs(){
  try{
    const body = JSON.parse(document.getElementById('logs').value);
    const r = await fetch('/ops/logs',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    document.getElementById('logsOut').textContent = JSON.stringify(await r.json(),null,2);
  }catch(e){document.getElementById('logsOut').textContent = e.toString();}
}
async function getIncident(){
  const iid = document.getElementById('iid').value.trim();
  if(!iid){ document.getElementById('iidOut').textContent='please input incident id'; return; }
  const r = await fetch('/ops/incident/'+iid);
  document.getElementById('iidOut').textContent = JSON.stringify(await r.json(),null,2);
}
refreshObs(); setInterval(refreshObs, 5000);
</script>
</body></html>"""

# --- GPT suggest (VN) ---
from openai import OpenAI
client_gpt = OpenAI()  # lấy OPENAI_API_KEY từ env

def _vn_prompt(log_text: str) -> str:
    return f"""
Bạn là chuyên gia DevOps/Platform. Hãy đọc LOG dưới đây và trả lời TIẾNG VIỆT, ngắn gọn, có cấu trúc:

LOG:
{log_text}

YÊU CẦU:
1) Tóm tắt lỗi chính (bullet).
2) Phân loại nguyên nhân gốc (network/cấu hình/phụ thuộc/lỗi mã…).
3) Cách sửa cụ thể (lệnh bash, vị trí file, biến môi trường).
4) Kiểm chứng sau sửa (câu lệnh kiểm tra).
Lưu ý giọng điệu như đang pair-programming, súc tích, có code-block khi cần.
"""

from fastapi import Body

@app.post("/ops/incident/gpt_suggest")
def gpt_suggest(payload: dict = Body(...)):
    """
    payload nhận:
      - incident_id: str (tuỳ chọn)
      - logs: List[str] hoặc 1 chuỗi lớn
    Ưu tiên lấy logs từ /ops/incident/{id} nếu chỉ truyền incident_id.
    """
    logs = ""

    # 1) Nếu gửi incident_id thì lấy log đang lưu
    iid = payload.get("incident_id")
    if iid and iid in INCIDENTS:
        items = list(INCIDENTS[iid])
        logs = "\n".join(f"[{it.level}] {it.source}: {it.message}" for it in items)

    # 2) Nếu gửi logs trực tiếp
    if not logs:
        raw = payload.get("logs", "")
        if isinstance(raw, list):
            logs = "\n".join(str(x) for x in raw)
        else:
            logs = str(raw)

    if not logs.strip():
        raise HTTPException(400, "No logs provided")

    # 3) Gọi GPT (Responses API)
    try:
        resp = client_gpt.responses.create(
            model="gpt-4o-mini",  # nhanh + rẻ; có thể đổi thành "gpt-4o"
            input=[{
                "role": "user",
                "content": [{"type":"input_text","text": _vn_prompt(logs)}],
            }],
        )
        answer = resp.output_text
    except Exception as e:
        raise HTTPException(502, f"GPT error: {e}")

    return {"incident_id": iid, "suggestion_vi": answer}

