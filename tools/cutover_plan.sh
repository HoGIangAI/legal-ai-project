#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "[1/8] Nạp ENV..."
set -a && source .env.neo4j && set +a

echo "[2/8] Kiểm tra Neo4j connectivity..."
python3 - <<'PY'
import os
from neo4j import GraphDatabase
uri=os.getenv("NEO4J_URI","bolt://localhost:7687")
usr=os.getenv("NEO4J_USER","neo4j")
pwd=os.getenv("NEO4J_PASSWORD","neo4j")
db =os.getenv("NEO4J_DB","neo4j")
drv=GraphDatabase.driver(uri,auth=(usr,pwd))
drv.verify_connectivity()
with drv.session(database=db) as s:
    rows=s.run("CALL dbms.components()").data()
print("[OK] Neo4j:", rows)
drv.close()
PY

echo "[3/8] Kiểm tra dữ liệu sample (>=1 edge)..."
docker exec -i legal-ai-platform-neo4j-1 cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
'MATCH ()-[r:CONFLICTS_WITH|SUPERSEDED_BY|CROSSES_DOMAIN]->() RETURN count(r) AS edges;' | tee /tmp/neo4j_edges_cnt.txt

if ! grep -q '[1-9][0-9]*' /tmp/neo4j_edges_cnt.txt; then
  echo "[WARN] Không thấy edge nào trong Neo4j. Bạn có muốn tiếp tục? (Ctrl+C để hủy)"
  sleep 3
fi

echo "[4/8] Bật feature flag đọc Neo4j (USE_NEO4J=true)..."
# Nếu service đọc flag từ env khi khởi động, bạn cần restart chúng.
# Ví dụ: docker compose restart answer intent ...
# Nếu chạy local: export USE_NEO4J=true

echo "[5/8] Chạy kiểm tra khói (smoke) sau cutover..."
python3 - <<'PY'
import os, json
from services.graph.graph_client import GraphClient
os.environ.setdefault("NEO4J_URI", os.getenv("NEO4J_URI","bolt://localhost:7687"))
os.environ.setdefault("NEO4J_USER", os.getenv("NEO4J_USER","neo4j"))
os.environ.setdefault("NEO4J_PASSWORD", os.getenv("NEO4J_PASSWORD","neo4j"))
os.environ.setdefault("NEO4J_DB", os.getenv("NEO4J_DB","neo4j"))
g=GraphClient()
print(json.dumps(g.neighbors("K001"), ensure_ascii=False))
g.close()
PY

echo "[6/8] Chạy mini AT (latency, DLQ giả lập nếu có)..."
# Tuỳ hệ thống của bạn, ở đây minh hoạ query latency bằng cypher-shell
/usr/bin/time -f '[perf] elapsed=%E cpu=%P mem=%MKB' docker exec -i legal-ai-platform-neo4j-1 cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
'MATCH (a:Concept)-[r:CONFLICTS_WITH|SUPERSEDED_BY|CROSSES_DOMAIN]->(b:Concept) RETURN a.concept_id, type(r), b.concept_id LIMIT 50;' >/dev/null

echo "[7/8] Canary: bật 10% traffic (nếu bạn có gateway/feature router)."
echo "      Hoặc skip bước này nếu bạn switch 100%."

echo "[8/8] Hoàn tất. Theo dõi metrics 15-30 phút đầu rồi nâng dần traffic."
