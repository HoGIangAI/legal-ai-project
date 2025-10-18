#!/usr/bin/env bash
set -euo pipefail

# ==== Config linh hoạt (đổi nếu container/cred khác) ====
NEO4J_CONT="${NEO4J_CONT:-legal-ai-platform-neo4j-1}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-legalai123}"

echo "[Neo4j container] $NEO4J_CONT"
echo "[Check] cypher-shell in container ..."
docker exec -i "$NEO4J_CONT" which cypher-shell >/dev/null

echo
echo "== Neo4j counts =="
docker exec -i "$NEO4J_CONT" cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" '
MATCH (n:Concept) RETURN count(n) AS nodes;
MATCH ()-[r:CONFLICTS_WITH|SUPERSEDED_BY|CROSSES_DOMAIN]->() RETURN count(r) AS edges;
' | sed 's/^/  /'

echo
echo "== Sample query latency =="
/usr/bin/time -f "[perf] elapsed=%E cpu=%P mem=%MKB" \
  docker exec -i "$NEO4J_CONT" cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" '
MATCH (a:Concept)-[r:CONFLICTS_WITH|SUPERSEDED_BY|CROSSES_DOMAIN]->(b:Concept)
RETURN a.concept_id, type(r), b.concept_id
LIMIT 200;
' >/dev/null

echo
echo "== Top 10 edges (for visual check) =="
docker exec -i "$NEO4J_CONT" cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" '
MATCH (a:Concept)-[r:CONFLICTS_WITH|SUPERSEDED_BY|CROSSES_DOMAIN]->(b:Concept)
RETURN a.concept_id AS src, type(r) AS rel, b.concept_id AS dst, r.version AS ver
LIMIT 10;
' | sed 's/^/  /'
