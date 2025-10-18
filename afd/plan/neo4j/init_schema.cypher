// Node chính cho ontology
CREATE CONSTRAINT concept_id_unique IF NOT EXISTS
FOR (n:Concept) REQUIRE n.concept_id IS UNIQUE;

// Edge idempotent theo edge_id cho 3 loại
CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:CONFLICTS_WITH]-() REQUIRE r.edge_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:SUPERSEDED_BY]-() REQUIRE r.edge_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:CROSSES_DOMAIN]-()  REQUIRE r.edge_id IS UNIQUE;

// Seed để đảm bảo relationship types tồn tại (không bắt buộc)
MERGE (a:Concept {concept_id:'__seed_A'})
MERGE (b:Concept {concept_id:'__seed_B'})
MERGE (a)-[:CONFLICTS_WITH]->(b)
MERGE (a)-[:SUPERSEDED_BY]->(b)
MERGE (a)-[:CROSSES_DOMAIN]->(b);
