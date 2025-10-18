import os
from neo4j import GraphDatabase

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "legalai123")
NEO4J_DB = os.getenv("NEO4J_DB", "neo4j")


class GraphClient:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
        )

    def close(self):
        try:
            self.driver.close()
        except Exception:
            pass

    def neighbors(self, concept_id: str):
        q = """
        MATCH (a:Concept {concept_id:$cid})
              -[r:CONFLICTS_WITH|SUPERSEDED_BY|CROSSES_DOMAIN]->
              (b:Concept)
        RETURN  a.concept_id                AS src,
                type(r)                     AS rel,
                b.concept_id                AS dst,
                r.version                   AS version,
                r.knowledge_context_id      AS kctx
        """
        with self.driver.session(database=NEO4J_DB) as s:
            return s.run(q, {"cid": concept_id}).data()

    def conflicts(self, concept_id: str):
        q = """
        MATCH (a:Concept {concept_id:$cid})-[r:CONFLICTS_WITH]->(b:Concept)
        RETURN a.concept_id AS src, b.concept_id AS dst, r.edge_id AS edge_id
        """
        with self.driver.session(database=NEO4J_DB) as s:
            return s.run(q, {"cid": concept_id}).data()

    def supersedes_of(self, concept_id: str):
        q = """
        MATCH (a:Concept {concept_id:$cid})-[r:SUPERSEDED_BY]->(b:Concept)
        RETURN a.concept_id AS src, b.concept_id AS dst,
               r.version AS version, r.edge_id AS edge_id
        """
        with self.driver.session(database=NEO4J_DB) as s:
            return s.run(q, {"cid": concept_id}).data()
