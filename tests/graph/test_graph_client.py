import os
os.environ.setdefault("NEO4J_URI", "bolt://localhost:7687")
os.environ.setdefault("NEO4J_USER", "neo4j")
os.environ.setdefault("NEO4J_PASSWORD", "legalai123")
os.environ.setdefault("NEO4J_DB", "neo4j")

from services.graph.graph_client import GraphClient

def test_neighbors_smoke():
    g = GraphClient()
    rows = g.neighbors("K001")  # hoặc concept_id bạn đã seed
    print(rows)
    assert isinstance(rows, list)
    g.close()
