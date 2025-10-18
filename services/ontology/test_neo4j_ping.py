import os
from neo4j import GraphDatabase

uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
user = os.getenv("NEO4J_USER", "neo4j")
pwd  = os.getenv("NEO4J_PASSWORD", "neo4j")
db   = os.getenv("NEO4J_DB", "neo4j")

driver = GraphDatabase.driver(uri, auth=(user, pwd))
driver.verify_connectivity()
with driver.session(database=db) as s:
    rows = s.run("CALL dbms.components()").data()
print("[OK] Connected:", rows)
driver.close()
