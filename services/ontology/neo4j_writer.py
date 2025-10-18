# services/ontology/neo4j_writer.py
from typing import Dict, Any, Optional
import re
from tenacity import retry, stop_after_attempt, wait_random_exponential
from neo4j import GraphDatabase

_LABEL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TYPE_RE  = re.compile(r"^[A-Z_][A-Z0-9_]*$")

def _assert_label(label: str) -> str:
    if not isinstance(label, str) or not _LABEL_RE.match(label):
        raise ValueError(f"Invalid label: {label!r}")
    return label

def _assert_type(rel_type: str) -> str:
    if not isinstance(rel_type, str) or not _TYPE_RE.match(rel_type):
        raise ValueError(f"Invalid relationship type: {rel_type!r} (must be UPPER_SNAKE)")
    return rel_type

class Neo4jWriter:
    """
    Upsert node/edge theo format GraphOps JSON.
    """

    def __init__(self, uri: str, user: str, password: str):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def ensure_unique_id_constraint(self, label: str):
        label = _assert_label(label)
        cypher = f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:`{label}`) REQUIRE n.id IS UNIQUE"
        with self._driver.session() as s:
            s.run(cypher).consume()

    @retry(stop=stop_after_attempt(3), wait=wait_random_exponential(exp_base=2, max=1.5))
    def upsert_node(self, label: str, _id: str, props: Optional[Dict[str, Any]] = None):
        label = _assert_label(label)
        props = props or {}
        self.ensure_unique_id_constraint(label)
        cypher = f"""
        MERGE (n:`{label}` {{id: $id}})
        SET n += $props
        """
        with self._driver.session() as s:
            s.run(cypher, id=_id, props=props).consume()

    @retry(stop=stop_after_attempt(3), wait=wait_random_exponential(exp_base=2, max=1.5))
    def upsert_edge(self, etype: str, src_label: str, src_id: str, dst_label: str, dst_id: str, props: Dict[str, Any] | None = None):
        etype = _assert_type(etype)
        src_label = _assert_label(src_label)
        dst_label = _assert_label(dst_label)
        props = props or {}

        self.ensure_unique_id_constraint(src_label)
        self.ensure_unique_id_constraint(dst_label)

        cypher = f"""
        MERGE (s:`{src_label}` {{id: $sid}})
        MERGE (d:`{dst_label}` {{id: $did}})
        MERGE (s)-[r:`{etype}`]->(d)
        SET r += $props, r.src_id = $sid, r.dst_id = $did
        RETURN COUNT{{ (s)-[r]->(d) }} AS c
        """
        with self._driver.session() as s:
            s.run(cypher, sid=src_id, did=dst_id, props=props).consume()

