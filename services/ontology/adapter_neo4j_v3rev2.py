from __future__ import annotations
import json
from typing import Any, Dict, Optional, List
from neo4j import GraphDatabase
import os

def _sanitize(v: Any):
    """Neo4j chỉ nhận primitive; dict/list -> json string."""
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)

def _sanitize_props(props: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not props:
        return {}
    return {k: _sanitize(v) for k, v in props.items()}

def _labels_from(label_or_labels: Any) -> List[str]:
    if not label_or_labels:
        return []
    if isinstance(label_or_labels, str):
        # chấp nhận "A|B" hoặc "A,B"
        if "|" in label_or_labels:
            return [x.strip() for x in label_or_labels.split("|") if x.strip()]
        if "," in label_or_labels:
            return [x.strip() for x in label_or_labels.split(",") if x.strip()]
        return [label_or_labels.strip()]
    if isinstance(label_or_labels, (list, tuple)):
        return [str(x).strip() for x in label_or_labels if str(x).strip()]
    return [str(label_or_labels).strip()]

class Neo4jAdapter:
    def __init__(self,
                 uri: Optional[str] = None,
                 user: Optional[str] = None,
                 password: Optional[str] = None,
                 database: Optional[str] = None):
        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "legalai123")
        self.database = database or os.getenv("NEO4J_DATABASE")  # None => default DB
        self._driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        self._driver.close()

    # ---------- Node ----------
    def upsert_node(self, node: Dict[str, Any]):
        """
        node: {"label": "LawArticle", "id": "A001", "props": {...}}
              hoặc {"labels": ["LawArticle","Active"], "id": "..."}
        """
        node_id = node.get("id")
        labels = node.get("labels") or node.get("label")
        labels = _labels_from(labels) or ["Thing"]
        props = _sanitize_props(node.get("props"))

        # Xây Cypher với nhiều label
        labels_cypher = ":" + ":".join(labels)
        cypher = f"""
        MERGE (n{labels_cypher} {{ id: $id }})
        SET n += $props
        RETURN id(n) as internal_id
        """
        with self._driver.session(database=self.database) as session:
            return session.run(cypher, id=node_id, props=props).single()

    # ---------- Edge ----------
    def upsert_edge(self, edge: Dict[str, Any]):
        """
        edge: {
          "type":"REFERS_TO",
          "from":{"labels":["LawArticle"],"id":"A001"},
          "to":{"labels":"LawConcept","id":"K002"},
          "props": {...}
        }
        """
        etype = edge.get("type") or edge.get("label") or "RELATES"
        s = edge.get("from") or {}
        t = edge.get("to") or {}
        s_labels = _labels_from(s.get("labels") or s.get("label") or "Thing")
        t_labels = _labels_from(t.get("labels") or t.get("label") or "Thing")
        s_id = s.get("id")
        t_id = t.get("id")
        props = _sanitize_props(edge.get("props"))

        s_cypher = ":" + ":".join(s_labels)
        t_cypher = ":" + ":".join(t_labels)
        cypher = f"""
        MATCH (a{s_cypher} {{id:$sid}}), (b{t_cypher} {{id:$tid}})
        MERGE (a)-[r:{etype}]->(b)
        SET r += $props
        RETURN type(r) as rel_type
        """
        with self._driver.session(database=self.database) as session:
            return session.run(cypher, sid=s_id, tid=t_id, props=props).single()

