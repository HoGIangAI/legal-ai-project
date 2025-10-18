#!/usr/bin/env python3
from __future__ import annotations
import os, sys, json
from pathlib import Path
from typing import Dict, Any
from neo4j import GraphDatabase
from common.env import Settings
from common.log import get_logger, new_correlation_id
EXIT_OK=0; EXIT_AUDIT=6

def neo4j_run(settings: Settings, cypher: str, **params: Any):
    drv = GraphDatabase.driver(settings.NEO4J_URI, auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD))
    try:
        with drv.session() as s:
            return [r.data() for r in s.run(cypher, **params)]
    finally:
        drv.close()

def audit_ontology(settings: Settings) -> Dict[str, Any]:
    node_counts = neo4j_run(settings, "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt ORDER BY cnt DESC")
    edge_counts = neo4j_run(settings, "MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS cnt ORDER BY cnt DESC")
    dup_nodes = neo4j_run(settings, """
      MATCH (n) WITH labels(n)[0] AS label, n.id AS id, count(*) AS c
      WHERE id IS NOT NULL AND c > 1
      RETURN label, id, c ORDER BY c DESC LIMIT 100
    """)
    orphan_src = neo4j_run(settings, """
      MATCH ()-[r]->(d) WHERE r.src_id IS NOT NULL AND NOT EXISTS { MATCH (s {id: r.src_id}) }
      RETURN type(r) AS type, r.src_id AS src_id, count(*) AS cnt LIMIT 100
    """)
    orphan_dst = neo4j_run(settings, """
      MATCH (s)-[r]->() WHERE r.dst_id IS NOT NULL AND NOT EXISTS { MATCH (d {id: r.dst_id}) }
      RETURN type(r) AS type, r.dst_id AS dst_id, count(*) AS cnt LIMIT 100
    """)
    invalid_edges = neo4j_run(settings, """
      MATCH ()-[r]->() WHERE (r.src_id IS NULL OR r.dst_id IS NULL)
      RETURN type(r) AS type, count(r) AS cnt
    """)
    ok = (len(dup_nodes)==0 and len(orphan_src)==0 and len(orphan_dst)==0 and (invalid_edges[0]["cnt"] if invalid_edges else 0)==0)
    return {
        "ok": ok,
        "node_counts": node_counts,
        "edge_counts": edge_counts,
        "issues": {
            "duplicate_nodes": dup_nodes,
            "orphan_edges_src": orphan_src,
            "orphan_edges_dst": orphan_dst,
            "invalid_edges_missing_ids": invalid_edges
        }
    }

def main()->int:
    settings = Settings.load(); log = get_logger(os.getenv("LOG_LEVEL","INFO")); corr = new_correlation_id()
    try:
        report = audit_ontology(settings)
        outdir=Path("build/reports"); outdir.mkdir(parents=True, exist_ok=True)
        (outdir/"ontology_audit.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        if report["ok"]: log.info("audit_ok", module="audit", correlation_id=corr); return EXIT_OK
        log.error("audit_issues", module="audit", error_code="AUDIT_ISSUES", hint="Check duplicate/orphan/invalid edges", correlation_id=corr)
        return EXIT_AUDIT
    except Exception as e:
        log.error("audit_failed", module="audit", error=str(e), error_code="UNHANDLED", correlation_id=corr); return EXIT_AUDIT

if __name__=="__main__": sys.exit(main())

