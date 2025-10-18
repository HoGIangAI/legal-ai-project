#!/usr/bin/env python3
from __future__ import annotations
import os, sys, json
from pathlib import Path
from neo4j import GraphDatabase
from common.env import Settings
from common.log import get_logger, new_correlation_id

def main()->int:
    st=Settings.load(); log=get_logger(os.getenv("LOG_LEVEL","INFO")); corr=new_correlation_id()
    drv=GraphDatabase.driver(st.NEO4J_URI, auth=(st.NEO4J_USER, st.NEO4J_PASSWORD))
    try:
        with drv.session() as s:
            nodes=[r.data() for r in s.run("MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt ORDER BY cnt DESC")]
            edges=[r.data() for r in s.run("MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS cnt ORDER BY cnt DESC")]
        out={"ok":True,"nodes":nodes,"edges":edges}
        outdir=Path("build/reports"); outdir.mkdir(parents=True, exist_ok=True)
        (outdir/"graph_summary.json").write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        log.info("graph_snapshot_done", module="graph_snapshot", correlation_id=corr); return 0
    except Exception as e:
        log.error("graph_snapshot_failed", module="graph_snapshot", error=str(e), error_code="UNHANDLED", correlation_id=corr); return 1
    finally:
        drv.close()
if __name__=="__main__": sys.exit(main())

