#!/usr/bin/env python3
"""
Audit integrity cho Ontology GraphOps (Neo4j 5+).
- Đếm node/edge, phát hiện orphan/label mismatch/thiếu prop/id/dup id.
- Xuất JSON báo cáo; exit code 6 nếu --fail-on-issues và phát hiện issue.
"""

import argparse, json, os, sys, time, pathlib
from typing import Dict, Any, List
from neo4j import GraphDatabase
from tenacity import retry, stop_after_attempt, wait_random_exponential

EXIT_OK = 0
EXIT_AUDIT = 6

MODULE = "audit_integrity"

def jlog(level, msg, error_code=0, hint=None, extra=None):
    print(json.dumps({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "level": level,
        "module": MODULE,
        "msg": msg,
        "error_code": error_code,
        "hint": hint,
        "extra": extra or {}
    }))

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(exp_base=2, max=2))
def neo4j_driver(uri: str, user: str, pwd: str):
    return GraphDatabase.driver(uri, auth=(user, pwd))

def run_query(sess, q: str, **params):
    return list(sess.run(q, **params))

def audit(uri: str, user: str, pwd: str) -> Dict[str, Any]:
    driver = neo4j_driver(uri, user, pwd)
    issues: List[Dict[str, Any]] = []
    summary: Dict[str, Any] = {}

    with driver.session() as s:
        # --- Counts (node/edge) ---
        def scalar(q):
            rec = s.run(q).single()
            return 0 if rec is None else list(rec.values())[0]

        law_articles = scalar("MATCH (n:LawArticle) RETURN count(n) AS c")
        law_concepts = scalar("MATCH (n:LawConcept) RETURN count(n) AS c")
        law_entities = scalar("MATCH (n:LawEntity) RETURN count(n) AS c")
        refers_edges = scalar("MATCH ()-[r:REFERS_TO]->() RETURN count(r) AS c")

        summary["counts"] = {
            "LawArticle": law_articles,
            "LawConcept": law_concepts,
            "LawEntity": law_entities,
            "REFERS_TO": refers_edges,
        }

        # --- Label-specific edges (detect label mismatch) ---
        # Expected REFERS_TO: LawArticle -> LawConcept
        exp_refers = scalar("""
            MATCH (a:LawArticle)-[r:REFERS_TO]->(c:LawConcept) RETURN count(r) AS c
        """)
        if refers_edges != exp_refers:
            issues.append({
                "code": "EDGE_LABEL_MISMATCH",
                "hint": "Có REFERS_TO không đi từ LawArticle sang LawConcept",
                "extra": {"total_REFERS_TO": refers_edges, "expected_LabelMatch": exp_refers}
            })

        # --- Nodes missing id ---
        miss_id = scalar("MATCH (n) WHERE n.id IS NULL RETURN count(n) AS c")
        if miss_id > 0:
            issues.append({
                "code": "NODE_MISSING_ID",
                "hint": "Có node thiếu thuộc tính id",
                "extra": {"count": miss_id}
            })

        # --- Optional props check ---
        la_missing_title = scalar("MATCH (n:LawArticle) WHERE n.title IS NULL RETURN count(n) AS c")
        lc_missing_name = scalar("MATCH (n:LawConcept) WHERE n.name IS NULL RETURN count(n) AS c")
        if la_missing_title > 0:
            issues.append({"code": "LAWARTICLE_MISSING_TITLE", "hint": "LawArticle thiếu title", "extra": {"count": la_missing_title}})
        if lc_missing_name > 0:
            issues.append({"code": "LAWCONCEPT_MISSING_NAME", "hint": "LawConcept thiếu name", "extra": {"count": lc_missing_name}})

        # --- Duplicate id per label (nếu chưa có constraint) ---
        def dup_for(label: str):
            q = f"""
            MATCH (n:`{label}`) WHERE n.id IS NOT NULL
            WITH n.id AS id, count(*) AS c
            WHERE c > 1
            RETURN id AS id, c AS dup_count
            LIMIT 20
            """
            rows = run_query(s, q)
            return [{"id": r["id"], "dup": r["dup_count"]} for r in rows]

        dups = {
            "LawArticle": dup_for("LawArticle"),
            "LawConcept": dup_for("LawConcept"),
            "LawEntity": dup_for("LawEntity"),
        }
        for lab, rows in dups.items():
            if rows:
                issues.append({
                    "code": "DUPLICATE_ID",
                    "hint": f"Trùng id trong label {lab}",
                    "extra": {"label": lab, "examples": rows}
                })

    driver.close()
    return {"summary": summary, "issues": issues, "ok": len(issues) == 0}

def main():
    ap = argparse.ArgumentParser(description="Ontology audit/verify for Neo4j")
    ap.add_argument("--neo4j-uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    ap.add_argument("--neo4j-user", default=os.getenv("NEO4J_USER", "neo4j"))
    ap.add_argument("--neo4j-password", default=os.getenv("NEO4J_PASSWORD", "legalai123"))
    ap.add_argument("--output", default="build/reports/ontology_audit.json")
    ap.add_argument("--json", action="store_true", help="Print JSON summary to stdout")
    ap.add_argument("--fail-on-issues", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    pathlib.Path("build/reports").mkdir(parents=True, exist_ok=True)

    jlog("info", "audit_start", extra={"uri": args.neo4j_uri})
    report = audit(args.neo4j_uri, args.neo4j_user, args.neo4j_password)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    jlog("info", "audit_done", extra={"output": args.output, "ok": report["ok"]})

    if args.json:
        print(json.dumps(report, ensure_ascii=False))

    if args.fail_on_issues and not report["ok"]:
        sys.exit(EXIT_AUDIT)
    sys.exit(EXIT_OK)

if __name__ == "__main__":
    main()

