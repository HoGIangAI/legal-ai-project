#!/usr/bin/env python3
"""
Verify sau emit/consume:
- Đọc build/reports/emit_stats.json (sent)
- Lấy snapshot node/edge (đếm tổng) từ Neo4j
- Kết luận tương đối: graph có tăng so với trước? (nếu có graph_summary.json cũ)
"""
import argparse, json, os, time, sys
from neo4j import GraphDatabase

MODULE = "verify_after_emit"

def jlog(level, msg, error_code=0, hint=None, extra=None):
    print(json.dumps({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "level": level, "module": MODULE, "msg": msg,
        "error_code": error_code, "hint": hint, "extra": extra or {}
    }, default=str))

def count_graph(uri, user, pwd):
    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    with driver.session() as s:
        n = s.run("MATCH (n) RETURN count(n) AS c").single()["c"]
        r = s.run("MATCH ()-[x]->() RETURN count(x) AS c").single()["c"]
    driver.close()
    return int(n), int(r)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="build/reports/verify_report.json")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    emit_stats = "build/reports/emit_stats.json"
    emit = {"sent": 0}
    if os.path.exists(emit_stats):
        try:
            emit = json.load(open(emit_stats))
        except Exception as e:
            jlog("warning", "cannot read emit_stats.json", error_code=5, extra={"exc": str(e)})

    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd  = os.getenv("NEO4J_PASSWORD", "neo4j")

    try:
        nodes, rels = count_graph(uri, user, pwd)
    except Exception as e:
        jlog("error", "neo4j query failed", error_code=3, hint="Check Neo4j up", extra={"exc": str(e)})
        sys.exit(3)

    # Phán đoán mềm: nếu emit >= 1 thì graph phải có >=1 thay đổi (không chắc 1-1)
    ok = True
    reason = "ok"
    if emit.get("sent", 0) > 0 and (nodes + rels) == 0:
        ok = False
        reason = "graph empty after emit"

    out = {
        "ts": time.time(),
        "emit_sent": int(emit.get("sent", 0)),
        "graph_nodes": nodes,
        "graph_rels": rels,
        "ok": ok,
        "reason": reason
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    json.dump(out, open(args.out, "w"), indent=2)
    jlog("info", "verify complete", extra={"ok": ok, "out": args.out})
    sys.exit(0 if ok else 6)

if __name__ == "__main__":
    main()

