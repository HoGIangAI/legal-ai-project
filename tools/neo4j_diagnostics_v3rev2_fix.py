#!/usr/bin/env python3
"""
neo4j_diagnostics_v3rev2_fix.py
- Fixes JSON serialization for Neo4j temporal & custom driver types (DateTime, Date, Time, Duration, Node, Relationship, Path...)
- Uses timezone-aware UTC timestamp (datetime.now(datetime.UTC))
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

def to_jsonable(obj):
    """Recursively convert Neo4j/driver-specific types to JSON-serializable structures."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, dict):
        return {str(k): to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [to_jsonable(x) for x in obj]

    for attr in ("iso_format", "isoformat"):
        if hasattr(obj, attr) and callable(getattr(obj, attr)):
            try:
                return getattr(obj, attr)()
            except Exception:
                pass

    if hasattr(obj, "_properties"):
        try:
            data = dict(obj._properties)
            if hasattr(obj, "element_id"):
                data["_element_id"] = getattr(obj, "element_id")
            if hasattr(obj, "labels"):
                data["_labels"] = list(getattr(obj, "labels"))
            if hasattr(obj, "type"):
                data["_type"] = str(getattr(obj, "type"))
            return to_jsonable(data)
        except Exception:
            return str(obj)
    return str(obj)

def json_dumps_safe(obj):
    return json.dumps(to_jsonable(obj), ensure_ascii=False, indent=2)

def main():
    args = parse_args()
    uri = args.uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = args.user or os.getenv("NEO4J_USER", "neo4j")
    password = args.password or os.getenv("NEO4J_PASSWORD", "")
    database = args.database or os.getenv("NEO4J_DB", "neo4j")
    outfile = args.outfile or "neo4j_report"

    try:
        from neo4j import GraphDatabase
    except Exception:
        print("[FATAL] The 'neo4j' Python driver is not installed. Install with: pip install neo4j", file=sys.stderr)
        sys.exit(1)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inputs": {"uri": uri, "database": database, "user": user},
        "connectivity": {},
        "components": {},
        "plugins": {},
        "config": {},
        "deployment": {},
        "schema": {},
        "data_stats": {},
        "security": {},
        "errors": []
    }

    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        report["connectivity"] = {"ok": True}
    except Exception as e:
        report["connectivity"] = {"ok": False, "error": str(e)}
        dump_reports(outfile, report, markdown_notice="[FATAL] Connectivity failed.")
        sys.exit(2)

    def run(query, params=None, db=database):
        try:
            with driver.session(database=db) as session:
                res = session.run(query, params or {})
                return [{k: to_jsonable(v) for k, v in r.items()} for r in res]
        except Exception as e:
            report["errors"].append({"query": query, "error": str(e)})
            return None

    report["components"]["dbms.components"] = run("CALL dbms.components()")
    report["components"]["dbms.procedures"] = run("CALL dbms.procedures() YIELD name RETURN name ORDER BY name LIMIT 5000")
    report["plugins"]["apoc"] = run("RETURN apoc.version() AS apoc_version")
    report["plugins"]["gds"] = run("CALL gds.version() YIELD version RETURN version AS gds_version")

    report["config"]["apoc_gds"] = run("""
        CALL dbms.listConfig() YIELD name, value
        WHERE name STARTS WITH 'apoc.' OR name STARTS WITH 'gds.'
        RETURN name, value ORDER BY name
    """)

    report["config"]["memory"] = run("""
        CALL dbms.listConfig() YIELD name, value
        WHERE name IN [
          'server.memory.heap.initial_size',
          'server.memory.heap.max_size',
          'server.memory.pagecache.size',
          'dbms.memory.heap.initial_size',
          'dbms.memory.pagecache.size'
        ]
        RETURN name, value ORDER BY name
    """)

    report["deployment"]["databases"] = run("SHOW DATABASES")
    report["deployment"]["current_database"] = run("SHOW CURRENT DATABASE")
    report["schema"]["labels"] = run("SHOW LABELS YIELD label RETURN label ORDER BY label")
    report["schema"]["relationship_types"] = run("SHOW RELATIONSHIP TYPES YIELD relationshipType AS type RETURN type ORDER BY type")
    report["schema"]["indexes"] = run("SHOW INDEXES")
    report["schema"]["constraints"] = run("SHOW CONSTRAINTS")
    report["schema"]["apoc_meta_schema"] = run("CALL apoc.meta.schema()")

    report["data_stats"]["db_stats_retrieve"] = run("CALL db.stats.retrieve('GRAPH COUNTS') YIELD section, data RETURN section, data")
    report["data_stats"]["count_nodes"] = run("MATCH (n) RETURN count(n) AS nodes")
    report["data_stats"]["count_relationships"] = run("MATCH ()-[r]->() RETURN count(r) AS rels")

    report["security"]["users"] = run("SHOW USERS")
    report["security"]["roles"] = run("SHOW ROLES")

    try:
        driver.close()
    except Exception:
        pass

    md = to_markdown(report)
    dump_reports(outfile, report, md)

def dump_reports(outfile, report, markdown=None, markdown_notice=None):
    json_path = f"{outfile}.json"
    md_path = f"{outfile}.md"
    with open(json_path, "w", encoding="utf-8") as f:
        f.write(json_dumps_safe(report))
    if markdown is None:
        markdown = to_markdown(report)
    if markdown_notice:
        markdown = f"{markdown_notice}\n\n" + markdown
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(markdown)
    print(f"[OK] Wrote: {json_path}")
    print(f"[OK] Wrote: {md_path}")

def to_markdown(report: dict) -> str:
    def j(obj): return json_dumps_safe(obj)
    lines = []
    lines.append(f"# Neo4j Diagnostics (V3_Rev2 alignment)")
    lines.append(f"- Generated at: {report.get('generated_at')}")
    sections = [
        ("Connectivity", "connectivity"),
        ("Components / Version / Edition", "components"),
        ("Plugins (APOC / GDS)", "plugins"),
        ("Config (APOC/GDS, Memory)", "config"),
        ("Deployment (Databases)", "deployment"),
        ("Schema (Labels, Relationship Types, Indexes, Constraints)", "schema"),
        ("Data Stats", "data_stats"),
        ("Security (Users/Roles)", "security")
    ]
    for title, key in sections:
        lines.append(f"\n## {title}\n```json\n{j(report.get(key))}\n```")
    if report.get("errors"):
        lines.append(f"\n## Errors (non-fatal)\n```json\n{j(report.get('errors'))}\n```")
    return "\n".join(lines)

def parse_args():
    p = argparse.ArgumentParser(description="Collect a full Neo4j diagnostic report (V3_Rev2 alignment) with safe JSON serialization.")
    p.add_argument("--uri", help="Neo4j URI, e.g., bolt://localhost:7687 or neo4j+s://host:7687")
    p.add_argument("--user", help="Username, e.g., neo4j")
    p.add_argument("--password", help="Password")
    p.add_argument("--database", help="Database name (default: neo4j)")
    p.add_argument("--outfile", help="Output filename prefix (default: neo4j_report)")
    return p.parse_args()

if __name__ == "__main__":
    main()

