#!/usr/bin/env python3
"""
neo4j_diagnostics_v3rev2.py
Collects a full diagnostic report from a running Neo4j instance,
aligned with V3_Rev2 + ontology_thuytinh_* migration checklist.

USAGE:
  python3 neo4j_diagnostics_v3rev2.py \
      --uri bolt://localhost:7687 \
      --user neo4j \
      --password 'your_password' \
      --database neo4j \
      --outfile neo4j_report

It will create:
  - neo4j_report.json (structured data)
  - neo4j_report.md   (human-readable summary)

ENV VARS (optional overrides):
  NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DB
"""
import argparse
import json
import os
import sys
from datetime import datetime

def main():
    args = parse_args()

    # Read from env if not provided
    uri = args.uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = args.user or os.getenv("NEO4J_USER", "neo4j")
    password = args.password or os.getenv("NEO4J_PASSWORD", "")
    database = args.database or os.getenv("NEO4J_DB", "neo4j")
    outfile = args.outfile or "neo4j_report"

    try:
        from neo4j import GraphDatabase
    except Exception as e:
        print("[FATAL] The 'neo4j' Python driver is not installed.", file=sys.stderr)
        print("        Install with: pip install neo4j", file=sys.stderr)
        sys.exit(1)

    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
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

    # Connect & verify
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        report["connectivity"] = {"ok": True}
    except Exception as e:
        report["connectivity"] = {"ok": False, "error": str(e)}
        dump_reports(outfile, report, markdown_notice="[FATAL] Connectivity failed.")
        sys.exit(2)

    # Helper to run a Cypher with graceful error handling
    def run(query, params=None, db=database):
        try:
            with driver.session(database=db) as session:
                res = session.run(query, params or {})
                try:
                    # Convert to list of dict rows
                    return [dict(r) for r in res]
                except Exception:
                    return {"info": "non-tabular result"}
        except Exception as e:
            report["errors"].append({"query": query, "error": str(e)})
            return None

    # 1) Components / version / edition
    report["components"]["dbms.components"] = run("CALL dbms.components()")

    # 2) Procedures (to confirm APOC/GDS presence)
    report["components"]["dbms.procedures"] = run("CALL dbms.procedures() YIELD name RETURN name ORDER BY name LIMIT 5000")

    # 3) APOC / GDS versions (if present)
    apoc_ver = run("RETURN apoc.version() AS apoc_version")
    gds_ver  = run("CALL gds.version() YIELD version RETURN version AS gds_version")
    report["plugins"]["apoc"] = apoc_ver or []
    report["plugins"]["gds"]  = gds_ver or []

    # 4) APOC/GDS config
    report["config"]["apoc_gds"] = run("""
        CALL dbms.listConfig() YIELD name, value
        WHERE name STARTS WITH 'apoc.' OR name STARTS WITH 'gds.'
        RETURN name, value ORDER BY name
    """)

    # 5) Memory/pagecache
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

    # 6) Databases & current database
    # Neo4j v5 supports: SHOW DATABASES; SHOW CURRENT DATABASE
    report["deployment"]["databases"] = run("SHOW DATABASES")
    report["deployment"]["current_database"] = run("SHOW CURRENT DATABASE")

    # 7) Labels / Relationship types / Indexes / Constraints
    # Prefer SHOW in v5; fallback to CALL if SHOW fails (handled by run())
    report["schema"]["labels"] = run("SHOW LABELS YIELD label RETURN label ORDER BY label")
    report["schema"]["relationship_types"] = run("SHOW RELATIONSHIP TYPES YIELD relationshipType AS type RETURN type ORDER BY type")
    report["schema"]["indexes"] = run("SHOW INDEXES")
    report["schema"]["constraints"] = run("SHOW CONSTRAINTS")

    # 8) APOC meta schema (overview) if APOC is present
    report["schema"]["apoc_meta_schema"] = run("CALL apoc.meta.schema()")

    # 9) Graph counts & cardinalities
    report["data_stats"]["db_stats_retrieve"] = run("CALL db.stats.retrieve('GRAPH COUNTS') YIELD section, data RETURN section, data")
    report["data_stats"]["count_nodes"] = run("MATCH (n) RETURN count(n) AS nodes")
    report["data_stats"]["count_relationships"] = run("MATCH ()-[r]->() RETURN count(r) AS rels")

    # 10) Security / roles (Enterprise only; will error on Community)
    report["security"]["users"] = run("SHOW USERS")
    report["security"]["roles"] = run("SHOW ROLES")

    # Close driver
    try:
        driver.close()
    except Exception:
        pass

    # Write out JSON + Markdown
    md = to_markdown(report)
    dump_reports(outfile, report, md)

def dump_reports(outfile, report, markdown=None, markdown_notice=None):
    json_path = f"{outfile}.json"
    md_path = f"{outfile}.md"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    if markdown is None:
        markdown = to_markdown(report)

    if markdown_notice:
        markdown = f"{markdown_notice}\n\n" + markdown

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(markdown)

    print(f"[OK] Wrote: {json_path}")
    print(f"[OK] Wrote: {md_path}")

def to_markdown(report: dict) -> str:
    def j(obj):
        return json.dumps(obj, ensure_ascii=False, indent=2)

    lines = []
    lines.append(f"# Neo4j Diagnostics (V3_Rev2 alignment)")
    lines.append(f"- Generated at: {report.get('generated_at')}")
    lines.append("")
    lines.append("## 1) Connectivity")
    lines.append("```json")
    lines.append(j(report.get("connectivity")))
    lines.append("```")

    lines.append("\n## 2) Components / Version / Edition")
    lines.append("```json")
    lines.append(j(report.get("components")))
    lines.append("```")

    lines.append("\n## 3) Plugins (APOC / GDS)")
    lines.append("```json")
    lines.append(j(report.get("plugins")))
    lines.append("```")

    lines.append("\n## 4) Config (APOC/GDS, Memory)")
    lines.append("```json")
    lines.append(j(report.get("config")))
    lines.append("```")

    lines.append("\n## 5) Deployment (Databases)")
    lines.append("```json")
    lines.append(j(report.get("deployment")))
    lines.append("```")

    lines.append("\n## 6) Schema (Labels, Relationship Types, Indexes, Constraints)")
    lines.append("```json")
    lines.append(j(report.get("schema")))
    lines.append("```")

    lines.append("\n## 7) Data Stats")
    lines.append("```json")
    lines.append(j(report.get("data_stats")))
    lines.append("```")

    lines.append("\n## 8) Security (Users/Roles)")
    lines.append("```json")
    lines.append(j(report.get("security")))
    lines.append("```")

    if report.get("errors"):
        lines.append("\n## 9) Errors (non-fatal)")
        lines.append("```json")
        lines.append(j(report.get("errors")))
        lines.append("```")

    return "\n".join(lines)

def parse_args():
    p = argparse.ArgumentParser(description="Collect a full Neo4j diagnostic report (V3_Rev2 alignment).")
    p.add_argument("--uri", help="Neo4j URI, e.g., bolt://localhost:7687 or neo4j+s://host:7687")
    p.add_argument("--user", help="Username, e.g., neo4j")
    p.add_argument("--password", help="Password")
    p.add_argument("--database", help="Database name (default: neo4j)")
    p.add_argument("--outfile", help="Output filename prefix (default: neo4j_report)")
    return p.parse_args()

if __name__ == "__main__":
    main()
