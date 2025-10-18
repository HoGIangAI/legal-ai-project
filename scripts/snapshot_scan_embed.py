#!/usr/bin/env python3
from __future__ import annotations
import os, sys, json, fnmatch
from pathlib import Path
from time import strftime

ROOT = Path(__file__).resolve().parents[1]

# BỎ QUA rác
EXCL_DIR = {".git",".github",".venv","venv","env","__pycache__", ".mypy_cache",".ruff_cache",".pytest_cache","node_modules","dist","build/coverage",".idea",".vscode"}
EXCL_GLOB = {"*.pyc","*.pyo","*.pyd","*.so","*.dll","*.log","*.tmp"}

# DANH SÁCH file/thư mục cần quét
INCLUDE_GLOBS = [
  "pyproject.toml", "Makefile", "README.md", ".env.example", ".env.dev",
  "docker-compose.kafka.yml", "docker-compose.neo4j.yml",
  "common/**/*.py","models/**/*.py","services/**/*.py","tools/**/*.py","tests/**/*.py",
]

REQUIRED_DIRS = [
  "common","models","services","services/ontology","tools","tools/ontology",
  "build/reports","build/quarantine","tests"
]
REQUIRED_FILES = [
  "pyproject.toml","Makefile","README.md",
  "docker-compose.kafka.yml","docker-compose.neo4j.yml",
  "common/env.py","common/log.py","common/errors.py","common/diagnostics.py","common/utils.py",
  "models/graphops.py",
  "services/ontology/kafka_config.py","services/ontology/neo4j_writer.py","services/ontology/ontology_consumer.py","services/ontology/producer_avro.py",
  "tools/ontology/emit_graphops_jsonl.py","tools/ontology/audit_integrity.py","tools/ontology/verify_after_emit.py",
  "tests/test_smoke.py",
]

def want(rel: str) -> bool:
    parts = Path(rel).parts
    if parts and parts[0] in EXCL_DIR: return False
    name = Path(rel).name
    if any(fnmatch.fnmatch(name,g) for g in EXCL_GLOB): return False
    return any(fnmatch.fnmatch(rel, g) for g in INCLUDE_GLOBS)

def main() -> int:
    os.chdir(ROOT)
    files = []
    seen = set()
    for p in ROOT.rglob("*"):
        if not p.is_file(): continue
        rel = str(p.relative_to(ROOT)).replace("\\","/")
        if not want(rel): continue
        if rel in seen: continue
        seen.add(rel)
        try:
            content = p.read_text(encoding="utf-8")
            enc = "utf-8"
        except Exception:
            content = p.read_bytes().decode("latin-1", errors="ignore")
            enc = "latin-1*"
        st = p.stat()
        files.append({"path": rel, "size": st.st_size, "mtime": int(st.st_mtime), "encoding": enc, "content": content})

    snapshot = {
        "ok": True,
        "root": str(ROOT),
        "generated_at": strftime("%Y-%m-%dT%H:%M:%S"),
        "required_dirs": REQUIRED_DIRS,
        "required_files": REQUIRED_FILES,
        "files_count": len(files),
        "files": files,
        "note": "This snapshot IS the source of truth for V3_Rev2"
    }
    out = Path("build/reports/repo_snapshot.json")
    out.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(out))
    return 0

if __name__ == "__main__":
    sys.exit(main())
