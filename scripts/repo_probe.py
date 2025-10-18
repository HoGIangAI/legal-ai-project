#!/usr/bin/env python3
from __future__ import annotations
import os, sys, json, re, fnmatch, hashlib, time, socket, subprocess, shutil
from pathlib import Path

EXIT_OK=0; EXIT_ENV=2; EXIT_UNHANDLED=7
ROOT = Path(__file__).resolve().parents[1]

DEFAULT_EXCLUDES = [
    ".git/",".github/",".venv/","venv/","env/","__pycache__/",".mypy_cache/",".ruff_cache/",".pytest_cache/",
    "node_modules/","dist/","build/coverage/",".idea/",".vscode/",
]
DEFAULT_EXCLUDE_GLOBS = ["*.pyc","*.pyo","*.pyd","*.so","*.dll"]

REQ_DIRS = [
    "common","models","services","services/ontology","tools","tools/ontology",
    "build/reports","build/quarantine","tests"
]
REQ_FILES = [
    "pyproject.toml","Makefile","README.md",
    "docker-compose.kafka.yml","docker-compose.neo4j.yml",
    "common/env.py","common/log.py","common/errors.py","common/diagnostics.py","common/utils.py",
    "models/graphops.py",
    "services/ontology/kafka_config.py","services/ontology/neo4j_writer.py","services/ontology/ontology_consumer.py","services/ontology/producer_avro.py",
    "tools/ontology/emit_graphops_jsonl.py","tools/ontology/audit_integrity.py","tools/ontology/verify_after_emit.py",
]
PKG_DIRS = ["common","models","services","services/ontology","tools","tools/ontology"]

def load_probeignore(root: Path) -> list[str]:
    p = root/".probeignore"
    if not p.exists():
        return []
    lines = [ln.strip() for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines()]
    return [ln for ln in lines if ln and not ln.startswith("#")]

def should_skip(rel: str, excludes: list[str], exclude_globs: list[str]) -> bool:
    for ex in excludes:
        if rel.startswith(ex):
            return True
    name = rel.split("/")[-1]
    for g in exclude_globs:
        if fnmatch.fnmatch(name, g):
            return True
    return False

def match_any(rel: str, patterns: list[str]) -> bool:
    if not patterns:
        return True
    for pat in patterns:
        if fnmatch.fnmatch(rel, pat):
            return True
    return False

def snapshot_tree(root: Path, include: list[str], exclude: list[str], exclude_globs: list[str]) -> dict:
    files = {}
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        rel = str(p.relative_to(root)).replace("\\", "/")
        if should_skip(rel, exclude, exclude_globs):
            continue
        if not match_any(rel, include):
            continue
        try:
            st = p.stat()
            files[rel] = {"size": st.st_size, "mtime": int(st.st_mtime)}
        except OSError:
            continue
    return files

def load_baseline(path: Path) -> dict:
    if not path.exists(): return {}
    try: return json.loads(path.read_text(encoding="utf-8"))
    except Exception: return {}

def diff_manifests(old: dict, new: dict) -> dict:
    added = sorted([k for k in new.keys() if k not in old])
    removed = sorted([k for k in old.keys() if k not in new])
    changed = []
    for k in new.keys() & old.keys():
        if new[k].get("mtime") != old[k].get("mtime") or new[k].get("size") != old[k].get("size"):
            changed.append(k)
    return {"added": added, "removed": removed, "changed": sorted(changed)}

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--include", default="", help="comma-separated globs to include (default: all)")
    ap.add_argument("--exclude", default="", help="comma-separated globs to exclude (appended to defaults)")
    ap.add_argument("--baseline", default="build/reports/probe_manifest.json")
    ap.add_argument("--write-baseline", action="store_true")
    args = ap.parse_args()

    os.chdir(ROOT)
    report = {"ok": True, "issues": [], "warnings": []}

    excludes = DEFAULT_EXCLUDES + load_probeignore(ROOT)
    exclude_globs = DEFAULT_EXCLUDE_GLOBS[:]
    if args.exclude:
        exclude_globs += [x.strip() for x in args.exclude.split(",") if x.strip()]
    includes = [x.strip() for x in args.include.split(",") if x.strip()]

    files = snapshot_tree(ROOT, includes, excludes, exclude_globs)
    report["files_count"] = len(files)

    for d in REQ_DIRS:
        if not (ROOT/d).exists():
            report["issues"].append(f"missing_dir:{d}"); report["ok"] = False
    for f in REQ_FILES:
        if f not in files and not (ROOT/f).exists():
            report["issues"].append(f"missing:{f}"); report["ok"] = False
    for d in PKG_DIRS:
        if not (ROOT/d/"__init__.py").exists():
            report["issues"].append(f"missing_init:{d}/__init__.py"); report["ok"] = False

    mk = ROOT/"Makefile"
    if mk.exists():
        txt = mk.read_text(encoding="utf-8", errors="ignore")
        for t in ["diagnose","emit","consume","audit","verify"]:
            if not re.search(rf"^\.?PHONY:.*\b{t}\b|^\s*{t}:", txt, flags=re.M):
                report["issues"].append(f"Makefile missing target: {t}")
                report["ok"] = False

    base_path = Path(args.baseline)
    old = load_baseline(base_path)
    manifest = {"files": files, "generated_at": int(time.time())}
    if old:
        report["diff"] = diff_manifests(old.get("files", {}), files)
        if report["diff"]["added"] or report["diff"]["removed"]:
            report["warnings"].append("TREE_CHANGED")
    if args.write_baseline:
        base_path.parent.mkdir(parents=True, exist_ok=True)
        base_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        report["baseline_written"] = str(base_path)

    report["tree_sample"] = sorted(list(files.keys()))[:60]
    print(json.dumps(report, indent=2))
    return EXIT_OK if report.get("ok") else EXIT_ENV

if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit as e:
        raise e
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}))
        sys.exit(EXIT_UNHANDLED)

