#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, os, re, sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any

ROOT = Path(__file__).resolve().parents[1]
BR = ROOT / "build" / "reports"
BR.mkdir(parents=True, exist_ok=True)

REQUIRED_INIT_DIRS = [
    "common","models","services","services/ontology","tools","tools/ontology","tests",
]
MAKEFILE_PATH = ROOT / "Makefile"
REQUIRED_MAKE_TARGETS = {
    "emit": "python3 -m tools.ontology.emit_graphops_jsonl --input $$ONTOLOGY_FILE --json",
    "consume": "python3 -m services.ontology.ontology_consumer --json",
}
JSON_DUMPS_PATCH_FILES = [
    "common/diagnostics.py",
    "tools/ontology/emit_graphops_jsonl.py",
    "tools/ontology/audit_integrity.py",
    "scripts/step1_quet_theo_danhsach.py",
    "scripts/make_reference_repo.py",
    "scripts/step2_chan_doan_loi.py",
    "scripts/sync_repo.py",
    "tools/ontology/verify_after_emit.py",
]
ENV_FILES = [".env.dev", ".env.example"]
ENV_REQUIRED = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "SCHEMA_REGISTRY_URL": "http://localhost:8081",
    "TOPIC_GRAPHOPS": "legal-ontology-graphops",
    "NEO4J_URI": "bolt://localhost:7687",
    "NEO4J_USER": "neo4j",
    "NEO4J_PASSWORD": "legalai123",
    "LOG_LEVEL": "INFO",
}
DUMPS_REGEX = re.compile(r"json\.dumps\s*\(", re.MULTILINE)

@dataclass
class Change: path: str; action: str; detail: str
@dataclass
class Report:
    ok: bool
    created_inits: List[Change]
    makefile_changes: List[Change]
    dumps_patches: List[Change]
    env_updates: List[Change]
    notes: List[str]

def _read_text(p: Path) -> str:
    try: return p.read_text(encoding="utf-8")
    except Exception: return p.read_text(encoding="latin-1", errors="ignore")
def _write_text(p: Path, content: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True); p.write_text(content, encoding="utf-8")

def ensure_inits(dry: bool) -> List[Change]:
    out=[]; 
    for d in REQUIRED_INIT_DIRS:
        ip = ROOT/d/"__init__.py"
        if not ip.exists():
            if not dry: ip.parent.mkdir(parents=True, exist_ok=True); ip.write_text("", encoding="utf-8")
            out.append(Change(str(ip.relative_to(ROOT)),"create","__init__.py"))
    return out

def ensure_make_targets(dry: bool) -> List[Change]:
    ch=[]; 
    if not MAKEFILE_PATH.exists(): return [Change(str(MAKEFILE_PATH.relative_to(ROOT)),"skip","Makefile not found")]
    text=_read_text(MAKEFILE_PATH)
    for target, cmd in REQUIRED_MAKE_TARGETS.items():
        if re.search(rf"^\s*{re.escape(target)}\s*:\s*$", text, flags=re.MULTILINE): continue
        block=f"\n{target}:\n\t{cmd}\n"
        if not dry: text+=block
        ch.append(Change("Makefile","append-target",f"{target} -> {cmd}"))
    if ch and not dry: _write_text(MAKEFILE_PATH, text)
    return ch

def patch_json_dumps_in_text(content: str) -> tuple[str,bool]:
    out=[]; i=0; changed=False
    while True:
        m=DUMPS_REGEX.search(content,i)
        if not m: out.append(content[i:]); break
        out.append(content[i:m.start()])
        start=m.end(); depth=1; j=start
        while j<len(content) and depth>0:
            if content[j]=="(": depth+=1
            elif content[j]==")": depth-=1
            j+=1
        inner=content[start:j-1]
        inner_patched=inner if "default=" in inner else (inner+", default=str" if inner.strip() else "default=str")
        changed = changed or (inner_patched!=inner)
        out.append("json.dumps("+inner_patched+")")
        i=j
    return ("".join(out), changed)

def patch_json_dumps_files(dry: bool) -> List[Change]:
    ch=[]
    for rel in JSON_DUMPS_PATCH_FILES:
        p=ROOT/rel
        if not p.exists() or not p.is_file(): continue
        original=_read_text(p)
        patched, changed=patch_json_dumps_in_text(original)
        if changed:
            if not dry: _write_text(p, patched)
            ch.append(Change(rel,"patch-json.dumps","insert default=str"))
    return ch

def ensure_env_files(dry: bool) -> List[Change]:
    ch=[]
    for rel in ENV_FILES:
        p=ROOT/rel; data={}
        if p.exists():
            for line in _read_text(p).splitlines():
                line=line.strip()
                if not line or line.startswith("#") or "=" not in line: continue
                k,v=line.split("=",1); data[k.strip()]=v.strip()
        updated=False
        for k,v in ENV_REQUIRED.items():
            if k not in data: data[k]=v; updated=True; ch.append(Change(rel,"env-add",f"{k}={v}"))
        if updated and not dry:
            body="\n".join([f"{k}={data[k]}" for k in sorted(data.keys())])+"\n"; _write_text(p, body)
        elif not p.exists() and not dry:
            body="\n".join([f"{k}={v}" for k,v in sorted(ENV_REQUIRED.items())])+"\n"; _write_text(p, body)
            ch.append(Change(rel,"env-create","new env file"))
    return ch

def main()->int:
    ap=argparse.ArgumentParser()
    ap.add_argument("--apply",action="store_true"); ap.add_argument("--dry-run",action="store_true")
    args=ap.parse_args(); dry=not args.apply
    report=Report(
        ok=True,
        created_inits=ensure_inits(dry),
        makefile_changes=ensure_make_targets(dry),
        dumps_patches=patch_json_dumps_files(dry),
        env_updates=ensure_env_files(dry),
        notes=[],
    )
    out=BR/"auto_fix_report.json"
    out.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"ok": report.ok, "report": str(out)}, ensure_ascii=False))
    return 0
if __name__=="__main__": sys.exit(main())

