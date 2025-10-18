#!/usr/bin/env python3
from __future__ import annotations
import sys, json
from pathlib import Path
EXIT_OK=0; EXIT_ENV=2
ROOT = Path(__file__).resolve().parents[1]

def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

def main(snap_path: str) -> int:
    sp = Path(snap_path)
    if not sp.exists():
        print(json.dumps({"ok": False, "error": f"missing {snap_path}"})); return EXIT_ENV
    try:
        snap = json.loads(sp.read_text(encoding="utf-8"))
    except Exception as e:
        print(json.dumps({"ok": False, "error": f"invalid json: {e}"})); return EXIT_ENV

    created, updated = [], []
    # ensure required dirs
    for d in snap.get("required_dirs", []):
        (ROOT/d).mkdir(parents=True, exist_ok=True)

    for f in snap.get("files", []):
        rel=f.get("path"); content=f.get("content","")
        if not rel: continue
        dst = ROOT/rel
        old = dst.read_text(encoding="utf-8") if dst.exists() else None
        write_file(dst, content)
        if old is None: created.append(rel)
        elif old != content: updated.append(rel)

    print(json.dumps({"ok": True,"created":created,"updated":updated,"applied_total":len(snap.get("files",[]))}, ensure_ascii=False, indent=2))
    return EXIT_OK

if __name__=="__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv)>1 else "build/reports/repo_snapshot.json"))
