#!/usr/bin/env python3
from __future__ import annotations
import sys, json
from pathlib import Path
EXIT_OK=0; EXIT_ENV=2

def main(path: str) -> int:
    p = Path(path)
    if not p.exists():
        print(json.dumps({"ok": False, "error": f"missing {path}"})); return EXIT_ENV
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(json.dumps({"ok": False, "error": f"invalid json: {e}"})); return EXIT_ENV

    issues = []
    files = data.get("files", [])
    if data.get("files_count") != len(files):
        issues.append({"code":"COUNT_MISMATCH","want":data.get("files_count"),"got":len(files)})

    seen=set()
    for f in files:
        path=f.get("path","")
        if not path: issues.append({"code":"PATH_INVALID","path":path})
        if path in seen: issues.append({"code":"DUP_PATH","path":path})
        seen.add(path)
        if f.get("content","")=="":
            issues.append({"code":"EMPTY_CONTENT","path":path})
        if f.get("size",0)==0:
            issues.append({"code":"ZERO_SIZE","path":path})

    req = set(data.get("required_files", []))
    miss = [x for x in req if x not in seen]
    if miss: issues.append({"code":"MISSING_REQUIRED_FILES","paths":miss})

    ok = len(issues)==0
    print(json.dumps({"ok":ok,"files_total":len(files),"issues":issues}, ensure_ascii=False, indent=2))
    return EXIT_OK if ok else EXIT_ENV

if __name__=="__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv)>1 else "build/reports/repo_snapshot.json"))
