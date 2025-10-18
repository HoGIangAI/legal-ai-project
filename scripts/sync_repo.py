#!/usr/bin/env python3
from __future__ import annotations
import json, argparse
from pathlib import Path
from typing import List, Dict

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reference", required=True)  # build/reports/reference_repo.json
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    ref = json.loads(Path(args.reference).read_text(encoding="utf-8"))
    files: List[Dict[str,str]] = ref.get("files", [])
    plan=[]; wrote=0
    for item in files:
        p = Path(item["path"]); want = item.get("content","")
        need_write = (not p.exists()) or (p.read_text(encoding="utf-8", errors="ignore") != want)
        if need_write:
            plan.append({"path": item["path"], "action": "write"})
            if args.apply:
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(want, encoding="utf-8"); wrote+=1
    outdir=Path("build/reports"); outdir.mkdir(parents=True, exist_ok=True)
    if args.apply:
        (outdir/"sync_apply.json").write_text(json.dumps({"wrote": wrote, "plan": plan}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(json.dumps({"ok": True, "wrote": wrote}, ensure_ascii=False, default=str))
    else:
        (outdir/"sync_plan.json").write_text(json.dumps({"plan": plan}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(json.dumps({"ok": True, "plan_len": len(plan)}, ensure_ascii=False, default=str))

if __name__ == "__main__": main()

