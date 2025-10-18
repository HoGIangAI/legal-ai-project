#!/usr/bin/env python3
from __future__ import annotations
import json, argparse
from pathlib import Path

def load_list(p: Path) -> list[str]:
    d = json.loads(p.read_text(encoding="utf-8"))
    raw = d["files"] if isinstance(d, dict) else d
    out=[]
    for x in raw:
        if isinstance(x, str): out.append(x)
        elif isinstance(x, dict) and "path" in x: out.append(x["path"])
    return sorted(set(out))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", default="build/reports/danhsach_file.json")
    ap.add_argument("--output", default="build/reports/reference_repo.json")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    lst = load_list(Path(args.list))
    files=[]; errors=0
    for rel in lst:
        fp = Path(rel)
        if not fp.exists() or not fp.is_file(): errors+=1; continue
        try: content = fp.read_text(encoding="utf-8")
        except Exception: content = fp.read_bytes().decode("latin-1", errors="ignore")
        files.append({"path": rel, "content": content})
    payload={"files": files}
    if not args.dry_run:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"✅ Baseline {'preview' if args.dry_run else 'saved'}: {args.output} ({len(files)} files, errors={errors})")

if __name__ == "__main__": main()

