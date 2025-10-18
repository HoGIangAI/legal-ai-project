#!/usr/bin/env python3
from __future__ import annotations
import json, argparse, hashlib
from pathlib import Path
from typing import Dict, Any

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()

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
    ap.add_argument("--list", required=True)
    args = ap.parse_args()

    lst = load_list(Path(args.list))
    idx, contents, missing = [], [], []
    for rel in lst:
        fp = Path(rel)
        meta: Dict[str, Any] = {"path": rel, "exists": fp.exists()}
        if fp.exists() and fp.is_file():
            try: txt = fp.read_text(encoding="utf-8")
            except Exception: txt = fp.read_bytes().decode("latin-1", errors="ignore")
            meta.update(size=fp.stat().st_size, mtime=int(fp.stat().st_mtime), sha256=sha256_text(txt))
            contents.append({"path": rel, "content": txt})
        else:
            missing.append(rel)
        idx.append(meta)

    outdir = Path("build/reports"); outdir.mkdir(parents=True, exist_ok=True)
    (outdir/"fs_index.json").write_text(json.dumps({"files": idx}, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    (outdir/"noidung_file.json").write_text(json.dumps({"files": contents}, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    have_paths = {x["path"] for x in contents}; want_paths = set(lst)
    mism = {"missing": sorted(missing), "extra": sorted(have_paths - want_paths), "ok": len(missing) == 0}
    (outdir/"fs_verify.json").write_text(json.dumps(mism, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"✅ Scan done. missing={len(missing)} extra={len(mism['extra'])} -> build/reports/fs_verify.json")

if __name__ == "__main__": main()

