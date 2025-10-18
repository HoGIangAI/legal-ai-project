#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path

def main():
    br = Path("build/reports"); br.mkdir(parents=True, exist_ok=True)
    # Rất đơn giản: kiểm tra 3 lỗi hay gặp
    issues=[]
    # 1) __init__.py trong các package
    for d in ["common","models","services","services/ontology","tools","tools/ontology","tests"]:
        p = Path(d)/"__init__.py"
        if not p.exists(): issues.append({"code":"MISSING_INIT","path":str(p)})

    # 2) Makefile target emit/consume
    mk = Path("Makefile").read_text(encoding="utf-8") if Path("Makefile").exists() else ""
    if "emit:" not in mk: issues.append({"code":"MISSING_TARGET","target":"emit"})
    if "consume:" not in mk: issues.append({"code":"MISSING_TARGET","target":"consume"})

    # 3) ENV variables
    envs = Path(".env.dev").read_text(encoding="utf-8").splitlines() if Path(".env.dev").exists() else []
    needed = ["KAFKA_BOOTSTRAP_SERVERS","NEO4J_URI","TOPIC_GRAPHOPS"]
    have = {line.split("=",1)[0] for line in envs if "=" in line and not line.strip().startswith("#")}
    for k in needed:
        if k not in have: issues.append({"code":"MISSING_ENV",".env":".env.dev","key":k})

    out={"total_files":None,"total_issues":len(issues),"diagnostics":issues}
    (br/"fix_loi.json").write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"ok": len(issues)==0, "issues": len(issues)}, ensure_ascii=False, default=str))

if __name__ == "__main__": main()

