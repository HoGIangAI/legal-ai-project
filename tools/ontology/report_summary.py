#!/usr/bin/env python3
from __future__ import annotations
import os, sys, json
from pathlib import Path
from typing import Any, Dict
from common.log import get_logger, new_correlation_id

FILES = ["fs_verify.json","fix_loi.json","diagnostics_report.json","ontology_audit.json","verify_report.json","graph_summary.json"]

def read_json(p: Path)->Any:
    try: return json.loads(p.read_text(encoding="utf-8"))
    except Exception: return {"error": f"cannot read {p.name}"}

def main()->int:
    log=get_logger(os.getenv("LOG_LEVEL","INFO")); corr=new_correlation_id()
    br=Path("build/reports"); br.mkdir(parents=True, exist_ok=True)
    out: Dict[str, Any] = {}
    for name in FILES:
        fp=br/name; out[name]=read_json(fp) if fp.exists() else {"missing": True}
    sp=br/"master_summary.json"
    sp.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    log.info("reports_summary_done", module="report_summary", file=str(sp), correlation_id=corr)
    print(json.dumps({"ok": True, "summary": str(sp)}, ensure_ascii=False)); return 0
if __name__=="__main__": sys.exit(main())

