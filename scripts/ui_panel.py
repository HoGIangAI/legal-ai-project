#!/usr/bin/env python3
from __future__ import annotations
import json, subprocess, sys
from pathlib import Path
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
BR = ROOT/"build/reports"
DS_LIST = BR/"danhsach_file.json"
REF = BR/"reference_repo.json"

st.set_page_config(page_title="Ontology GraphOps — V3_Rev2 Panel", layout="wide")
def run(cmd): 
    p = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    return p.returncode, (p.stdout or "") + (p.stderr or "")
def read_json(p: Path): 
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None

st.title("Ontology GraphOps — Control Panel (V3_Rev2)")
tabs = st.tabs(["Baseline","Sync","Verify / Diagnose","Analyze"])

with tabs[0]:
    st.subheader("1) Baseline")
    if st.button("📦 Build Baseline"):
        code,out = run([sys.executable,"scripts/make_reference_repo.py","--list",str(DS_LIST),"--output",str(REF)])
        st.code(out); st.success("Done" if code==0 else "Errored")
    if st.button("🔍 Scan (step1)"):
        code,out = run([sys.executable,"scripts/step1_quet_theo_danhsach.py","--list",str(DS_LIST)])
        st.code(out)
    st.write("fs_verify.json:"); st.json(read_json(BR/"fs_verify.json") or {"hint":"run Scan"})

with tabs[1]:
    st.subheader("2) Sync")
    if st.button("📝 Dry-run"):
        code,out=run([sys.executable,"scripts/sync_repo.py","--reference",str(REF),"--dry-run"]); st.code(out)
        st.json(read_json(BR/"sync_plan.json") or {})
    if st.button("✅ Apply"):
        code,out=run([sys.executable,"scripts/sync_repo.py","--reference",str(REF),"--apply"]); st.code(out)
        st.json(read_json(BR/"sync_apply.json") or {})

with tabs[2]:
    st.subheader("3) Verify / Diagnose")
    if st.button("🧪 Diagnose content (step2)"):
        code,out=run([sys.executable,"scripts/step2_chan_doan_loi.py"]); st.code(out)
        st.json(read_json(BR/"fix_loi.json") or {})
    if st.button("🩺 make diagnose"):
        code,out=run(["make","diagnose"]); st.code(out)
        st.json(read_json(BR/"diagnostics_report.json") or {})

with tabs[3]:
    st.subheader("4) Analyze")
    st.json(read_json(BR/"fix_loi.json") or {"hint":"Run step2 first"})

