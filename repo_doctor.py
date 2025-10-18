#!/usr/bin/env python3
from __future__ import annotations
import json, os, sys, re, shutil, socket, subprocess
from pathlib import Path

# Exit codes (contract)
EXIT_OK=0; EXIT_ENV=2; EXIT_CONN=3; EXIT_SCHEMA=4; EXIT_DATA=5; EXIT_AUDIT=6; EXIT_UNHANDLED=7

ROOT = Path(__file__).resolve().parents[1]
REQ_DIRS = [
    "common","models","services","services/ontology","tools","tools/ontology",
    "build/reports","build/quarantine","tests"
]
REQ_FILES = [
    "common/env.py","common/log.py","common/errors.py","common/diagnostics.py","common/utils.py",
    "models/graphops.py",
    "services/ontology/kafka_config.py","services/ontology/neo4j_writer.py","services/ontology/ontology_consumer.py","services/ontology/producer_avro.py",
    "tools/ontology/emit_graphops_jsonl.py","tools/ontology/audit_integrity.py","tools/ontology/verify_after_emit.py",
    "docker-compose.kafka.yml","docker-compose.neo4j.yml","pyproject.toml","Makefile","README.md",
]
PKG_DIRS = ["common","models","services","services/ontology","tools","tools/ontology"]
MAKE_TARGETS = ["doctor","install","lint","type","test","diagnose","emit","consume","audit","verify","format"]

ENV_KEYS = [
    "KAFKA_BOOTSTRAP_SERVERS","SCHEMA_REGISTRY_URL","TOPIC_GRAPHOPS",
    "NEO4J_URI","NEO4J_USER","NEO4J_PASSWORD","ONTOLOGY_FILE","LOG_LEVEL"
]

RE_HOSTPORT = re.compile(r"^[a-zA-Z0-9_.:-]+:\d{2,5}$")
RE_URL = re.compile(r"^https?://[a-zA-Z0-9_.:-]+(/.*)?$")

def sh(cmd: list[str], timeout: int = 10) -> tuple[int,str,str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout.strip(), p.stderr.strip()
    except Exception as e:
        return 1, "", str(e)

def tcp_ok(host: str, port: int, timeout: float = 1.5) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        try:
            s.connect((host, port))
            return True
        except OSError:
            return False

def ensure_inits(autofix: bool, report: dict) -> None:
    created = []
    for d in PKG_DIRS:
        p = ROOT / d / "__init__.py"
        if not p.exists() and autofix:
            p.write_text("# package marker\n", encoding="utf-8")
            created.append(str(p.relative_to(ROOT)))
    if created:
        report["fixes"].extend([f"touch {x}" for x in created])

def check_makefile(report: dict) -> None:
    mk = ROOT/"Makefile"
    if not mk.exists():
        report["issues"].append("missing:Makefile"); report["ok"] = False; return
    txt = mk.read_text(encoding="utf-8", errors="ignore")
    for t in MAKE_TARGETS:
        if not re.search(rf"^\.?PHONY:.*\b{t}\b|^\s*{t}:", txt, flags=re.M):
            report["issues"].append(f"Makefile missing target: {t}")
            report["ok"] = False

def check_python_env(report: dict) -> None:
    v = sys.version_info
    if v.major < 3 or (v.major==3 and v.minor < 12):
        report["issues"].append(f"PYTHON_VERSION<3.12: {v.major}.{v.minor}")
        report["ok"] = False
    if not os.getenv("VIRTUAL_ENV"):
        report["warnings"].append("VENV_NOT_ACTIVE")
    missing = []
    for mod in ["confluent_kafka","neo4j","pydantic","tenacity","httpx","cachetools","authlib"]:
        try: __import__(mod)
        except Exception: missing.append(mod)
    if missing:
        report["issues"].append({"missing_py_deps": missing, "hint": "pip install -e .[dev]"})

def check_os_limits(report: dict) -> None:
    try:
        _, _, free = shutil.disk_usage(ROOT)
        if free < 1_000_000_000:
            report["warnings"].append("LOW_DISK_SPACE")
    except Exception: pass
    code,out,_ = sh(["bash","-lc","ulimit -n"])
    if code==0:
        try:
            if int(out) < 4096:
                report["warnings"].append("LOW_ULIMIT_NOFILE")
        except Exception: pass

def parse_hostport(hp: str) -> tuple[str,int]:
    h,p = hp.split(":",1); return h, int(p)

def check_env_values(report: dict) -> None:
    env = {k: os.getenv(k, "") for k in ENV_KEYS}
    bad = []
    if env["KAFKA_BOOTSTRAP_SERVERS"] and not RE_HOSTPORT.match(env["KAFKA_BOOTSTRAP_SERVERS"].split(",")[0]):
        bad.append("KAFKA_BOOTSTRAP_SERVERS")
    if env["SCHEMA_REGISTRY_URL"] and not RE_URL.match(env["SCHEMA_REGISTRY_URL"]):
        bad.append("SCHEMA_REGISTRY_URL")
    if env["NEO4J_URI"] and not env["NEO4J_URI"].startswith("bolt://"):
        bad.append("NEO4J_URI")
    if bad:
        report["issues"].append({"env_invalid": bad}); report["ok"] = False

    ofile = Path(env.get("ONTOLOGY_FILE") or "")
    if str(ofile) and ofile.exists():
        try:
            sz = ofile.stat().st_size
            if sz == 0:
                report["issues"].append("ONTOLOGY_FILE_EMPTY"); report["ok"] = False
            else:
                with ofile.open("r", encoding="utf-8") as f:
                    for i,_ in enumerate(f,1):
                        if i>1: break
        except UnicodeDecodeError:
            report["issues"].append("ONTOLOGY_FILE_ENCODING"); report["ok"] = False
    report["env_snapshot"] = env

def check_docker_daemon(report: dict) -> None:
    code,_,_ = sh(["docker","info"], timeout=6)
    if code!=0:
        report["warnings"].append("DOCKER_DAEMON_NOT_RUNNING")

def tcp_matrix(report: dict) -> None:
    env = report.get("env_snapshot", {})
    hp = env.get("KAFKA_BOOTSTRAP_SERVERS","localhost:9092").split(",")[0]
    sr = env.get("SCHEMA_REGISTRY_URL","http://localhost:8081").replace("http://","",1).split("/",1)[0]
    bolt = env.get("NEO4J_URI","bolt://localhost:7687").replace("bolt://","",1)
    checks = {}
    for label in [hp, sr, bolt]:
        try:
            h,p = parse_hostport(label); checks[label] = tcp_ok(h,p)
        except Exception:
            checks[label] = False
    report["tcp"] = checks

def ensure_basic_dirs(autofix: bool, report: dict) -> None:
    for d in REQ_DIRS:
        p = ROOT/d
        if not p.exists():
            if autofix:
                p.mkdir(parents=True, exist_ok=True)
                report["fixes"].append(f"mkdir {d}")
            else:
                report["issues"].append(f"missing_dir:{d}"); report["ok"] = False

def require_files(report: dict) -> None:
    for f in REQ_FILES:
        if not (ROOT/f).exists():
            report["issues"].append(f"missing:{f}"); report["ok"] = False

def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--autofix", action="store_true")
    args = ap.parse_args()

    os.chdir(ROOT)
    report = {"ok": True, "cwd": str(ROOT), "fixes": [], "issues": [], "warnings": []}

    ensure_basic_dirs(args.autofix, report)
    require_files(report)
    ensure_inits(args.autofix, report)
    check_makefile(report)
    check_python_env(report)
    check_os_limits(report)
    check_env_values(report)
    check_docker_daemon(report)
    tcp_matrix(report)

    if str(ROOT) not in sys.path:
        report["warnings"].append("PYTHONPATH_MISSING_ROOT")

    print(json.dumps(report, indent=2))
    return EXIT_OK if report["ok"] else EXIT_ENV

if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit as e:
        raise e
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}))
        sys.exit(EXIT_UNHANDLED)
