#!/usr/bin/env python3
import json, os, time, subprocess, shutil, socket
from pathlib import Path

# optional: httpx để chọc UI /health, /counts (đã có trong env bạn)
try:
    import httpx
except Exception:
    httpx = None

def sh(cmd: str) -> str:
    try:
        out = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, text=True, timeout=5)
        return out.strip()
    except Exception as e:
        return f"ERR: {e}"

def tcp_open(host: str, port: int, timeout=1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout): return True
    except Exception:
        return False

def git_info():
    return {
        "branch": sh("git rev-parse --abbrev-ref HEAD"),
        "commit": sh("git rev-parse --short HEAD"),
        "status": sh("git status --porcelain"),
        "remotes": sh("git remote -v"),
        "tags_head": sh("git tag --points-at HEAD"),
    }

def docker_info():
    return {
        "ps": sh('docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"'),
        "kafka_container": "legal-ai-platform-kafka-1",
        "neo4j_container": "neo4j",
    }

def env_info():
    keys = [
        "KAFKA_BOOTSTRAP_SERVERS","SCHEMA_REGISTRY_URL","TOPIC_GRAPHOPS",
        "NEO4J_URI","NEO4J_USER","NEO4J_PASSWORD","LOG_LEVEL","ONTOLOGY_FILE",
    ]
    return {k: os.getenv(k) for k in keys}

def kafka_info(topic: str):
    # mô tả topic & nhóm consumer (nếu có container kafka)
    out = {}
    name = "legal-ai-platform-kafka-1"
    if "Up " in sh(f'docker ps --format "{{{{.Names}}}} {{{{.Status}}}}" | grep "^{name} " || true'):
        out["topic_desc"] = sh(
            f'docker exec {name} kafka-topics --bootstrap-server localhost:9092 --describe --topic {topic} || true'
        )
        # nhóm gần đây (không bắt buộc): liệt kê toàn bộ group để bạn nhìn nhanh
        out["groups"] = sh(
            f'docker exec {name} kafka-consumer-groups --bootstrap-server localhost:9092 --list || true'
        )
    else:
        out["error"] = "kafka container not running"
    return out

def neo4j_counts():
    try:
        # dùng cypher-shell từ container neo4j
        cmd = (
            "docker exec neo4j cypher-shell -u ${NEO4J_USER:-neo4j} -p '${NEO4J_PASSWORD:-legalai123}' "
            '"MATCH (n:LawArticle) RETURN count(n) as a; '
            'MATCH (n:LawConcept) RETURN count(n) as c; '
            'MATCH (n:LawEntity)  RETURN count(n) as e; '
            'MATCH ()-[r:REFERS_TO]->() RETURN count(r) as r;"'
        )
        raw = sh(cmd)
        return {"raw": raw}
    except Exception as e:
        return {"error": str(e)}

def ui_info():
    port = 8088
    o = {"port": port, "listening": tcp_open("127.0.0.1", port)}
    if httpx and o["listening"]:
        try:
            o["/health"] = httpx.get(f"http://127.0.0.1:{port}/health", timeout=2).json()
        except Exception as e:
            o["/health_err"] = str(e)
        try:
            o["/counts"] = httpx.get(f"http://127.0.0.1:{port}/counts", timeout=2).json()
        except Exception as e:
            o["/counts_err"] = str(e)
    return o

def main():
    topic = os.getenv("TOPIC_GRAPHOPS", "legal-ontology-graphops")
    out = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "cwd": os.getcwd(),
        "env": env_info(),
        "git": git_info(),
        "docker": docker_info(),
        "kafka": kafka_info(topic),
        "neo4j": neo4j_counts(),
        "ui": ui_info(),
        "hints": [
            "Run: make diagnose | make emit | python -m services.ontology.ontology_consumer --once | make audit",
            "UI: http://localhost:8088/dashboard",
        ],
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()

