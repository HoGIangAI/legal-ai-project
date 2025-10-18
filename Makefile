# ==============================================================================
# CẤU HÌNH CƠ BẢN VÀ MÔI TRƯỜNG ẢO
# ==============================================================================
.PHONY: venv install ... # thêm venv

PY?=python3
PIP?=pip

# ĐỊNH NGHĨA VENV
VENV_DIR := .venv
VENV_PYTHON := $(VENV_DIR)/bin/python
VENV_PIP := $(VENV_DIR)/bin/pip

# MACRO CHẠY TRONG VENV (Đảm bảo môi trường đã được cài đặt)
define RUN_IN_VENV
@if [ ! -d "$(VENV_DIR)" ]; then \
    echo "⚠️ Lỗi: Môi trường ảo không tồn tại. Vui lòng chạy 'make install' trước."; \
    exit 2; \
fi
$(VENV_PYTHON) -m $(1) $(2)
endef

# TARGET VENV
venv:
	@echo "🛠️ Creating virtual environment in $(VENV_DIR)..."
	$(PY) -m venv $(VENV_DIR)

install: venv
	@echo "📦 Installing dependencies..."
	$(VENV_PIP) install -e .[dev,ui]


# ==============================================================================
# SỬ DỤNG VENV CHO DÒNG DỮ LIỆU/DIAGNOSTICS
# ==============================================================================

# --- hạ tầng / preflight ---


# --- dòng dữ liệu ---
# Lưu ý: $(ONTOLOGY_FILE) là biến Makefile, không cần $$ (shell escape) nếu đã export
emit:
	@$(call RUN_IN_VENV,tools.ontology.emit_graphops_jsonl,--input $(ONTOLOGY_FILE) --json)

consume:
	@$(call RUN_IN_VENV,services.ontology.ontology_consumer,$(if $(VERBOSE),--verbose ,)$(if $(JSON),--json ,))


# ... Áp dụng RUN_IN_VENV cho audit, verify, và các lệnh chạy Python khác

# Makefile (fragment)
# Đảm bảo file .env được load, hoặc định nghĩa giá trị mặc định trực tiếp
export ONTOLOGY_FILE ?= ./data/ontology/ontology_thuytinh_dev.jsonl

# ========= Dev QoL (Quality of Life) =========
KAFKA_CONTAINER ?= legal-ai-platform-kafka-1
NEO4J_CONTAINER ?= neo4j
TOPIC           ?= $(TOPIC_GRAPHOPS)
GROUP_STAMP     := $(shell date +%s)

# 1) Khởi stack nhanh
up:
	docker compose -f docker-compose.kafka.yml up -d
	docker compose -f docker-compose.neo4j.yml up -d

# 2) Emit nhanh (sample 3 dòng)
emit-sample:
	head -n 3 $(ONTOLOGY_FILE) > /tmp/sample.jsonl
	@$(call RUN_IN_VENV,tools.ontology.emit_graphops_jsonl,--input /tmp/sample.jsonl --json --verbose)

# 3) Emit 1 edge demo
emit-edge-demo:
	printf '%s\n' '{"op":"upsert","edge":{"type":"REFERS_TO","src_label":"LawArticle","src_id":"A001","dst_label":"LawConcept","dst_id":"K001","props":{"note":"demo"}}}' > /tmp/edge_demo.jsonl
	@$(call RUN_IN_VENV,tools.ontology.emit_graphops_jsonl,--input /tmp/edge_demo.jsonl --json --verbose)

# 4) Consumer chạy nền (group mới + from-beginning)
replay:
	CONSUMER_GROUP=ontology-consumer-rerun-$(GROUP_STAMP) \
	AUTO_OFFSET_RESET=earliest \
	$(VENV_PYTHON) -m services.ontology.ontology_consumer --verbose --json

# 5) Consumer one-shot (thoát khi idle 3s) — cần patch nhỏ ở B)
consume-once:
	CONSUMER_GROUP=ontology-consumer-once-$(GROUP_STAMP) \
	AUTO_OFFSET_RESET=earliest \
	$(VENV_PYTHON) -m services.ontology.ontology_consumer --verbose --json --once --idle-timeout-sec 3

# 6) Count nhanh trong Neo4j
counts:
	docker exec $(NEO4J_CONTAINER) cypher-shell -u neo4j -p 'legalai123' "MATCH (n:LawArticle) RETURN count(n) AS law_articles;"
	docker exec $(NEO4J_CONTAINER) cypher-shell -u neo4j -p 'legalai123' "MATCH (n:LawConcept) RETURN count(n) AS law_concepts;"
	docker exec $(NEO4J_CONTAINER) cypher-shell -u neo4j -p 'legalai123' "MATCH ()-[r:REFERS_TO]->() RETURN count(r) AS refers_edges;"

# 7) Peek topic & DLQ
peek:
	docker exec -it $(KAFKA_CONTAINER) kafka-console-consumer --bootstrap-server localhost:9092 \
		--topic $(TOPIC) --from-beginning --max-messages 5 -timeout-ms 2000 || true
peek-dlq:
	docker exec -it $(KAFKA_CONTAINER) kafka-console-consumer --bootstrap-server localhost:9092 \
		--topic $(TOPIC).dlq.v1 --from-beginning --max-messages 5 -timeout-ms 2000 || true

# 8) Xem group offsets (tự động dùng group cuối cùng bạn export)
groups:
	docker exec -it $(KAFKA_CONTAINER) kafka-consumer-groups --bootstrap-server localhost:9092 --list | head -n 50

describe:
	@if [ -z "$$CONSUMER_GROUP" ]; then echo "Set CONSUMER_GROUP first"; exit 1; fi
	docker exec -it $(KAFKA_CONTAINER) kafka-consumer-groups --bootstrap-server localhost:9092 \
		--group "$$CONSUMER_GROUP" --describe || true
# --- Audit / Verify ---
audit:
	@$(call RUN_IN_VENV,tools.ontology.audit_integrity,--json --fail-on-issues --output build/reports/ontology_audit.json) || true

verify: audit
	@echo "📄 Audit report → build/reports/ontology_audit.json"

# --- Diagnostics ---
diagnose:
	@$(call RUN_IN_VENV,common.diagnostics,--json)
ui:
	@$(call RUN_IN_VENV,uvicorn,tools.monitor.api:app --host 0.0.0.0 --port 8088 --reload)

