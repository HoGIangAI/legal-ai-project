#!/usr/bin/env bash
set -euo pipefail
root="$(pwd)"
export PYTHONPATH="$root:${PYTHONPATH:-}"

# Ensure package inits
mkdir -p common services/ontology tools/ontology models
touch common/__init__.py services/__init__.py services/ontology/__init__.py tools/__init__.py tools/ontology/__init__.py models/__init__.py

# Must-have files
req_files=(
  "common/env.py"
  "common/log.py"
  "common/errors.py"
  "common/diagnostics.py"
  "models/graphops.py"
  "services/ontology/kafka_config.py"
  "tools/ontology/emit_graphops_jsonl.py"
)

echo "== Check required files =="
missing=0
for f in "${req_files[@]}"; do
  if [[ -f "$f" ]]; then
    echo "OK  $f"
  else
    echo "MISS $f"; missing=$((missing+1))
  fi
done

echo "== Import smoke =="
python3 - <<'PY' || true
import importlib, sys
mods = [
    "common.env",
    "common.log",
    "common.errors",
    "common.diagnostics",
    "models.graphops",
    "services.ontology.kafka_config",
    "tools.ontology.emit_graphops_jsonl",
]
for m in mods:
    try:
        mod = importlib.import_module(m)
        print("OK:", m, "->", getattr(mod, "__file__", "built-in"))
    except Exception as e:
        print("FAIL:", m, "->", e)
PY

if [[ $missing -gt 0 ]]; then
  echo "===> Missing $missing file(s). Please add them."
  exit 1
else
  echo "===> All required files present."
fi
