#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

mkdir -p "$ROOT/build/reports"

# Luôn chạy từ root, set PYTHONPATH chống import lỗi
cd "$ROOT"
export PYTHONPATH="$ROOT"

# Tạo/bổ sung baseline nếu chưa có, đồng thời xuất bản chạy này
python3 scripts/repo_probe.py --write-baseline > "$ROOT/build/reports/probe_probe_run.json"

echo "✅ generated: $ROOT/build/reports/probe_probe_run.json"
