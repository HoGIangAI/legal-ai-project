#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JSON="$ROOT/build/reports/probe_probe_run.json"

# 1) Tồn tại
if [ ! -f "$JSON" ]; then
  echo "❌ Missing $JSON. Run: bash scripts/probe_run.sh" >&2
  exit 2
fi

# 2) Cú pháp hợp lệ
if ! jq empty "$JSON" >/dev/null 2>&1; then
  echo "❌ Invalid JSON syntax or empty: $JSON" >&2
  exit 2
fi

# 3) Có khóa cốt lõi
if ! jq 'has("ok") and (has("issues") or has("files_count"))' "$JSON" | grep -q true; then
  echo "❌ JSON missing required keys (ok/issues/files_count)" >&2
  exit 2
fi

# 4) Thông tin nhanh cho người vận hành
echo "✅ JSON valid."
jq '{ok, files_count, issues_count:(.issues|length), warnings_count:(.warnings|length)}' "$JSON"

# 5) Nếu muốn fail khi có issues, bật STRICT=1
if [ "${STRICT:-0}" = "1" ]; then
  if [ "$(jq '.issues|length' "$JSON")" -gt 0 ]; then
    echo "❌ Issues present (STRICT=1). Review $JSON" >&2
    exit 2
  fi
fi
