#!/usr/bin/env bash
set -euo pipefail
GATEWAY="${OPS_GATEWAY_URL:-http://localhost:8080}"

# usage:
#   ops2chat new "Title" < file.log
#   some_cmd 2>&1 | ops2chat new "Build Failed"
#   tail -n 200 loop.log | ops2chat append <incident_id>

cmd="${1:-}"; shift || true

json_array() {
  # stdin to JSON items
  awk '{gsub(/"/,"\\\""); print "{\"message\":\""$0"\"}"}' | paste -sd, - | sed 's/^/[/' | sed 's/$/]/'
}

if [ "$cmd" = "new" ]; then
  title="${1:-Incident}"; shift || true
  lines="$(cat || true)"
  items="$(printf "%s\n" "$lines" | json_array)"
  payload=$(printf '{"title":"%s","items":%s}' "$title" "$items")
  curl -s -X POST "$GATEWAY/ops/incident" -H 'Content-Type: application/json' -d "$payload" | jq -r '.chat_snippet'
elif [ "$cmd" = "append" ]; then
  iid="${1:-}"; shift || true
  if [ -z "$iid" ]; then echo "Usage: ops2chat append <incident_id>"; exit 1; fi
  lines="$(cat || true)"
  items="$(printf "%s\n" "$lines" | json_array)"
  payload=$(printf '{"incident_id":"%s","items":%s}' "$iid" "$items")
  curl -s -X POST "$GATEWAY/ops/incident/append" -H 'Content-Type: application/json' -d "$payload" | jq -r '.chat_snippet'
else
  echo "Usage:"
  echo "  some_cmd 2>&1 | tools/ops/ops2chat.sh new \"Build Failed\""
  echo "  tail -n 200 loop.log | tools/ops/ops2chat.sh append <incident_id>"
  exit 1
fi
