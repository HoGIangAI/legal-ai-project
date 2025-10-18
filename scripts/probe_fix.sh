#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JSON="$ROOT/build/reports/probe_probe_run.json"

need_jq() {
  if ! command -v jq >/dev/null 2>&1; then
    echo "❌ 'jq' not found. Install: sudo apt-get install -y jq  (or: brew install jq)" >&2
    exit 2
  fi
}

validate_json() {
  if [ ! -f "$JSON" ]; then
    echo "❌ Missing $JSON. Run: bash scripts/probe_run.sh" >&2
    exit 2
  fi
  if ! jq empty "$JSON" >/dev/null 2>&1; then
    echo "❌ Invalid JSON: $JSON. Re-run probe." >&2
    exit 2
  fi
}

autofix_dirs() {
  echo "==> Fix: make missing dirs"
  jq -r '.issues[]?|select(type=="string")|select(startswith("missing_dir:"))|sub("missing_dir:";"")' "$JSON" \
  | while read -r d; do
      [ -z "$d" ] && continue
      echo "mkdir -p $ROOT/$d"
      mkdir -p "$ROOT/$d"
    done
}

autofix_inits() {
  echo "==> Fix: add __init__.py"
  jq -r '.issues[]?|select(type=="string")|select(startswith("missing_init:"))|sub("missing_init:";"")' "$JSON" \
  | while read -r p; do
      [ -z "$p" ] && continue
      f="$ROOT/$p"
      if [ ! -f "$f" ]; then
        echo "touch $f"
        mkdir -p "$(dirname "$f")"
        printf "# package marker\n" > "$f"
      fi
    done
}

autofix_makefile() {
  echo "==> Fix: ensure Makefile targets"
  MK="$ROOT/Makefile"
  if [ ! -f "$MK" ]; then
    echo "⚠️  Makefile not found — creating minimal Makefile"
    cat > "$MK" <<'EOF'
.PHONY: diagnose emit consume audit verify
diagnose:
	python3 -m common.diagnostics --mode diagnose --json
emit:
	python3 -m tools.ontology.emit_graphops_jsonl --input $$ONTOLOGY_FILE --json
consume:
	python3 -m services.ontology.ontology_consumer --json
audit:
	python3 -m tools.ontology.audit_integrity --mode audit --json
verify:
	python3 -m tools.ontology.verify_after_emit --json
EOF
    return
  fi

  # Bổ sung các target tối thiểu nếu thiếu
  grep -Eq '^\s*diagnose:' "$MK" || cat >> "$MK" <<'EOF'

diagnose:
	python3 -m common.diagnostics --mode diagnose --json
EOF

  grep -Eq '^\s*emit:' "$MK" || cat >> "$MK" <<'EOF'

emit:
	python3 -m tools.ontology.emit_graphops_jsonl --input $$ONTOLOGY_FILE --json
EOF

  grep -Eq '^\s*consume:' "$MK" || cat >> "$MK" <<'EOF'

consume:
	python3 -m services.ontology.ontology_consumer --json
EOF

  grep -Eq '^\s*audit:' "$MK" || cat >> "$MK" <<'EOF'

audit:
	python3 -m tools.ontology.audit_integrity --mode audit --json
EOF

  grep -Eq '^\s*verify:' "$MK" || cat >> "$MK" <<'EOF'

verify:
	python3 -m tools.ontology.verify_after_emit --json
EOF
}

rerun_probe() {
  echo "==> Re-run probe to verify"
  python3 "$ROOT/scripts/repo_probe.py" --baseline "$ROOT/build/reports/probe_manifest.json" > "$ROOT/build/reports/probe_after_fix.json" || true
  echo "==> Summary:"
  if command -v jq >/dev/null 2>&1; then
    jq '{ok, issues, warnings, diff} | .issues |= (if .==null then [] else . end) | .warnings |= (if .==null then [] else . end)' "$ROOT/build/reports/probe_after_fix.json"
  else
    cat "$ROOT/build/reports/probe_after_fix.json"
  fi
}

main() {
  need_jq
  validate_json
  autofix_dirs
  autofix_inits
  autofix_makefile
  rerun_probe
  echo "✅ Fix done."
}

main "$@"
