#!/usr/bin/env bash
# usage: tee2 <cmd...>
# ví dụ: tee2 docker logs neo4j -n 50
set -euo pipefail
LOG_FILE=${LOOP_LOG:-loop.log}
{ "$@" 2>&1 | tee -a "$LOG_FILE" ; } || true
