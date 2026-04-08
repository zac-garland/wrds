#!/usr/bin/env bash
# Restart Ollama's HTTP API on a clean port (default 11434).
# Run from anywhere:  bash final_project/scripts/ensure_ollama.sh
# Or:                 ./scripts/ensure_ollama.sh   (from final_project/)
#
# Stops whatever is listening on OLLAMA_PORT, then runs `ollama serve` in the background.
# If the Ollama menu-bar app holds the port, quit it first (Ollama → Quit) or this script
# will kill the listener so `ollama serve` can bind.

set -euo pipefail

HOST="${OLLAMA_HOST:-127.0.0.1}"
PORT="${OLLAMA_PORT:-11434}"
LOG="${OLLAMA_LOG:-${TMPDIR:-/tmp}/ollama-serve.log}"
PIDFILE="${OLLAMA_PIDFILE:-${TMPDIR:-/tmp}/ollama-serve.pid}"

if ! command -v ollama >/dev/null 2>&1; then
  echo "error: 'ollama' not found in PATH. Install from https://ollama.com" >&2
  exit 1
fi

echo "Ollama: stopping anything listening on ${HOST}:${PORT} ..."

# Kill processes listening on PORT (macOS / Linux lsof)
if command -v lsof >/dev/null 2>&1; then
  PIDS="$(lsof -ti TCP:"$PORT" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "${PIDS}" ]]; then
    # shellcheck disable=SC2086
    kill -9 ${PIDS} 2>/dev/null || true
    sleep 1
  fi
fi

# Stop CLI servers we may have started earlier
pkill -f "ollama serve" 2>/dev/null || true
sleep 1

# If port still busy (e.g. app respawn), warn
if command -v lsof >/dev/null 2>&1; then
  if lsof -ti TCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "error: port ${PORT} still in use. Quit the Ollama desktop app (menu bar) and retry." >&2
    exit 1
  fi
fi

echo "Ollama: starting \`ollama serve\` (log: ${LOG}) ..."
nohup ollama serve >"$LOG" 2>&1 &
echo $! >"$PIDFILE"

# Wait for API
BASE="http://${HOST}:${PORT}"
for i in $(seq 1 30); do
  if curl -sf "${BASE}/api/tags" >/dev/null 2>&1; then
    echo "Ollama: ready at ${BASE}"
    echo "  Notebook: OLLAMA_URL = \"${BASE}/api/generate\""
    exit 0
  fi
  sleep 0.5
done

echo "error: Ollama did not become ready within ~15s. See ${LOG}" >&2
exit 1
