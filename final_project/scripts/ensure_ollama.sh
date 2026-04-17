#!/usr/bin/env bash
# Restart Ollama's HTTP API on clean port(s) (default 11434).
# Run from anywhere:  bash final_project/scripts/ensure_ollama.sh
# Or:                 ./scripts/ensure_ollama.sh   (from final_project/)
#
# Stops whatever is listening on OLLAMA_PORT (or OLLAMA_PORTS), then runs `ollama serve` in the background.
# If the Ollama menu-bar app holds the port, quit it first (Ollama → Quit) or this script
# will kill the listener so `ollama serve` can bind.

set -euo pipefail

HOST="${OLLAMA_HOST:-127.0.0.1}"
PORT="${OLLAMA_PORT:-11434}"
# Optional multi-port mode:
#   export OLLAMA_PORTS="11434,11435,11436"
PORTS_RAW="${OLLAMA_PORTS:-$PORT}"

_tmp_base="${TMPDIR:-/tmp}"
LOG_DIR="${OLLAMA_LOG_DIR:-${_tmp_base}/ollama-logs}"
PID_DIR="${OLLAMA_PID_DIR:-${_tmp_base}/ollama-pids}"

mkdir -p "$LOG_DIR" "$PID_DIR"

declare -a PORTS=()
IFS=',' read -r -a _ports_split <<<"$PORTS_RAW"
for p in "${_ports_split[@]}"; do
  p="$(echo "$p" | xargs)"  # trim
  [[ -z "$p" ]] && continue
  PORTS+=("$p")
done

if [[ ${#PORTS[@]} -eq 0 ]]; then
  echo "error: no ports specified (OLLAMA_PORTS / OLLAMA_PORT empty?)" >&2
  exit 1
fi

if ! command -v ollama >/dev/null 2>&1; then
  echo "error: 'ollama' not found in PATH. Install from https://ollama.com" >&2
  exit 1
fi

echo "Ollama: stopping anything listening on ${HOST}:{${PORTS[*]}} ..."

# Kill processes listening on PORT(S) (macOS / Linux lsof)
if command -v lsof >/dev/null 2>&1; then
  for port in "${PORTS[@]}"; do
    PIDS="$(lsof -ti TCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
    if [[ -n "${PIDS}" ]]; then
      # shellcheck disable=SC2086
      kill -9 ${PIDS} 2>/dev/null || true
      sleep 0.2
    fi
  done
fi

# Stop CLI servers we may have started earlier (by PID files)
for port in "${PORTS[@]}"; do
  pidfile="${PID_DIR}/ollama-serve-${port}.pid"
  if [[ -f "$pidfile" ]]; then
    old_pid="$(cat "$pidfile" 2>/dev/null || true)"
    if [[ -n "$old_pid" ]]; then
      kill "$old_pid" 2>/dev/null || true
      sleep 0.2
      kill -9 "$old_pid" 2>/dev/null || true
    fi
    rm -f "$pidfile" 2>/dev/null || true
  fi
done

# If any port still busy (e.g. app respawn), warn
if command -v lsof >/dev/null 2>&1; then
  for port in "${PORTS[@]}"; do
    if lsof -ti TCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "error: port ${port} still in use. Quit the Ollama desktop app (menu bar) and retry." >&2
      exit 1
    fi
  done
fi

echo "Ollama: starting ${#PORTS[@]} instance(s) of \`ollama serve\` ..."

for port in "${PORTS[@]}"; do
  log="${LOG_DIR}/ollama-serve-${port}.log"
  pidfile="${PID_DIR}/ollama-serve-${port}.pid"

  echo "  - starting on ${HOST}:${port} (log: ${log})"
  # Ollama uses OLLAMA_HOST as bind host:port (e.g. 127.0.0.1:11434)
  nohup env OLLAMA_HOST="${HOST}:${port}" ollama serve >"$log" 2>&1 &
  echo $! >"$pidfile"
done

# Wait for API(s)
for port in "${PORTS[@]}"; do
  base="http://${HOST}:${port}"
  for i in $(seq 1 40); do
    if curl -sf "${base}/api/tags" >/dev/null 2>&1; then
      echo "Ollama: ready at ${base}"
      break
    fi
    sleep 0.5
    if [[ "$i" -eq 40 ]]; then
      log="${LOG_DIR}/ollama-serve-${port}.log"
      echo "error: Ollama on port ${port} did not become ready within ~20s. See ${log}" >&2
      exit 1
    fi
  done
done

echo ""
echo "Notebook config:"
echo "  OLLAMA_URLS = ("
for port in "${PORTS[@]}"; do
  echo "      \"http://${HOST}:${port}/api/generate\","
done
echo "  )"
exit 0
