#!/usr/bin/env bash
source .venv/bin/activate
set -euo pipefail
ENV_PATH="${1:-.env}"

set -a
source "$ENV_PATH"
set +a

WEB_HOST="${WEB_HOST:-127.0.0.1}"
WEB_PORT="${WEB_PORT:-8000}"

python -m uvicorn app.web.main:app --host "$WEB_HOST" --port "$WEB_PORT" --workers 1 &
WEB_PID=$!

python -m app.daemon.main &
DAEMON_PID=$!

echo "WEB:    http://${WEB_HOST}:${WEB_PORT}"
echo "EVENTS: http://${WEB_HOST}:${WEB_PORT}/api/events"
echo "CTRL+C to stop"

trap 'kill $DAEMON_PID $WEB_PID 2>/dev/null || true' INT TERM
wait
