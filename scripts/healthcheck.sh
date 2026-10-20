#!/usr/bin/env bash
# Prints OK/FAIL for Airflow, Streamlit and the warehouse; exits non-zero if any check fails.
# The stack publishes its ports on 127.0.0.1 only, so the checks use that address.
set -u

AIRFLOW_URL="${AIRFLOW_URL:-http://127.0.0.1:8080}"
STREAMLIT_URL="${STREAMLIT_URL:-http://127.0.0.1:8501}"
WAREHOUSE_HOST_PORT="${WAREHOUSE_HOST_PORT:-5433}"

status=0
ok() { echo "OK    $1"; }
fail() { echo "FAIL  $1"; status=1; }

# Airflow: scheduler, triggerer and dag_processor must all report healthy.
if body=$(curl -fsS "${AIRFLOW_URL}/api/v2/monitor/health" 2>/dev/null) \
  && [ "$(echo "$body" | jq -r '[.scheduler.status, .triggerer.status, .dag_processor.status] | all(. == "healthy")')" = "true" ]; then
  ok "airflow (scheduler, triggerer, dag_processor healthy)"
else
  fail "airflow (${AIRFLOW_URL}/api/v2/monitor/health: ${body:-no response})"
fi

# Streamlit: the health endpoint returns the literal string "ok".
if [ "$(curl -fsS "${STREAMLIT_URL}/_stcore/health" 2>/dev/null)" = "ok" ]; then
  ok "streamlit control tower"
else
  fail "streamlit control tower (${STREAMLIT_URL}/_stcore/health)"
fi

# Warehouse: use the host's pg_isready when present, else the one inside the container.
if command -v pg_isready >/dev/null 2>&1; then
  pg_isready -q -h 127.0.0.1 -p "${WAREHOUSE_HOST_PORT}"
else
  docker compose exec -T warehouse pg_isready -q -h localhost -p 5432
fi
if [ $? -eq 0 ]; then
  ok "warehouse postgres (host port ${WAREHOUSE_HOST_PORT})"
else
  fail "warehouse postgres (host port ${WAREHOUSE_HOST_PORT})"
fi

exit "$status"
