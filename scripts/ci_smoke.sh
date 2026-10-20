#!/usr/bin/env bash
# Builds the whole project from scratch with no Airflow: applies the warehouse init SQL, seeds 90
# days of history, loads every landing file and the FX rates, then runs `dbt build`.
# Exits non-zero at the first failure (including any failing dbt error-severity test).
#
# The target comes from the environment, so the same script runs in CI and locally:
#   WAREHOUSE_HOST WAREHOUSE_PORT WAREHOUSE_USER WAREHOUSE_PASSWORD WAREHOUSE_DB  (dbt profile)
#   WAREHOUSE_DSN   LANDING_DIR   FX_SOURCE (use "stub": no network)
# Optional: DBT_BIN (default "dbt"). Python needs the `drct` package on PYTHONPATH (default: src).
# Point it at a throwaway database and a throwaway LANDING_DIR: it re-seeds with --force.
set -euo pipefail

for var in WAREHOUSE_HOST WAREHOUSE_PORT WAREHOUSE_USER WAREHOUSE_PASSWORD WAREHOUSE_DB \
    WAREHOUSE_DSN LANDING_DIR FX_SOURCE; do
    : "${!var:?$var is required}"
done

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PYTHONPATH="${PYTHONPATH:-$ROOT/src}"
DBT="${DBT_BIN:-dbt}"
DBT_ARGS=(--project-dir dbt --profiles-dir dbt)

echo "==> applying warehouse init SQL"
# *.sql only, in filename order (06_role_password.sh is the docker entrypoint's job; nothing here
# logs in as the read-only role). Same approach as the test harness.
python - warehouse/init <<'PY'
import sys
from pathlib import Path

import psycopg

from drct.common.config import get_settings

with psycopg.connect(get_settings().warehouse_dsn, autocommit=True) as conn:
    for sql_file in sorted(Path(sys.argv[1]).glob("*.sql")):
        print(f"    {sql_file.name}")
        conn.execute(sql_file.read_text())
PY

echo "==> seeding"
mkdir -p "$LANDING_DIR"
python -m drct.ops seed --force --no-trigger

echo "==> loading landing files and FX"
python -m drct.ops load-all

echo "==> dbt deps"
"$DBT" deps "${DBT_ARGS[@]}"

echo "==> dbt build"
"$DBT" build "${DBT_ARGS[@]}"

echo "==> smoke test passed"
