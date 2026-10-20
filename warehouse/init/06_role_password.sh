#!/bin/bash
# Applies the control_tower_ro password from the environment so no secret lives in SQL.
# Runs only via /docker-entrypoint-initdb.d on first warehouse start (the test harness runs *.sql only).
set -euo pipefail
psql -v ON_ERROR_STOP=1 -v pw="${CONTROL_TOWER_RO_PASSWORD:?CONTROL_TOWER_RO_PASSWORD is required}" \
    --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
alter role control_tower_ro password :'pw';
SQL
