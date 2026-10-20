COMPOSE := docker compose

.PHONY: up down nuke test test-dags health logs psql seed simulate-day reprocess monitor chaos chaos-reset replay-quarantine verify-scenarios demo dbt-deps dbt-build docs

# A fresh clone has no .env: seed it from the committed placeholders.
.env:
	cp .env.example .env

up: .env
	$(COMPOSE) up -d --build

down: .env
	$(COMPOSE) down

# Also empties landing/ (keeping the folder and any .gitkeep), so a re-seeded stack never inherits the
# previous stack's quarantined, re-sent or processed files. The stack is stopped first so nothing is
# writing; the cleanup runs in the app container because the files may belong to the container user.
nuke: .env
	$(COMPOSE) down --remove-orphans
	$(COMPOSE) --profile tools run --rm --no-deps app sh -c \
		"find /opt/drct/landing -mindepth 1 ! -type d ! -name .gitkeep -delete \
		&& find /opt/drct/landing -mindepth 1 -type d -empty -delete"
	$(COMPOSE) down -v --remove-orphans

# tests/dags needs Airflow, so it is run by test-dags in the Airflow containers instead.
test: .env
	$(COMPOSE) --profile tools run --rm --build app pytest tests/unit tests/integration -q

test-dags: .env
	$(COMPOSE) exec -T airflow-scheduler pytest -p no:cacheprovider tests/dags -q

health: .env
	./scripts/healthcheck.sh

logs: .env
	$(COMPOSE) logs -f --tail=100

psql: .env
	$(COMPOSE) exec warehouse psql -U drct -d warehouse

# Operator commands (Task 5): seed history, then simulate days. DAYS=n simulates n days.
DAYS ?= 1

# Seed and simulate-day land files (picked up by the file-arrival triggers), then trigger ingest_fx
# through the Airflow API so FX syncs straight away instead of at the next 10-minute cron tick.
seed: .env
	$(COMPOSE) --profile tools run --rm app python -m drct.ops seed

simulate-day: .env
	$(COMPOSE) --profile tools run --rm app python -m drct.ops simulate-day --days $(DAYS)

# Re-ingest a date range from _resend/ or _processed/ and wait until the pipeline is idle again.
# Usage: make reprocess FROM=2026-07-01 TO=2026-07-03
reprocess: .env
	@test -n "$(FROM)" -a -n "$(TO)" || { echo "usage: make reprocess FROM=YYYY-MM-DD TO=YYYY-MM-DD"; exit 2; }
	$(COMPOSE) --profile tools run --rm app python -m drct.ops reprocess --from $(FROM) --to $(TO)

# Chaos (Task 16): switch a failure scenario on for the next simulated day.
# Usage: make chaos SCENARIO=volume_spike PARAMS="multiplier=0.1"
chaos: .env
	@test -n "$(SCENARIO)" || { echo "usage: make chaos SCENARIO=<name> [PARAMS=\"k=v k2=v2\"]"; exit 2; }
	$(COMPOSE) --profile tools run --rm app python -m drct.ops chaos enable $(SCENARIO) $(foreach p,$(PARAMS),--param $(p))

# Switch chaos off and re-send clean files for the affected dates (then run the printed reprocess).
chaos-reset: .env
	$(COMPOSE) --profile tools run --rm app python -m drct.ops chaos reset

# Move a source's quarantined files back to landing so they are ingested again.
# Usage: make replay-quarantine SOURCE=sales
replay-quarantine: .env
	@test -n "$(SOURCE)" || { echo "usage: make replay-quarantine SOURCE=<source>"; exit 2; }
	$(COMPOSE) --profile tools run --rm app python -m drct.ops replay-quarantine --source $(SOURCE)

# Force a monitoring cycle (freshness, volume, SLA, incident reconcile) and wait until it has finished.
monitor: .env
	$(COMPOSE) --profile tools run --rm app python -m drct.ops monitor

# End to end (Task 17): every chaos scenario injected, detected, fixed and resolved on the live stack.
# Exits non-zero unless all are detected and resolved. Takes tens of minutes.
verify-scenarios: .env
	$(COMPOSE) --profile tools run --rm app python -m drct.ops verify-scenarios

# From nothing to a running, seeded, monitored stack with three more simulated days. Safe to re-run:
# on an already-seeded stack the seed step is skipped (it still syncs FX) and the days carry on.
demo: .env
	$(MAKE) up
	$(COMPOSE) --profile tools run --rm app python -m drct.ops seed --if-needed
	$(MAKE) simulate-day DAYS=3
	$(MAKE) monitor

# dbt (Task 10). The app image has dbt on PATH; dbt-deps installs dbt_utils / dbt_expectations into dbt/dbt_packages.
DBT := $(COMPOSE) --profile tools run --rm app dbt
DBT_ARGS := --project-dir dbt --profiles-dir dbt

dbt-deps: .env
	$(DBT) deps $(DBT_ARGS)

dbt-build: .env
	$(DBT) build $(DBT_ARGS)

# Generates the catalog and lineage graph, then serves it on localhost:8081 (short-lived run with its own
# port mapping, bound to 127.0.0.1 only).
docs: .env
	$(COMPOSE) --profile tools run --rm -p 127.0.0.1:8081:8081 app sh -c \
		"dbt docs generate $(DBT_ARGS) && dbt docs serve $(DBT_ARGS) --port 8081 --host 0.0.0.0"
