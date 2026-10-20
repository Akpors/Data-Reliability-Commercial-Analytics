# Data Reliability Control Tower — Project Plan & Design

*Design spec: the goals, constraints and decisions the build follows. Where details were refined during the build (pinned versions, thresholds), the code and the README are authoritative.*

---

## 1. Context

**Why this project:** It's a portfolio project to showcase on LinkedIn, the first in a series.

**The idea:** Build a miniature, production-shaped data platform where the interesting part is *everything before the dashboard*. Data flows from sources through ingestion, a raw layer, transformations, quality checks and an analytical layer to BI. Then we deliberately break it and show that the platform detects, explains and surfaces each failure.

**The LinkedIn angle:** *"Most analytics failures don't start in the dashboard. They start much earlier."*


**Time box:** About 2 weeks of focused work (10 build days). It's an MVP that covers *every* item in the brief, with no stretch goals unless time remains.

---

## 2. Goals, non-goals, anti-goals

**Goals**
- Run an end-to-end pipeline: 5 sources → Python ingestion → raw → dbt (staging → intermediate → marts) → monitoring → Streamlit Control Tower.
- Use **Airflow 3 Assets / event-driven scheduling**, not just cron.
- Inject all 7 failure types on demand, and have each one detected and surfaced as an incident:
  - duplicate transactions
  - missing dates
  - broken foreign keys
  - schema changes
  - negative revenue
  - stale data
  - volume spikes
- Cover all 6 Control Tower panels: **Freshness | Quality | Volume anomalies | Failed tests | Pipeline SLA | Data incidents**.
- Show the technical depth the brief asks for:
  - SQL modelling
  - data tests
  - freshness checks
  - schema validation
  - anomaly detection
  - retries and backfills
  - structured logging
  - lineage
- Publish a public GitHub repo with a polished README, an architecture diagram, a 2–3 minute demo video and a LinkedIn post draft.

**Non-goals**
- Commercial analytics KPIs (that's a separate future project; the marts just need to be clean enough to build on).
- Cloud deployment or a hosted live demo.
- Real alerting integrations (Slack, PagerDuty). Incidents live in the Control Tower.
- Big-data scale, streaming or Kafka.
- Power BI. It doesn't run on macOS; Streamlit replaces it.

**Anti-goals** (failure even if it technically works)
- A pretty dashboard sitting on a thin pipeline. The depth must live in the pipeline and monitoring.
- Failures that are "detected" only because the detector was hard-coded for the injected case. Detectors must be generic rules that would catch real occurrences.
- Anything installed into or altering the host's global environment beyond Docker (or OrbStack) itself.
- A repo a recruiter can't run with one command, or one that leaks secrets.

---

## 3. Constraints & environment isolation

**Machine:** Runs on any machine with Docker (Compose v2) or OrbStack, on arm64 or x86-64. Nothing else is installed on the host, and the host's own Python is never used.

**Isolation:**
- **Docker or OrbStack** is the only host install. Everything else runs in containers via `docker compose`.
- Use a dedicated compose project name (`drct`) with its own network and named volumes, so `docker compose down -v` removes everything cleanly.
- Pick non-default host ports to avoid clashes:

  | Service | Host port |
  |---|---|
  | Airflow UI | 8080 |
  | Warehouse Postgres | 5433 |
  | Streamlit | 8501 |

- Python 3.12 runs inside the images. Optionally, `uv` can manage a project-local `.venv` for editor tooling and tests outside containers. It never touches the host's system Python.
- Secrets go in `.env` (git-ignored). A committed `.env.example` documents them.
- A `Makefile` is the single entry point: `make up`, `make down`, `make seed`, `make chaos SCENARIO=…`, `make test`, `make demo`.

---

## 4. Stack (decided)

| Layer | Choice | Notes |
|---|---|---|
| Containers | Docker or OrbStack + docker compose | Nothing else on the host |
| Orchestration | Apache Airflow 3.x (latest stable, pinned at setup), LocalExecutor | LocalExecutor keeps RAM low; no Celery/Redis |
| Warehouse | PostgreSQL 16 (separate container from Airflow's metadata DB) | *My call:* familiar, fully featured SQL, and dbt-postgres is mature |
| Ingestion | Python 3.12, pandas/pyarrow, Pydantic (or pandera) data contracts | |
| Transformation | dbt-core + dbt-postgres, plus `dbt_utils` and `dbt_expectations` | |
| Monitoring | Custom Python detectors + dbt `run_results.json` / `sources.json` parsing, written to a `monitoring` schema | *My call:* custom logic is more impressive to explain than a black-box tool |
| BI / Control Tower | Streamlit (+ Plotly) | Python-native and runs in a container |
| External API | Frankfurter FX-rates API (free, no key, ECB data) | *My call:* GBP base currency with multi-currency sales; the API is a real network dependency, so retries matter |
| Testing / CI | pytest, ruff, sqlfluff (optional), GitHub Actions | |

### Final pinned versions (recorded at the end of the build)

| Component | Version |
|---|---|
| Apache Airflow | 3.3.2 (`apache/airflow:3.3.2-python3.12`, official constraints file for 3.3.2 / Python 3.12) |
| Python (all images) | 3.12 |
| PostgreSQL (warehouse and Airflow metadata) | 16 (`postgres:16`) |
| dbt-core / dbt-postgres | 1.12.6 / 1.10.2, in a separate venv in each image (R11 bumped them from the planned 1.10.x) |
| dbt packages | `dbt-labs/dbt_utils` 1.4.1, `metaplane/dbt_expectations` 0.10.10 |
| App libraries | pandas 3.0.6, pyarrow 26.0.0, psycopg 3.3.6, streamlit 1.66.0, plotly 7.1.0, structlog 26.1.0, httpx 0.28.1, Faker 40.43.0 |
| Tooling | pytest 9.1.1, pytest-asyncio 1.4.0, ruff 0.17.1 |

The file-arrival design question in §9 was settled with the primary path: an `AssetWatcher` with a
custom, non-deleting `FileArrivalTrigger` works at 3.3.2 (see `docs/architecture.md`).

---

## 5. Architecture

```
 landing/ (files)  ─┐
 FX API (HTTP)     ─┼─► Ingestion DAGs (Python) ──► raw.*  ──┐   quarantine.* (rejected rows/files)
 generator (sim)   ─┘     contracts · retries · logs          │
                              │ emits Asset events            ▼
                              └────────────► dbt DAG (Asset-scheduled)
                                             staging → intermediate → marts
                                             dbt tests · source freshness
                                                      │ emits Asset event
                                                      ▼
                                       Monitoring DAG (Asset-scheduled)
                                       volume anomalies · SLA · incidents
                                                      │
                                                      ▼
                                       monitoring.* ──► Streamlit Control Tower
```

**Warehouse schemas:** `raw`, `quarantine`, `staging`, `intermediate`, `marts`, `monitoring`.

### 5.1 The synthetic company

*My call:* a UK online retailer selling in GBP/EUR/USD.

- A seeded Python generator (Faker) produces **90 days of history** so that anomaly baselines exist.
- After that it runs on a **simulated clock**: `make simulate-day` drops the next day's files into `landing/`.
- This lets the demo "run the business" for days in minutes.

**The simulated clock is the single source of "now"** *(my call)*:
- `control.sim_clock` holds one row, the current simulated timestamp, and `make simulate-day` advances it.
- A SQL function `control.sim_now()` returns it. (`control` schema holds sim clock + chaos state, hidden from the read-only Control Tower user.)
- Every "now" comparison uses `sim_now()`: freshness, SLAs, stale-data checks, and the incidents' `first_seen` / `last_seen`.
- dbt source freshness is made sim-aware by overriding the `collect_freshness` macro in the project to compare against `control.sim_now()` instead of `current_timestamp`.
- `_loaded_at` stays real wall-clock time, for audit. Alongside it, `_sim_loaded_at` carries sim time and is what freshness reads.
- **Calendar anchoring:** the simulated calendar sits on real past dates. Its "day 0" is 180 days before the real date of the first seed (superseded: this plan first said 90, which leaves no room to simulate forward before sim dates pass today, where FX has no rates; the code and README use 180), so Frankfurter has real historical rates for every sim date. Weekends and holidays carry the last published rate forward, as the API itself does.
- **Result:** "stale for 2 sim-days" can be demonstrated in seconds by advancing the clock without a delivery. It fits comfortably in the demo video.

| Source | Format | Cadence | Grain |
|---|---|---|---|
| Daily sales | CSV file per day in `landing/sales/` | daily | order line |
| Customers | JSON full extract | daily | customer |
| Products | JSON / CSV catalogue | daily (slowly changing) | product |
| Payment transactions | Parquet per day | daily | payment |
| FX rates | Frankfurter API | daily | currency/date |

The format mix is deliberate, to show ingestion range.

### 5.2 Ingestion (Python)

- **Data contracts:** one per source (columns, types, nullability, allowed values), held in code.
- **Schema validation:** each incoming file is checked against its contract.
  - Additive changes (new column) → load, and log a `schema_change` warning.
  - Breaking changes (missing or renamed column, type change) → reject the file to `quarantine` and raise an incident.
- **Idempotent loads:** delete-then-insert per `(source, business_date)`, so reruns and backfills are safe.
- **Lineage columns:** every raw row carries `_loaded_at`, `_source_file` and `_batch_id`.
- **Run logging:** each run writes structured JSON logs and a row to `monitoring.pipeline_runs` (`dag_id`, `run_id`, `started`, `ended`, `status`, `rows_in`, `rows_rejected`).
- **Retries:** the FX API call uses Airflow task retries with exponential backoff. A chaos switch makes the API flaky, to prove that retries work.

### 5.3 Orchestration (Airflow 3)

- **One ingestion DAG per source.** Each declares `outlets=[Asset("warehouse://raw/<source>")]`.
- **Event-driven triggering.**
  - File sources (sales, payments, customers, products) are triggered by file arrival in `landing/`.
  - Note: the built-in `FileDeleteTrigger` used with Asset watchers *deletes* the file, so it isn't usable as-is. *My call:* write a small custom trigger that fires on arrival without deleting, plugged into an `AssetWatcher`. That's the stronger showcase story. The fallback is a deferrable `FileSensor` DAG that emits the raw Asset.
  - After a successful load, ingestion moves files to `landing/_processed/` (or `landing/_quarantine/` if rejected).
  - Only the FX API ingestion is cron-scheduled. That makes the "not everything is cron" story explicit.
- **The dbt DAG runs on Assets.** It's scheduled on a conditional Asset expression, e.g. `(raw/sales & raw/payments) | raw/customers | raw/products`. Final design choice is the builder's. It emits `Asset("warehouse://marts")`.
- **Monitoring must run even when dbt fails.** An Asset is only emitted when its producing task succeeds, so a blocking test failure would otherwise never wake the monitor. *My call:* the dbt DAG ends with a `publish_dbt_artifacts` task (`trigger_rule="all_done"`). It always runs, parses `run_results.json` / `sources.json` / `manifest.json` into `monitoring`, and emits `Asset("warehouse://monitoring/dbt_results")`. The monitoring DAG is scheduled on *that* Asset, plus a cron fallback every 30 minutes. The fallback catches *stale data*, because a missing upstream event never triggers anything.
- **Backfills / reprocessing.** Airflow 3's built-in backfill only works on time-scheduled DAGs, and it walks real logical dates, not sim dates. *My call:*
  - A dedicated `reprocess` DAG, triggered manually with a `start_sim_date` / `end_sim_date` parameter.
  - It re-ingests the landing (or regenerated) files for those dates idempotently, then emits the raw Assets, so dbt and monitoring follow automatically.
  - The FX DAG is time-scheduled, so it *also* demonstrates native Airflow 3 backfill for a date range. Both mechanisms are shown, and the README explains why each exists.
- **Pipeline SLA:** Airflow 3 removed the old SLA-miss feature. SLAs are defined in config, e.g. "marts refreshed by 07:00 sim-time" and "ingestion ≤ 10 min". They're evaluated by the monitoring DAG against `pipeline_runs`. Airflow 3.1+ Deadline Alerts may be used additionally if stable at the pinned version.

### 5.4 Transformation (dbt)

- **staging:** typed and renamed views over `raw`, one per source. They do **not** deduplicate, so quality tests on staging see the data as it arrived. *My call:* otherwise duplicates would be silently hidden and never detected.
- **intermediate:** deduplication happens here, with the duplicates counted and exposed as a metric. Models include `int_orders_enriched` (sales + products + FX to GBP) and `int_payments_matched` (payments joined to orders).
- **marts:**
  - `fct_sales` (order-line grain, GBP revenue)
  - `fct_payments`
  - `dim_customer`, `dim_product`
  - `dim_date`
- **Tests** (all results captured into the monitoring schema):

  | Test | Catches |
  |---|---|
  | `unique` on transaction/payment ids | duplicates |
  | `not_null` on dates and keys | missing dates |
  | `relationships` from fact to dims | broken foreign keys |
  | `dbt_expectations` range checks (revenue ≥ 0 unless refund type) | negative revenue |
  | *(volume spikes are caught by the robust-z detector in monitoring, not static dbt bounds)* | volume spikes |
  | `dbt_utils` date-gap checks | missing dates |

- **Freshness:** `dbt source freshness` with warn/error thresholds per source.
- **Severity — a "circuit breaker"** *(my call)*. Integrity failures stop bad data reaching BI. Business-rule and statistical oddities still load, but are flagged.

  | Severity | Tests | Effect |
  |---|---|---|
  | `error` (blocking) | `unique` on transaction/payment ids, `relationships`, `not_null` on keys and dates | Staging-layer failures stop the marts building. The marts keep yesterday's good data and go stale, which feeds the SLA panel. |
  | `warn` | Negative revenue range check, missing-dates check | Data flows through, and an incident is still raised. |

  Freshness has its own warn/error thresholds. Schema-contract breaks are handled earlier, at ingestion (the file is quarantined).

  This gives the demo a clear story: *"bad data never reached the dashboard, and here's the incident explaining why it's a day behind."*
- **Package pin:** use the maintained `metaplane/dbt_expectations` fork. The original `calogica` package is archived.
- **Lineage:** dbt docs lineage graph, plus Airflow's Asset graph. The Control Tower uses `manifest.json` to show each incident's **blast radius** (which downstream models and panels are affected).
- **Running dbt:** a single `dbt build` via BashOperator (decided at planning: one invocation = one `run_results.json` to publish). Cosmos is a possible later enhancement.

### 5.5 Monitoring & incidents (`monitoring` schema)

| Table | Content |
|---|---|
| `pipeline_runs` | every DAG/task run, durations, row counts, status |
| `test_results` | parsed from dbt `run_results.json` per run |
| `freshness` | per source: last loaded, age, status (from dbt `sources.json` + raw `_loaded_at`) |
| `volume_metrics` | daily row counts per source/model |
| `volume_anomalies` | rows flagged by a rolling 14-day z-score (or MAD), with \|z\| > 3 as the default threshold |
| `schema_events` | contract diffs detected at ingestion |
| `sla_results` | each SLA check: expected, actual, met/breached |
| `incidents` | one row per open/resolved issue (details below) |

**Incident columns:** `id`, `type`, `severity`, `asset`, `first_seen`, `last_seen`, `status`, `detail`, `affected_downstream`.

**Incident engine rules:**
- Any failed check opens an incident, or updates `last_seen` on an existing open one (deduplicated by `type + asset`).
- When the same check passes again, the incident auto-resolves.
- That lifecycle is what makes the demo video compelling.

### 5.6 Failure injection ("chaos") toolkit

Run with `make chaos SCENARIO=<name>`. Each scenario mutates the *next* simulated day's source data (or the environment) in a realistic way. Detectors are generic, not keyed to scenarios (see anti-goals).

| Scenario | Injection | Detected by | Panel |
|---|---|---|---|
| `duplicates` | Re-emits ~5% of payment transactions | dbt `unique` | Quality / Failed tests |
| `missing_dates` | Nulls some order dates; skips a day's file | `not_null` + date-gap test + freshness | Quality / Freshness |
| `broken_fk` | Sales rows reference non-existent product/customer ids | `relationships` | Quality / Failed tests |
| `schema_change` | Renames a column, or adds one / changes a type | Ingestion contract check | Incidents (schema) |
| `negative_revenue` | Negative prices on non-refund lines | Range expectation | Quality |
| `stale_data` | Stops a source producing for N sim-days | Freshness + cron-fallback monitor | Freshness |
| `volume_spike` | 5–10× normal rows (or a sudden drop) | z-score volume detector | Volume anomalies |
| `api_flaky` (bonus) | FX API mock returns 500s/timeouts | Retries succeed, or the SLA breaches | Pipeline SLA |

**What "fix" means** *(my call)*. Recovery is realistic per scenario, not a magic wipe:

| Scenario type | Fix |
|---|---|
| **Data-corruption scenarios** (duplicates, missing_dates, broken_fk, negative_revenue, volume_spike) | `make chaos-reset` turns chaos off and **regenerates clean files for the affected sim dates** (the "source system re-sends corrected files"). Then `make reprocess FROM=… TO=…` triggers the reprocess DAG. Idempotent loads replace the bad rows, tests pass, and incidents auto-resolve. |
| **schema_change** | Two valid fixes, both demonstrable: (a) the source reverts and the file is re-sent; or (b) the team *accepts* the change. Update the contract version, then `make replay-quarantine` loads the held file. Option (b) is the stronger story. |
| **stale_data** | Chaos off; the next `simulate-day` delivers again (optionally including the missed days); freshness recovers and the incident resolves. |
| **api_flaky** | Chaos off; the next scheduled or backfilled FX run succeeds. |

`make verify-scenarios` runs inject → detect → fix → resolved for every scenario and asserts that each lifecycle completes.

### 5.7 Streamlit Control Tower

**Layout:**
- **Top KPI strip:** overall health, plus open incidents by severity.
- **Six panels:**
  - **Freshness:** age per source vs threshold
  - **Quality:** pass rate trend
  - **Volume anomalies:** time series with flagged points
  - **Failed tests:** latest failures with SQL/model links
  - **Pipeline SLA:** met/breached timeline
  - **Data incidents:** open/resolved table, with a blast-radius drill-down

**Data access:** it reads only from `monitoring` (and `marts` for context). It's read-only, using a dedicated read-only DB user.

**Visual direction:** designed to screenshot well (dark "ops console" look). Final styling is decided at build time.

---

## 6. Repository structure (planned)

```
data-reliability-control-tower/
├── docker-compose.yml   Makefile   .env.example   README.md
├── docs/                PROJECT_PLAN.md, architecture diagram, demo script
├── src/drct/            one Python package: common, generator, chaos, ingestion,
│                        monitoring, control_tower, orchestration, ops
├── airflow/dags/        ingest_*, dbt_transform, monitoring
├── dbt/                 models/{staging,intermediate,marts}, tests, macros
├── warehouse/init/      SQL for schemas, roles, read-only user
└── tests/               pytest (generator, contracts, detectors, incidents)
```

---

## 7. Two-week schedule (10 build days)

| Day | Deliverable | "Done" means |
|---|---|---|
| 1 | Environment & skeleton | Docker or OrbStack ready; `make up` brings up Airflow UI, warehouse and a Streamlit hello page; git repo initialised and pushed |
| 2 | Synthetic data generator | 90 days of history generated deterministically; `make simulate-day` works |
| 3 | Ingestion + contracts | All 5 sources load into `raw` idempotently; bad files go to `quarantine`; `pipeline_runs` populated |
| 4 | Airflow Assets & orchestration | File-triggered and cron DAGs emit Assets; dbt DAG runs on Asset events; retries on the FX API |
| 5 | dbt models + tests | staging/intermediate/marts built; full test suite + source freshness; artifacts parsed into `monitoring` |
| 6 | Monitoring | Volume z-score detector, SLA evaluator, freshness table, cron-fallback monitor DAG |
| 7 | Chaos + incidents | All 7 (+1) scenarios inject; incident engine opens/updates/resolves; blast radius from `manifest.json` |
| 8 | Control Tower | All 6 panels live, with drill-downs |
| 9 | Hardening | Reprocess DAG + native FX backfill demo; `make verify-scenarios`; pytest + GitHub Actions CI (lint, unit tests, `dbt build` on a CI Postgres); README draft |
| 10 | Showcase | Architecture diagram, screenshots/GIF, 2–3 min demo video, LinkedIn post draft, final repo polish |

If anything slips, cut in this order: the `api_flaky` scenario, then sqlfluff, then the date-gap test (freshness already covers missing days).

---

## 8. Showcase deliverables (LinkedIn)

- **Public GitHub repo** with:
  - a README containing the hook, an architecture diagram (Mermaid or Excalidraw export), a "run it in one command" quickstart, the failure-scenario table, a Control Tower screenshot, and design decisions/trade-offs
  - a clear licence (MIT)
- **Demo video (2–3 min):**
  1. The healthy Control Tower.
  2. `make chaos SCENARIO=schema_change` and `SCENARIO=duplicates`.
  3. Airflow picks up the files via Assets, ingestion quarantines the bad file, and dbt tests fail.
  4. Incidents appear, with their blast radius.
  5. Fix → backfill → incidents auto-resolve.
- **LinkedIn post draft:** leads with *"Most analytics failures don't start in the dashboard. They start much earlier."* Then 3–4 concrete lessons (event-driven scheduling, contracts at the edge, generic detectors, the incident lifecycle), the video, and the repo link.
- **Pre-publish check:** scan for secrets/PII; confirm `.env` is ignored; confirm a fresh clone runs with `make up && make seed && make demo`.

---

## 9. Left to the builder

- Exact pinned versions (Airflow 3.x, dbt) once compatibility is confirmed at setup.
- Whether the custom non-deleting Asset-watcher trigger works cleanly at the pinned version. If not, fall back to the deferrable sensor.
- Z-score vs MAD for volume anomalies, and exact thresholds.
- Control Tower visual styling details.
- Exact conditional Asset expression for the dbt DAG.

---

## 10. Verification (how we know it works)

1. **Fresh-clone test:** `git clone` → `make up && make seed` on a clean machine/state. Airflow, warehouse and Streamlit are all healthy within about 5 minutes.
2. **Healthy baseline:** `make simulate-day` × 3. All panels are green, no open incidents, and `pipeline_runs` shows Asset-triggered (not cron) runs for the file sources.
3. **Each failure:** for each of the 7 scenarios, run `make chaos SCENARIO=x` + `make simulate-day`. The expected incident type appears in the Control Tower within one monitoring cycle. An automated `make verify-scenarios` script asserts this.
4. **Recovery:** after each scenario's fix path (see 5.6: `chaos-reset` + `reprocess`, `replay-quarantine`, or the next delivery), the incidents auto-resolve. Blocking failures also show the marts holding the last good data rather than bad data.
5. **Unit tests:**
   - generator determinism
   - contract diffing
   - z-score detector on known series
   - incident open/dedupe/resolve logic
6. **CI:** GitHub Actions is green on push (ruff, pytest, `dbt build` against a Postgres service container).
7. **Teardown:** `make down -v` leaves no containers, volumes or networks behind (check with `docker ps -a`, `docker volume ls`).

