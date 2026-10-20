# Demo video script (2–3 minutes)

A shot list for a screen recording. Each shot gives the commands to run, what to point at, and a
rough voiceover line. The timings are for the **edited** video. Each `make` chain below takes
about 40–70 s live (measured on a fresh clone), so record each shot in full and cut the waiting.

## Before you record

- **Pre-seed.** Start from a clean, healthy stack, about 2 minutes before you record:

  ```bash
  make nuke && make demo
  ```

  Then check that the Control Tower shows **Healthy** and **0 open** incidents.
- **Keep the Mac awake.** Run long commands under `caffeinate -dims …`, or keep
  `caffeinate -dims` running in a spare terminal. If the host sleeps, the pipeline timing breaks.
- **Open three windows:**
  1. The Control Tower at <http://localhost:8501>, in a dark browser window at about 1600 px wide.
  2. The Airflow asset graph at <http://localhost:8080/assets>. Click `raw.sales` for the full
     graph.
  3. A terminal with a large font, in the repo folder.
- **Refresh the Control Tower by hand** after each `make monitor`. It caches queries for 15 s and
  does not auto-refresh.
- Have `src/drct/ingestion/contracts/sales.yml` open in your editor, ready for shot 5.
- **After recording,** run `git checkout src/drct/ingestion/contracts/sales.yml` to undo the
  contract edit.

## Shot list

| # | Time | Screen | Do | Point at | Voiceover |
| --- | --- | --- | --- | --- | --- |
| 1 | 0:00–0:15 | Control Tower | Nothing; slow scroll | The KPI strip: **Healthy**, 0 open, 5/5 fresh, 100% tests | "Most analytics failures don't start in the dashboard. They start much earlier. This is a Control Tower for everything *before* the dashboard, and right now it's healthy." |
| 2 | 0:15–0:35 | Airflow asset graph | Hover along the graph left to right | `landing.*` → `ingest_*` → `raw.*` → `dbt_transform` → `monitoring.dbt_results` → `monitoring` | "Nothing here waits for a cron tick. When a file lands, a custom trigger fires an Airflow Asset event. Ingestion runs, and that wakes dbt, which wakes monitoring. Only the FX API is on a schedule." |
| 3 | 0:35–0:55 | Terminal, then Airflow | `make chaos SCENARIO=schema_change`<br>`make simulate-day` | The terminal: "Simulated business date …". In Airflow: a new `ingest_sales` run triggered by the asset event. | "Let's break it. Tomorrow, the sales system renames a column without telling anyone." |
| 4 | 0:55–1:10 | Terminal, then Control Tower | `ls landing/sales/_quarantine/`<br>`make monitor`<br>Refresh | The quarantined file. The incidents: **schema_change · raw.sales** (critical): "missing ['unit_price']", plus a stale_data warning because no sales rows landed that day. | "The file broke its data contract, so it was quarantined before it touched the warehouse. The incident says exactly which column went missing." |
| 5 | 1:10–1:30 | Editor, then terminal | In `sales.yml`, change `version: 1` to `version: 2`, and add `aliases: [price]` to the `unit_price` column (see below)<br>`make replay-quarantine SOURCE=sales`<br>`make chaos-reset`<br>`make monitor`<br>Refresh | The incident list: both incidents switch to **resolved** | "Sometimes the right fix is to accept the change. I version the contract, map the new name, and replay the held file. Same data, nothing lost, and the incident closes itself." |
| 6 | 1:30–1:50 | Terminal, then Control Tower | `make chaos SCENARIO=duplicates`<br>`make simulate-day`<br>`make monitor`<br>Refresh | **Critical**: `duplicate_records` on `unique_stg_payments_payment_id`, with N failing rows. Also `pipeline_failure` and the `transform_duration` SLA breach. | "Next: the payments provider retries and sends some transactions twice. A blocking dbt test catches it and stops the build." |
| 7 | 1:50–2:15 | Control Tower, incident drill-down | Expand the duplicate_records drill-down | The blast radius: `stg_payments` → `int_payments_deduped` / `int_payments_matched` → `fct_payments` → **Control Tower (dashboard)**. Then the failed-test SQL. Then the SLA panel's red cell. | "Here's the blast radius, every model and dashboard it would have hit. It's a circuit breaker: the marts kept yesterday's good data, so the dashboard is a day behind, never wrong. Here's the SQL to find the offending rows." |
| 8 | 2:15–2:40 | Terminal, then Control Tower | `make chaos-reset` (it prints the dates)<br>`make reprocess FROM=<date> TO=<date>`<br>`make monitor`<br>Refresh | The **resolved** ticks in the incident list; the KPI strip back to **Healthy** | "The fix is what you'd do in real life. The source re-sends clean files, and a reprocess DAG reloads that day idempotently. Then dbt and monitoring re-run on their own, and every incident auto-resolves." |
| 9 | 2:40–2:55 | Terminal (pre-recorded) | Show the end of a `make verify-scenarios` run | "8/8 scenarios detected and resolved" | "That's one of eight failure types. A single command injects, detects, fixes and verifies all of them. The repo is linked below. It runs with one command." |

### The contract edit (shot 5)

```yaml
source: sales
version: 2            # was 1
...
  - {name: unit_price, dtype: decimal, nullable: false, aliases: [price]}
```

`make chaos-reset` turns the scenario off, so the next simulated day is clean again. It also
re-sends a clean copy of that day to `_resend/` and suggests a `make reprocess`. Skip that in the
accept path: the replayed file is already loaded.

### Rehearsal notes (abridged terminal output from a `make nuke && make demo` rehearsal)

Lines starting with `$` are the commands. Lines starting with `#` are notes on what to check
(in the Control Tower or with `make psql`); they are not CLI output. The `docker compose …` echo
lines are left out, and run ids are shortened to `…`.

```text
$ make chaos SCENARIO=schema_change && make simulate-day
Chaos scenario schema_change enabled; it takes effect from the next simulated day
Simulated business date 2026-07-25
Triggered ingest_fx (run manual__…)

$ ls landing/sales/_quarantine/
sales_2026-07-25.csv

$ make monitor
Triggered monitoring (run manual__…); waiting for it to finish
monitoring run succeeded; waiting for the pipeline to idle
Pipeline idle
# open: schema_change · raw.sales (critical) "sales_2026-07-25.csv: missing ['unit_price'], type mismatches []"
#       stale_data · raw.sales (warning) "raw.sales warn: age 24.0h"

$ make replay-quarantine SOURCE=sales && make chaos-reset && make monitor
Replayed sales_2026-07-25.csv; ingest_sales picks it up from /opt/drct/landing/sales
Chaos off; clean files re-sent to landing/<source>/_resend/ for:
  2026-07-25
Next: make reprocess FROM=2026-07-25 TO=2026-07-25
Triggered monitoring (run manual__…); waiting for it to finish
monitoring run succeeded; waiting for the pipeline to idle
Pipeline idle
# both incidents resolved; no other incident opens
# monitoring.schema_events: contract_version 2, not breaking, aliased {"price": "unit_price"}

$ make chaos SCENARIO=duplicates && make simulate-day && make monitor
Chaos scenario duplicates enabled; it takes effect from the next simulated day
Simulated business date 2026-07-26
Triggered ingest_fx (run manual__…)
Triggered monitoring (run manual__…); waiting for it to finish
monitoring run succeeded; waiting for the pipeline to idle
Pipeline idle
# open: duplicate_records (18 failing rows, reaches 4 downstream),
#       sla_breach transform_duration, pipeline_failure dbt_transform
# marts.fct_payments stays on 2026-07-25 (circuit breaker)

$ make chaos-reset && make reprocess FROM=2026-07-26 TO=2026-07-26 && make monitor
Chaos off; clean files re-sent to landing/<source>/_resend/ for:
  2026-07-26
Next: make reprocess FROM=2026-07-26 TO=2026-07-26
Triggered reprocess 2026-07-26 .. 2026-07-26 (run manual__…); waiting for it to finish
reprocess run succeeded; waiting for the pipeline to idle
Pipeline idle
Triggered monitoring (run manual__…); waiting for it to finish
monitoring run succeeded; waiting for the pipeline to idle
Pipeline idle
# all three resolved; marts.fct_payments catches up to 2026-07-26
```

Give the quarantine a few seconds after `make simulate-day` before the `ls`: the file-arrival
trigger polls every 5 s. The dates depend on the day you seed, so use the ones your terminal
prints.

## Optional cut-ins

- **Native backfill (5 s):**
  `docker compose exec airflow-scheduler airflow backfill create --dag-id ingest_fx --from-date <D> --to-date <D>`.
  Use one day at a time. On a `*/10` cron, a multi-day range creates about 144 runs per day.
- **Stale data (10 s):** `make chaos SCENARIO=stale_data && make simulate-day DAYS=2 && make monitor`.
  The Freshness panel turns red for `raw.payments` (48h, past the 44h error line). Fix it with
  `make chaos-reset && make simulate-day && make monitor`.
