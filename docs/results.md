# Measured results

Numbers below are from actual runs, not estimates.

## Phase 1 — Synthetic data generator

- `pytest tests/test_generator.py`: 8/8 passed.
- Sample dataset (seed=42, 3 days, 5 machines, 20 events/machine/day):
  5 machines, 305 production events, 261 quality checks, 5 maintenance events.

## Phase 2 — Ingestion (first run against data/sample, local PostgreSQL 16)

Command: `python scripts/ingest_sample_data.py`

| Table | Inserted | Skipped (duplicate) | Quarantined |
|---|---|---|---|
| machines | 5 | 0 | 0 |
| production_events | 294 | 5 | 6 |
| quality_checks | 255 | 0 | 6 |
| maintenance_events | 5 | 0 | 0 |

- The 5 duplicate `production_events` on the first run are the ~2% resend
  duplicates from the generator, caught even within the same batch.
- The 6 quarantined `production_events` are the corrupted (year-1900) timestamps.
- The 6 quarantined `quality_checks` reference a non-existent `production_event_id`.

## Phase 2 — Idempotency check (second run, same file, same database)

| Table | Inserted | Skipped (duplicate) | Quarantined |
|---|---|---|---|
| machines | 0 | 5 | 0 |
| production_events | 0 | 299 | 0 |
| quality_checks | 0 | 255 | 0 |
| maintenance_events | 0 | 5 | 0 |

Test suite: `pytest tests/test_ingest.py` — 6/6 passed (real local PostgreSQL 16, database `factoryflow_test`).
Full suite (`pytest`): 14/14 passed.

## Phase 4 — Airflow orchestration

Command: airflow dags test factoryflow_pipeline <date>, run for three
consecutive dates (2026-02-01, 2026-02-02, 2026-02-03) against the same
local PostgreSQL 16 instance.

- All three DAG runs finished in state=success, each executing all 6
  tasks in order: generate_or_detect_file -> ingest_to_postgres -> dbt_run
  (9 models) -> dbt_test (46/46) -> score_anomalies (placeholder) ->
  publish_results (placeholder).
- Re-running the same date a second time: ingest_to_postgres reports
  0 new inserts everywhere (idempotent), and generate_or_detect_file
  detects the existing file and skips regeneration.

A genuine bug was found and fixed here, not assumed in advance: the
generator's event/check/maintenance IDs restarted from 1 on every call, so
a second day's ~100 events collided with the first day's IDs and were
silently discarded as duplicates during ingestion -- only 4 of ~100 events
were actually inserted for the second day before the fix. Root-caused and
fixed with a per-run id_prefix; re-verified afterwards:

| Date | production_events inserted |
|---|---|
| data/sample (Phase 1-3 baseline) | 294 |
| airflow/2026-02-01 | 96 |
| airflow/2026-02-02 | 95 |

machines stayed at exactly 5 rows throughout -- confirming the fix keeps
machine_id stable across days while making event-level IDs unique per day.

## Phase 5 -- Quality rules, statistical baseline, IsolationForest

Command: airflow dags test factoryflow_pipeline <date> (score_anomalies now
runs real scoring), against local PostgreSQL 16 with 13 days of data.

- dbt: 48/48 tests pass (46 from Phase 3 + 2 new quality-rule singular tests).
- Ingestion: critical rule quarantines quantity_produced <= 0, backed by a
  database CHECK constraint as defense in depth.
- Scoring (real run, 70 machine-days): 3 flagged by the statistical
  baseline, 20 flagged by IsolationForest, isolation_forest_ran: true.

Two real bugs found and fixed here:
1. The baseline included each day in its own mean/stddev, masking real
   outliers (a 4x-normal test day scored z=2.0 before the fix). Fixed with
   leave-one-out computation.
2. The Phase 4 DAG never passed the actual run date to the generator, so
   every simulated day collapsed onto the same production_date. Fixed by
   passing start_date explicitly in generate_or_detect_file.

Limitations (also documented next to the code in scoring.py):
- Anomaly scores are illustrative signals for human review, kept in a
  separate raw.anomaly_scores table, never blended into the dbt marts.
- The baseline has no held-out validation period and is unstable with few
  days per machine.
- IsolationForest's contamination estimate is not validated against real
  incidents.
