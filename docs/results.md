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
