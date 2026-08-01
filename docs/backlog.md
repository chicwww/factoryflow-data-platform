# Backlog — FactoryFlow Data Platform

Status: Phase 1 in progress (initialization complete, generator not yet built).

## Phase 1 — Initialization & synthetic data — COMPLETE
**Acceptance criteria**
- [x] Git repo initialized locally with standard structure
- [x] README, LICENSE (MIT), .gitignore, .env.example, Makefile in place
- [x] Deterministic synthetic data generator (seeded) — `src/factoryflow/generator.py`
- [x] Small example dataset committed under `data/sample/` (seed=42, 3 days, 5 machines)
- [x] Reproducibility verified: same seed → identical output, checked by 8 pytest tests (all passing)

## Phase 2 — Idempotent ingestion into PostgreSQL — COMPLETE
- [x] Raw tables created with primary/foreign keys
- [x] ingested_at, source_file, batch_id columns present
- [x] Re-running ingestion on the same file does not duplicate rows (verified against real PostgreSQL 16, see docs/results.md)
- [x] Rejected rows land in a quarantine table, not silently dropped
- [x] Structured logging in place (JSON lines via log_event)
- [x] Python tests covering ingestion logic (6 tests, all passing, against real Postgres)

## Phase 3 — dbt staging / intermediate / marts — COMPLETE
- [x] Sources declared with descriptions
- [x] At least 15 relevant dbt tests across layers (46 tests, all passing)
- [x] dbt docs generate without errors
      
## Phase 4 — Airflow orchestration — COMPLETE
- [x] DAG covers: detect/generate file → ingest → dbt run → dbt test → scoring → publish
- [x] Retries and timeouts configured (2 retries, 5 min delay, 15 min execution timeout)
- [x] DAG survives a mid-run failure without duplicating data (verified via idempotent re-run of the same date)

Note: scoring and publish are intentionally placeholder tasks until Phase 5
(statistical baseline + IsolationForest) and Phase 6 (dashboard) land — they
run and log what they will do, but compute/publish nothing yet.

A real bug was found and fixed while building this phase: the synthetic
generator reused event/check/maintenance IDs starting from 1 on every call,
so a second day's batch collided with the first day's already-ingested rows
and was silently treated as duplicates (data loss, not an error). Fixed with
an id_prefix parameter (dated per DAG run); machine_id is deliberately NOT
prefixed since machines are stable equipment referenced identically every day.

## Phase 5 — Quality rules & anomaly detection
- [ ] Statistical baseline implemented and documented before any ML model
- [ ] IsolationForest added on top of the baseline, with explicit limitations stated
- [ ] Quality errors block or quarantine data depending on declared severity

## Phase 6 — Dashboard
- [ ] Streamlit dashboard reads only from marts (not raw tables)
- [ ] Shows freshness, volumes, rejects, defect rate, trends, anomalies

## Phase 7 — CI, documentation, demo
- [ ] GitHub Actions CI passes on a clean environment
- [ ] Architecture diagram, data dictionary, ADRs, screenshots added
- [ ] All numbers in the README trace back to `docs/results.md`

## Global final criteria
- A single documented command starts the platform
- A full run is reproducible
- Duplicates are never re-inserted
- The dashboard reflects marts only
- CI is green
- No metric in the README is unmeasured
