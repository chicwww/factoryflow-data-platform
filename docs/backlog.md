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

## Phase 3 — dbt staging / intermediate / marts
- [ ] Sources declared with descriptions
- [ ] At least 15 relevant dbt tests across layers
- [ ] dbt docs generate without errors

## Phase 4 — Airflow orchestration
- [ ] DAG covers: detect/generate file → ingest → dbt run → dbt test → scoring → publish
- [ ] Retries and timeouts configured
- [ ] DAG survives a mid-run failure without duplicating data

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
