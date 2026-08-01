# FactoryFlow Data Platform

A portfolio project demonstrating an industrial batch data platform: synthetic
data generation, idempotent ingestion, dbt transformations, data quality
checks, anomaly detection, orchestration, and a monitoring dashboard.

> Status: Phase 1 complete (synthetic generator + reproducibility tests). Ingestion, dbt, Airflow, anomaly detection, and dashboard land in later phases.
> No part of this project is described as "production-ready"; see [Limitations](#limitations).
> > Status: Phase 2 complete (synthetic generator + idempotent PostgreSQL ingestion with quarantine).

## Context

Factories generate daily files describing production, quality checks, machine
status, and maintenance. Real-world files are messy: duplicates, nulls,
inconsistent units, invalid timestamps, late-arriving data. This project
simulates that environment with synthetic data only, and builds the platform
needed to turn messy inputs into trustworthy indicators.

## Problem

How do you build a batch pipeline that stays correct under duplicate reruns,
late data, and partial failures — and that surfaces anomalies without
overstating what a model actually knows?

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the full diagram and
trade-off table.

## Technologies

Python, PostgreSQL, dbt Core (dbt-postgres), Apache Airflow, scikit-learn
(IsolationForest), Streamlit, pytest, Ruff, GitHub Actions, Docker Compose.

Exact pinned versions will appear here once verified against official
documentation, phase by phase.

## Data model

- `machines`
- `production_events`
- `quality_checks`
- `maintenance_events`
- `anomaly_scores`

See [`docs/architecture.md`](docs/architecture.md) for details.

## Installation

```bash
git clone <this-repo>
cd factoryflow-data-platform
cp .env.example .env
make setup
```

## Running

```bash
make start                          # start local PostgreSQL (Airflow added in Phase 4)
python scripts/generate_sample_data.py   # (re)generate data/sample/*.csv, seed=42
make test                           # run the test suite
make stop                           # stop local services
python scripts/ingest_sample_data.py      # apply schema + ingest into PostgreSQL (idempotent)
```

*(Ingestion, dbt, and orchestration commands will be added as those phases land.)*

## Tests

Run with `make test`. As of Phase 1: **8/8 tests passing**, covering
generator reproducibility (identical output for a fixed seed across all four
tables), differing output for a different seed, expected row counts, and the
intentionally-injected data-quality issues (orphan foreign keys, mixed
units) used by later phases.

## Results

- Sample dataset (seed=42, 3 days, 5 machines, 20 events/machine/day):
  5 machines, 305 production events (includes ~2% intentional duplicates),
  261 quality checks (~85% coverage by design, plus a few intentional
  orphan records), 5 maintenance events.
- Full benchmark numbers (ingestion throughput, dbt test pass rate, etc.)
  will be recorded in `docs/results.md` starting in Phase 2, once there is
  a pipeline to measure.

## Screenshots

To be added in Phase 6 (dashboard) and Phase 7 (final documentation pass).

## Limitations

- Portfolio project: all data is synthetic or public.
- Nothing in this repository is production-ready; this README will always
  name current gaps explicitly rather than implying completeness.
- Anomaly detection is illustrative and not validated against real incidents.

## Next steps

See [`docs/backlog.md`](docs/backlog.md) for the full phase-by-phase plan.
Immediate next step: Phase 2 — idempotent ingestion of the generated data
into PostgreSQL, with a quarantine table for rejected rows.

## License

MIT — see [LICENSE](LICENSE).
