# Architecture — FactoryFlow Data Platform

## Problem

A synthetic factory produces daily files describing production events, quality
checks, machine status, and maintenance events. Files may contain duplicates,
nulls, inconsistent units, invalid timestamps, and late arrivals. The platform
must ingest, validate, transform, and expose reliable indicators, then flag
anomalies — without ever presenting a model's output as ground truth.

## Proposed architecture (batch, local-first)

```
[Synthetic Generator] --> [Raw files, data/sample/]
        |
        v
[Python Ingestion] --idempotent--> [PostgreSQL: raw schema + quarantine table]
        |
        v
[dbt Core: staging -> intermediate -> marts] (dbt-postgres)
        |
        v
[Quality rules + statistical baseline + IsolationForest] --> anomaly_scores
        |
        v
[Streamlit dashboard] <-- marts (freshness, volumes, rejects, defect rate, trends)
        |
[Airflow] orchestrates generation/detection -> ingestion -> dbt run -> dbt test -> scoring -> publish
```

## Key trade-offs

| Decision | Alternative considered | Reason for choice |
|---|---|---|
| PostgreSQL over SQLite | SQLite | Matches production-like RDBMS behavior (constraints, concurrent writes), still fully local via Docker |
| dbt Core over raw SQL scripts | Hand-written SQL pipeline | dbt gives testable, documented, versioned transformations — closer to real DE practice |
| IsolationForest only after a statistical baseline | ML-first anomaly detection | Avoids presenting an unvalidated model as truth; baseline gives an interpretable reference point |
| Airflow over cron | Cron scripts, Dagster | Airflow is the most commonly expected orchestrator in DE job postings; added in Phase 4 once the pipeline logic is proven without it |
| Streamlit over a BI tool for the demo | Power BI only | Streamlit is free, scriptable, and reproducible in CI; a CSV export keeps Power BI compatibility as a secondary view |

## Data model (minimal)

- `machines` — machine reference data
- `production_events` — one row per production event
- `quality_checks` — quality control results linked to production events
- `maintenance_events` — maintenance interventions per machine
- `anomaly_scores` — output of the anomaly detection layer, kept separate from marts used for reporting

## Known limitations (to be kept honest throughout)

- This is a portfolio project: data is synthetic, and no claim of "production-ready" is made anywhere in this repository.
- Anomaly detection results are illustrative, not validated against real incidents.
- Airflow, dbt, and Streamlit versions will be pinned only after checking their official compatibility documentation, phase by phase.
