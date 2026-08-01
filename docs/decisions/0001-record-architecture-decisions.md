# ADR 0001: Record architecture decisions

## Status
Accepted

## Context
This is a portfolio project meant to demonstrate Data Engineering judgment,
not just working code. Reviewers (recruiters, master's admissions committees)
benefit from seeing *why* choices were made, not only *what* was built.

## Decision
We will keep lightweight Architecture Decision Records under `docs/decisions/`
for every non-trivial technical choice (e.g., PostgreSQL vs SQLite, dbt vs raw
SQL, when Airflow is introduced).

## Consequences
- Slightly more writing overhead per phase.
- Much easier to explain trade-offs during interviews or a soutenance.
