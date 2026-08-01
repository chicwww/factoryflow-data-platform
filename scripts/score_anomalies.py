#!/usr/bin/env python3
"""Run anomaly scoring (baseline + IsolationForest) against the dbt marts.

Requires mart_production_daily to already exist (run `dbt run` first).

Usage:
    python scripts/score_anomalies.py
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from factoryflow.ingest import apply_schema, get_connection  # noqa: E402
from factoryflow.scoring import score_anomalies  # noqa: E402


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    conn = get_connection()
    try:
        apply_schema(conn)
        summary = score_anomalies(conn)
        print("Scoring results:")
        for key, value in summary.items():
            print(f"  {key}: {value}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
