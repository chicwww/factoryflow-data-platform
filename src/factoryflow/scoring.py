"""Anomaly scoring for FactoryFlow: statistical baseline, then IsolationForest.

Design principle (stated explicitly because it matters): the statistical
baseline is computed and written first, and IsolationForest is layered on
top of it -- never used alone as "ground truth". Both are illustrative
signals for a human to look at, not a verdict on whether a machine is
actually malfunctioning.
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass

import psycopg2.extras

logger = logging.getLogger("factoryflow.scoring")

MIN_ROWS_FOR_ISOLATION_FOREST = 20
BASELINE_Z_THRESHOLD = 2.5


@dataclass
class MachineDayFeatures:
    machine_day_key: str
    machine_id: str
    production_date: object
    total_quantity: int
    checked_count: int
    fail_count: int
    unit_inconsistency_count: int

    @property
    def defect_rate(self) -> float:
        if self.checked_count == 0:
            return 0.0
        return self.fail_count / self.checked_count


def fetch_machine_day_features(conn) -> list[MachineDayFeatures]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            cur.execute(
                """
                SELECT production_day_id AS machine_day_key, machine_id, production_date,
                       total_quantity, checked_count, fail_count, unit_inconsistency_count
                FROM public_marts.mart_production_daily
                """
            )
            rows = cur.fetchall()
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            logger.warning(
                "mart_production_daily does not exist yet -- run `dbt run` before scoring. "
                "Returning no rows instead of failing."
            )
            return []
    return [MachineDayFeatures(**dict(row)) for row in rows]


def compute_baseline_z_scores(
    rows: list[MachineDayFeatures],
) -> dict[str, tuple[float | None, float | None, float | None, bool]]:
    """Per-machine baseline on total_quantity: mean, stddev, z-score, flag.

    The baseline for each day is computed leave-one-out (excluding that
    day's own value) -- including a point in its own baseline biases the
    mean toward it and inflates the stddev, which can mask a real outlier.

    Limitation: with only a handful of days per machine, the leave-one-out
    mean/stddev are themselves unstable, and computed with no held-out
    validation period. A machine with fewer than 3 total days gets
    stddev=None and is never flagged.
    """
    by_machine: dict[str, list[MachineDayFeatures]] = {}
    for row in rows:
        by_machine.setdefault(row.machine_id, []).append(row)

    results: dict[str, tuple[float | None, float | None, float | None, bool]] = {}
    for machine_rows in by_machine.values():
        for row in machine_rows:
            others = [
                r.total_quantity for r in machine_rows if r.machine_day_key != row.machine_day_key
            ]
            if len(others) < 2:
                results[row.machine_day_key] = (None, None, None, False)
                continue

            mean = statistics.fmean(others)
            stddev = statistics.pstdev(others)

            if stddev == 0:
                results[row.machine_day_key] = (mean, stddev, None, False)
                continue

            z = (row.total_quantity - mean) / stddev
            results[row.machine_day_key] = (mean, stddev, z, abs(z) > BASELINE_Z_THRESHOLD)

    return results


def run_isolation_forest(
    rows: list[MachineDayFeatures],
) -> dict[str, tuple[float | None, bool]]:
    """IsolationForest over (total_quantity, defect_rate, unit_inconsistency_count).

    Limitation: with only a few dozen machine-days, IsolationForest's
    contamination estimate is not statistically meaningful -- included to
    demonstrate the technique, not as a validated detector. Below
    MIN_ROWS_FOR_ISOLATION_FOREST rows, it is skipped entirely.
    """
    if len(rows) < MIN_ROWS_FOR_ISOLATION_FOREST:
        logger.info(
            "isolation_forest_skipped row_count=%s minimum_required=%s",
            len(rows),
            MIN_ROWS_FOR_ISOLATION_FOREST,
        )
        return {row.machine_day_key: (None, False) for row in rows}

    from sklearn.ensemble import IsolationForest

    features = [[r.total_quantity, r.defect_rate, r.unit_inconsistency_count] for r in rows]

    model = IsolationForest(contamination="auto", random_state=42)
    model.fit(features)
    scores = model.decision_function(features)
    predictions = model.predict(features)

    return {
        row.machine_day_key: (float(score), bool(pred == -1))
        for row, score, pred in zip(rows, scores, predictions, strict=True)
    }


def upsert_anomaly_scores(
    conn, rows: list[MachineDayFeatures], baseline: dict, iso_forest: dict
) -> int:
    written = 0
    with conn.cursor() as cur:
        for row in rows:
            mean, stddev, z, is_baseline_anomaly = baseline[row.machine_day_key]
            iso_score, is_iso_anomaly = iso_forest[row.machine_day_key]
            cur.execute(
                """
                INSERT INTO raw.anomaly_scores
                    (machine_day_key, machine_id, production_date, total_quantity,
                     baseline_mean, baseline_stddev, baseline_z_score, is_baseline_anomaly,
                     isolation_forest_score, is_isolation_forest_anomaly, scored_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (machine_day_key) DO UPDATE SET
                    total_quantity = EXCLUDED.total_quantity,
                    baseline_mean = EXCLUDED.baseline_mean,
                    baseline_stddev = EXCLUDED.baseline_stddev,
                    baseline_z_score = EXCLUDED.baseline_z_score,
                    is_baseline_anomaly = EXCLUDED.is_baseline_anomaly,
                    isolation_forest_score = EXCLUDED.isolation_forest_score,
                    is_isolation_forest_anomaly = EXCLUDED.is_isolation_forest_anomaly,
                    scored_at = now()
                """,
                (
                    row.machine_day_key, row.machine_id, row.production_date, row.total_quantity,
                    mean, stddev, z, is_baseline_anomaly, iso_score, is_iso_anomaly,
                ),
            )
            written += 1
    conn.commit()
    return written


def score_anomalies(conn) -> dict:
    """Run the full scoring pass: baseline first, then IsolationForest.

    Limitations:
    - Scores machine-days, not individual events; a bad single event can be
      diluted into a normal-looking day average.
    - The baseline has no held-out validation period.
    - IsolationForest is skipped below a minimum row count, and even above
      it, its contamination estimate is not independently validated.
    - Nothing here is a verified anomaly -- it is a candidate for a human to
      review on the dashboard (Phase 6), not an automatic verdict.
    """
    rows = fetch_machine_day_features(conn)
    if not rows:
        logger.info("score_anomalies: no machine-day rows found in mart_production_daily")
        return {"machine_days_scored": 0, "baseline_anomalies": 0, "isolation_forest_anomalies": 0}

    baseline = compute_baseline_z_scores(rows)
    iso_forest = run_isolation_forest(rows)
    written = upsert_anomaly_scores(conn, rows, baseline, iso_forest)

    baseline_anomalies = sum(1 for v in baseline.values() if v[3])
    iso_anomalies = sum(1 for v in iso_forest.values() if v[1])

    summary = {
        "machine_days_scored": written,
        "baseline_anomalies": baseline_anomalies,
        "isolation_forest_anomalies": iso_anomalies,
        "isolation_forest_ran": len(rows) >= MIN_ROWS_FOR_ISOLATION_FOREST,
    }
    logger.info("score_anomalies_finished %s", summary)
    return summary
