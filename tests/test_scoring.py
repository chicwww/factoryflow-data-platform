"""Tests for src/factoryflow/scoring.py."""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from factoryflow.scoring import (  # noqa: E402
    MIN_ROWS_FOR_ISOLATION_FOREST,
    MachineDayFeatures,
    compute_baseline_z_scores,
    run_isolation_forest,
)


def _feature(machine_id, date, total_quantity, checked=10, fail=1, unit_inconsistency=0):
    return MachineDayFeatures(
        machine_day_key=f"{machine_id}_{date}",
        machine_id=machine_id,
        production_date=date,
        total_quantity=total_quantity,
        checked_count=checked,
        fail_count=fail,
        unit_inconsistency_count=unit_inconsistency,
    )


def test_baseline_flags_a_clear_outlier():
    rows = [
        _feature("M001", "2026-01-01", 500),
        _feature("M001", "2026-01-02", 510),
        _feature("M001", "2026-01-03", 490),
        _feature("M001", "2026-01-04", 505),
        _feature("M001", "2026-01-05", 2000),
    ]
    results = compute_baseline_z_scores(rows)
    _, _, z, is_anomaly = results["M001_2026-01-05"]
    assert is_anomaly is True
    assert z > 2.5


def test_baseline_does_not_flag_normal_variation():
    rows = [
        _feature("M001", "2026-01-01", 500),
        _feature("M001", "2026-01-02", 510),
        _feature("M001", "2026-01-03", 495),
        _feature("M001", "2026-01-04", 505),
    ]
    results = compute_baseline_z_scores(rows)
    for _, _, _, is_anomaly in results.values():
        assert is_anomaly is False


def test_baseline_with_single_day_never_flags_and_has_no_stddev():
    rows = [_feature("M001", "2026-01-01", 500)]
    mean, stddev, z, is_anomaly = compute_baseline_z_scores(rows)["M001_2026-01-01"]
    assert stddev is None
    assert z is None
    assert is_anomaly is False


def test_baseline_is_computed_independently_per_machine():
    rows = [
        _feature("M001", "2026-01-01", 500),
        _feature("M001", "2026-01-02", 510),
        _feature("M001", "2026-01-03", 495),
        _feature("M002", "2026-01-01", 5000),
        _feature("M002", "2026-01-02", 5100),
        _feature("M002", "2026-01-03", 4950),
    ]
    results = compute_baseline_z_scores(rows)
    mean_m001, _, _, _ = results["M001_2026-01-01"]
    mean_m002, _, _, _ = results["M002_2026-01-01"]
    assert mean_m001 < 1000
    assert mean_m002 > 1000


def test_isolation_forest_skipped_below_minimum_row_count():
    rows = [_feature("M001", f"2026-01-{i:02d}", 500 + i) for i in range(1, 5)]
    assert len(rows) < MIN_ROWS_FOR_ISOLATION_FOREST
    results = run_isolation_forest(rows)
    for score, is_anomaly in results.values():
        assert score is None
        assert is_anomaly is False


def test_isolation_forest_runs_above_minimum_row_count():
    n_rows = MIN_ROWS_FOR_ISOLATION_FOREST + 5
    rows = [_feature("M001", f"2026-01-{i:02d}", 500 + (i % 5)) for i in range(1, n_rows)]
    results = run_isolation_forest(rows)
    scores = [score for score, _ in results.values()]
    assert all(score is not None for score in scores)


os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_USER", "factoryflow")
os.environ.setdefault("POSTGRES_PASSWORD", "change_me_locally")
os.environ["POSTGRES_DB"] = "factoryflow_test"

from factoryflow.ingest import apply_schema, get_connection, ingest_dataset  # noqa: E402
from factoryflow.scoring import score_anomalies  # noqa: E402

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "data" / "sample"


@pytest.fixture()
def conn():
    connection = get_connection()
    apply_schema(connection)
    with connection.cursor() as cur:
        cur.execute(
            "TRUNCATE raw.anomaly_scores, raw.quarantine, raw.quality_checks, "
            "raw.maintenance_events, raw.production_events, raw.machines RESTART IDENTITY CASCADE"
        )
    connection.commit()
    yield connection
    connection.close()


def _mart_schema_exists(connection) -> bool:
    with connection.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public_marts' AND table_name = 'mart_production_daily')"
        )
        return cur.fetchone()[0]


def test_score_anomalies_handles_missing_marts_gracefully(conn):
    ingest_dataset(conn, SAMPLE_DIR, source_file="data/sample")
    if _mart_schema_exists(conn):
        pytest.skip("marts already exist in this environment; covered by the next test instead")
    summary = score_anomalies(conn)
    assert summary["machine_days_scored"] == 0


def test_score_anomalies_writes_rows_when_marts_exist(conn):
    ingest_dataset(conn, SAMPLE_DIR, source_file="data/sample")
    if not _mart_schema_exists(conn):
        pytest.skip("run `dbt run` against factoryflow_test before this test to build the marts")

    summary = score_anomalies(conn)
    assert summary["machine_days_scored"] > 0

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw.anomaly_scores")
        assert cur.fetchone()[0] == summary["machine_days_scored"]
