import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_USER", "factoryflow")
os.environ.setdefault("POSTGRES_PASSWORD", "change_me_locally")
os.environ["POSTGRES_DB"] = "factoryflow_test"

from factoryflow.ingest import apply_schema, get_connection, ingest_dataset

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "data" / "sample"


@pytest.fixture()
def conn():
    connection = get_connection()
    apply_schema(connection)
    with connection.cursor() as cur:
        cur.execute(
            "TRUNCATE raw.quarantine, raw.quality_checks, raw.maintenance_events, "
            "raw.production_events, raw.machines RESTART IDENTITY CASCADE"
        )
    connection.commit()
    yield connection
    connection.close()


def _count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM raw.{table}")  # noqa: S608
        return cur.fetchone()[0]


def test_ingest_sample_dataset_inserts_machines_and_maintenance(conn):
    results = ingest_dataset(conn, SAMPLE_DIR, source_file="data/sample")
    assert results["machines"]["inserted"] == 5
    assert results["maintenance_events"]["inserted"] == 5
    assert _count(conn, "machines") == 5
    assert _count(conn, "maintenance_events") == 5


def test_ingest_sample_dataset_quarantines_invalid_timestamps(conn):
    results = ingest_dataset(conn, SAMPLE_DIR, source_file="data/sample")
    assert results["production_events"]["quarantined"] > 0


def test_ingest_sample_dataset_quarantines_orphan_quality_checks(conn):
    ingest_dataset(conn, SAMPLE_DIR, source_file="data/sample")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM raw.quarantine WHERE table_name = 'quality_checks' "
            "AND row_reference LIKE 'QC-ORPHAN%'"
        )
        orphans_in_quarantine = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM raw.quality_checks WHERE check_id LIKE 'QC-ORPHAN%'")
        orphans_inserted = cur.fetchone()[0]
    assert orphans_in_quarantine > 0
    assert orphans_inserted == 0


def test_reingesting_same_file_is_idempotent(conn):
    tables = ["machines", "production_events", "quality_checks", "maintenance_events"]

    first = ingest_dataset(conn, SAMPLE_DIR, source_file="data/sample")
    counts_after_first = {t: _count(conn, t) for t in tables}
    quarantine_after_first = _count(conn, "quarantine")

    second = ingest_dataset(conn, SAMPLE_DIR, source_file="data/sample")
    counts_after_second = {t: _count(conn, t) for t in tables}
    quarantine_after_second = _count(conn, "quarantine")

    assert counts_after_first == counts_after_second
    assert quarantine_after_first == quarantine_after_second
    for t in tables:
        assert second[t]["inserted"] == 0


def test_inserted_rows_carry_traceability_columns(conn):
    ingest_dataset(conn, SAMPLE_DIR, source_file="data/sample")
    with conn.cursor() as cur:
        cur.execute("SELECT source_file, batch_id, ingested_at FROM raw.machines LIMIT 1")
        source_file, batch_id, ingested_at = cur.fetchone()
    assert source_file == "data/sample"
    assert batch_id
    assert ingested_at is not None


def test_quarantined_rows_preserve_original_data_as_json(conn):
    ingest_dataset(conn, SAMPLE_DIR, source_file="data/sample")
    with conn.cursor() as cur:
        cur.execute("SELECT raw_data FROM raw.quarantine WHERE table_name = 'production_events' LIMIT 1")
        row = cur.fetchone()
    assert row is not None
    assert "event_id" in row[0]
    assert "timestamp" in row[0]
