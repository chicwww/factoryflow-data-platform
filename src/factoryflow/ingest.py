import csv
import hashlib
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg2

logger = logging.getLogger("factoryflow.ingest")

MIN_VALID_YEAR = 2000  # tout ce qui est avant est considéré comme un timestamp corrompu


def log_event(event: str, **fields: Any) -> None:
    logger.info(json.dumps({"event": event, **fields}, default=str))


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "factoryflow"),
        user=os.environ.get("POSTGRES_USER", "factoryflow"),
        password=os.environ.get("POSTGRES_PASSWORD", "change_me_locally"),
    )


def apply_schema(conn) -> None:
    schema_path = Path(__file__).resolve().parent / "schema.sql"
    with conn.cursor() as cur:
        cur.execute(schema_path.read_text())
    conn.commit()


def parse_timestamp(value: str) -> datetime | None:
    try:
        ts = datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None
    if ts.year < MIN_VALID_YEAR:
        return None
    return ts


def _row_reference(row: dict, natural_id_field: str | None) -> str:
    if natural_id_field and row.get(natural_id_field):
        return str(row[natural_id_field])
    digest = hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()
    return f"hash:{digest[:16]}"


@dataclass
class IngestStats:
    inserted: int = 0
    skipped_duplicate: int = 0
    quarantined: int = 0

    def as_dict(self) -> dict:
        return {
            "inserted": self.inserted,
            "skipped_duplicate": self.skipped_duplicate,
            "quarantined": self.quarantined,
        }


def _quarantine_row(conn, table_name, row, reason, natural_id_field, source_file, batch_id) -> bool:
    ref = _row_reference(row, natural_id_field)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.quarantine
                (table_name, row_reference, reason, raw_data, source_file, batch_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (table_name, row_reference, source_file) DO NOTHING
            """,
            (table_name, ref, reason, json.dumps(row, default=str), source_file, batch_id),
        )
        return cur.rowcount > 0


def read_csv(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def ingest_machines(conn, rows, source_file, batch_id) -> IngestStats:
    stats = IngestStats()
    for row in rows:
        if not row.get("machine_id") or not row.get("machine_name") or not row.get("install_date"):
            stats.quarantined += int(
                _quarantine_row(conn, "machines", row, "missing required field", "machine_id", source_file, batch_id)
            )
            continue
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.machines
                    (machine_id, machine_name, machine_type, install_date, source_file, batch_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (machine_id) DO NOTHING
                """,
                (row["machine_id"], row["machine_name"], row["machine_type"], row["install_date"], source_file, batch_id),
            )
            if cur.rowcount > 0:
                stats.inserted += 1
            else:
                stats.skipped_duplicate += 1
    return stats


def ingest_production_events(conn, rows, known_machine_ids, source_file, batch_id) -> IngestStats:
    stats = IngestStats()
    for row in rows:
        ts = parse_timestamp(row.get("timestamp", ""))
        if row.get("machine_id") not in known_machine_ids:
            reason = "unknown machine_id (foreign key would be violated)"
        elif ts is None:
            reason = "invalid or corrupted timestamp"
        elif not row.get("quantity_produced"):
            reason = "missing quantity_produced"
        else:
            reason = None

        if reason:
            stats.quarantined += int(
                _quarantine_row(conn, "production_events", row, reason, "event_id", source_file, batch_id)
            )
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.production_events
                    (event_id, machine_id, event_timestamp, product_code,
                     quantity_produced, unit, shift, source_file, batch_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    row["event_id"], row["machine_id"], ts, row["product_code"],
                    int(row["quantity_produced"]), row["unit"], row["shift"], source_file, batch_id,
                ),
            )
            if cur.rowcount > 0:
                stats.inserted += 1
            else:
                stats.skipped_duplicate += 1
    return stats


def ingest_quality_checks(conn, rows, known_event_ids, source_file, batch_id) -> IngestStats:
    stats = IngestStats()
    for row in rows:
        ts = parse_timestamp(row.get("timestamp", ""))
        if row.get("production_event_id") not in known_event_ids:
            reason = "unknown production_event_id (foreign key would be violated)"
        elif ts is None:
            reason = "invalid or corrupted timestamp"
        else:
            reason = None

        if reason:
            stats.quarantined += int(
                _quarantine_row(conn, "quality_checks", row, reason, "check_id", source_file, batch_id)
            )
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.quality_checks
                    (check_id, production_event_id, check_timestamp, result,
                     defect_type, inspector_id, source_file, batch_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (check_id) DO NOTHING
                """,
                (
                    row["check_id"], row["production_event_id"], ts, row["result"],
                    row.get("defect_type") or None, row.get("inspector_id") or None, source_file, batch_id,
                ),
            )
            if cur.rowcount > 0:
                stats.inserted += 1
            else:
                stats.skipped_duplicate += 1
    return stats


def ingest_maintenance_events(conn, rows, known_machine_ids, source_file, batch_id) -> IngestStats:
    stats = IngestStats()
    for row in rows:
        start = parse_timestamp(row.get("start_time", ""))
        end = parse_timestamp(row.get("end_time", ""))
        if row.get("machine_id") not in known_machine_ids:
            reason = "unknown machine_id (foreign key would be violated)"
        elif start is None or end is None:
            reason = "invalid or corrupted timestamp"
        else:
            reason = None

        if reason:
            stats.quarantined += int(
                _quarantine_row(conn, "maintenance_events", row, reason, "maintenance_id", source_file, batch_id)
            )
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.maintenance_events
                    (maintenance_id, machine_id, start_time, end_time,
                     maintenance_type, technician_id, source_file, batch_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (maintenance_id) DO NOTHING
                """,
                (
                    row["maintenance_id"], row["machine_id"], start, end,
                    row["maintenance_type"], row["technician_id"], source_file, batch_id,
                ),
            )
            if cur.rowcount > 0:
                stats.inserted += 1
            else:
                stats.skipped_duplicate += 1
    return stats


def _existing_ids(conn, table: str, id_column: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {id_column} FROM raw.{table}")  # noqa: S608
        return {row[0] for row in cur.fetchall()}


def ingest_dataset(conn, data_dir: Path, source_file: str) -> dict[str, dict]:
    batch_id = str(uuid.uuid4())
    log_event("ingest_batch_started", batch_id=batch_id, source_file=source_file)

    machines_rows = read_csv(data_dir / "machines.csv")
    production_rows = read_csv(data_dir / "production_events.csv")
    quality_rows = read_csv(data_dir / "quality_checks.csv")
    maintenance_rows = read_csv(data_dir / "maintenance_events.csv")

    results: dict[str, dict] = {}

    machines_stats = ingest_machines(conn, machines_rows, source_file, batch_id)
    conn.commit()
    results["machines"] = machines_stats.as_dict()
    log_event("table_ingested", table="machines", batch_id=batch_id, **machines_stats.as_dict())

    known_machine_ids = _existing_ids(conn, "machines", "machine_id")

    production_stats = ingest_production_events(conn, production_rows, known_machine_ids, source_file, batch_id)
    conn.commit()
    results["production_events"] = production_stats.as_dict()
    log_event("table_ingested", table="production_events", batch_id=batch_id, **production_stats.as_dict())

    known_event_ids = _existing_ids(conn, "production_events", "event_id")

    quality_stats = ingest_quality_checks(conn, quality_rows, known_event_ids, source_file, batch_id)
    conn.commit()
    results["quality_checks"] = quality_stats.as_dict()
    log_event("table_ingested", table="quality_checks", batch_id=batch_id, **quality_stats.as_dict())

    maintenance_stats = ingest_maintenance_events(conn, maintenance_rows, known_machine_ids, source_file, batch_id)
    conn.commit()
    results["maintenance_events"] = maintenance_stats.as_dict()
    log_event("table_ingested", table="maintenance_events", batch_id=batch_id, **maintenance_stats.as_dict())

    log_event("ingest_batch_finished", batch_id=batch_id, source_file=source_file, results=results)
    return results
