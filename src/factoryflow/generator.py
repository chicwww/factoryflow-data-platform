"""Deterministic synthetic data generator for FactoryFlow.

Design goals:
- Fully deterministic given a seed: same seed + same parameters -> byte-identical output.
- Produces four related tables: machines, production_events, quality_checks,
  maintenance_events.
- Injects realistic messiness on purpose (duplicates, nulls, unit
  inconsistencies, invalid timestamps, late-arriving records) so that later
  phases (ingestion, dbt tests, quality rules) have something real to catch.

This module has no third-party dependencies on purpose, to keep Phase 1
dependency-free and easy to review.
"""

from __future__ import annotations

import csv
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

MACHINE_TYPES = ["stamping_press", "cnc_mill", "welding_robot", "assembly_line", "packaging_unit"]
PRODUCT_CODES = ["PRD-A100", "PRD-B200", "PRD-C300", "PRD-D400"]
SHIFTS = ["morning", "afternoon", "night"]
MAINTENANCE_TYPES = ["preventive", "corrective", "inspection"]
DEFECT_TYPES = ["dimension", "surface_finish", "assembly", "material", None]

# Two units used inconsistently on purpose: quantity is sometimes logged in
# "units" and sometimes mistakenly in "kg" for the same product line, which
# downstream quality checks must catch.
UNITS = ["units", "kg"]


@dataclass
class GeneratedDataset:
    machines: list[dict] = field(default_factory=list)
    production_events: list[dict] = field(default_factory=list)
    quality_checks: list[dict] = field(default_factory=list)
    maintenance_events: list[dict] = field(default_factory=list)


def generate_machines(rng: random.Random, n_machines: int) -> list[dict]:
    machines = []
    base_install = datetime(2018, 1, 1)
    for i in range(1, n_machines + 1):
        install_offset_days = rng.randint(0, 365 * 5)
        machine_type = MACHINE_TYPES[i % len(MACHINE_TYPES)]
        install_date = (base_install + timedelta(days=install_offset_days)).date()
        machines.append(
            {
                "machine_id": f"M{i:03d}",
                "machine_name": f"{machine_type.replace('_', ' ').title()} {i}",
                "machine_type": machine_type,
                "install_date": install_date.isoformat(),
            }
        )
    return machines


def generate_production_events(
    rng: random.Random,
    machines: list[dict],
    start_date: datetime,
    days: int,
    events_per_day_per_machine: int,
) -> list[dict]:
    events = []
    event_counter = 1
    for day_offset in range(days):
        day = start_date + timedelta(days=day_offset)
        for machine in machines:
            for _ in range(events_per_day_per_machine):
                hour = rng.randint(0, 23)
                minute = rng.randint(0, 59)
                timestamp = day.replace(hour=hour, minute=minute, second=0)

                # ~3% invalid timestamps: naive corruption (year 1900) to
                # simulate a known sensor bug.
                if rng.random() < 0.03:
                    timestamp = timestamp.replace(year=1900)

                unit = "units" if rng.random() > 0.05 else "kg"  # ~5% wrong unit
                quantity = rng.randint(50, 500)

                events.append(
                    {
                        "event_id": f"PE{event_counter:06d}",
                        "machine_id": machine["machine_id"],
                        "timestamp": timestamp.isoformat(),
                        "product_code": rng.choice(PRODUCT_CODES),
                        "quantity_produced": quantity,
                        "unit": unit,
                        "shift": rng.choice(SHIFTS),
                    }
                )
                event_counter += 1

    # ~2% duplicate events: simulate a resend from the factory floor system.
    duplicates = [dict(e) for e in events if rng.random() < 0.02]
    events.extend(duplicates)

    return events


def generate_quality_checks(rng: random.Random, production_events: list[dict]) -> list[dict]:
    checks = []
    check_counter = 1
    inspectors = [f"INS{i:02d}" for i in range(1, 6)]
    for event in production_events:
        # Not every event gets a quality check (~85% coverage), which is realistic
        # and gives later "coverage" quality rules something to measure.
        if rng.random() > 0.85:
            continue

        result = "pass" if rng.random() > 0.1 else "fail"
        defect_type = rng.choice(DEFECT_TYPES) if result == "fail" else None

        # ~2% of checks arrive with a null inspector, on purpose.
        inspector = rng.choice(inspectors) if rng.random() > 0.02 else None

        checks.append(
            {
                "check_id": f"QC{check_counter:06d}",
                "production_event_id": event["event_id"],
                "timestamp": event["timestamp"],
                "result": result,
                "defect_type": defect_type,
                "inspector_id": inspector,
            }
        )
        check_counter += 1

    # Introduce a handful of orphan quality checks referencing a
    # non-existent production event, to exercise foreign-key quality tests
    # in later phases.
    for i in range(max(1, len(production_events) // 500)):
        checks.append(
            {
                "check_id": f"QC-ORPHAN{i:03d}",
                "production_event_id": "PE999999",
                "timestamp": production_events[0]["timestamp"] if production_events else None,
                "result": "pass",
                "defect_type": None,
                "inspector_id": rng.choice(inspectors),
            }
        )

    return checks


def generate_maintenance_events(
    rng: random.Random, machines: list[dict], start_date: datetime, days: int
) -> list[dict]:
    events = []
    counter = 1
    technicians = [f"TEC{i:02d}" for i in range(1, 4)]
    for machine in machines:
        n_events = rng.randint(1, max(1, days // 10))
        for _ in range(n_events):
            day_offset = rng.randint(0, days - 1)
            start = start_date + timedelta(days=day_offset, hours=rng.randint(0, 20))
            duration_hours = rng.randint(1, 6)
            events.append(
                {
                    "maintenance_id": f"MT{counter:05d}",
                    "machine_id": machine["machine_id"],
                    "start_time": start.isoformat(),
                    "end_time": (start + timedelta(hours=duration_hours)).isoformat(),
                    "maintenance_type": rng.choice(MAINTENANCE_TYPES),
                    "technician_id": rng.choice(technicians),
                }
            )
            counter += 1
    return events


def generate_dataset(
    seed: int,
    days: int = 3,
    n_machines: int = 5,
    events_per_day_per_machine: int = 20,
    start_date: datetime | None = None,
) -> GeneratedDataset:
    """Generate a full, deterministic synthetic dataset.

    Calling this twice with the same arguments always returns identical data:
    every source of randomness goes through the single seeded `rng` instance,
    and no wall-clock time or unseeded randomness is used anywhere.
    """
    rng = random.Random(seed)
    start = start_date or datetime(2026, 1, 1)

    machines = generate_machines(rng, n_machines)
    production_events = generate_production_events(
        rng, machines, start, days, events_per_day_per_machine
    )
    quality_checks = generate_quality_checks(rng, production_events)
    maintenance_events = generate_maintenance_events(rng, machines, start, days)

    return GeneratedDataset(
        machines=machines,
        production_events=production_events,
        quality_checks=quality_checks,
        maintenance_events=maintenance_events,
    )


def write_dataset_to_csv(dataset: GeneratedDataset, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    tables = {
        "machines": dataset.machines,
        "production_events": dataset.production_events,
        "quality_checks": dataset.quality_checks,
        "maintenance_events": dataset.maintenance_events,
    }

    for name, rows in tables.items():
        path = output_dir / f"{name}.csv"
        if not rows:
            path.write_text("")
            continue
        fieldnames = list(rows[0].keys())
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
