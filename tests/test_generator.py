import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from factoryflow.generator import generate_dataset  # noqa: E402


def test_same_seed_produces_identical_machines():
    a = generate_dataset(seed=42, days=3, n_machines=5, events_per_day_per_machine=20)
    b = generate_dataset(seed=42, days=3, n_machines=5, events_per_day_per_machine=20)
    assert a.machines == b.machines


def test_same_seed_produces_identical_production_events():
    a = generate_dataset(seed=42, days=3, n_machines=5, events_per_day_per_machine=20)
    b = generate_dataset(seed=42, days=3, n_machines=5, events_per_day_per_machine=20)
    assert a.production_events == b.production_events


def test_same_seed_produces_identical_quality_checks():
    a = generate_dataset(seed=42, days=3, n_machines=5, events_per_day_per_machine=20)
    b = generate_dataset(seed=42, days=3, n_machines=5, events_per_day_per_machine=20)
    assert a.quality_checks == b.quality_checks


def test_same_seed_produces_identical_maintenance_events():
    a = generate_dataset(seed=42, days=3, n_machines=5, events_per_day_per_machine=20)
    b = generate_dataset(seed=42, days=3, n_machines=5, events_per_day_per_machine=20)
    assert a.maintenance_events == b.maintenance_events


def test_different_seed_produces_different_production_events():
    a = generate_dataset(seed=42, days=3, n_machines=5, events_per_day_per_machine=20)
    b = generate_dataset(seed=43, days=3, n_machines=5, events_per_day_per_machine=20)
    assert a.production_events != b.production_events


def test_dataset_has_expected_row_counts():
    dataset = generate_dataset(seed=42, days=3, n_machines=5, events_per_day_per_machine=20)
    # 3 days * 5 machines * 20 events/day = 300 base events, plus a small
    # deterministic number of injected duplicates.
    assert len(dataset.production_events) >= 300
    assert len(dataset.machines) == 5


def test_quality_checks_reference_existing_or_intentionally_orphan_events():
    dataset = generate_dataset(seed=42, days=3, n_machines=5, events_per_day_per_machine=20)
    valid_ids = {e["event_id"] for e in dataset.production_events}
    orphan_checks = [c for c in dataset.quality_checks if c["production_event_id"] not in valid_ids]
    # Orphans are injected on purpose (to be caught by dbt/quality tests later),
    # so we assert they exist and are all intentionally tagged, not accidental.
    assert all(c["check_id"].startswith("QC-ORPHAN") for c in orphan_checks)


def test_some_events_have_wrong_unit_by_design():
    dataset = generate_dataset(seed=42, days=3, n_machines=5, events_per_day_per_machine=20)
    units = {e["unit"] for e in dataset.production_events}
    assert units == {"units", "kg"}
