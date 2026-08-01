#!/usr/bin/env python3
"""Generate the small example dataset committed under data/sample/.

Usage:
    python scripts/generate_sample_data.py

This intentionally uses small, fixed parameters (3 days, 5 machines) so the
committed sample stays small and fast to inspect. Larger runs (for local
testing against Postgres in later phases) will use GENERATOR_DAYS from .env
and write to data/raw/ (gitignored), not here.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from factoryflow.generator import generate_dataset, write_dataset_to_csv  # noqa: E402

SEED = 42
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "sample"


def main() -> None:
    dataset = generate_dataset(seed=SEED, days=3, n_machines=5, events_per_day_per_machine=20)
    write_dataset_to_csv(dataset, OUTPUT_DIR)
    print(f"Wrote sample dataset to {OUTPUT_DIR} (seed={SEED})")
    print(f"  machines: {len(dataset.machines)}")
    print(f"  production_events: {len(dataset.production_events)}")
    print(f"  quality_checks: {len(dataset.quality_checks)}")
    print(f"  maintenance_events: {len(dataset.maintenance_events)}")


if __name__ == "__main__":
    main()
