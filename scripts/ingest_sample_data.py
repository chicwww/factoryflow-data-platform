import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from factoryflow.ingest import apply_schema, get_connection, ingest_dataset

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "sample"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    conn = get_connection()
    try:
        apply_schema(conn)
        results = ingest_dataset(conn, DATA_DIR, source_file="data/sample")
        print("Ingestion results:")
        for table, stats in results.items():
            print(f"  {table}: {stats}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
