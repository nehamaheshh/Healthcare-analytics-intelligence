"""
Load raw CSVs into DuckDB. Creates schemas raw, staging, intermediate, marts.
Raw tables are replaced on each run (idempotent).
"""
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
DB_PATH = PROJECT_ROOT / "solace_ops.duckdb"

RAW_TABLES = [
    ("patients", "patients.csv"),
    ("advocates", "advocates.csv"),
    ("cases", "cases.csv"),
    ("interactions", "interactions.csv"),
    ("case_events", "case_events.csv"),
    ("patient_feedback", "patient_feedback.csv"),
]


def main():
    raw_path = str(RAW_DIR).replace("\\", "/")
    db_path_str = str(DB_PATH).replace("\\", "/")

    conn = duckdb.connect(db_path_str)

    # Create schemas
    for schema in ["raw", "staging", "intermediate", "marts"]:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # Drop and load raw tables
    for table, filename in RAW_TABLES:
        csv_path = f"{raw_path}/{filename}"
        conn.execute(f"DROP TABLE IF EXISTS raw.{table}")
        conn.execute(f"""
            CREATE TABLE raw.{table} AS
            SELECT * FROM read_csv_auto('{csv_path}', header=true)
        """)
        print(f"Loaded raw.{table} from {filename}")

    conn.close()
    print(f"Database written to {DB_PATH}")


if __name__ == "__main__":
    main()
