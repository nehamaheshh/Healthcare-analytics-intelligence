"""
Run all SQL transformations in order: staging -> intermediate -> marts.
Reads .sql files from sql/staging, sql/intermediate, sql/marts and executes against solace_ops.duckdb.
"""
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "solace_ops.duckdb"

SQL_DIRS = [
    ("staging", PROJECT_ROOT / "sql" / "staging"),
    ("intermediate", PROJECT_ROOT / "sql" / "intermediate"),
    ("marts", PROJECT_ROOT / "sql" / "marts"),
]


def run_sql_file(conn: duckdb.DuckDBPyConnection, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        # Skip empty or comment-only lines; strip leading comment lines from stmt
        while stmt and stmt.split("\n")[0].strip().startswith("--"):
            stmt = "\n".join(stmt.split("\n")[1:]).strip()
        if stmt:
            conn.execute(stmt)


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}. Run load_duckdb.py first.")

    conn = duckdb.connect(str(DB_PATH))

    for layer, dir_path in SQL_DIRS:
        if not dir_path.exists():
            print(f"Skip {layer}: {dir_path} not found")
            continue
        files = sorted(dir_path.glob("*.sql"))
        for f in files:
            print(f"Running {layer}/{f.name}")
            run_sql_file(conn, f)

    conn.close()
    print("Pipeline complete.")


if __name__ == "__main__":
    main()
