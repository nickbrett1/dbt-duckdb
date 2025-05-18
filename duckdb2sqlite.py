#!/usr/bin/env python3
# filepath: /workspaces/dbt-duckdb/duckdb2sqlite.py

import os
import duckdb
import sqlite3
import subprocess
import re
import argparse


def map_type(duck_type: str) -> str:
    """
    Map DuckDB types to SQLite types.
    Adjust as needed.
    """
    dt = duck_type.lower()
    if "int" in dt:
        return "INTEGER"
    elif any(x in dt for x in ["double", "float", "decimal", "numeric", "real"]):
        return "REAL"
    elif "bool" in dt:
        return "INTEGER"  # SQLite does not have a separate boolean type.
    else:
        return "TEXT"


def export_table(duck_conn, sqlite_conn, table_name: str):
    # Get table structure from DuckDB using DESCRIBE.
    columns_info = duck_conn.execute(f"DESCRIBE {table_name}").fetchall()
    if not columns_info:
        print(f"Warning: No column info for table {table_name}")
        return

    # Build SQLite CREATE TABLE statement.
    columns_def = []
    for col in columns_info:
        col_name = col[0]
        col_type = map_type(col[1])
        columns_def.append(f'"{col_name}" {col_type}')
    create_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns_def)});'
    sqlite_conn.execute(create_stmt)
    print(f'Created table "{table_name}" in SQLite.')

    # Fetch all rows from the table in DuckDB.
    rows = duck_conn.execute(f"SELECT * FROM {table_name}").fetchall()
    if not rows:
        print(f'Warning: Table "{table_name}" has no rows.')
    else:
        placeholders = ", ".join(["?"] * len(columns_info))
        insert_stmt = f'INSERT INTO "{table_name}" VALUES ({placeholders});'
        sqlite_conn.executemany(insert_stmt, rows)
        print(f'Inserted {len(rows)} rows into "{table_name}".')
    sqlite_conn.commit()


def export_duckdb_to_sqlite(duckdb_filename: str, sqlite_filename: str):
    # Remove existing SQLite file if it exists.
    if os.path.exists(sqlite_filename):
        print(f"Overwriting existing {sqlite_filename} file.")
        os.remove(sqlite_filename)

    # Connect to DuckDB (persistent file) in read-only mode.
    duck_conn = duckdb.connect(duckdb_filename, read_only=True)
    # Connect to SQLite.
    sqlite_conn = sqlite3.connect(sqlite_filename)

    # Get all tables from DuckDB.
    tables = duck_conn.execute("SHOW TABLES;").fetchall()
    if not tables:
        print("No tables found in DuckDB.")
        duck_conn.close()
        sqlite_conn.close()
        return

    # Only export marts tables. Adjust prefixes as needed.
    mart_prefixes = ("fct_", "dim_")
    print("Exporting marts tables from DuckDB to SQLite:")
    for table in tables:
        table_name = table[0]
        # Export only if the table name starts with one of the mart prefixes.
        if table_name.startswith(mart_prefixes):
            print(f" * Exporting table: {table_name}")
            export_table(duck_conn, sqlite_conn, table_name)
        else:
            print(f" - Skipping non-mart table: {table_name}")

    duck_conn.close()
    sqlite_conn.close()
    print("DuckDB export to SQLite complete.")


def dump_and_clean_sqlite(sqlite_filename: str, output_sql_filename: str):
    print(f"Dumping SQLite database '{sqlite_filename}' to SQL statements...")
    dump_cmd = f'sqlite3 {sqlite_filename} .dump'
    result = subprocess.run(dump_cmd, shell=True,
                            capture_output=True, text=True, check=True)
    dump_text = result.stdout
    print("Dump complete. Starting to clean the dump...")

    cleaned_lines = []
    skip_kv_block = False
    total_lines = 0
    skipped_lines = 0
    kv_pattern = re.compile(r'^CREATE TABLE _cf_KV ')

    for line in dump_text.splitlines():
        total_lines += 1
        # Skip transaction lines.
        if line.startswith("BEGIN TRANSACTION;") or line.startswith("COMMIT;"):
            skipped_lines += 1
            continue

        # Check if line begins the _cf_KV table block.
        if kv_pattern.match(line):
            skip_kv_block = True
            skipped_lines += 1
            continue

        # If we are in _cf_KV block, skip until we get to a line that ends with "WITHOUT ROWID;"
        if skip_kv_block:
            skipped_lines += 1
            if "WITHOUT ROWID;" in line:
                skip_kv_block = False
            continue

        cleaned_lines.append(line)

    cleaned_dump = "\n".join(cleaned_lines)
    print(
        f"Cleaning complete. Processed {total_lines} lines, skipped {skipped_lines} lines.")

    with open(output_sql_filename, "w", encoding="utf-8") as f:
        f.write(cleaned_dump)
    print(f"Cleaned SQL dump is written to {output_sql_filename}")


def main():
    parser = argparse.ArgumentParser(
        description="Export DuckDB to SQLite and/or dump and clean SQLite SQL statements."
    )
    parser.add_argument("--mode", choices=["all", "export", "dump"], default="all",
                        help="Mode to run: 'all' runs both export and dump; 'export' runs only export; 'dump' runs only dump (requires SQLite file).")
    args = parser.parse_args()

    duckdb_filename = "wdi.duckdb"
    sqlite_filename = "wdi.sqlite3"
    output_sql_filename = "wdi.sql"

    if args.mode in ("export", "all"):
        export_duckdb_to_sqlite(duckdb_filename, sqlite_filename)

    if args.mode in ("dump", "all"):
        dump_and_clean_sqlite(sqlite_filename, output_sql_filename)


if __name__ == '__main__':
    main()
