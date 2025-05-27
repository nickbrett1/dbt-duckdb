#!/usr/bin/env python3
# filepath: /workspaces/dbt-duckdb/update_d1.py
import os
import subprocess
import tempfile
import sqlite3
import duckdb
import re
import json
import argparse


def export_duckdb_to_sqlite(duckdb_filename: str, sqlite_filename: str, sample: bool = False, tables_to_export: list = None) -> None:
    if not tables_to_export:
        print("No tables provided for export. Skipping export.")
        return
    if os.path.exists(sqlite_filename):
        print(f"Overwriting existing {sqlite_filename} file.")
        os.remove(sqlite_filename)
    duck_conn = duckdb.connect(duckdb_filename, read_only=True)
    sqlite_conn = sqlite3.connect(sqlite_filename)
    all_tables = duck_conn.execute("SHOW TABLES;").fetchall()
    tables = [t for t in all_tables if t[0] in tables_to_export]
    if not tables:
        print("No matching tables found in DuckDB. Skipping export.")
        duck_conn.close()
        sqlite_conn.close()
        return
    print(
        f"Exporting only the following tables: {', '.join([t[0] for t in tables])}")
    for row in tables:
        table_name = row[0]
        print(f" * Exporting table: {table_name}")
        columns_info = duck_conn.execute(f"DESCRIBE {table_name}").fetchall()
        if not columns_info:
            print(f"Warning: No column info for table {table_name}")
            continue
        columns_def = []
        for col in columns_info:
            col_name = col[0]
            col_type = "TEXT" if "text" in col[1].lower() else "REAL" if (
                "double" in col[1].lower() or "int" in col[1].lower()) else "TEXT"
            columns_def.append(f'"{col_name}" {col_type}')
        create_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns_def)});'
        sqlite_conn.execute(create_stmt)
        query = f"SELECT * FROM {table_name}" if not sample else f"SELECT * FROM {table_name} WHERE random() < 0.01"
        rows = duck_conn.execute(query).fetchall()
        if not rows:
            print(f'Warning: Table "{table_name}" has no rows.')
        else:
            placeholders = ", ".join(["?"] * len(columns_info))
            insert_stmt = f'INSERT INTO "{table_name}" VALUES ({placeholders});'
            sqlite_conn.executemany(insert_stmt, rows)
            print(f'Inserted {len(rows)} rows into "{table_name}".')
        sqlite_conn.commit()
    duck_conn.close()
    sqlite_conn.close()
    print("Export to SQLite complete.")


def dump_table_from_sqlite(db_filename: str, table: str, output_file: str) -> None:
    dump_cmd = f'sqlite3 {db_filename} ".dump {table}"'
    result = subprocess.run(dump_cmd, shell=True,
                            capture_output=True, text=True, check=True)
    dump_text = result.stdout
    cleaned_lines = []
    skip_kv_block = False
    total_lines = 0
    skipped_lines = 0
    kv_pattern = re.compile(r'^CREATE TABLE _cf_KV ')
    for line in dump_text.splitlines():
        total_lines += 1
        if line.startswith("BEGIN TRANSACTION;") or line.startswith("COMMIT;"):
            skipped_lines += 1
            continue
        if kv_pattern.match(line):
            skip_kv_block = True
            skipped_lines += 1
            continue
        if skip_kv_block:
            skipped_lines += 1
            if "WITHOUT ROWID;" in line:
                skip_kv_block = False
            continue
        cleaned_lines.append(line)
    cleaned_dump = "\n".join(cleaned_lines)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(cleaned_dump)
    print(
        f"Dumped and cleaned table {table} to {output_file}. Processed {total_lines} lines, skipped {skipped_lines} lines.")


def drop_mart_table_from_d1(table: str, d1_mode: str) -> None:
    flag = "--local" if d1_mode == "local" else "--remote"
    drop_cmd = f"npx wrangler@latest d1 execute wdi {flag} --command \"DROP TABLE IF EXISTS {table};\" --yes"
    subprocess.run(drop_cmd, shell=True, check=True)
    print(f"Dropped table {table} from D1 database 'wdi'.")


def update_d1_table_from_dump(sql_dump_file: str, d1_mode: str) -> None:
    flag = "--local" if d1_mode == "local" else "--remote"
    update_cmd = f"npx wrangler@latest d1 execute wdi {flag} --file {sql_dump_file} --yes"
    subprocess.run(update_cmd, shell=True, check=True)
    print(f"Updated D1 table via dump file {sql_dump_file}.")


def main():
    parser = argparse.ArgumentParser(
        description="Update D1 tables using changed tables JSON output")
    parser.add_argument("changed_tables_json",
                        help="JSON file containing table names to update")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--local", action="store_true",
                       help="Set D1 mode to local")
    group.add_argument("--remote", action="store_true",
                       help="Set D1 mode to remote")
    parser.add_argument("--sample", action="store_true",
                        help="Enable sampling mode for export (default is unsampled)")
    args = parser.parse_args()

    with open(args.changed_tables_json, "r", encoding="utf-8") as f:
        changed_tables = json.load(f)
    duckdb_filename = "wdi.duckdb"
    d1_mode = "local" if args.local else "remote"

    with tempfile.TemporaryDirectory() as sqlite_dir:
        sqlite_filename = os.path.join(sqlite_dir, "wdi.sqlite3")
        print(
            f"Exporting selected tables to SQLite database in {sqlite_dir} ...")
        export_duckdb_to_sqlite(
            duckdb_filename, sqlite_filename, sample=args.sample, tables_to_export=changed_tables)
        for table in changed_tables:
            print(f"Processing table {table} for D1 update...")
            table_dump_file = os.path.join(sqlite_dir, f"{table}.sql")
            dump_table_from_sqlite(sqlite_filename, table, table_dump_file)
            drop_mart_table_from_d1(table, d1_mode)
            update_d1_table_from_dump(table_dump_file, d1_mode)
        print("D1 update process complete.")


if __name__ == '__main__':
    main()
