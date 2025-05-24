#!/usr/bin/env python3
# filepath: /workspaces/dbt-duckdb/duckdbexport.py
import os
import duckdb
import sqlite3
import subprocess
import re
import argparse
import tempfile


def map_type(duck_type: str) -> str:
    dt = duck_type.lower()
    if "int" in dt:
        return "INTEGER"
    elif any(x in dt for x in ["double", "float", "decimal", "numeric", "real"]):
        return "REAL"
    elif "bool" in dt:
        return "INTEGER"
    else:
        return "TEXT"


def export_table(duck_conn, sqlite_conn, table_name: str):
    columns_info = duck_conn.execute(f"DESCRIBE {table_name}").fetchall()
    if not columns_info:
        print(f"Warning: No column info for table {table_name}")
        return
    columns_def = []
    for col in columns_info:
        col_name = col[0]
        col_type = map_type(col[1])
        columns_def.append(f'"{col_name}" {col_type}')
    create_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns_def)});'
    sqlite_conn.execute(create_stmt)
    print(f'Created table "{table_name}" in SQLite.')
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
    if os.path.exists(sqlite_filename):
        print(f"Overwriting existing {sqlite_filename} file.")
        os.remove(sqlite_filename)
    duck_conn = duckdb.connect(duckdb_filename, read_only=True)
    sqlite_conn = sqlite3.connect(sqlite_filename)
    tables = duck_conn.execute("SHOW TABLES;").fetchall()
    if not tables:
        print("No tables found in DuckDB.")
        duck_conn.close()
        sqlite_conn.close()
        return
    mart_prefixes = ("fct_", "dim_")
    print("Exporting marts tables from DuckDB to SQLite:")
    for table in tables:
        table_name = table[0]
        if table_name.startswith(mart_prefixes):
            print(f" * Exporting table: {table_name}")
            export_table(duck_conn, sqlite_conn, table_name)
        else:
            print(f" - Skipping non-mart table: {table_name}")
    duck_conn.close()
    sqlite_conn.close()
    print("DuckDB export to SQLite complete.")


def dump_and_clean_sqlite(sqlite_filename: str, output_sql_filename: str) -> None:
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
    print(
        f"Cleaning complete. Processed {total_lines} lines, skipped {skipped_lines} lines.")
    with open(output_sql_filename, "w", encoding="utf-8") as f:
        f.write(cleaned_dump)
    print(f"Cleaned SQL dump is written to {output_sql_filename}")


def update_d1_from_dump(sql_dump_file: str):
    list_cmd = "npx wrangler d1 list"
    result = subprocess.run(list_cmd, shell=True,
                            capture_output=True, text=True)
    if "wdi" in result.stdout:
        print("Existing Cloudflare D1 database 'wdi' found; deleting it...")
        delete_cmd = "npx wrangler d1 delete wdi -y"
        subprocess.run(delete_cmd, shell=True, check=True)
    else:
        print("Cloudflare D1 database 'wdi' does not exist; no need to delete.")
    print("Creating Cloudflare D1 database 'wdi'...")
    create_cmd = "npx wrangler d1 create wdi"
    subprocess.run(create_cmd, shell=True, check=True)
    print("Updating Cloudflare D1 database 'wdi' using the new SQL dump...")
    update_cmd = f"npx wrangler d1 execute wdi --file {sql_dump_file} --yes"
    subprocess.run(update_cmd, shell=True, check=True)
    print("Cloudflare D1 database 'wdi' updated using the new SQL dump.")


def export_mart_tables_parquet(duckdb_filename: str, output_dir: str):
    """
    Export all marts tables from DuckDB as local Parquet files.
    The exported data is ordered by all columns so that repeated exports
    generate identical files if the underlying data doesn't change.
    """
    duck_conn = duckdb.connect(duckdb_filename, read_only=True)
    tables = duck_conn.execute("SHOW TABLES;").fetchall()
    if not tables:
        print("No tables found in DuckDB.")
        duck_conn.close()
        return
    mart_prefixes = ("fct_", "dim_")
    print("Exporting marts tables from DuckDB to local Parquet files:")
    for row in tables:
        table_name = row[0]
        if table_name.startswith(mart_prefixes):
            parquet_filename = os.path.join(
                output_dir, f"{table_name}.parquet")
            # Get column names for the table.
            cols_info = duck_conn.execute(f"DESCRIBE {table_name}").fetchall()
            if not cols_info:
                print(f"Warning: Could not retrieve columns for {table_name}")
                continue
            col_names = [col[0] for col in cols_info]
            order_by = ", ".join(col_names)
            copy_query = f"COPY (SELECT * FROM {table_name} ORDER BY {order_by}) TO '{parquet_filename}' (FORMAT 'parquet')"
            duck_conn.execute(copy_query)
            print(f"Exported table {table_name} to {parquet_filename}")
        else:
            print(f"Skipping non-mart table: {table_name}")
    duck_conn.close()
    print("Local Parquet export complete.")


def parquet_changes_exist(parquet_dir: str) -> bool:
    """
    Use rclone check with --one-way to compare .parquet files in parquet_dir with those stored in R2.
    Returns True if differences are detected.
    """
    print("Checking for differences in Parquet files in temporary directory compared to R2...")
    try:
        subprocess.run(
            ["rclone", "check", parquet_dir, "r2:wdi", "--include",
                "*.parquet", "--one-way"],
            check=True, capture_output=True, text=True
        )
        print("No differences found in Parquet files on R2.")
        return False
    except subprocess.CalledProcessError:
        print("Differences detected in Parquet files on R2.")
        return True


def main():
    parser = argparse.ArgumentParser(
        description=("Exports DuckDB marts tables as temporary Parquet files "
                     "and syncs them to Cloudflare R2. By default, the D1 export "
                     "process is skipped and is only executed if --force-d1 is provided, "
                     "using temporary SQLite and SQL dump files.")
    )
    parser.add_argument("--force-d1", action="store_true",
                        help="Force D1 export process.")
    args = parser.parse_args()

    duckdb_filename = "wdi.duckdb"  # Existing DuckDB file

    # Step 1: Generate temporary local Parquet files.
    with tempfile.TemporaryDirectory(prefix="parquet_") as parquet_dir:
        print(f"Using temporary directory for Parquet files: {parquet_dir}")
        export_mart_tables_parquet(duckdb_filename, parquet_dir)

        # Sync the Parquet files to R2.
        print("Syncing Parquet files to R2...")
        subprocess.run(
            ["rclone", "copy", parquet_dir, "r2:wdi",
                "--include", "*.parquet", "--checksum"],
            check=True
        )

        # Step 2: Run D1 export process only if --force-d1 is provided.
        if args.force_d1:
            with tempfile.TemporaryDirectory(prefix="sqlite_") as tmp_sqlite_dir:
                sqlite_filename = os.path.join(tmp_sqlite_dir, "wdi.sqlite3")
                sql_dump_file = os.path.join(tmp_sqlite_dir, "wdi.sql")
                print(
                    f"Using temporary directory for SQLite export: {tmp_sqlite_dir}")
                export_duckdb_to_sqlite(duckdb_filename, sqlite_filename)
                dump_and_clean_sqlite(sqlite_filename, sql_dump_file)
                update_d1_from_dump(sql_dump_file)
        else:
            print("D1 export process skipped.")

    print("Process complete.")


if __name__ == '__main__':
    main()
