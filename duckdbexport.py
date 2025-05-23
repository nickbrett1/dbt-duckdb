#!/usr/bin/env python3
# filepath: /workspaces/dbt-duckdb/duckdbexport.py

import os
import duckdb
import sqlite3
import subprocess
import re
import argparse
import hashlib


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
    """
    Create a new SQLite database by exporting all DuckDB marts tables.
    """
    if os.path.exists(sqlite_filename):
        print(f"Overwriting existing {sqlite_filename} file.")
        os.remove(sqlite_filename)

    duck_conn = duckdb.connect(duckdb_filename, read_only=True)
    sqlite_conn = sqlite3.connect(sqlite_filename)

    # Get all tables from DuckDB.
    tables = duck_conn.execute("SHOW TABLES;").fetchall()
    if not tables:
        print("No tables found in DuckDB.")
        duck_conn.close()
        sqlite_conn.close()
        return

    # Only export marts tables.
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


def dump_and_clean_sqlite(sqlite_filename: str, output_sql_filename: str):
    """
    Dump the SQLite database to SQL statements and clean unwanted lines.
    """
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


def compute_file_hash(file_path: str) -> str:
    """Compute the SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def sync_sql_dump_hash_to_r2(local_sql_file: str, remote_path: str) -> bool:
    """
    Compute a hash of the local SQL dump file,
    write it to 'wdi.hash', and sync that with Cloudflare R2.
    The function then retrieves the remote hash (if any) and compares.
    Returns True if the hashes differ (or remote hash doesn't exist), otherwise False.
    """
    local_hash = compute_file_hash(local_sql_file)
    hash_file = "wdi.hash"
    # Write the local hash to a file.
    with open(hash_file, "w", encoding="utf-8") as f:
        f.write(local_hash)
    print(f"Local hash computed: {local_hash}")

    remote_file = os.path.join(remote_path, "wdi.hash")
    try:
        # Try to retrieve the remote hash using rclone cat.
        result = subprocess.run(
            ["rclone", "cat", remote_file],
            capture_output=True, text=True, check=True)
        remote_hash = result.stdout.strip()
        print(f"Remote hash retrieved: {remote_hash}")
    except subprocess.CalledProcessError:
        print("Remote hash file not found.")
        remote_hash = None

    if remote_hash != local_hash:
        print("Hashes differ; updating remote hash file.")
        # Sync only the hash file to R2.
        subprocess.run(["rclone", "copy", hash_file,
                       remote_path, "--checksum"], check=True)
        return True
    else:
        print("Remote hash matches local hash.")
        return False


def update_d1_from_dump(sql_dump_file: str):
    """
    If a Cloudflare D1 database named 'wdi' exists, delete it.
    Then create a new D1 database and update it using Wrangler (invoked via npx) to execute the SQL dump.
    """
    # List existing D1 databases and check if 'wdi' exists.
    list_cmd = "npx wrangler d1 list"
    result = subprocess.run(list_cmd, shell=True,
                            capture_output=True, text=True)
    if "wdi" in result.stdout:
        print("Existing Cloudflare D1 database 'wdi' found; deleting it...")
        delete_cmd = "npx wrangler d1 delete wdi"
        subprocess.run(delete_cmd, shell=True, check=True)
    else:
        print("Cloudflare D1 database 'wdi' does not exist; no need to delete.")

    print("Creating Cloudflare D1 database 'wdi'...")
    create_cmd = "npx wrangler d1 create wdi"
    subprocess.run(create_cmd, shell=True, check=True)

    print("Updating Cloudflare D1 database 'wdi' using the new SQL dump...")
    update_cmd = f"npx wrangler d1 execute wdi --file {sql_dump_file}"
    subprocess.run(update_cmd, shell=True, check=True)
    print("Cloudflare D1 database 'wdi' updated using the new SQL dump.")


def export_mart_tables_parquet(duckdb_filename: str):
    """
    Export all marts tables from DuckDB to Parquet files and sync them to Cloudflare R2.
    """
    duck_conn = duckdb.connect(duckdb_filename, read_only=True)
    tables = duck_conn.execute("SHOW TABLES;").fetchall()
    if not tables:
        print("No tables found in DuckDB.")
        duck_conn.close()
        return

    mart_prefixes = ("fct_", "dim_")
    print("Exporting marts tables from DuckDB to Parquet:")
    for row in tables:
        table_name = row[0]
        if table_name.startswith(mart_prefixes):
            parquet_filename = f"{table_name}.parquet"
            copy_query = f"COPY (SELECT * FROM {table_name}) TO '{parquet_filename}' (FORMAT 'parquet')"
            duck_conn.execute(copy_query)
            print(f"Exported table {table_name} to {parquet_filename}")
            # Sync the parquet file to Cloudflare R2; destination folder is r2:wdi.
            subprocess.run(["rclone", "copy", parquet_filename,
                           "r2:wdi", "--checksum"], check=True)
        else:
            print(f"Skipping non-mart table: {table_name}")
    duck_conn.close()
    print("DuckDB export to Parquet complete.")


def main():
    parser = argparse.ArgumentParser(
        description=("Export DuckDB marts tables either to Cloudflare D1 or as Parquet files "
                     "synced to Cloudflare R2. In 'd1' mode, a new SQLite DB is generated from DuckDB, "
                     "dumped and cleaned into an SQL dump which is then synced to R2. If the remote dump "
                     "differs, the Cloudflare D1 database (named 'wdi') is deleted and updated via Wrangler. "
                     "In 'parquet' mode, each mart table is exported as Parquet and synced to R2.")
    )
    parser.add_argument("--mode", choices=["d1", "parquet", "all"], default="all",
                        help="Operation mode: 'd1' for Cloudflare D1 export; 'parquet' for Parquet export; 'all' for both.")
    args = parser.parse_args()

    duckdb_filename = "wdi.duckdb"        # Existing DuckDB file
    sqlite_filename = "wdi.sqlite3"       # SQLite file to be created from DuckDB
    sql_dump_file = "wdi.sql"             # Cleaned SQL dump output
    r2_d1_path = "r2:wdi"                 # Remote destination for the SQL dump on R2

    if args.mode in ("d1", "all"):
        print("Starting D1 export process...")
        # Create SQLite file from DuckDB marts tables.
        export_duckdb_to_sqlite(duckdb_filename, sqlite_filename)
        # Dump and clean the SQLite database.
        dump_and_clean_sqlite(sqlite_filename, sql_dump_file)
        # Instead of syncing the entire SQL dump, sync the hash file.
        updated = sync_sql_dump_hash_to_r2(sql_dump_file, r2_d1_path)
        if updated:
            # Update Cloudflare D1 via Wrangler if the dump is updated.
            update_d1_from_dump(sql_dump_file)
        else:
            print("Cloudflare D1 export is up-to-date. No update needed.")

    if args.mode in ("parquet", "all"):
        print("Starting Parquet export process...")
        export_mart_tables_parquet(duckdb_filename)


if __name__ == '__main__':
    main()
