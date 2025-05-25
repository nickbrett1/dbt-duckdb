#!/usr/bin/env python3
import os
import duckdb
import sqlite3
import subprocess
import re
import argparse
import sys
import filecmp
import hashlib
import pandas as pd
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
    The exported data is ordered by all columns so that repeated exports generate
    identical files if the underlying data doesn't change.
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
            cols_info = duck_conn.execute(f"DESCRIBE {table_name}").fetchall()
            if not cols_info:
                print(f"Warning: Could not retrieve columns for {table_name}")
                continue
            order_by = ", ".join([col[0] for col in cols_info])
            select_exprs = []
            for col in cols_info:
                col_name = col[0]
                # Simply select the column.
                expr = col_name
                select_exprs.append(expr)
            select_list = ", ".join(select_exprs)
            copy_query = (
                f"COPY (SELECT {select_list} FROM {table_name} ORDER BY {order_by}) "
                f"TO '{parquet_filename}' (FORMAT 'parquet')"
            )
            duck_conn.execute(copy_query)
            print(f"Exported table {table_name} to {parquet_filename}")
        else:
            print(f"Skipping non-mart table: {table_name}")
    duck_conn.close()
    print("Local Parquet export complete.")


def download_remote_parquet(remote_dir: str) -> None:
    """
    Download marts Parquet files (those starting with 'fct_' or 'dim_')
    from R2 (folder r2:wdi) to a local directory.
    """
    os.makedirs(remote_dir, exist_ok=True)
    subprocess.run(
        [
            "rclone", "copy", "r2:wdi", remote_dir,
            "--include", "fct_*.parquet",
            "--include", "dim_*.parquet"
        ],
        check=True
    )
    print(f"Downloaded remote marts Parquet files to {remote_dir}")


def md5sum(filename: str) -> str:
    """Compute the MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def load_and_sort_parquet(filename: str) -> pd.DataFrame:
    try:
        df = pd.read_parquet(filename)
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        raise
    # Sort the DataFrame by all columns to ensure deterministic order.
    sort_cols = df.columns.tolist()
    df = df.sort_values(by=sort_cols).reset_index(drop=True)
    return df


def parquet_files_differ(local_dir: str, remote_dir: str) -> bool:
    """
    Compare local and remote Parquet files in a one-way diff,
    tolerating small differences in floating point numbers.
    For each local Parquet file, ensure that an identical file exists in remote_dir,
    up to a specified tolerance.
    Returns True if any local file is missing or differs beyond the tolerance.
    """
    local_files = sorted([f for f in os.listdir(
        local_dir) if f.endswith(".parquet")])
    if not local_files:
        print("No local Parquet files to compare.")
        return False

    # Tolerance value for differences in floating point numbers.
    tolerance = 5e-4  # Accepts a difference of 0.0005

    for f in local_files:
        local_path = os.path.join(local_dir, f)
        remote_path = os.path.join(remote_dir, f)
        if not os.path.exists(remote_path):
            print(f"Remote file is missing: {f}")
            return True
        try:
            local_df = load_and_sort_parquet(local_path)
            remote_df = load_and_sort_parquet(remote_path)
        except Exception as e:
            print(f"Failed to load Parquet file {f}: {e}")
            return True

        try:
            pd.testing.assert_frame_equal(
                local_df, remote_df, rtol=0, atol=tolerance, check_exact=False
            )
        except AssertionError as e:
            print(
                f"Data mismatch in file: {f} beyond tolerance (atol={tolerance}).")
            print(f"Local file: {local_path}")
            print(f"Remote file: {remote_path}")
            print("AssertionError:", e)
            return True

    print("All local Parquet files are within tolerance in the remote directory.")
    return False


def parquet_changes_exist(parquet_dir: str) -> bool:
    """
    Download Parquet files from R2 to a temporary directory and compare to the local files.
    Returns True if differences are detected.
    """
    with tempfile.TemporaryDirectory() as remote_parquet_dir:
        download_remote_parquet(remote_parquet_dir)
        return parquet_files_differ(parquet_dir, remote_parquet_dir)


def main():
    parser = argparse.ArgumentParser(
        description=("Exports DuckDB marts tables as Parquet files, "
                     "checks for differences in the Parquet files on R2, "
                     "and if differences are detected (or if --force-d1 is provided), triggers "
                     "the D1 export process and syncs the Parquet files back to R2.")
    )
    parser.add_argument("--force-d1", action="store_true",
                        help="Force D1 export process.")
    args = parser.parse_args()

    duckdb_filename = "wdi.duckdb"   # Existing DuckDB file

    # Use temporary directories for storing the parquet and sqlite files.
    with tempfile.TemporaryDirectory() as parquet_dir, tempfile.TemporaryDirectory() as sqlite_dir:
        print(f"Using temporary directory for Parquet files: {parquet_dir}")
        print(f"Using temporary directory for SQLite files: {sqlite_dir}")

        # Step 1: Export local Parquet files.
        export_mart_tables_parquet(duckdb_filename, parquet_dir)

        # Step 2: Check for differences in Parquet files on R2.
        differences = parquet_changes_exist(parquet_dir)
        if differences or args.force_d1:
            print("Differences detected (or forced). Syncing Parquet files back to R2 and triggering D1 export process.")
            subprocess.run(
                ["rclone", "copy", parquet_dir, "r2:wdi",
                 "--include", "*.parquet", "--checksum"],
                check=True
            )
            # Step 3: Run D1 export process.
            sqlite_filename = os.path.join(sqlite_dir, "wdi.sqlite3")
            sql_dump_file = os.path.join(sqlite_dir, "wdi.sql")
            print(
                f"Exporting to SQLite database in temporary directory: {sqlite_dir}")
            export_duckdb_to_sqlite(duckdb_filename, sqlite_filename)
            dump_and_clean_sqlite(sqlite_filename, sql_dump_file)
            update_d1_from_dump(sql_dump_file)
        else:
            print("No differences found in Parquet files; D1 export process skipped.")

        print("Process complete.")


if __name__ == '__main__':
    main()
