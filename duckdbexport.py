#!/usr/bin/env python3
import os
import subprocess
import sys
import tempfile
import sqlite3
import pandas as pd
import json
import re
import argparse
import duckdb


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


def export_table(duck_conn, sqlite_conn, table_name: str, sample: bool = False):
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
    # If sample is True, select approximately 1% of rows.
    if sample:
        query = f"SELECT * FROM {table_name} WHERE random() < 0.01"
    else:
        query = f"SELECT * FROM {table_name}"
    rows = duck_conn.execute(query).fetchall()
    if not rows:
        print(f'Warning: Table "{table_name}" has no rows.')
    else:
        placeholders = ", ".join(["?"] * len(columns_info))
        insert_stmt = f'INSERT INTO "{table_name}" VALUES ({placeholders});'
        sqlite_conn.executemany(insert_stmt, rows)
        print(f'Inserted {len(rows)} rows into "{table_name}".')
    sqlite_conn.commit()


def export_duckdb_to_sqlite(duckdb_filename: str, sqlite_filename: str, sample: bool = False, tables_to_export: list = None):
    if os.path.exists(sqlite_filename):
        print(f"Overwriting existing {sqlite_filename} file.")
        os.remove(sqlite_filename)
    duck_conn = duckdb.connect(duckdb_filename, read_only=True)
    sqlite_conn = sqlite3.connect(sqlite_filename)

    # Get all tables from DuckDB.
    all_tables = duck_conn.execute("SHOW TABLES;").fetchall()
    if tables_to_export:
        # Filter to only export the specified tables.
        tables = [t for t in all_tables if t[0] in tables_to_export]
        print(
            f"Exporting only the following tables from DuckDB: {', '.join(t[0] for t in tables)}")
    else:
        tables = all_tables

    if not tables:
        print("No tables found for export in DuckDB.")
        duck_conn.close()
        sqlite_conn.close()
        return

    print("Exporting marts tables from DuckDB to SQLite:")
    for row in tables:
        table_name = row[0]
        print(f" * Exporting table: {table_name}")
        export_table(duck_conn, sqlite_conn, table_name, sample)
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


def split_sql_dump(sql_dump_file: str, out_dir: str, max_statements: int = 6000) -> list:
    """
    Splits the SQL dump file into multiple files, each containing no more than max_statements.
    Returns a list of output file paths.
    """
    with open(sql_dump_file, "r", encoding="utf-8") as f:
        content = f.read()
    # Split on semicolon; assumes that semicolon correctly ends each statement.
    # (If there are semicolons in literals, a more robust parser will be needed.)
    statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]
    output_files = []
    for i in range(0, len(statements), max_statements):
        chunk = statements[i:i+max_statements]
        # Re-add semicolon and newline after each statement.
        chunk_text = ";\n".join(chunk) + ";\n"
        out_file = os.path.join(
            out_dir, f"wdi_part{(i//max_statements)+1}.sql")
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(chunk_text)
        output_files.append(out_file)
        print(
            f"Created SQL dump chunk: {out_file} with {len(chunk)} statements.")
    return output_files


# Updated D1 functions: they now use a d1_mode string that is either "local" or "remote"
def drop_mart_tables_from_d1(d1_mode: str) -> None:
    """
    List tables from the Cloudflare D1 'wdi' database and drop all marts tables using
    wrangler@latest.
    d1_mode should be "local" or "remote" which determines the flag passed to wrangler.
    """
    flag = "--local" if d1_mode == "local" else "--remote"
    print("Listing tables in Cloudflare D1 database 'wdi' using wrangler@latest (using valid SQLite query)...")
    list_tables_cmd = f"npx wrangler@latest d1 execute wdi {flag} --command \"SELECT name FROM sqlite_master WHERE type='table';\""
    result = subprocess.run(list_tables_cmd, shell=True,
                            capture_output=True, text=True)

    mart_tables = []
    stdout = result.stdout.strip()
    # Attempt to find and parse JSON output from the command.
    json_start = stdout.find('[')
    if json_start != -1:
        try:
            json_text = stdout[json_start:]
            data = json.loads(json_text)
            # Assume the structure is a list with one entry having a "results" list
            if isinstance(data, list) and len(data) > 0:
                results = data[0].get("results", [])
                for item in results:
                    table = item.get("name", "").strip()
                    if table.lower() == "name" or not table:
                        continue
                    if table.startswith("fct_") or table.startswith("dim_"):
                        mart_tables.append(table)
        except Exception as e:
            print("Failed to parse JSON output:", e)
    else:
        # Fallback processing on line-by-line output (for alternative formats)
        for line in stdout.splitlines():
            if "│" in line:
                parts = line.split("│")
                if len(parts) < 2:
                    continue
                table = parts[1].strip()
                if table.lower() == "name" or not table:
                    continue
                if table.startswith("fct_") or table.startswith("dim_"):
                    mart_tables.append(table)

    if not mart_tables:
        print("No marts tables to drop in D1 database 'wdi'.")
        return

    for table in mart_tables:
        drop_cmd = f"npx wrangler@latest d1 execute wdi {flag} --command \"DROP TABLE IF EXISTS {table};\" --yes"
        subprocess.run(drop_cmd, shell=True, check=True)
        print(f"Dropped table {table} from D1 database 'wdi'.")


def update_d1_from_dump(sql_dump_file: str, d1_mode: str):
    print("Dropping marts tables from Cloudflare D1 database 'wdi' using wrangler@latest...")
    drop_mart_tables_from_d1(d1_mode)
    print("Updating Cloudflare D1 database 'wdi' using the SQL dump chunk with wrangler@latest...")
    flag = "--local" if d1_mode == "local" else "--remote"
    update_cmd = f"npx wrangler@latest d1 execute wdi {flag} --file {sql_dump_file} --yes"
    subprocess.run(update_cmd, shell=True, check=True)
    print(f"Cloudflare D1 database 'wdi' updated using {sql_dump_file}.")


def update_d1_chunk(sql_dump_file: str, d1_mode: str):
    """
    Update Cloudflare D1 database by executing a single SQL dump chunk using wrangler@latest.
    This function does NOT drop marts tables.
    """
    flag = "--local" if d1_mode == "local" else "--remote"
    update_cmd = f"npx wrangler@latest d1 execute wdi {flag} --file {sql_dump_file} --yes"
    subprocess.run(update_cmd, shell=True, check=True)
    print(f"Cloudflare D1 database 'wdi' updated using {sql_dump_file}.")


def export_mart_tables_parquet(duckdb_filename: str, output_dir: str):
    """
    Export all marts tables from DuckDB as local Parquet files.
    The exported data is ordered by all columns.
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
            select_exprs = [col[0] for col in cols_info]
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


def load_and_sort_parquet(filename: str) -> pd.DataFrame:
    try:
        df = pd.read_parquet(filename)
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        raise
    sort_cols = df.columns.tolist()
    df = df.sort_values(by=sort_cols).reset_index(drop=True)
    return df


def get_changed_mart_tables_from_parquet(local_dir: str, remote_dir: str) -> list:
    """
    Compare each local Parquet file with its counterpart in remote_dir.
    Returns a list of table names (derived from the file name without ".parquet")
    for which the files differ (or the remote file is missing).
    """
    changed_tables = []
    local_files = sorted([f for f in os.listdir(
        local_dir) if f.endswith(".parquet")])
    for f in local_files:
        local_path = os.path.join(local_dir, f)
        remote_path = os.path.join(remote_dir, f)
        if not os.path.exists(remote_path):
            print(f"Remote file is missing: {f}")
            changed_tables.append(f[:-8])  # remove ".parquet"
            continue
        try:
            local_df = load_and_sort_parquet(local_path)
            remote_df = load_and_sort_parquet(remote_path)
        except Exception as e:
            print(f"Failed to load Parquet file {f}: {e}")
            changed_tables.append(f[:-8])
            continue
        try:
            pd.testing.assert_frame_equal(
                local_df, remote_df,
                rtol=1e-5, atol=5e-4, check_exact=False
            )
        except AssertionError as e:
            print(f"Data mismatch in file: {f}.")
            changed_tables.append(f[:-8])
    return changed_tables


def dump_table_from_sqlite(db_filename: str, table: str, output_file: str) -> None:
    """
    Dumps only the specified table from the SQLite database and cleans the output
    by removing transaction markers and any _cf_KV related blocks, similar to dump_and_clean_sqlite.
    """
    dump_cmd = f'sqlite3 {db_filename} ".dump {table}"'
    result = subprocess.run(dump_cmd, shell=True,
                            capture_output=True, text=True, check=True)
    dump_text = result.stdout
    print(f"Dumped raw SQL for table {table}. Starting clean-up...")

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


# New per-table D1 functions

def drop_mart_table_from_d1(table: str, d1_mode: str) -> None:
    """
    Drop a single mart table from the D1 database using wrangler@latest.
    """
    flag = "--local" if d1_mode == "local" else "--remote"
    drop_cmd = f"npx wrangler@latest d1 execute wdi {flag} --command \"DROP TABLE IF EXISTS {table};\" --yes"
    subprocess.run(drop_cmd, shell=True, check=True)
    print(f"Dropped table {table} from D1 database 'wdi'.")


def update_d1_table_from_dump(sql_dump_file: str, d1_mode: str) -> None:
    """
    Update one table in D1 via a SQL dump file using wrangler@latest.
    """
    flag = "--local" if d1_mode == "local" else "--remote"
    update_cmd = f"npx wrangler@latest d1 execute wdi {flag} --file {sql_dump_file} --yes"
    subprocess.run(update_cmd, shell=True, check=True)
    print(f"Updated D1 table via dump file {sql_dump_file}.")


def main():
    parser = argparse.ArgumentParser(
        description=("Exports DuckDB marts tables as Parquet files, compares them with Parquet files on R2, "
                     "and if differences are detected for specific tables (or if --force is provided), "
                     "syncs the corresponding tables to R2 and updates them in D1.")
    )
    parser.add_argument("--force", action="store_true",
                        help="Force export process for all tables.")
    parser.add_argument("--local-d1", action="store_true",
                        help="Use local D1 database (adds --local to wrangler commands).")
    parser.add_argument("--remote-d1", action="store_true",
                        help="Use remote D1 database (adds --remote to wrangler commands).")
    parser.add_argument("--ignore-d1", action="store_true",
                        help="Ignore the D1 export process and exit after detecting differences.")
    parser.add_argument("--sample-d1", action="store_true",
                        help="Only export 1% of rows for D1 export (for testing purposes).")
    args = parser.parse_args()

    if (args.local_d1 and args.remote_d1) or (not args.local_d1 and not args.remote_d1):
        print("Error: Please specify exactly one of --local-d1 or --remote-d1.")
        sys.exit(1)
    d1_mode = "local" if args.local_d1 else "remote"
    duckdb_filename = "wdi.duckdb"

    with tempfile.TemporaryDirectory() as parquet_dir, tempfile.TemporaryDirectory() as sqlite_dir:
        print(f"Using temporary directory for Parquet files: {parquet_dir}")
        print(f"Using temporary directory for SQLite files: {sqlite_dir}")

        # Step 1: Export all mart tables from DuckDB as local Parquet files.
        export_mart_tables_parquet(duckdb_filename, parquet_dir)

        # Step 2: Download remote Parquet files to a temporary folder.
        with tempfile.TemporaryDirectory() as remote_parquet_dir:
            download_remote_parquet(remote_parquet_dir)
            # Get list of changed mart tables.
            changed_tables = get_changed_mart_tables_from_parquet(
                parquet_dir, remote_parquet_dir)

        if not changed_tables and not args.force:
            print("No changes detected in mart tables; D1 update process skipped.")
        else:
            # If forced, update ALL tables; otherwise, update only changed ones.
            if args.force:
                parquet_files = [f for f in os.listdir(
                    parquet_dir) if f.endswith(".parquet")]
                tables_to_update = [os.path.splitext(
                    f)[0] for f in parquet_files]
                print(
                    f"Force update enabled: All tables will be updated: {', '.join(tables_to_update)}")
            else:
                tables_to_update = changed_tables
                print(
                    f"Changed mart tables detected: {', '.join(changed_tables)}")

            # Sync only the relevant Parquet files to R2.
            for f in os.listdir(parquet_dir):
                table = f[:-8]  # remove '.parquet'
                if args.force or table in tables_to_update:
                    print(f"Syncing Parquet file for table {table}...")
                    subprocess.run(
                        ["rclone", "copy", os.path.join(parquet_dir, f), "r2:wdi",
                         "--include", f],
                        check=True
                    )
            if args.ignore_d1:
                print("Ignoring D1 export process as per flag. Exiting now.")
                return

            # Step 3: Export to SQLite and dump clean SQL.
            sqlite_filename = os.path.join(sqlite_dir, "wdi.sqlite3")
            print(f"Exporting to SQLite database in {sqlite_dir} ...")
            export_duckdb_to_sqlite(duckdb_filename, sqlite_filename,
                                    sample=args.sample_d1, tables_to_export=tables_to_update)

            # For each table to update, dump just that table and update D1.
            for table in tables_to_update:
                print(f"Processing table {table} for D1 update...")
                table_dump_file = os.path.join(sqlite_dir, f"{table}.sql")
                dump_table_from_sqlite(sqlite_filename, table, table_dump_file)
                drop_mart_table_from_d1(table, d1_mode)
                update_d1_table_from_dump(table_dump_file, d1_mode)

        print("Process complete.")


if __name__ == '__main__':
    main()
