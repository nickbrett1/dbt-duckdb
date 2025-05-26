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


def export_duckdb_to_sqlite(duckdb_filename: str, sqlite_filename: str, sample: bool = False):
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
            export_table(duck_conn, sqlite_conn, table_name, sample)
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
    wrangler@3.103.2 as a workaround for workers-sdk/issues/8153.
    d1_mode should be "local" or "remote" which determines the flag passed to wrangler.
    """
    flag = "--local" if d1_mode == "local" else "--remote"
    print("Listing tables in Cloudflare D1 database 'wdi' using wrangler@3.103.2 (using valid SQLite query)...")
    # Use a valid SQLite query instead of "SHOW TABLES;"
    list_tables_cmd = f"npx wrangler@3.103.2 d1 execute wdi {flag} --command \"SELECT name FROM sqlite_master WHERE type='table';\""
    result = subprocess.run(list_tables_cmd, shell=True,
                            capture_output=True, text=True)
    mart_tables = []
    # Skip a header if present and process each line.
    for line in result.stdout.splitlines():
        table = line.strip()
        if table.lower() == "name":  # skip header line if any
            continue
        if table.startswith("fct_") or table.startswith("dim_"):
            mart_tables.append(table)
    if not mart_tables:
        print("No marts tables to drop in D1 database 'wdi'.")
        return
    for table in mart_tables:
        drop_cmd = f"npx wrangler@3.103.2 d1 execute wdi {flag} --command \"DROP TABLE IF EXISTS {table};\" --yes"
        subprocess.run(drop_cmd, shell=True, check=True)
        print(f"Dropped table {table} from D1 database 'wdi'.")


def update_d1_from_dump(sql_dump_file: str, d1_mode: str):
    print("Dropping marts tables from Cloudflare D1 database 'wdi' using wrangler@3.103.2...")
    drop_mart_tables_from_d1(d1_mode)
    print("Updating Cloudflare D1 database 'wdi' using the SQL dump chunk with wrangler@3.103.2...")
    flag = "--local" if d1_mode == "local" else "--remote"
    update_cmd = f"npx wrangler@3.103.2 d1 execute wdi {flag} --file {sql_dump_file} --yes"
    subprocess.run(update_cmd, shell=True, check=True)
    print(f"Cloudflare D1 database 'wdi' updated using {sql_dump_file}.")


def update_d1_chunk(sql_dump_file: str, d1_mode: str):
    """
    Update Cloudflare D1 database by executing a single SQL dump chunk using wrangler@3.103.2.
    This function does NOT drop marts tables.
    """
    flag = "--local" if d1_mode == "local" else "--remote"
    update_cmd = f"npx wrangler@3.103.2 d1 execute wdi {flag} --file {sql_dump_file} --yes"
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
    # Sort the DataFrame by all columns to ensure deterministic order.
    sort_cols = df.columns.tolist()
    df = df.sort_values(by=sort_cols).reset_index(drop=True)
    return df


def parquet_files_differ(local_dir: str, remote_dir: str) -> bool:
    """
    Compare local and remote Parquet files, tolerating small floating point differences.
    """
    local_files = sorted([f for f in os.listdir(
        local_dir) if f.endswith(".parquet")])
    if not local_files:
        print("No local Parquet files to compare.")
        return False
    tolerance_rel = 1e-5
    tolerance_abs = 5e-4
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
                local_df, remote_df,
                rtol=tolerance_rel, atol=tolerance_abs, check_exact=False
            )
        except AssertionError as e:
            print(
                f"Data mismatch in file: {f} beyond tolerances (rtol={tolerance_rel}, atol={tolerance_abs}).")
            print(f"Local file: {local_path}")
            print(f"Remote file: {remote_path}")
            print("AssertionError:", e)
            return True
    print("All local Parquet files are within tolerances in the remote directory.")
    return False


def parquet_changes_exist(parquet_dir: str) -> bool:
    """
    Download Parquet files from R2 to a temporary directory and compare to local files.
    Returns True if differences are detected.
    """
    with tempfile.TemporaryDirectory() as remote_parquet_dir:
        download_remote_parquet(remote_parquet_dir)
        return parquet_files_differ(parquet_dir, remote_parquet_dir)


def drop_mart_tables_from_sqlite(db_filename: str) -> None:
    """
    List and drop all marts tables (names starting with 'fct_' or 'dim_') in SQLite.
    """
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'fct_%' OR name LIKE 'dim_%');"
    )
    mart_tables = [row[0] for row in cursor.fetchall()]
    if not mart_tables:
        print("No marts tables to drop in SQLite database.")
        conn.close()
        return
    for table in mart_tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
        print(f"Dropped table {table} from SQLite database.")
    conn.commit()
    conn.close()


def update_sqlite_from_dump(db_filename: str, sql_dump_file: str):
    """
    Drop marts tables and update the SQLite database using the new SQL dump.
    """
    print("Dropping marts tables from SQLite database...")
    drop_mart_tables_from_sqlite(db_filename)
    print("Updating SQLite database using the new SQL dump...")
    update_cmd = f'sqlite3 {db_filename} ".read {sql_dump_file}"'
    subprocess.run(update_cmd, shell=True, check=True)
    print("SQLite database updated using the new SQL dump.")


def main():
    parser = argparse.ArgumentParser(
        description=("Exports DuckDB marts tables as Parquet files, "
                     "checks differences in Parquet files on R2, and if differences are detected "
                     "(or if --force-d1 is provided), triggers the D1 export process and syncs the files back to R2.")
    )
    parser.add_argument("--force-d1", action="store_true",
                        help="Force D1 export process.")
    parser.add_argument("--local-d1", action="store_true",
                        help="Use local D1 database (adds --local to wrangler commands).")
    parser.add_argument("--remote-d1", action="store_true",
                        help="Use remote D1 database (adds --remote to wrangler commands).")
    parser.add_argument("--ignore-d1", action="store_true",
                        help="Ignore the D1 export process and exit after detecting differences.")
    parser.add_argument("--sample-d1", action="store_true",
                        help="Only export 1% of rows for D1 export (for testing purposes).")
    args = parser.parse_args()

    # Require that exactly one of --local-d1 or --remote-d1 be specified.
    if (args.local_d1 and args.remote_d1) or (not args.local_d1 and not args.remote_d1):
        print("Error: Please specify exactly one of --local-d1 or --remote-d1.")
        sys.exit(1)

    # Set the d1_mode based on the flag.
    d1_mode = "local" if args.local_d1 else "remote"

    duckdb_filename = "wdi.duckdb"  # Existing DuckDB file

    with tempfile.TemporaryDirectory() as parquet_dir, tempfile.TemporaryDirectory() as sqlite_dir:
        print(f"Using temporary directory for Parquet files: {parquet_dir}")
        print(f"Using temporary directory for SQLite files: {sqlite_dir}")

        # Step 1: Export local Parquet files.
        export_mart_tables_parquet(duckdb_filename, parquet_dir)

        # Step 2: Check for differences in Parquet files on R2.
        differences = parquet_changes_exist(parquet_dir)
        if differences or args.force_d1:
            print("Differences detected (or forced).")
            print("Syncing Parquet files back to R2 and triggering D1 export process.")
            subprocess.run(
                ["rclone", "copy", parquet_dir, "r2:wdi",
                 "--include", "*.parquet", "--checksum"],
                check=True
            )

            if args.ignore_d1:
                print("Ignoring D1 export process as per flag. Exiting now.")
                return

            # Step 3: Export to SQLite and dump clean SQL.
            sqlite_filename = os.path.join(sqlite_dir, "wdi.sqlite3")
            sql_dump_file = os.path.join(sqlite_dir, "wdi.sql")
            print(
                f"Exporting to SQLite database in temporary directory: {sqlite_dir}")
            export_duckdb_to_sqlite(
                duckdb_filename, sqlite_filename, sample=args.sample_d1)
            dump_and_clean_sqlite(sqlite_filename, sql_dump_file)
            sql_files = split_sql_dump(
                sql_dump_file, sqlite_dir, max_statements=6000)

            print(
                "Dropping marts tables from Cloudflare D1 database 'wdi' once before updates...")
            drop_mart_tables_from_d1(d1_mode)

            total_chunks = len(sql_files)
            print(
                f"Updating Cloudflare D1 database using {total_chunks} SQL dump chunk(s)...")
            for i, sql_file in enumerate(sql_files, 1):
                print(f"Updating chunk {i}/{total_chunks}: {sql_file}")
                update_d1_chunk(sql_file, d1_mode)
        else:
            print("No differences found in Parquet files; D1 export process skipped.")

        print("Process complete.")


if __name__ == '__main__':
    main()
