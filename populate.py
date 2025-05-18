#!/usr/bin/env python3
# filepath: /workspaces/dbt-duckdb/populate.py
import os
import shutil
import tempfile
import zipfile
import duckdb
import subprocess
import psycopg2
import argparse

# This script downloads the World Bank WDI data, unzips it, and loads it into a PostgreSQL or DuckDB database.

SOURCE = "https://databank.worldbank.org/data/download/WDI_CSV.zip"
POSTGRES_DB = "wdi"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
# Align the names to not require conditionals in the dbt code
DUCKDB_DATABASE = f"{POSTGRES_DB}.duckdb"

# Configure your Cloudflare R2 bucket remote (ensure rclone is configured accordingly)
R2_BUCKET = "r2:mybucket/wdi_data"


def download_file(url, dest_dir):
    print("Downloading WDI data from World Bank using rclone...")
    result = subprocess.run(
        ["rclone", "copyurl", url, dest_dir, "--auto-filename", "--print-filename"],
        capture_output=True,
        text=True,
        check=True
    )
    filename = result.stdout.strip()
    print(f"Downloaded file: {filename}")
    return os.path.join(dest_dir, filename)


def sync_to_r2(zip_path):
    # Check if the remote already has the same file.
    print("Checking if data is new by comparing with Cloudflare R2 bucket...")
    check_cmd = ["rclone", "check", zip_path, R2_BUCKET]
    result = subprocess.run(check_cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print("Data has not changed on Cloudflare R2 bucket. Skipping further processing.")
        return False
    else:
        print("New data detected. Syncing file to Cloudflare R2 bucket...")
        copy_cmd = ["rclone", "copy", zip_path, R2_BUCKET, "--checksum"]
        subprocess.run(copy_cmd, check=True)
        return True


def unzip_file(zip_path, dest_dir):
    print("Unzipping file...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)
    print("Unzipped files:")
    for file in os.listdir(dest_dir):
        print(f" - {file}")


def process_csv_files(temp_dir, process_function):
    for file in os.listdir(temp_dir):
        if file.endswith(".csv"):
            file_path = os.path.join(temp_dir, file)
            print(f"Processing file: {file_path}")
            table_name = os.path.splitext(file)[0].replace("-", "")
            process_function(file_path, table_name)


def process_csv_duckdb(file_path, table_name):
    # Open a connection to the DuckDB file (ensuring data is persisted to disk)
    con = duckdb.connect(DUCKDB_DATABASE)

    # Create the 'public' schema if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS public")

    query = f"""
    CREATE TABLE IF NOT EXISTS public.{table_name} AS
    SELECT * FROM read_csv_auto('{file_path}', header=True);
    """
    con.execute(query)

    # Perform error checking by verifying the table contains data.
    count = con.execute(
        f"SELECT COUNT(*) FROM public.{table_name}").fetchone()[0]
    if count == 0:
        print(f"Error: No data was inserted into table public.{table_name}.")
    else:
        print(
            f"Table public.{table_name} successfully populated with {count} rows.")

    # Close the connection to ensure changes are saved to disk.
    con.close()


def process_csv_postgres(file_path, table_name, conn):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Dynamically create the table based on the CSV file headers
    with open(file_path, "r", encoding="utf-8") as f:
        headers = f.readline().strip().split(",")
        columns = ", ".join([f'"{header}" TEXT' for header in headers])
        create_table_query = f"CREATE TABLE {table_name} ({columns})"
        cursor.execute(create_table_query)

    # Perform the COPY operation using UTF-8 encoding
    with open(file_path, "r", encoding="utf-8") as f:
        cursor.copy_expert(
            f"COPY {table_name} FROM STDIN WITH CSV HEADER ENCODING 'UTF8'", f
        )
    conn.commit()
    print(f"Table {table_name} created and data inserted.")


def setup_postgres_database():
    conn = psycopg2.connect(
        dbname="postgres",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Create the database if it doesn't exist
    cursor.execute(
        f"SELECT 1 FROM pg_database WHERE datname = '{POSTGRES_DB}'")
    if not cursor.fetchone():
        print(f"Database {POSTGRES_DB} does not exist. Creating it...")
        cursor.execute(
            f"CREATE DATABASE {POSTGRES_DB} WITH ENCODING 'UTF8' TEMPLATE template0;")
    conn.close()

    # Connect to the new database
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )


def main():
    parser = argparse.ArgumentParser(
        description="Load WDI data into a database.")
    parser.add_argument(
        "--use-duckdb",
        action="store_true",
        help="Use DuckDB instead of PostgreSQL"
    )
    args = parser.parse_args()

    temp_dir = tempfile.mkdtemp()
    print(f"Temporary directory created at {temp_dir}")

    try:
        zip_file = download_file(SOURCE, temp_dir)
        # Sync to Cloudflare R2 and check if data is new
        if not sync_to_r2(zip_file):
            # Output already provided from sync_to_r2, exiting.
            return

        unzip_file(zip_file, temp_dir)

        if args.use_duckdb:
            if os.path.exists(DUCKDB_DATABASE):
                print(
                    f"Database {DUCKDB_DATABASE} already exists. Deleting it...")
                os.remove(DUCKDB_DATABASE)
            process_csv_files(temp_dir, process_csv_duckdb)
            print("DuckDB database population successful.")
        else:
            conn = setup_postgres_database()
            process_csv_files(temp_dir, lambda file_path, table_name: process_csv_postgres(
                file_path, table_name, conn))
            conn.commit()
            conn.close()
    finally:
        print("Cleaning up temporary files...")
        shutil.rmtree(temp_dir)
        print("Temporary files cleaned up.")
    print("Done.")


if __name__ == "__main__":
    main()
