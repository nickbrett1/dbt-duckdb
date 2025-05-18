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
import requests

# This script downloads the World Bank WDI data, unzips it, and loads it into a PostgreSQL or DuckDB database.
# It also fetches current country population data from the World Bank API and loads it directly into a new table.

SOURCE = "https://databank.worldbank.org/data/download/WDI_CSV.zip"
# World Bank API for total population (SP.POP.TOTL) for 2022.
POPULATION_SERVICE_URL = "http://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&date=2022"

POSTGRES_DB = "wdi"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
DUCKDB_DATABASE = f"{POSTGRES_DB}.duckdb"

# Configure your Cloudflare R2 bucket remote (ensure rclone is configured accordingly)
R2_BUCKET = "r2:mybucket/wdi_data"


def download_file(url, dest_dir):
    print(f"Downloading data from {url} using rclone...")
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
    con = duckdb.connect(DUCKDB_DATABASE)
    con.execute("CREATE SCHEMA IF NOT EXISTS public")
    query = f"""
    CREATE TABLE IF NOT EXISTS public.{table_name} AS
    SELECT * FROM read_csv_auto('{file_path}', header=True);
    """
    con.execute(query)
    count = con.execute(
        f"SELECT COUNT(*) FROM public.{table_name}").fetchone()[0]
    if count == 0:
        print(f"Error: No data was inserted into table public.{table_name}.")
    else:
        print(
            f"Table public.{table_name} successfully populated with {count} rows.")
    con.close()


def process_csv_postgres(file_path, table_name, conn):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    with open(file_path, "r", encoding="utf-8") as f:
        headers = f.readline().strip().split(",")
        columns = ", ".join([f'"{header}" TEXT' for header in headers])
        create_table_query = f"CREATE TABLE {table_name} ({columns})"
        cursor.execute(create_table_query)
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
    cursor.execute(
        f"SELECT 1 FROM pg_database WHERE datname = '{POSTGRES_DB}'")
    if not cursor.fetchone():
        print(f"Database {POSTGRES_DB} does not exist. Creating it...")
        cursor.execute(
            f"CREATE DATABASE {POSTGRES_DB} WITH ENCODING 'UTF8' TEMPLATE template0;")
    conn.close()
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )


def fetch_population_data():
    print("Fetching population data from World Bank API...")
    try:
        response = requests.get(POPULATION_SERVICE_URL)
        response.raise_for_status()
        data = response.json()
        # The API returns a two-element JSON array; the second element contains the records.
        records = data[1] if len(data) > 1 else []
        if not records:
            print("Warning: Population data is empty.")
        else:
            print(f"Retrieved population data for {len(records)} records.")
        # Build a list of tuples (country_code, population)
        pop_list = [
            (row.get("countryiso3code"), row.get("value"))
            for row in records if row.get("countryiso3code") is not None
        ]
        return pop_list
    except Exception as e:
        print(f"Error fetching population data: {e}")
        return []


def import_population_data_duckdb_direct(pop_data):
    print("Importing population data into DuckDB directly...")
    con = duckdb.connect(DUCKDB_DATABASE)
    con.execute("CREATE SCHEMA IF NOT EXISTS public")
    con.execute("DROP TABLE IF EXISTS public.country_population")
    con.execute("""
        CREATE TABLE public.country_population (
            country_code VARCHAR,
            population BIGINT
        );
    """)
    con.executemany(
        "INSERT INTO public.country_population VALUES (?, ?)", pop_data)
    count = con.execute(
        "SELECT COUNT(*) FROM public.country_population").fetchone()[0]
    if count == 0:
        print("Error: No data was inserted into the country_population table.")
    else:
        print(
            f"country_population table successfully populated with {count} rows.")
    con.close()


def import_population_data_postgres_direct(pop_data, conn):
    print("Importing population data into PostgreSQL directly...")
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS country_population")
    cursor.execute("""
        CREATE TABLE country_population (
            country_code VARCHAR,
            population BIGINT
        );
    """)
    insert_query = "INSERT INTO country_population (country_code, population) VALUES (%s, %s)"
    cursor.executemany(insert_query, pop_data)
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM country_population")
    count = cursor.fetchone()[0]
    if count == 0:
        print("Error: No data was inserted into country_population table.")
    else:
        print(
            f"country_population table successfully populated with {count} rows.")


def main():
    parser = argparse.ArgumentParser(
        description="Load WDI data and population mapping into a database."
    )
    parser.add_argument("--use-duckdb", action="store_true",
                        help="Use DuckDB instead of PostgreSQL")
    args = parser.parse_args()

    temp_dir = tempfile.mkdtemp()
    print(f"Temporary directory created at {temp_dir}")

    try:
        # Process the main WDI data ZIP.
        zip_file = download_file(SOURCE, temp_dir)
        if not sync_to_r2(zip_file):
            print("Skipping processing for WDI data because it is not new.")
        else:
            unzip_file(zip_file, temp_dir)
            if args.use_duckdb:
                if os.path.exists(DUCKDB_DATABASE):
                    print(
                        f"Database {DUCKDB_DATABASE} already exists. Deleting it...")
                    os.remove(DUCKDB_DATABASE)
                process_csv_files(temp_dir, process_csv_duckdb)
                print("DuckDB database population successful for WDI data.")
            else:
                conn = setup_postgres_database()
                process_csv_files(temp_dir, lambda fp,
                                  tn: process_csv_postgres(fp, tn, conn))
                conn.commit()
                conn.close()

        # Fetch and import population data directly into the database.
        pop_data = fetch_population_data()
        if pop_data:
            if args.use_duckdb:
                import_population_data_duckdb_direct(pop_data)
            else:
                conn = setup_postgres_database()
                import_population_data_postgres_direct(pop_data, conn)
                conn.commit()
                conn.close()
    finally:
        print("Cleaning up temporary files...")
        shutil.rmtree(temp_dir)
        print("Temporary files cleaned up.")
    print("Done.")


if __name__ == "__main__":
    main()
