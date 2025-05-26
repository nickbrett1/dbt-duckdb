#!/usr/bin/env python3
# filepath: /workspaces/dbt-duckdb/populate.py
import os
import shutil
import tempfile
import subprocess
import duckdb
import argparse
import pandas as pd
from sqlalchemy import create_engine, text

# Constants for processing
# These will be the local mirror copies from the R2 bucket.
# Contains Parquet files for each table of WDI data
R2_BUCKET_WDI = "r2:wdi"

POSTGRES_DB = "wdi"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
DUCKDB_DATABASE = f"{POSTGRES_DB}.duckdb"


def process_parquet_duckdb(parquet_path, table_name):
    print(
        f"Processing table {table_name} in DuckDB from file {parquet_path}...")

    con = duckdb.connect(DUCKDB_DATABASE)
    con.execute("CREATE SCHEMA IF NOT EXISTS public")
    query = f"""
    CREATE TABLE IF NOT EXISTS public.{table_name} AS
    SELECT * FROM read_parquet('{parquet_path}');
    """
    con.execute(query)
    count = con.execute(
        f"SELECT COUNT(*) FROM public.{table_name}").fetchone()[0]
    if count == 0:
        print(f"Error: No data loaded into public.{table_name}.")
    else:
        print(
            f"Table public.{table_name} loaded with {count} rows from {parquet_path}.")
    con.close()


def process_parquet_postgres(parquet_path, table_name, engine):
    print(
        f"Processing table {table_name} in PostgreSQL from file {parquet_path}...")
    try:
        # Use a connection (with engine.begin()) to drop the table with CASCADE.
        with engine.begin() as conn:
            conn.execute(
                text(f"DROP TABLE IF EXISTS public.{table_name} CASCADE"))
        df = pd.read_parquet(parquet_path, engine="pyarrow")
        # Write the new table to the database with if_exists="replace"
        df.to_sql(table_name, engine, schema="public",
                  if_exists="replace", index=False)
        print(
            f"Table {table_name} loaded with {len(df)} rows from {parquet_path}.")
    except Exception as e:
        print(f"Error processing {parquet_path} for table {table_name}: {e}")


def setup_postgres_engine():
    from sqlalchemy.engine.url import URL
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB
    )
    engine = create_engine(url)
    return engine


def main():
    parser = argparse.ArgumentParser(
        description="Populate databases by reading WDI Parquet files from Cloudflare R2."
    )
    parser.add_argument("--use-duckdb", action="store_true",
                        help="Use DuckDB instead of PostgreSQL")
    parser.add_argument("--use-postgres", action="store_true",
                        help="Populate PostgreSQL (instead of DuckDB)")
    args = parser.parse_args()

    # Validate that exactly one flag is provided.
    if args.use_duckdb == args.use_postgres:
        print("Error: Please specify either --use-duckdb or --use-postgres (but not both).")
        exit(1)

    temp_dir = tempfile.mkdtemp()
    print(f"Temporary directory created at {temp_dir}")

    try:
        # Copy all Parquet files from the R2 "sources" folder.
        subprocess.run(["rclone", "copy", f"{R2_BUCKET_WDI}/sources",
                        temp_dir, "--checksum"], check=True)
        parquet_files = [f for f in os.listdir(
            temp_dir) if f.endswith(".parquet")]
        if not parquet_files:
            print("No Parquet files found in local copy.")
        else:
            if args.use_duckdb:
                for file in parquet_files:
                    parquet_path = os.path.join(temp_dir, file)
                    table_name = os.path.splitext(file)[0].replace("-", "")
                    print(
                        f"Starting DuckDB processing for table: {table_name}")
                    process_parquet_duckdb(parquet_path, table_name)
                print("DuckDB population complete.")
            elif args.use_postgres:
                engine = setup_postgres_engine()
                for file in parquet_files:
                    parquet_path = os.path.join(temp_dir, file)
                    table_name = os.path.splitext(file)[0].replace("-", "")
                    print(
                        f"Starting PostgreSQL processing for table: {table_name}")
                    process_parquet_postgres(parquet_path, table_name, engine)
                print("PostgreSQL population complete.")
    finally:
        print("Cleaning up temporary files...")
        shutil.rmtree(temp_dir)
        print("Temporary files cleaned up.")
    print("Done.")


if __name__ == "__main__":
    main()
