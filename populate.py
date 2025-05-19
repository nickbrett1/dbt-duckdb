#!/usr/bin/env python3
import os
import shutil
import tempfile
import subprocess
import duckdb
import argparse
import pandas as pd
from sqlalchemy import create_engine

# Constants for processing
# These will be the local mirror copies from the R2 bucket.
# Contains Parquet files for each table of WDI data
R2_BUCKET_WDI = "r2:mybucket/wdi_data"

POSTGRES_DB = "wdi"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
DUCKDB_DATABASE = f"{POSTGRES_DB}.duckdb"


def copy_from_r2(remote_path, local_dir):
    print(f"Copying {remote_path} from R2 to local directory {local_dir}...")
    copy_cmd = ["rclone", "copy", remote_path, local_dir, "--checksum"]
    subprocess.run(copy_cmd, check=True)
    local_file = os.path.join(local_dir, os.path.basename(remote_path))
    print(f"Copied file: {local_file}")
    return local_file


def process_parquet_duckdb(parquet_path, table_name):
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
    try:
        df = pd.read_parquet(parquet_path, engine="pyarrow")
        # Write to the database using pandas. Replace table if it exists.
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
    args = parser.parse_args()

    temp_dir = tempfile.mkdtemp()
    print(f"Temporary directory created at {temp_dir}")

    try:
        # Copy all Parquet files from the R2 bucket.
        # This copies all files in the R2 bucket folder.
        subprocess.run(["rclone", "copy", R2_BUCKET_WDI,
                       temp_dir, "--checksum"], check=True)
        parquet_files = [f for f in os.listdir(
            temp_dir) if f.endswith(".parquet")]
        if not parquet_files:
            print("No Parquet files found in local copy.")
        else:
            if args.use_duckdb:
                for file in parquet_files:
                    parquet_path = os.path.join(temp_dir, file)
                    # Use the file basename (without extension) as the table name.
                    table_name = os.path.splitext(file)[0].replace("-", "")
                    process_parquet_duckdb(parquet_path, table_name)
                print("DuckDB population complete.")
            else:
                engine = setup_postgres_engine()
                for file in parquet_files:
                    parquet_path = os.path.join(temp_dir, file)
                    table_name = os.path.splitext(file)[0].replace("-", "")
                    process_parquet_postgres(parquet_path, table_name, engine)
                print("PostgreSQL population complete.")
    finally:
        print("Cleaning up temporary files...")
        shutil.rmtree(temp_dir)
        print("Temporary files cleaned up.")
    print("Done.")


if __name__ == "__main__":
    main()
