#!/usr/bin/env python3
# filepath: /workspaces/dbt-duckdb/download_population.py
import os
import subprocess
import tempfile
import argparse
import requests
import pandas as pd
import shutil

# World Bank API for total population (SP.POP.TOTL) for 2022.
POPULATION_SERVICE_URL = "http://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&date=2022"

# Cloudflare R2 bucket destination for population data (stored as Parquet).
# The Parquet file will be synced to the "sources" folder.
R2_BUCKET_POP = "r2:wdi"


def fetch_population_data():
    print("Fetching population data from World Bank API...")
    pop_list = []
    try:
        page = 1
        per_page = 1000  # adjust as necessary
        total_pages = None
        while True:
            url = f"{POPULATION_SERVICE_URL}&page={page}&per_page={per_page}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            # The first part of the response contains pagination info.
            if total_pages is None:
                pagination = data[0]
                total_pages = pagination.get("pages", 1)
                print(f"Total pages to fetch: {total_pages}")
            # The second element has the data records.
            records = data[1] if len(data) > 1 else []
            print(
                f"Retrieved {len(records)} records from page {page} of {total_pages}.")
            pop_list.extend([
                {"country_code": row.get(
                    "countryiso3code"), "population": row.get("value")}
                for row in records if row.get("countryiso3code") is not None
            ])
            if page >= total_pages:
                break
            page += 1
        print(f"Total population data records: {len(pop_list)}")
        return pop_list
    except Exception as e:
        print(f"Error fetching population data: {e}")
        return []


def save_population_parquet(pop_list, dest_file):
    print(f"Saving population data to Parquet file {dest_file} ...")
    try:
        df = pd.DataFrame(pop_list)
        df.to_parquet(dest_file, engine="pyarrow", index=False)
        print(f"Saved population data as Parquet to {dest_file}.")
    except Exception as e:
        print(f"Error saving population data to Parquet: {e}")


def sync_to_r2(local_path, r2_bucket):
    print(
        f"Checking if {os.path.basename(local_path)} is new by comparing with {r2_bucket}...")
    check_cmd = ["rclone", "check", local_path, r2_bucket]
    result = subprocess.run(check_cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"{os.path.basename(local_path)} has not changed on {r2_bucket}.")
        return False
    else:
        print(
            f"New data detected for {os.path.basename(local_path)}. Syncing to {r2_bucket}...")
        copy_cmd = ["rclone", "copy", local_path, r2_bucket, "--checksum"]
        subprocess.run(copy_cmd, check=True)
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Fetch population data from the World Bank API, store as Parquet, and sync to Cloudflare R2."
    )
    args = parser.parse_args()

    temp_dir = tempfile.mkdtemp()
    print(f"Temporary directory: {temp_dir}")

    try:
        pop_list = fetch_population_data()
        if pop_list:
            dest_file = os.path.join(temp_dir, "population_data.parquet")
            save_population_parquet(pop_list, dest_file)
            # Sync the generated Parquet file to the "sources" directory in R2.
            sync_to_r2(dest_file, f"{R2_BUCKET_POP}/sources")
        else:
            print("No population data fetched.")
    finally:
        print("Cleaning up temporary files...")
        shutil.rmtree(temp_dir)
        print("Done.")


if __name__ == "__main__":
    main()
