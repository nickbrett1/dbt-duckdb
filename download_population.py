#!/usr/bin/env python3
import os
import subprocess
import tempfile
import argparse
import requests
import pandas as pd
import shutil

# World Bank API for total population (SP.POP.TOTL) for 2022.
POPULATION_SERVICE_URL = "http://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&date=2022"

# Cloudflare R2 bucket destination for population data (stored as Parquet)
R2_BUCKET_POP = "r2:wdi"


def fetch_population_data():
    print("Fetching population data from World Bank API...")
    try:
        response = requests.get(POPULATION_SERVICE_URL)
        response.raise_for_status()
        data = response.json()
        # The API returns two elements, where the second includes the records.
        records = data[1] if len(data) > 1 else []
        if not records:
            print("Warning: Population data is empty.")
        else:
            print(f"Retrieved population data for {len(records)} records.")
        pop_list = [
            {"country_code": row.get("countryiso3code"),
             "population": row.get("value")}
            for row in records if row.get("countryiso3code") is not None
        ]
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
    check_cmd = ["doppler", "run", "--",
                 "rclone", "check", local_path, r2_bucket]
    result = subprocess.run(check_cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"{os.path.basename(local_path)} has not changed on {r2_bucket}.")
        return False
    else:
        print(
            f"New data detected for {os.path.basename(local_path)}. Syncing to {r2_bucket}...")
        copy_cmd = ["doppler", "run", "--", "rclone",
                    "copy", local_path, r2_bucket, "--checksum"]
        subprocess.run(copy_cmd, check=True)
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Fetch population data from the World Bank API, store as Parquet, and sync to Cloudflare R2 using Doppler."
    )
    parser.parse_args()

    temp_dir = tempfile.mkdtemp()
    print(f"Temporary directory: {temp_dir}")

    try:
        pop_list = fetch_population_data()
        if pop_list:
            parquet_file = os.path.join(temp_dir, "population_data.parquet")
            save_population_parquet(pop_list, parquet_file)
            sync_to_r2(parquet_file, R2_BUCKET_POP)
        else:
            print("No population data fetched. Nothing to sync.")
    finally:
        print("Cleaning up temporary files...")
        shutil.rmtree(temp_dir)
        print("Done.")


if __name__ == "__main__":
    main()
