#!/usr/bin/env python3
import os
import subprocess
import tempfile
import zipfile
import argparse
import pandas as pd
import shutil

# Constants for downloading WDI data
SOURCE = "https://databank.worldbank.org/data/download/WDI_CSV.zip"

# Cloudflare R2 bucket destination for WDI data (raw ZIP and Parquet files)
R2_BUCKET_WDI = "r2:wdi"


def download_file(url, dest_dir):
    print(f"Downloading WDI data from {url} using rclone...")
    result = subprocess.run(
        ["rclone", "copyurl", url, dest_dir, "--auto-filename", "--print-filename"],
        capture_output=True,
        text=True,
        check=True
    )
    filename = result.stdout.strip()
    print(f"Downloaded file: {filename}")
    return os.path.join(dest_dir, filename)


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


def unzip_file(zip_path, dest_dir):
    print("Unzipping file...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)
    print("Unzipped files:")
    for file in os.listdir(dest_dir):
        print(f" - {file}")


def convert_csv_to_parquet(csv_path, parquet_path):
    print(f"Converting {csv_path} to Parquet file {parquet_path} ...")
    try:
        df = pd.read_csv(csv_path)
        df.to_parquet(parquet_path, engine="pyarrow", index=False)
        print(f"Converted {csv_path} to {parquet_path}.")
    except Exception as e:
        print(f"Error converting {csv_path} to Parquet: {e}")


def process_wdi_data(raw_zip, work_dir):
    unzip_file(raw_zip, work_dir)
    for file in os.listdir(work_dir):
        if file.endswith(".csv"):
            csv_path = os.path.join(work_dir, file)
            parquet_filename = os.path.splitext(file)[0] + ".parquet"
            parquet_path = os.path.join(work_dir, parquet_filename)
            convert_csv_to_parquet(csv_path, parquet_path)


def main():
    parser = argparse.ArgumentParser(
        description="Download WDI data, convert CSV files to Parquet, and sync to Cloudflare R2."
    )
    args = parser.parse_args()

    temp_dir = tempfile.mkdtemp()
    print(f"Temporary directory: {temp_dir}")
    try:
        raw_zip = download_file(SOURCE, temp_dir)
        sync_to_r2(raw_zip, R2_BUCKET_WDI)
        process_wdi_data(raw_zip, temp_dir)
        for file in os.listdir(temp_dir):
            if file.endswith(".parquet"):
                local_file = os.path.join(temp_dir, file)
                sync_to_r2(local_file, R2_BUCKET_WDI)
    finally:
        print("Cleaning up temporary files...")
        shutil.rmtree(temp_dir)
        print("Done.")


if __name__ == "__main__":
    main()
