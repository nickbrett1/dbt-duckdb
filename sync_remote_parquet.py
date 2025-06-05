#!/usr/bin/env python3
# filepath: /workspaces/dbt-duckdb/sync_remote_parquet.py
import os
import sys
import subprocess
import tempfile
import json
import pandas as pd
import argparse


def download_remote_parquet(remote_dir: str) -> None:
    os.makedirs(remote_dir, exist_ok=True)
    subprocess.run(
        [
            "rclone", "copy", "r2:wdi", remote_dir,
            "--include", "fct_*.parquet",
            "--include", "dim_*.parquet",
            "--include", "agg_*.parquet"
        ],
        check=True
    )
    print(f"Downloaded remote marts Parquet files to {remote_dir}")


def load_and_sort_parquet_file(filename: str) -> pd.DataFrame:
    df = pd.read_parquet(filename, engine="pyarrow")
    sort_cols = df.columns.tolist()
    return df.sort_values(by=sort_cols).reset_index(drop=True)


def get_changed_files(local_files: list, remote_dir: str) -> list:
    changed = []
    for local_file in local_files:
        filename = os.path.basename(local_file)
        remote_file = os.path.join(remote_dir, filename)
        if not os.path.exists(remote_file):
            print(f"Remote file missing: {filename}")
            changed.append(local_file)
            continue
        try:
            local_df = load_and_sort_parquet_file(local_file)
            remote_df = load_and_sort_parquet_file(remote_file)
        except Exception as e:
            print(f"Failed to load file {filename}: {e}")
            changed.append(local_file)
            continue
        try:
            pd.testing.assert_frame_equal(
                local_df, remote_df,
                rtol=1e-5, atol=5e-4, check_exact=False
            )
        except AssertionError:
            print(f"Data mismatch in file: {filename}")
            changed.append(local_file)
    return changed


def sync_local_to_remote(files: list) -> None:
    for local_file in files:
        filename = os.path.basename(local_file)
        print(f"Syncing {local_file} to R2...")
        subprocess.run(
            ["rclone", "copy", local_file, "r2:wdi", "--include", filename],
            check=True
        )


def main():
    parser = argparse.ArgumentParser(
        description="Sync changed Parquet files with remote and output changed table names as JSON."
    )
    parser.add_argument(
        "input_json", help="JSON file containing list of local exported Parquet files"
    )
    parser.add_argument(
        "output_json", help="Output JSON file to write changed table names"
    )
    parser.add_argument(
        "--no-updates", action="store_true",
        help="Run in no-updates mode; perform all checks but do not sync updates to remote."
    )
    args = parser.parse_args()

    with open(args.input_json, "r", encoding="utf-8") as f:
        exported_files = json.load(f)

    with tempfile.TemporaryDirectory() as remote_dir:
        download_remote_parquet(remote_dir)
        changed_files = get_changed_files(exported_files, remote_dir)
        if changed_files:
            if args.no_updates:
                print("No-updates mode enabled: skipping syncing to remote.")
            else:
                sync_local_to_remote(changed_files)
            # Remove the .parquet extension from the filenames
            changed_tables = [os.path.splitext(os.path.basename(f))[
                0] for f in changed_files]
            print("Changed tables detected:")
            for table in changed_tables:
                print(table)
        else:
            changed_tables = []
            print("No changes detected in Parquet files.")

    with open(args.output_json, "w", encoding="utf-8") as out_file:
        json.dump(changed_tables, out_file)
    print(f"JSON of changed tables written to {args.output_json}")


if __name__ == "__main__":
    main()
