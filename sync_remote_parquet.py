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
    # Group files by directory to batch rclone calls
    files_by_dir = {}
    for local_file in files:
        dirname = os.path.dirname(local_file)
        # If path is just a filename, dirname is empty string, convert to current dir
        if not dirname:
            dirname = "."
        if dirname not in files_by_dir:
            files_by_dir[dirname] = []
        files_by_dir[dirname].append(os.path.basename(local_file))

    for dirname, filenames in files_by_dir.items():
        print(f"Syncing {len(filenames)} files from {dirname} to R2...")
        temp_list_path = None
        try:
            # Create a temporary file list for --files-from
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as tf:
                for filename in filenames:
                    # Escape backslashes first, as they are the escape character
                    filename_esc = filename.replace("\\", "\\\\")
                    # Escape filenames starting with # or ; as rclone treats them as comments
                    if filename_esc.startswith("#") or filename_esc.startswith(";"):
                        filename_esc = "\\" + filename_esc
                    tf.write(filename_esc + "\n")
                temp_list_path = tf.name

            subprocess.run(
                ["rclone", "copy", dirname, "r2:wdi", "--files-from", temp_list_path],
                check=True
            )
        finally:
            if temp_list_path and os.path.exists(temp_list_path):
                os.remove(temp_list_path)


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
