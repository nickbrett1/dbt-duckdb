#!/usr/bin/env python3
# filepath: /workspaces/dbt-duckdb/sync_remote_parquet.py
import os
import sys
import subprocess
import tempfile
import json
import pandas as pd
import argparse


def load_and_sort_parquet_file(filename: str) -> pd.DataFrame:
    df = pd.read_parquet(filename, engine="pyarrow")
    sort_cols = df.columns.tolist()
    return df.sort_values(by=sort_cols).reset_index(drop=True)


def get_changed_files(local_files: list, remote_base_path: str, temp_download_dir: str) -> list:
    if not local_files:
        return []

    # Identify common directory for local files
    # We assume all exported files are in the same directory (as per export_parquet.py)
    common_dir = os.path.dirname(local_files[0])
    if not common_dir:
        common_dir = "."

    # Create a list of files relative to common_dir
    relative_files = []
    for f in local_files:
        rel = os.path.relpath(f, common_dir)
        relative_files.append(rel)

    changed = []
    temp_list_path = None
    try:
        # Create a temporary file list for rclone --files-from
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tf:
            for f in relative_files:
                # Escape backslashes first
                f_esc = f.replace("\\", "\\\\")
                # Escape filenames starting with # or ;
                if f_esc.startswith("#") or f_esc.startswith(";"):
                    f_esc = "\\" + f_esc
                tf.write(f_esc + "\n")
            temp_list_path = tf.name

        print(f"Checking for changes between {common_dir} and {remote_base_path}...")
        # Run rclone check
        cmd = [
            "rclone", "check", common_dir, remote_base_path,
            "--files-from", temp_list_path,
            "--combined", "-"
        ]

        # rclone check returns exit code 1 if differences found, so check=False
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)

        files_to_download = []

        for line in result.stdout.splitlines():
            if not line:
                continue
            # Output format: status path (separated by space)
            # But path can contain spaces. rclone check --combined output is fixed width?
            # No, it's `X path`.
            parts = line.split(" ", 1)
            if len(parts) < 2:
                continue
            status = parts[0]
            filename = parts[1].strip()

            if status == "=":
                continue
            elif status == "*" or status == "!":
                # Changed or Error: need to verify with fuzzy comparison
                files_to_download.append(filename)
            elif status == "+":
                # Missing on remote (remote is dest). So it's a new file.
                print(f"New file detected: {filename}")
                changed.append(os.path.join(common_dir, filename))
            # '-' means missing on local, which shouldn't happen as we limit checks to local files

        if files_to_download:
            print(f"Downloading {len(files_to_download)} potentially changed files for inspection...")
            # Create download list
            dl_list_path = None
            try:
                with tempfile.NamedTemporaryFile(mode="w", delete=False) as dl_tf:
                    for f in files_to_download:
                        f_esc = f.replace("\\", "\\\\")
                        if f_esc.startswith("#") or f_esc.startswith(";"):
                            f_esc = "\\" + f_esc
                        dl_tf.write(f_esc + "\n")
                    dl_list_path = dl_tf.name

                subprocess.run(
                    ["rclone", "copy", remote_base_path, temp_download_dir, "--files-from", dl_list_path],
                    check=True
                )
            finally:
                if dl_list_path and os.path.exists(dl_list_path):
                    os.remove(dl_list_path)

            # Compare downloaded files with local files
            for filename in files_to_download:
                local_file = os.path.join(common_dir, filename)
                remote_file = os.path.join(temp_download_dir, filename)

                try:
                    local_df = load_and_sort_parquet_file(local_file)
                    remote_df = load_and_sort_parquet_file(remote_file)
                    pd.testing.assert_frame_equal(
                        local_df, remote_df,
                        rtol=1e-5, atol=5e-4, check_exact=False
                    )
                except AssertionError:
                    print(f"Data mismatch in file: {filename}")
                    changed.append(local_file)
                except Exception as e:
                    print(f"Failed to compare file {filename}: {e}")
                    # If comparison fails (e.g. load error), assume changed to be safe?
                    # Or if remote file is corrupt/missing (shouldn't be missing if downloaded).
                    changed.append(local_file)

    finally:
        if temp_list_path and os.path.exists(temp_list_path):
            os.remove(temp_list_path)

    return changed


def sync_local_to_remote(files: list, remote_path: str = "r2:wdi") -> None:
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
        print(f"Syncing {len(filenames)} files from {dirname} to {remote_path}...")
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
                ["rclone", "copy", dirname, remote_path, "--files-from", temp_list_path],
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
    parser.add_argument(
        "--remote-path", default="r2:wdi",
        help="Remote path to sync with (default: r2:wdi)."
    )
    args = parser.parse_args()

    with open(args.input_json, "r", encoding="utf-8") as f:
        exported_files = json.load(f)

    with tempfile.TemporaryDirectory() as remote_dir:
        # We pass remote_dir as the temp dir for downloads
        changed_files = get_changed_files(exported_files, args.remote_path, remote_dir)
        if changed_files:
            if args.no_updates:
                print("No-updates mode enabled: skipping syncing to remote.")
            else:
                sync_local_to_remote(changed_files, args.remote_path)
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
