#!/usr/bin/env python3
# filepath: /workspaces/dbt-duckdb/export_parquet.py
import os
import tempfile
import duckdb
import json
import argparse


def export_mart_tables_parquet(duckdb_filename: str, output_dir: str) -> list:
    duck_conn = duckdb.connect(duckdb_filename, read_only=True)
    tables = duck_conn.execute("SHOW TABLES;").fetchall()
    mart_prefixes = ("fct_", "dim_")
    exported_files = []
    print("Exporting marts tables from DuckDB to local Parquet files:")
    for row in tables:
        table_name = row[0]
        if table_name.startswith(mart_prefixes):
            parquet_filename = os.path.join(
                output_dir, f"{table_name}.parquet")
            cols_info = duck_conn.execute(f"DESCRIBE {table_name}").fetchall()
            if not cols_info:
                print(f"Warning: Could not retrieve columns for {table_name}")
                continue
            order_by = ", ".join([col[0] for col in cols_info])
            select_exprs = [col[0] for col in cols_info]
            select_list = ", ".join(select_exprs)
            copy_query = (
                f"COPY (SELECT {select_list} FROM {table_name} ORDER BY {order_by}) "
                f"TO '{parquet_filename}' (FORMAT 'parquet')"
            )
            duck_conn.execute(copy_query)
            print(f"Exported table {table_name} to {parquet_filename}")
            exported_files.append(parquet_filename)
        else:
            print(f"Skipping non-mart table: {table_name}")
    duck_conn.close()
    return exported_files


def main():
    parser = argparse.ArgumentParser(
        description="Export DuckDB mart tables to Parquet files."
    )
    parser.add_argument(
        "json_output", help="File to write JSON list of exported files"
    )
    parser.add_argument(
        "--output-dir", help="Directory to export Parquet files"
    )
    args = parser.parse_args()

    duckdb_filename = "wdi.duckdb"
    if args.output_dir:
        output_dir = args.output_dir
        os.makedirs(output_dir, exist_ok=True)
        exported_files = export_mart_tables_parquet(
            duckdb_filename, output_dir)
        print(f"Using specified output directory: {output_dir}")
    else:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = temp_dir
            exported_files = export_mart_tables_parquet(
                duckdb_filename, output_dir)
            print(
                f"Using temporary directory for Parquet export: {output_dir}")

    print("Exported files:")
    for f in exported_files:
        print(f)

    with open(args.json_output, "w", encoding="utf-8") as json_file:
        json.dump(exported_files, json_file)
    print(f"JSON output written to {args.json_output}")


if __name__ == '__main__':
    main()
