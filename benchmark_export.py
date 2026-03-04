import duckdb
import time
import os
import shutil
import export_parquet
import contextlib
import io

def setup_db(db_name, num_tables):
    conn = duckdb.connect(db_name)
    for i in range(num_tables):
        conn.execute(f"CREATE TABLE fct_table_{i} (id INTEGER, name VARCHAR, value DOUBLE)")
        conn.execute(f"INSERT INTO fct_table_{i} VALUES (1, 'test', 1.0), (2, 'test2', 2.0)")
    conn.close()

def benchmark_original(db_name, output_dir):
    start = time.time()
    # suppress stdout
    with contextlib.redirect_stdout(io.StringIO()):
        export_parquet.export_mart_tables_parquet(db_name, output_dir)
    return time.time() - start

def main():
    db_name = "test_benchmark.duckdb"
    output_dir = "test_output"

    # We will test on a larger number of tables to amplify the N+1 issue
    num_tables = 500

    if os.path.exists(db_name):
        os.remove(db_name)
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    setup_db(db_name, num_tables)

    # Run original
    duration = benchmark_original(db_name, output_dir)
    print(f"Original duration ({num_tables} tables): {duration:.4f} seconds")

if __name__ == "__main__":
    main()
