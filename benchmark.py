import duckdb
import time
import os

con = duckdb.connect("test.duckdb")
con.execute("CREATE SCHEMA IF NOT EXISTS public")

# create dummy parquet files
for i in range(100):
    con.execute(f"COPY (SELECT * FROM range(1000)) TO 'test_{i}.parquet' (FORMAT PARQUET)")

# Measure current N+1 approach
con.execute("DROP SCHEMA public CASCADE")
con.execute("CREATE SCHEMA public")
start = time.time()
for i in range(100):
    query = f"CREATE TABLE IF NOT EXISTS public.t_{i} AS SELECT * FROM read_parquet('test_{i}.parquet');"
    con.execute(query)
    count = con.execute(f"SELECT COUNT(*) FROM public.t_{i}").fetchone()[0]
end = time.time()
n_plus_1_time = end - start

# Measure optimized approach
con.execute("DROP SCHEMA public CASCADE")
con.execute("CREATE SCHEMA public")
start = time.time()
for i in range(100):
    query = f"CREATE TABLE IF NOT EXISTS public.t_{i} AS SELECT * FROM read_parquet('test_{i}.parquet');"
    res = con.execute(query).fetchone()
    if res is not None:
        count = res[0]
    else:
        count = con.execute(f"SELECT COUNT(*) FROM public.t_{i}").fetchone()[0]
end = time.time()
optimized_time = end - start

print(f"N+1 time: {n_plus_1_time:.4f}s")
print(f"Optimized time: {optimized_time:.4f}s")
print(f"Improvement: {(n_plus_1_time - optimized_time) / n_plus_1_time * 100:.2f}%")

# cleanup
for i in range(100):
    os.remove(f"test_{i}.parquet")
os.remove("test.duckdb")
