import duckdb
import time

conn = duckdb.connect()

# Setup
for i in range(1000):
    conn.execute(f"CREATE TABLE fct_test_{i} (id INT, name VARCHAR, v1 DOUBLE, v2 DOUBLE, v3 DOUBLE, v4 DOUBLE)")

# Approach 1: N+1
start = time.time()
for i in range(1000):
    cols = conn.execute(f"DESCRIBE fct_test_{i}").fetchall()
n_plus_1_time = time.time() - start

# Approach 2: single query
start = time.time()
all_cols = conn.execute("SELECT table_name, column_name FROM information_schema.columns ORDER BY table_name, ordinal_position").fetchall()
table_cols = {}
for t, c in all_cols:
    table_cols.setdefault(t, []).append(c)
for i in range(1000):
    cols = table_cols.get(f"fct_test_{i}", [])
single_query_time = time.time() - start

print(f"N+1 time: {n_plus_1_time:.4f}s")
print(f"Single query time: {single_query_time:.4f}s")

conn.close()
