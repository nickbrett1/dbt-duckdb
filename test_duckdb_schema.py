import duckdb

conn = duckdb.connect()
conn.execute("CREATE TABLE fct_test (id INT, name VARCHAR)")
print("DESCRIBE:")
print(conn.execute("DESCRIBE fct_test").fetchall())
print("information_schema:")
print(conn.execute("SELECT table_name, column_name FROM information_schema.columns ORDER BY table_name, ordinal_position").fetchall())
conn.close()
