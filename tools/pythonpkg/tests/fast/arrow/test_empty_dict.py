import duckdb
import pyarrow as pa

def test_pyarrow_empty_dict():
    con = duckdb.connect()
    con.execute("CREATE TYPE myEnum AS ENUM('first', 'second')")
    con.execute("CREATE TABLE example(e myEnum)")
    arrow_df = con.execute("SELECT * FROM example").arrow()
