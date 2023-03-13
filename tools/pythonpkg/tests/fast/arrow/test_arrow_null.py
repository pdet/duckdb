import duckdb
import pytest
import pandas as pd

pa = pytest.importorskip('pyarrow')

def test_pyarrow_null():
    con = duckdb.connect()

    df = pd.DataFrame(
            {
                "notes": [None] * 9000000
            }
        )

    arrow_tbl = pa.Table.from_pandas(df)

    res = con.execute("SELECT count(*) FROM arrow_tbl WHERE notes is NULL").fetchall()
    assert res[0][0] == 9000000