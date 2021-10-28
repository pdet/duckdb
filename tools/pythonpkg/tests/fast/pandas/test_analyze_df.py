import pandas as pd
import duckdb
import datetime

class TestAnalyzeDF(object):

    def test_analyze_single_row(self, duckdb_cursor):
        return                  
        df_in = pd.DataFrame([[datetime.date(1992, 7, 30)]])
        new_df = duckdb.analyze_df(df_in)
        print(new_df)
        assert 0 == 1
