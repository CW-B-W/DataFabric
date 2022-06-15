import pandas as pd
from pandasql import sqldf
from sqlalchemy import create_engine
import cx_Oracle
from sqlalchemy import *
def preview_table_oracle(username, password, ip, port, db, table, limit):
    db_engine = create_engine(r"oracle+cx_oracle://%s:%s@%s:%s" % (username, password, ip, port))
    df = pd.read_sql("SELECT column_name FROM all_tab_cols WHERE owner = '%s' and table_name = '%s'" % (db_name, table_name), con=db_engine);
    return sorted(df1.iloc[:,0].tolist())

def query_table_oracle(username, password, ip, port, db, table, columns, start_time, end_time, time_column):
    db_engine = create_engine(r"oracle+cx_oracle://%s:%s@%s:%s" % (username, password, ip, port))
    sql = generate_query(table, columns, start_time, end_time, time_column)
    df = pd.read_sql("SELECT column_name FROM all_tab_cols WHERE owner = '%s' and table_name = '%s'" % (db_name, table_name), con=db_engine);
    return sorted(df.iloc[:,0].tolist())

def generate_query(table, columns, start_time, end_time, time_column):
    req_cols = ','.join(columns)
    optional_fields = ''
    if start_time is not None and end_time is not None and time_column is not None:
        optional_fields = f"WHERE {time_column} BETWEEN TO_DATE ('{starttime}','YYYY-MM-DD\"T\"HH24:MI:SS') AND TO_DATE('{endtime}','YYYY-MM-DD\"T\"HH24:MI:SS')"
    sql = f'SELECT {req_cols} FROM {table} {optional_fields}' 
    return sql