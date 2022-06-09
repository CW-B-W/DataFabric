import pymysql
import pandas as pd
from sqlalchemy import create_engine

def preview_table_mysql(username, password, ip, port, db, table, limit):
    mysql_settings = {
        "host": ip,
        "port": int(port),
        "user": username,
        "password": password,
        "db": db,
        "charset": "utf8"
    }
    mysql_db = pymysql.connect(**mysql_settings)
    cursor = mysql_db.cursor(pymysql.cursors.DictCursor)
    cursor.execute(f"SELECT * FROM {table} LIMIT {limit};")
    result = cursor.fetchall()
    return result

def query_table_mysql(username, password, ip, port, db, table, key_names, start_time, end_time, time_column):
    sql       = generate_query(table, key_names, start_time, end_time, time_column)
    db_url    = 'mysql+pymysql://%s:%s@%s:%s/%s' % (username, password, ip, port, db)
    db_engine = create_engine(db_url)
    df = pd.read_sql(sql, con=db_engine)
    return df

def generate_query(table, key_names, start_time, end_time, time_column):
    req_cols = ','.join(key_names)
    optional_fields = ''
    if start_time is not None and end_time is not None and time_column is not None:
        optional_fields = f"WHERE {time_column} BETWEEN '{start_time}' and '{end_time}'"
    sql = f'SELECT {req_cols} FROM {table} {optional_fields};' 
    return sql
