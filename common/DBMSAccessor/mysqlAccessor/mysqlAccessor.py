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

def query_table_mysql(username, password, ip, port, db, table, columns, start_time, end_time, time_column):
    def generate_query(table, columns, start_time, end_time, time_column):
        req_cols = ','.join(columns)
        optional_fields = ''
        if start_time is not None and end_time is not None and time_column is not None:
            optional_fields = f"WHERE {time_column} BETWEEN '{start_time}' and '{end_time}'"
        sql = f'SELECT {req_cols} FROM {table} {optional_fields};' 
        return sql
    sql       = generate_query(table, columns, start_time, end_time, time_column)
    db_url    = 'mysql+pymysql://%s:%s@%s:%s/%s' % (username, password, ip, port, db)
    db_engine = create_engine(db_url)
    df = pd.read_sql(sql, con=db_engine)
    return df

def list_dbs_mysql(username, password, ip, port):
    engine = create_engine("mysql+pymysql://%s:%s@%s:%s/" % (username, password, ip, port))
    df = pd.read_sql("SHOW DATABASES;", con=engine)
    return sorted(df.iloc[:,0].tolist())

def list_tables_mysql(username, password, ip, port, db):
    engine = create_engine("mysql+pymysql://%s:%s@%s:%s/%s" % (username, password, ip, port, db))
    df = pd.read_sql("SHOW TABLES", con=engine)
    return sorted(df.iloc[:,0].tolist())

def list_columns_mysql(username, password, ip, port, db, table):
    engine = create_engine("mysql+pymysql://%s:%s@%s:%s/%s" % (username, password, ip, port, db))
    df = pd.read_sql("SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='%s' AND `TABLE_NAME`='%s'" % (db, table), con=engine)
    return sorted(df['COLUMN_NAME'].tolist())