import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
def preview_table_cassandra(username, password, ip, port, db, table, limit):
    auth_provider = PlainTextAuthProvider(username, password)
    cluster = Cluster([ip],port=port , auth_provider=auth_provider)
    session = cluster.connect()
    rows = session.execute(f"SELECT * FROM {db}.{table} LIMIT {limit}")
    df = pd.DataFrame(rows)
    js = df.to_json(orient = 'records')
    return js

def query_table_cassandra(username, password, ip, port, db, table, columns, start_time, end_time, time_column):
    sql       = generate_query(db, table, columns, start_time, end_time, time_column)
    auth_provider = PlainTextAuthProvider(username, password)
    cluster = Cluster([ip], port , auth_provider=auth_provider)
    session = cluster.connect()
    rows = session.execute(sql)
    df = pd.DataFrame(rows)
    return df

def generate_query(db, table, columns, start_time, end_time, time_column):
    req_cols = ','.join(columns)
    optional_fields = ''
    if start_time is not None and end_time is not None and time_column is not None:
        optional_fields = f"WHERE {time_column} BETWEEN {start_time} and {end_time}"
    sql = f'SELECT {req_cols} FROM {db}.{table} {optional_fields}' 
    return sql
