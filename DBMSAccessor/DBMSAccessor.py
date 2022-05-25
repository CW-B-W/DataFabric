def hello():
    return 'hello'

def preview_mysql(db, table, username, password, limit=5):
    query = f'SELECT * FROM {table} LIMIT {limit}'
    return