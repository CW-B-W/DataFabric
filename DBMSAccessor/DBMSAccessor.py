from sqlalchemy import create_engine

def hello():
    return 'hello'

def get_table_info(tableid):
    return {
        'ip'    : '192.168.103.52',
        'port'  : '3306',
        'dbms'  : 'MySQL',
        'db'    : 'ExamScore',
        'table' : 'Final2020'
    }

def preview_mysql(tableid, username, password, limit=5):
    table_info = get_table_info(tableid)
    ip_port    = f"{table_info['ip']}:{table_info['port']}"
    dbms       = f"{table_info['dbms']}"
    db         = f"{table_info['db']}"
    table      = f"{table_info['table']}"
    
    db_engine = create_engine("mysql+pymysql://%s:%s@%s/" % (username, password, ip_port))
    query     = f'SELECT * FROM {table} LIMIT {limit}'
    return {}