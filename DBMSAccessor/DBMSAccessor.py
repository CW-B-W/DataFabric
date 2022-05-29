from sqlalchemy import create_engine

def hello():
    return 'hello'



def preview_table(
    username: str, password: str, 
    ip: str, port: str, dbms: str, 
    db: str, table: str, limit: int = 5
) -> dict:
    dbms = dbms.lower()
    # Use reflection to call function (Better extensibility for adding new DBMS)
    # The target function name should be like such as 'preview_mysql(...)'
    return globals()[f'preview_{dbms}'](username, password, ip, port, db, table, limit)

import pymysql
import sys
def preview_mysql(username, password, ip, port, db, table, limit):
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