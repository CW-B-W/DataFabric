import pymysql
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
    print(result)
    return result