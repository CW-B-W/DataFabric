import pymysql
from pymysqlpool.pool import Pool
class MySQLDB:
    def __init__(self, db: str):
        self.__mysql_pool    = None
        self.__mysql_conn_db = db
        self.__mysql_conn    = {
            "host": "datafabric_mysql_1",
            "port": 3306,
            "user": "root",
            "password": "my-secret-pw",
            "charset": "utf8"
        }

    def __connect_mysql(self, retry=3) -> bool:
        if self.__mysql_pool is not None:
            return True
        while retry >= 0:
            try:
                # First check if database exists
                mysql_db = pymysql.connect(**self.__mysql_conn)
                cursor   = mysql_db.cursor(pymysql.cursors.DictCursor)
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.__mysql_conn_db};")
                mysql_db.commit()
                cursor.close()
                mysql_db.close()

                self.__mysql_conn['db'] = self.__mysql_conn_db
                self.__mysql_pool = Pool(**self.__mysql_conn)
                self.__mysql_pool.init()
                return True
            except Exception as ex:
                if retry == 0:
                    raise ex
                retry -= 1
        return False

    def query(self, query: str) -> bool:
        if self.__connect_mysql() == False:
            raise "Failed to connect MySQL."
        conn   = self.__mysql_pool.get_conn()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query)
        conn.commit()
        result = cursor.fetchall()
        cursor.close()
        self.__mysql_pool.release(conn)
        return result
