import pymysql
class MySQLDB:
    def __init__(self, db: str):
        self.__mysql_db      = None
        self.__mysql_conn_db = db
        self.__mysql_conn    = {
            "host": "datafabric-mysql",
            "port": 3306,
            "user": "root",
            "password": "my-secret-pw",
            "charset": "utf8"
        }

    def __connect_mysql(self, retry=3) -> bool:
        if self.__mysql_db is not None:
            return True
        while retry >= 0:
            try:
                # First check if database exists
                self.__mysql_db = pymysql.connect(**self.__mysql_conn)
                cursor = self.__mysql_db.cursor(pymysql.cursors.DictCursor)
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.__mysql_conn_db};")
                self.__mysql_db.commit()
                cursor.close()
                self.__mysql_db.close()

                self.__mysql_conn['db'] = self.__mysql_conn_db
                self.__mysql_db = pymysql.connect(**self.__mysql_conn)
                return True
            except Exception as ex:
                if retry == 0:
                    raise ex
                retry -= 1
        return False

    def query(self, query: str) -> bool:
        if self.__connect_mysql() == False:
            raise "Failed to connect MySQL."
        cursor = self.__mysql_db.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query)
        self.__mysql_db.commit()
        result = cursor.fetchall()
        cursor.close()
        return result
