from InternalDB.MySQLDB import MySQLDB

mysqldb = MySQLDB('datafabric')

def recommend(user_id: int) -> list:
    result = mysqldb.query(
        f"SELECT * FROM CatalogManager "
        f"ORDER BY RAND() LIMIT 50;"
    )
    return result