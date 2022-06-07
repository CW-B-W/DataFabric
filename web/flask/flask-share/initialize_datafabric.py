import random
import pymysql
import pymongo
import argparse
from tqdm import tqdm
import time

def create_mysql_database(db, replace=True):
    db_settings = {
        "host": "datafabric-mysql",
        "port": 3306,
        "user": "root",
        "password": "my-secret-pw",
        "charset": "utf8"
    }
    try:
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            if replace:
                command = f"""
                    DROP DATABASE IF EXISTS {db};
                """
                cursor.execute(command)
            
            command = f"""
                CREATE DATABASE IF NOT EXISTS {db};
            """
            cursor.execute(command)
        conn.commit()
    except Exception as ex:
        print(ex)

def create_mysql_table(db, table, create_sql, replace=True):
    db_settings = {
        "host": "datafabric-mysql",
        "port": 3306,
        "user": "root",
        "password": "my-secret-pw",
        "db": db,
        "charset": "utf8"
    }
    try:
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            if replace:
                command = f"""
                    DROP TABLE IF EXISTS {table};
                """
                cursor.execute(command)
            
            command = create_sql
            cursor.execute(command)
        conn.commit()
    except Exception as ex:
        print(ex)


# def create_mongo_database(db, replace=True):
#     myclient = pymongo.MongoClient('mongodb://%s:%s@datafabric-mongo' % ('root', 'example'))

#     if replace:
#         myclient.drop_database(db)
#     mydb = myclient[db]

# def create_mongo_collection(db, collection, replace=True):
#     myclient = pymongo.MongoClient('mongodb://%s:%s@datafabric-mongo' % ('root', 'example'))
#     myclient.drop_database(db)
#     mydb = myclient[db]
#     if replace:
#         if collection in mydb.list_collection_names():
#             mydb[collection].drop()
#     mycol = mydb[collection]

def main():
    print("[Creating MySQL databases]")
    
    print("Creating database 'datafabric'")
    create_mysql_database('datafabric')
    print("Creating table 'datafabric.TableInfo'")
    create_mysql_table('datafabric', 'TableInfo', 
            f"""
                CREATE TABLE TableInfo (
                    ID              INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    Connection      VARCHAR(100),
                    DBMS            VARCHAR(20),
                    DB              VARCHAR(100),
                    TableName       VARCHAR(100),
                    Columns         TEXT
                );
            """)
    print("Creating table 'datafabric.CatalogManager'")
    create_mysql_table('datafabric', 'CatalogManager', 
            f"""
                CREATE TABLE CatalogManager (
                    ID              INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    CatalogName     VARCHAR(50),
                    TableMembers    TEXT,
                    TableIds        TEXT,
                    ColumnMembers   TEXT,
                    Description     VARCHAR(300),
                    ViewCount       INT,
                    UsedCount       INT,
                    PopularTop3     VARCHAR(300),
                    CatalogUpvote   INT,
                    CatalogDownvote INT
                );
            """)

    print("Creating database 'datafabric_transaction'")
    create_mysql_database('datafabric_transaction')
    print("Creating table 'datafabric_transaction.TransactionLogs'")
    create_mysql_table('datafabric_transaction', 'TransactionLogs', 
            f"""
                CREATE TABLE TransactionLogs (
                    ID         BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    Action     VARCHAR(100),
                    Args       TEXT,
                    User       VARCHAR(100),
                    Status     VARCHAR(100),
                    Result     TEXT,
                    Timestamp  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    Datetime   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                );
            """)
    
    
    
if __name__ == '__main__':
    main()
