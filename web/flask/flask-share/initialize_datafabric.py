import sys
sys.path.append('/')
import random
import pymysql
import pymongo
import argparse
from tqdm import tqdm
import time

from DatafabricManager import UserManager

def mysql_connect(db_settings):
    while True:
        try:
            # First check if database exists
            conn = pymysql.connect(**db_settings)
            return conn
        except Exception as ex:
            print(f"Waiting for MySQL to startup. Retry after 3s.")
            time.sleep(3)

def create_mysql_database(db, replace=True):
    db_settings = {
        "host": "datafabric-mysql",
        "port": 3306,
        "user": "root",
        "password": "my-secret-pw",
        "charset": "utf8"
    }
    try:
        conn = mysql_connect(db_settings)
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
        conn = mysql_connect(db_settings)
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


def create_mongo_database(db, replace=True):
    myclient = pymongo.MongoClient('mongodb://%s:%s@datafabric-mongo' % ('root', 'example'))

    if replace:
        myclient.drop_database(db)
    mydb = myclient[db]

def create_mongo_collection(db, collection, replace=True):
    myclient = pymongo.MongoClient('mongodb://%s:%s@datafabric-mongo' % ('root', 'example'))
    mydb = myclient[db]
    if replace:
        if collection in mydb.list_collection_names():
            mydb[collection].drop()
    mycol = mydb[collection]

def create_user_admin():
    UserManager.add_user({
        'id'       : 0,
        'username' : f'admin',
        'password' : f'admin',
        'db_account' : {
            'datafabric-mysql:3306': {
                'ip': 'datafabric-mysql',
                'port': '3306',
                'username': 'root',
                'password': 'my-secret-pw'
            },
            'datafabric-mongo:27017': {
                'ip': 'datafabric-mongo',
                'port': '27017',
                'username': 'root',
                'password': 'example'
            }
        },
        'data_permission': {
            'catalog_id' : {
                '*': True
            },
            'table_id' : {
                '*': True
            }
        },
        'action_permission': {
            '*': True
        }
    })

def main():
    print("[Creating MySQL databases]")
    
    print("Creating MySQL database 'datafabric'")
    create_mysql_database('datafabric')
    print("Creating MySQL table 'datafabric.TableInfo'")
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
    print("Creating MySQL table 'datafabric.CatalogManager'")
    create_mysql_table('datafabric', 'CatalogManager', 
            f"""
                CREATE TABLE CatalogManager (
                    ID              INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    CatalogName     VARCHAR(50),
                    TableIds        TEXT,
                    TableMembers    TEXT,
                    Keywords        TEXT,
                    Description     VARCHAR(300),
                    ViewCount       INT,
                    UsedCount       INT,
                    CatalogUpvote   INT,
                    CatalogDownvote INT
                );
            """)

    print("Creating MySQL database 'datafabric_transaction'")
    create_mysql_database('datafabric_transaction')
    print("Creating MySQL table 'datafabric_transaction.TransactionLogs'")
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

    print("[Creating MongoDB databases]")
    print("Creating database 'datafabric'")
    create_mongo_database('datafabric')

    print("Creating collection 'datafabric.user_info'")
    create_mongo_collection('datafabric', 'user_info')
    print("Creating collection 'datafabric.ratings'")
    create_mongo_collection('datafabric', 'ratings')

    print("Creating user admin")
    create_user_admin()
    
    
    
if __name__ == '__main__':
    main()
