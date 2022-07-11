import sys
sys.path.append('/')
import random
import pymysql
import pymongo
import argparse
from tqdm import tqdm
import time

from DatafabricManager import TableManager
from DatafabricManager import CatalogManager
from DatafabricManager import UserManager

table_info_idx = 1
column_cnt     = 0
def new_random_table_info():
    global table_info_idx
    global column_cnt
    table_info = {
        'ID'         : table_info_idx,
        'Connection' : 'datafabric_mysql_1:3306',
        'DBMS'       : 'MySQL',
        'DB'         : 'testdata',
        'TableName'  : f'MyTable{table_info_idx}',
        'Columns'    : ''
    }
    dbms = table_info['DBMS']
    db   = table_info['DB']
    tbl  = table_info['TableName']
    table_info['Columns'] += f'id'
    for i in range(10):
        table_info['Columns'] += f',Column{column_cnt}'
        column_cnt += 1
    table_info_idx += 1
    return table_info

catalog_idx = 1
def new_random_catalog(table_info_list):
    global catalog_idx
    n_table = random.randint(2, 10)
    sampled = random.sample(table_info_list, n_table)

    catalog = {
        'ID'              : catalog_idx,
        'CatalogName'     : f'Catalog{catalog_idx}',
        'TableIds'        : [],
        'TableMembers'    : [],
        'Keywords'        : [],
        'Description'     : f'This is Catalog{catalog_idx}',
        'ViewCount'       : random.randint(0, 100),
        'UsedCount'       : 0,
        'CatalogUpvote'   : random.randint(0, 100),
        'CatalogDownvote' : random.randint(0, 100)
    }

    catalog['Keywords'].append(catalog['CatalogName'])
    for table_info in sampled:
        catalog['Keywords'].append(table_info['TableName'])
        catalog['TableMembers'].append(f"{table_info['DBMS']}@{table_info['DB']}@{table_info['TableName']}")
        catalog['TableIds'].append(f"{table_info['ID']}")
        for catalog_name in table_info['Columns'].split(','):
            catalog['Keywords'].append(catalog_name)

    catalog['TableMembers']  = ','.join(catalog['TableMembers'])
    catalog['TableIds']      = ','.join(catalog['TableIds'])
    catalog['Keywords']      = ','.join(catalog['Keywords'])

    catalog_idx += 1
    return catalog

def create_testdata_table_random(table_info):
    db_settings = {
        "host": "datafabric_mysql_1",
        "port": 3306,
        "user": "root",
        "password": "my-secret-pw",
        "db": "testdata",
        "charset": "utf8"
    }

    column_create_str = ','.join([f'{column_name} TEXT' for column_name in table_info['Columns'].split(',')])

    try:
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            command = f"""
                DROP TABLE IF EXISTS {table_info['TableName']};
            """
            cursor.execute(command)
            command = f"""
                CREATE TABLE {table_info['TableName']} (
                    {column_create_str}
                );
            """
            cursor.execute(command)
            for i in range(50):
                command = f"""
                    INSERT INTO {table_info['TableName']} VALUES (
                        "{random.randint(0, 100)}",
                        "{random.randint(0, 1000)}",
                        "{random.randint(0, 1000)}",
                        "{random.randint(0, 1000)}",
                        "{random.randint(0, 1000)}",
                        "{random.randint(0, 1000)}",
                        "{random.randint(0, 1000)}",
                        "{random.randint(0, 1000)}",
                        "{random.randint(0, 1000)}",
                        "{random.randint(0, 1000)}",
                        "{random.randint(0, 1000)}"
                    );
                """
                cursor.execute(command)
        conn.commit()
    except Exception as ex:
        print(ex)

def generate_catalogs(n_table, n_catalog):
    print("[Generating catalogs testdata]")

    db_settings = {
        "host": "datafabric_mysql_1",
        "port": 3306,
        "user": "root",
        "password": "my-secret-pw",
        "charset": "utf8"
    }
    try:
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            command = f"""
                DROP DATABASE IF EXISTS testdata;
            """
            cursor.execute(command)
            command = f"""
                CREATE DATABASE testdata;
            """
            cursor.execute(command)
    except Exception as e:
        print(e)
    
    table_info_list = []
    print("Generating tables")
    for i in tqdm(range(n_table)):
        table_info_list.append(new_random_table_info())
        create_testdata_table_random(table_info_list[-1])
    print("Finished!")

    print("Adding into TableInfo table")
    for i in tqdm(range(len(table_info_list))):
        table_info = table_info_list[i]
        TableManager.add_table_info(
            table_info['Connection'],
            table_info['DBMS'],
            table_info['DB'],
            table_info['TableName'],
            table_info['Columns']
        )
    print("Finished!")

    print("Generating catalogs")
    catalog_list = []
    for i in tqdm(range(n_catalog)):
        catalog_list.append(new_random_catalog(table_info_list))
    print("Finished!")

    print("Adding into CatalogManager table")
    for i in tqdm(range(len(catalog_list))):
        catalog = catalog_list[i]
        CatalogManager.add_catalog(
            catalog['CatalogName'],
            catalog['TableIds'],
            catalog['TableMembers'],
            catalog['Keywords'],
            catalog['Description'],
            catalog['ViewCount'],
            catalog['UsedCount'],
            catalog['CatalogUpvote'],
            catalog['CatalogDownvote'],
        )
    print("Finished!")

    return catalog_list


user_info_idx = 1
def new_random_user_info():
    global user_info_idx
    user_info = {
        'id'       : user_info_idx,
        'username' : f'user{user_info_idx}',
        'password' : f'{user_info_idx}',
        'db_account' : {
            'mysql' : {
                'datafabric_mysql_1:3306' : {
                    'ip'       : 'datafabric_mysql_1',
                    'port'     : '3306',
                    'username' : 'root',
                    'password' : 'my-secret-pw'
                },
                '192.168.103.52:3306' : {
                    'ip'       : '192.168.103.52',
                    'port'     : '3306',
                    'username' : f'user{user_info_idx}',
                    'password' : f'mypwd{user_info_idx}'
                }
            }
        },
        'data_permission': {
            'catalog_id' : {
                '*': False
            },
            'table_id' : {
                '*': False
            }
        },
        'action_permission': {
            'search': True,
            'recommend': True,
            'catalog_page': False,
            'get_catalog': False,
            'table_preview': False
        }
    }
    for i in range(user_info_idx):
        user_info['data_permission']['catalog_id'][str(i)] = True
        user_info['data_permission']['table_id'][str(i)] = True

    user_info_idx += 1
    return user_info

def generate_users(n_user):
    print("[Generating users testdata]")

    user_info_list = []
    print("Generating users")
    admin_user = {
        'id'       : 0,
        'username' : f'admin',
        'password' : f'admin',
        'db_account' : {
            'mysql': {
                'datafabric_mysql_1:3306': {
                    'ip': 'datafabric_mysql_1',
                    'port': '3306',
                    'username': 'root',
                    'password': 'my-secret-pw'
                },
                '192.168.103.52:3306' : {
                    'ip'       : '192.168.103.52',
                    'port'     : '3306',
                    'username' : f'brad',
                    'password' : f'00000000'
                }
            },
            'mongodb' : {
                'datafabric_mongo_1:27017': {
                    'ip': 'datafabric_mongo_1',
                    'port': '27017',
                    'username': 'root',
                    'password': 'example'
                }
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
    }
    user_info_list.append(admin_user)
    UserManager.set_user_info(0, admin_user)
    for i in tqdm(range(n_user)):
        user_info_list.append(new_random_user_info())
        UserManager.add_user(user_info_list[-1])
    print("Finished!")

    return user_info_list

def new_random_rating(user, catalog_list, rate_ratio=1.0/5.0):
    rating = {
        'user' : user['id'],
        'catalog_ratings' : {

        }
    }

    sampled_catalogs = random.sample(catalog_list, random.randint(0, int(len(catalog_list)*rate_ratio)))
    for catalog in sampled_catalogs:
        rating['catalog_ratings'][str(catalog['ID'])] = random.randint(0, 5)

    return rating

def generate_ratings(user_info_list, catalog_list):
    user_rating_list = []

    for user in tqdm(user_info_list):
        user_rating_list.append(new_random_rating(user, catalog_list))
    
    for rating in tqdm(user_rating_list):
        for key in rating['catalog_ratings']:
            catalog_id = int(key)
            score      = float(rating['catalog_ratings'][key])
            UserManager.set_rating(rating['user'], catalog_id, score)

    for user in tqdm(user_info_list):
        for catalog in catalog_list:
            UserManager.set_viewcount(int(user['id']), int(catalog['ID']), random.randint(0, 10))

    print("Finished!")

    return user_rating_list

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table", help="Number of testdata tables", type=int)
    parser.add_argument("-c", "--catalog", help="Number of catalogs", type=int)
    parser.add_argument("-u", "--user", help="Number of users", type=int)
    parser.add_argument("-r", "--rating", help="Generate rating", action='store_true')
    args = parser.parse_args()

    n_table    = args.table
    n_catalog  = args.catalog
    n_user     = args.user
    gen_rating = args.rating
    random.seed(0)

    if not (n_table is None or n_catalog is None):
        start = time.time()
        catalog_list = generate_catalogs(n_table, n_catalog)
        end = time.time()
        print(f"[generate_catalogs] elapsed time: {end-start}")

    if not (n_user is None):
        start = time.time()
        user_info_list = generate_users(n_user)
        end = time.time()
        print(f"[generate_users] elapsed time: {end-start}")

    if gen_rating:
        start = time.time()
        user_rating_list = generate_ratings(user_info_list, catalog_list)
        end = time.time()
        print(f"[generate_ratings] elapsed time: {end-start}")
    
if __name__ == '__main__':
    main()
