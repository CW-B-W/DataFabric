import random
import pymysql
import argparse
from tqdm import tqdm

table_info_idx = 0
column_cnt     = 0
def new_random_table_info():
    global table_info_idx
    global column_cnt
    table_info = {
        'ID'         : table_info_idx,
        'Connection' : 'datafabric-mysql:3306',
        'DBMS'       : 'MySQL',
        'DB'         : 'testdata',
        'TableName'  : f'MyTable{table_info_idx}',
        'Columns'    : ''
    }
    dbms = table_info['DBMS']
    db   = table_info['DB']
    tbl  = table_info['TableName']
    table_info['Columns'] += f'{dbms}@{db}@{tbl}@id'
    for i in range(10):
        table_info['Columns'] += f',{dbms}@{db}@{tbl}@Column{column_cnt}'
        column_cnt += 1
    table_info_idx += 1
    return table_info

catalog_idx = 0
def new_random_catalog(table_info_list):
    global catalog_idx
    n_table = random.randint(2, 10)
    sampled = random.sample(table_info_list, n_table)

    catalog = {
        'CatalogName'     : f'Catalog{catalog_idx}',
        'TableMembers'    : '',
        'TableIds'        : '',
        'ColumnMembers'   : '',
        'Description'     : f'This is Catalog{catalog_idx}',
        'ViewCount'       : random.randint(0, 100),
        'UsedCount'       : 0,
        'PopularTop3'     : '',
        'CatalogUpvote'   : random.randint(0, 100),
        'CatalogDownvote' : random.randint(0, 100)
    }

    for table_info in sampled:
        catalog['TableMembers']  += f"{table_info['DBMS']}@{table_info['DB']}@{table_info['TableName']},"
        catalog['TableIds']      += f"{table_info['ID']},"
        catalog['ColumnMembers'] += f"{table_info['Columns']},"
    catalog['TableMembers']  = catalog['TableMembers'][:-1]
    catalog['TableIds']      = catalog['TableIds'][:-1]
    catalog['ColumnMembers'] = catalog['ColumnMembers'][:-1]

    columns     = catalog['ColumnMembers'].split(',')
    populartop3 = random.sample(columns, 3)
    for col in populartop3:
        catalog['PopularTop3'] += col + ','
    catalog['PopularTop3'] = catalog['PopularTop3'][:-1]

    catalog_idx += 1
    return catalog

def create_testdata_table_random(table_info):
    db_settings = {
        "host": "datafabric-mysql",
        "port": 3306,
        "user": "root",
        "password": "my-secret-pw",
        "db": "testdata",
        "charset": "utf8"
    }
    columns_full = table_info['Columns'].split(',')
    column_create_str = ''
    for c in columns_full:
        column_name = c.split('@')[-1]
        column_create_str += f'{column_name} TEXT,'
    column_create_str = column_create_str[:-1]

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
        conn.commit()

        with conn.cursor() as cursor:
            for i in range(50):
                command = f"""
                    INSERT INTO {table_info['TableName']} VALUES (
                        "{random.randint(0, 1000)}",
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

def create_table_info_table(table_info_list):
    db_settings = {
        "host": "datafabric-mysql",
        "port": 3306,
        "user": "root",
        "password": "my-secret-pw",
        "db": "datafabric",
        "charset": "utf8"
    }
    try:
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            command = f"""
                DROP TABLE IF EXISTS TableInfo;
            """
            cursor.execute(command)
            command = f"""
                CREATE TABLE TableInfo (
                    ID              INT PRIMARY KEY,
                    Connection      VARCHAR(100),
                    DBMS            VARCHAR(20),
                    DB              VARCHAR(100),
                    TableName       VARCHAR(100),
                    Columns         TEXT
                );
            """
            cursor.execute(command)
        conn.commit()

        for table_info in tqdm(table_info_list):
            with conn.cursor() as cursor:
                command = f"""
                    INSERT INTO TableInfo VALUES (
                        {table_info['ID']},
                        "{table_info['Connection']}",
                        "{table_info['DBMS']}",
                        "{table_info['DB']}",
                        "{table_info['TableName']}",
                        "{table_info['Columns']}"
                    );
                """
                cursor.execute(command)
            conn.commit()
    except Exception as ex:
        print(ex)

def create_catalog_table(catalog_list):
    db_settings = {
        "host": "datafabric-mysql",
        "port": 3306,
        "user": "root",
        "password": "my-secret-pw",
        "db": "datafabric",
        "charset": "utf8"
    }
    try:
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            command = f"""
                DROP TABLE IF EXISTS CatalogManager;
            """
            cursor.execute(command)
            command = f"""
                CREATE TABLE CatalogManager (
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
            """
            cursor.execute(command)
        conn.commit()

        with conn.cursor() as cursor:
            command = """
                INSERT INTO CatalogManager VALUES (
                    "期末考成績分析",
                    "MySQL@ScoreDb@Final2020,MySQL@ScoreDb@Final2021",
                    "12,13",
                    "MySQL@ScoreDb@Final2020@EnglishScore,MySQL@ScoreDb@Final2020@ChineseScore,MySQL@ScoreDb@Final2020@MathScore,MySQL@ScoreDb@Final2021@EnglishScore,MySQL@ScoreDb@Final2021@ChineseScore,MySQL@ScoreDb@Final2021@MathScore",
                    "歷年期末考成績分析",
                    36,
                    27,
                    "MySQL@ScoreDb@Final2020@EnglishScore,MySQL@ScoreDb@Final2020@ChineseScore,MySQL@ScoreDb@Final2020@MathScore",
                    12,
                    2
                );
            """
            cursor.execute(command)
        conn.commit()

        for catalog in tqdm(catalog_list):
            with conn.cursor() as cursor:
                command = f"""
                    INSERT INTO CatalogManager VALUES (
                        "{catalog['CatalogName']}",
                        "{catalog['TableMembers']}",
                        "{catalog['TableIds']}",
                        "{catalog['ColumnMembers']}",
                        "{catalog['Description']}",
                        {catalog['ViewCount']},
                        {catalog['UsedCount']},
                        "{catalog['PopularTop3']}",
                        {catalog['CatalogUpvote']},
                        {catalog['CatalogUpvote']}
                    );
                """
                cursor.execute(command)
            conn.commit()
    except Exception as ex:
        print(ex)

def create_databases():
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
            command = f"""
                DROP DATABASE IF EXISTS datafabric;
            """
            cursor.execute(command)
            command = f"""
                CREATE DATABASE datafabric;
            """
            cursor.execute(command)

            command = f"""
                DROP DATABASE IF EXISTS testdata;
            """
            cursor.execute(command)
            command = f"""
                CREATE DATABASE testdata;
            """
            cursor.execute(command)
        conn.commit()
    except Exception as ex:
        print(ex)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table", help="Number of testdata tables", type=int)
    parser.add_argument("-c", "--catalog", help="Number of catalogs", type=int)
    args = parser.parse_args()

    n_table   = args.table
    n_catalog = args.catalog
    random.seed(0)

    create_databases()

    table_info_list = []
    for i in tqdm(range(n_table)):
        table_info_list.append(new_random_table_info())
        create_testdata_table_random(table_info_list[-1])
    create_table_info_table(table_info_list)

    catalog_list = []
    for i in tqdm(range(n_catalog)):
        catalog_list.append(new_random_catalog(table_info_list))
    create_catalog_table(catalog_list)

if __name__ == '__main__':
    main()
