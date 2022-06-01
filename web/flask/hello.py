#!/usr/bin/env python

import json
import sys, os, time
import random

from DBMSAccessor import DBMSAccessor

''' ================ Flask Init ================ '''
import flask
from flask import Flask, request, render_template, redirect, url_for
from flask import render_template
from flask import session
from flask_cors import CORS
#You need to use following line [app Flask(__name__)]
app = Flask(__name__, template_folder='template')
class Config(object):
    SECRET_KEY = "cwbw"
app.config.from_object(Config())
CORS(app)
''' ================ Flask Init ================ '''


''' ================ MySQL Init ================ '''
import pymysql
class MySQLDB:
    def __init__(self, mysql_conn: dict):
        self.__mysql_db      = None
        self.__mysql_conn_db = mysql_conn['db']
        self.__mysql_conn    = mysql_conn
        del self.__mysql_conn['db']

    def __connect_mysql(self, retry=3) -> bool:
        if self.__mysql_db is not None:
            return True
        while retry >= 0:
            try:
                # First check if database exists
                print(self.__mysql_conn, file=sys.stderr)
                self.__mysql_db = pymysql.connect(**self.__mysql_conn)
                cursor = self.__mysql_db.cursor(pymysql.cursors.DictCursor)
                print(f"CREATE DATABASE IF NOT EXISTS {self.__mysql_conn_db};", file=sys.stderr)
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.__mysql_conn_db};")
                self.__mysql_db.commit()
                cursor.close()
                self.__mysql_db.close()

                self.__mysql_conn['db'] = self.__mysql_conn_db
                print(self.__mysql_conn, file=sys.stderr)
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

sql_db = MySQLDB({
    "host": "datafabric-mysql",
    "port": 3306,
    "user": "root",
    "password": "my-secret-pw",
    "db": "datafabric",
    "charset": "utf8"
})

class TransactionDB(MySQLDB):
    def __init__(self, mysql_conn: dict, transaction_table: str):
        MySQLDB.__init__(self, mysql_conn)
        self.__transaction_table = transaction_table
        self.query(
            f"CREATE TABLE IF NOT EXISTS {self.__transaction_table} ("
            f"    ID         BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
            f"    Action     VARCHAR(100),"
            f"    Args       TEXT,"
            f"    User       VARCHAR(100),"
            f"    Status     VARCHAR(100),"
            f"    Result     TEXT,"
            f"    Timestamp  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
            f"    Datetime   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
            f");"
        )
    def add_transaction(self, action: str, args: dict, username: str, status: str = None, result: str = None):
        status_str = ("'" + status + "'") if status is not None else 'NULL'
        result_str = ("'" + result + "'") if result is not None else 'NULL'
        self.query(
            f"INSERT INTO {self.__transaction_table} (Action, Args, User, status, result) "
            f"VALUES ('{action}', '{json.dumps(args)}', '{username}', {status_str}, {result_str}"
            f");"
        )
transaction_db = TransactionDB({
    "host": "datafabric-mysql",
    "port": 3306,
    "user": "root",
    "password": "my-secret-pw",
    "db": "datafabric_transaction",
    "charset": "utf8"
}, 'TransactionLogs')
''' ================ MySQL Init ================ '''


''' ================ Redis Init ================ '''
import redis
class RedisDB:
    def __init__(self, host, port=6379, db=0):
        self.__redis_db = None
        self.__host     = host
        self.__port     = port
        self.__db       = db

    def __connect_redis(self, retry=3) -> bool:
        if self.__redis_db is not None:
            return True
        while retry >= 0:
            try:
                self.__redis_db = redis.Redis(host=self.__host, port=self.__port, db=self.__db)
                return True
            except Exception as ex:
                if retry == 0:
                    raise ex
                retry -= 1
        return False

    def set_json(self, key: str, obj: dict, expire: int = None) -> bool:
        if self.__connect_redis() == True:
            self.__redis_db.set(key, json.dumps(obj))
            if expire is not None:
                self.__redis_db.expire(key, expire)
            return True
        else:
            return False

    def get_json(self, key: str) -> dict:
        if self.__connect_redis() == True and self.__redis_db.exists(key):
            return json.loads(self.__redis_db.get(key).decode('utf-8'))
        else:
            return None

cache_db = RedisDB('datafabric-redis')
''' ================ Redis Init ================ '''


''' ================ Mongo Init ================ '''
import pymongo
from bson import json_util
class MongoDB:
    def __init__(self, host, username, password):
        self.__mongo_client = None
        self.__mongo_db     = None
        self.__host         = host
        self.__username     = username
        self.__password     = password

    def __connect_mongo(self, retry=3) -> bool:
        if self.__mongo_client is not None:
            return True
        while retry >= 0:
            try:
                mongo_client = pymongo.MongoClient(
                        f'mongodb://{self.__username}:{self.__password}@{self.__host}',
                        serverSelectionTimeoutMS=3000)
                mongo_client.server_info()
                mongo_db = mongo_client['datafabric']
                self.__mongo_client = mongo_client
                self.__mongo_db     = mongo_db
                return True
            except Exception as ex:
                if retry == 0:
                    raise ex
                retry -= 1
        return False
    
    def query(self, collection_name: str, arg1: dict = {}, arg2: dict = {}) -> dict:
        if self.__connect_mongo() == False:
            raise "Failed to connect Mongo."
        collection = self.__mongo_db[collection_name]
        result = collection.find(arg1, arg2)
        return result
    
    @staticmethod
    def parse_bson(data):
        return json.loads(json_util.dumps(data))

nosql_db = MongoDB('datafabric-mongo', 'root', 'example')
''' ================ Mongo Init ================ '''


def validate_user() -> bool:
    '''
    To check whether user had login.
    '''
    if 'user_info' in session:
        return True
    else:
        return False

def validate_permission(action_info: dict) -> bool:
    '''
    To check whether user has the permission to access the requested data.
    '''

    user_info = session['user_info']
    # simple example of permission check
    if action_info['action'] == 'catalog':
        catalog_id = str(action_info['catalog_id'])
        if user_info['permission']['catalog_id']['*'] == True:
            return True
        elif catalog_id in user_info['permission']['catalog_id']:
            if user_info['permission']['catalog_id'][catalog_id] == True:
                return True

        return False
    elif action_info['action'] == 'table_preview':
        table_id = str(action_info['table_id'])
        if user_info['permission']['table_id']['*'] == True:
            return True
        elif table_id in user_info['permission']['table_id']:
            if user_info['permission']['table_id'][table_id] == True:
                return True

        return False

    return False

@app.route('/')
def index():
    if not validate_user():
        transaction_db.add_transaction('index', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    with open('./flask_config.json', 'r') as rf:
        flask_config = json.load(rf)

    transaction_db.add_transaction('index', {}, session['user_info']['username'], None, None)
    return render_template('index.html', flaskConfig = flask_config)


def get_user_info(username: str, password: str) -> dict:
    try:
        result = nosql_db.query('user_info', {"username": {"$eq": username}})
        if result[0]['password'] == password:
            return MongoDB.parse_bson(result[0]) # To make sure it's serializable
        else:
            return None
    except Exception as e:
        return None

@app.route('/login', methods=['GET', 'POST'])
def login():
    try:
        if request.method == 'GET':
            transaction_db.add_transaction('login.GET', {}, '', None, None)
            return render_template('login.html')
        elif request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')

            user_info = get_user_info(username, password)
            if user_info is not None:
                session['user_info'] = user_info
                transaction_db.add_transaction('login.POST', request.form.to_dict(), session['user_info']['username'], 'SUCCEEDED', None)
                return redirect(url_for('index'))
            else:
                transaction_db.add_transaction('login.POST', request.form.to_dict(), '', 'FAILED', "Login Failed!")
                return "Login Failed!"
    except Exception as e:
        return str(e), 500

@app.route('/searchhints')
def searchhints():
    try:
        return json.dumps([
            'MyTable',
            'Catalog',
            'Column'
        ])
    except Exception as e:
        return str(e), 500

@app.route('/search')
def search():
    if not validate_user():
        transaction_db.add_transaction('search', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    try:
        page        = request.args.get('page', default=1, type=int)
        page_base   = int((page-1)/5)*5 # if page=1~5, page_base=0; page=6~10, page_base=5
        search_text = request.args.get('text', default='', type=str)
        if search_text == '':
            return json.dumps([])

        query_id = f"user={session['user_info']['username']}&search={search_text}&page={page_base+1}~{page_base+5}"
        result = cache_db.get_json(query_id)
        if result is None:
            result = sql_db.query(
                f"SELECT * FROM CatalogManager "
                f"WHERE LOWER(ColumnMembers) LIKE '%{search_text.lower()}%' LIMIT {page_base*10},50;"
            )
            cache_db.set_json(query_id, result, 15*60)
        
        result_page = result[10*(page-page_base-1):10*(page-page_base)]
        
        transaction_db.add_transaction('search', request.args.to_dict(), session['user_info']['username'], 'SUCCEEDED', None)
        return json.dumps(result_page)
    except Exception as e:
        return str(e), 500

@app.route('/recommend')
def recommend():
    if not validate_user():
        transaction_db.add_transaction('recommend', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    try:
        query_id = f"user={session['user_info']['username']}&recommend=random"
        result = cache_db.get_json(query_id)
        if result is None:
            result = sql_db.query(
                f"SELECT * FROM CatalogManager "
                f"ORDER BY RAND() LIMIT 50;"
            )
            cache_db.set_json(query_id, result, 30*60)

        result = random.sample(result, 10)

        transaction_db.add_transaction('recommend', request.args.to_dict(), session['user_info']['username'], 'SUCCEEDED', None)
        return json.dumps(result)
    except Exception as e:
        return str(e), 500

@app.route('/catalog')
def catalog():
    if not validate_user():
        transaction_db.add_transaction('catalog', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    catalog_id = request.args.get('catalog_id', type=int)
    if not validate_permission({'action': 'catalog','catalog_id' : catalog_id}):
        transaction_db.add_transaction('catalog', request.args.to_dict(), session['user_info']['username'], 'FAILED', 'Permission denied.')
        return "Permission denied.", 403

    transaction_db.add_transaction('catalog', request.args.to_dict(), session['user_info']['username'], 'SUCCEEDED', None)
    return render_template('catalog.html', catalogId = catalog_id)

def get_table_info(tableid: int) -> dict:
    result = sql_db.query(f"SELECT * FROM TableInfo WHERE ID = {tableid};")
    return result[0]

@app.route('/get_catalog')
def get_catalog():
    if not validate_user():
        transaction_db.add_transaction('get_catalog', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    catalog_id = request.args.get('catalog_id', type=int)
    if not validate_permission({'action': 'get_catalog','catalog_id' : catalog_id}):
        transaction_db.add_transaction('get_catalog', request.args.to_dict(), session['user_info']['username'], 'FAILED', 'Permission denied.')
        return "Permission denied.", 403

    query_id = f"catalog={catalog_id}"
    catalog = cache_db.get_json(query_id)
    if catalog is None:
        catalog = sql_db.query(f"SELECT * FROM CatalogManager WHERE ID = {catalog_id};")
        cache_db.set_json(query_id, catalog, 60*60)
    
    transaction_db.add_transaction('get_catalog', request.args.to_dict(), session['user_info']['username'], 'SUCCEEDED', None)
    return json.dumps(catalog)

@app.route('/table_preview')
def table_preview():
    if not validate_user():
        transaction_db.add_transaction('table_preview', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    try:
        limit     = request.args.get('limit', default=5, type=int)
        table_id  = request.args.get('table_id', type=int)
        if not validate_permission({'action': 'table_preview','table_id' : table_id}):
            transaction_db.add_transaction('table_preview', request.args.to_dict(), session['user_info']['username'], 'FAILED', 'Permission denied.')
            return "Permission denied.", 403

        table_info = get_table_info(table_id)
        ip, port   = table_info['Connection'].split(':')
        dbms  = table_info['DBMS'].lower()
        db    = table_info['DB']
        table = table_info['TableName']
        conn_username = session['user_info']['db_account'][dbms][f'{ip}:{port}']['username']
        conn_password = session['user_info']['db_account'][dbms][f'{ip}:{port}']['password']

        result = DBMSAccessor.preview_table(
            conn_username, conn_password,
            ip, port, dbms, db, table, limit
        )

        transaction_db.add_transaction('table_preview', request.args.to_dict(), session['user_info']['username'], 'SUCCEEDED', None)
        return json.dumps(result)
    except Exception as e:
        return str(e), 500
