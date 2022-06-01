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
mysql_settings = {
    "host": "datafabric-mysql",
    "port": 3306,
    "user": "root",
    "password": "my-secret-pw",
    "db": "datafabric",
    "charset": "utf8"
}
mysql_db = None
def connect_mysql(retry=3) -> bool:
    global mysql_db
    if not mysql_db is None:
        return True
    while retry >= 0:
        try:
            mysql_db = pymysql.connect(**mysql_settings)
            return True
        except Exception as ex:
            print(ex)
            retry -= 1
    return False

def mysql_query(query: str) -> bool:
    if connect_mysql() == False:
        raise "Failed to connect MySQL."
    cursor = mysql_db.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    mysql_db.commit()
    result = cursor.fetchall()
    return result
''' ================ MySQL Init ================ '''


''' ================ Redis Init ================ '''
import redis
redis_db = None
def connect_redis(retry=3) -> bool:
    global redis_db
    if not redis_db is None:
        return True
    while retry >= 0:
        try:
            redis_db = redis.Redis(host='datafabric-redis', port=6379, db=0)
            return True
        except Exception as ex:
            print(ex)
            retry -= 1
    return False

def redis_set_json(key: str, obj: dict, expire: int = None) -> bool:
    redis_db.set(key, json.dumps(obj))
    if expire is not None:
        redis_db.expire(key, expire)

def redis_get_json(key: str) -> dict:
    if connect_redis() == True and redis_db.exists(key):
        return json.loads(redis_db.get(key).decode('utf-8'))
    else:
        return None
''' ================ Redis Init ================ '''


''' ================ Mongo Init ================ '''
import pymongo
from bson import json_util
mongo_client = None
mongo_db     = None
def connect_mongo(retry=3) -> bool:
    global mongo_client
    global mongo_db
    if not mongo_client is None:
        return True
    while retry >= 0:
        try:
            mongo_client = pymongo.MongoClient(
                    'mongodb://%s:%s@datafabric-mongo' % ('root', 'example'), 
                    serverSelectionTimeoutMS=3000)
            mongo_client.server_info()
            mongo_db = mongo_client['datafabric']
            return True
        except Exception as ex:
            print(ex)
            retry -= 1
    return False

def mongo_query(collection_name: str, arg1: dict = {}, arg2: dict = {}) -> dict:
    if connect_mongo() == False:
        raise "Failed to connect MySQL."
    collection = mongo_db[collection_name]
    result = collection.find(arg1, arg2)
    return result

def parse_bson(data):
    return json.loads(json_util.dumps(data))
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
        print(table_id, file=sys.stderr)
        print(user_info['permission']['table_id'], file=sys.stderr)
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
        return redirect(url_for('login'))

    with open('./flask_config.json', 'r') as rf:
        flask_config = json.load(rf)
    return render_template('index.html', flaskConfig = flask_config)


def get_user_info(username: str, password: str) -> dict:
    try:
        result = mongo_query('user_info', {"username": {"$eq": username}})
        if result[0]['password'] == password:
            return parse_bson(result[0]) # To make sure it's serializable
        else:
            return None
    except Exception as e:
        return None

@app.route('/login', methods=['GET', 'POST'])
def login():
    try:
        if request.method == 'GET':
            return render_template('login.html')
        elif request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')

            user_info = get_user_info(username, password)
            if user_info is not None:
                session['user_info'] = user_info
                return redirect(url_for('index'))
            else:
                return "Login Failed!"
    except Exception as e:
        return str(e), 500

@app.route('/searchhints')
def searchhints():
    if not validate_user():
        return redirect(url_for('login'))

    try:
        return json.dumps([
            'ChineseExamScore',
            'EnglishExamScore',
            'MathExamScore'
        ])
    except Exception as e:
        return str(e), 500

@app.route('/search')
def search():
    global redis_db

    if not validate_user():
        return redirect(url_for('login'))

    try:
        page        = request.args.get('page', default=1, type=int)
        page_base   = int((page-1)/5)*5 # if page=1~5, page_base=0; page=6~10, page_base=5
        search_text = request.args.get('text', default='', type=str)
        if search_text == '':
            return json.dumps([])

        query_id = f"user={session['user_info']['username']}&search={search_text}&page={page_base+1}~{page_base+5}"
        result = redis_get_json(query_id)
        if result is None:
            result = mysql_query(
                f"SELECT * FROM CatalogManager "
                f"WHERE LOWER(ColumnMembers) LIKE '%{search_text.lower()}%' LIMIT {page_base*10},50;"
            )
            redis_set_json(query_id, result, 15*60)
        
        result_page = result[10*(page-page_base-1):10*(page-page_base)]
        return json.dumps(result_page)
    except Exception as e:
        return str(e), 500

@app.route('/recommend')
def recommend():
    if not validate_user():
        return redirect(url_for('login'))

    try:
        query_id = f"user={session['user_info']['username']}&recommend=random"
        result = redis_get_json(query_id)
        if result is None:
            result = mysql_query(
                f"SELECT * FROM CatalogManager "
                f"ORDER BY RAND() LIMIT 50;"
            )
            redis_set_json(query_id, result, 30*60)

        result = random.sample(result, 10)
        return json.dumps(result)
    except Exception as e:
        return str(e), 500

@app.route('/catalog')
def catalog():
    if not validate_user():
        return redirect(url_for('login'))

    catalog_id = request.args.get('catalog_id', type=int)
    if not validate_permission({'action': 'catalog','catalog_id' : catalog_id}):
        return "Permission denied.", 403

    return render_template('catalog.html', catalogId = catalog_id)

def get_table_info(tableid: int) -> dict:
    result = mysql_query(f"SELECT * FROM TableInfo WHERE ID = {tableid};")
    return result[0]

@app.route('/get_catalog')
def get_catalog():
    if not validate_user():
        return redirect(url_for('login'))

    catalog_id = request.args.get('catalog_id', type=int)
    if not validate_permission({'action': 'get_catalog','catalog_id' : catalog_id}):
        return "Permission denied.", 403

    query_id = f"catalog={catalog_id}"
    catalog = redis_get_json(query_id)
    if catalog is None:
        catalog = mysql_query(f"SELECT * FROM CatalogManager WHERE ID = {catalog_id};")
        redis_set_json(query_id, catalog, 60*60)
    return json.dumps(catalog)

@app.route('/table_preview')
def table_preview():
    if not validate_user():
        return redirect(url_for('login'))

    try:
        limit     = request.args.get('limit', default=5, type=int)
        table_id  = request.args.get('table_id', type=int)
        if not validate_permission({'action': 'table_preview','table_id' : table_id}):
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
        print(result, file=sys.stderr)
        return json.dumps(result)
    except Exception as e:
        return str(e), 500
