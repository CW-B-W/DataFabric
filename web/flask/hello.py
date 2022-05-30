#!/usr/bin/env python

import json
import sys, os, time

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
def connect_mysql(retry=3):
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

def mysql_query(query):
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
def connect_redis(retry=3):
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
''' ================ Redis Init ================ '''

def validate_user():
    if 'user_info' in session:
        return True
    else:
        return False

@app.route('/')
def index():
    if not validate_user():
        return redirect(url_for('login'))

    with open('./flask_config.json', 'r') as rf:
        flask_config = json.load(rf)
    return render_template('index.html', flaskConfig = flask_config)


@app.route('/login', methods=['GET', 'POST'])
def login():
    def validate_login(username, password):
        if username == 'admin' and password == 'admin':
            return True
        else:
            return False
    def get_user_info(username, password):
        return {
            'username' : username,
            'password' : password,
            'db_account' : {
                'mysql' : {
                    'datafabric-mysql:3306' : {
                        'ip'       : 'datafabric-mysql',
                        'port'     : '3306',
                        'username' : 'root',
                        'password' : 'my-secret-pw'
                    },
                    '192.168.103.52:3306' : {
                        'ip'       : '192.168.103.52',
                        'port'     : '3306',
                        'username' : 'brad',
                        'password' : '00000000'
                    }
                }
            }
        }

    try:
        if request.method == 'GET':
            return render_template('login.html')
        elif request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')

            if validate_login(username, password):
                session['user_info'] = get_user_info(username, password)
                return redirect(url_for('index'))
            else:
                return "Login Failed!"
    except Exception as e:
        return str(e)

@app.route('/searchhints')
def searchhints():
    try:
        if not validate_user():
            return redirect(url_for('login'))

        return json.dumps([
            'ChineseExamScore',
            'EnglishExamScore',
            'MathExamScore'
        ])
    except Exception as e:
        return str(e)

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
        if connect_redis() == True and redis_db.exists(query_id):
            result = json.loads(redis_db.get(query_id).decode('utf-8'))
        else:
            result = mysql_query(
                f"SELECT * FROM CatalogManager "
                f"WHERE LOWER(ColumnMembers) LIKE '%{search_text.lower()}%' LIMIT {page_base*10},50;"
            )
            redis_db.set(query_id, json.dumps(result))
        
        result_page = result[10*(page-page_base-1):10*(page-page_base)]
        return json.dumps(result_page)
    except Exception as e:
        return str(e)

@app.route('/recommend')
def recommend():
    try:
        return json.dumps([
            {
                'ID' : '1',
                'CatalogName' : 'Recommend1: 期末考成績分析',
                'TableMembers' : 'MySQL@ExamScore@Final2020,MySQL@ExamScore@Final2021,MySQL@ExamScore@Final2022',
                'TableIds' : '12,13,14',
                'Descriptions' : '歷年期末考成績分析',
                'ViewCount' : '36',
                'UsedCount' : '27',
                'PopularTop3' : 'MySQL@ExamScore@Final2020@EnglishExamScore,MySQL@ExamScore@Final2020@ChineseExamScore,MySQL@ExamScore@Final2020@MathExamScore'
            },
            {
                'ID' : '2',
                'CatalogName' : 'Recommend2: 成績性向分析',
                'TableMembers' : 'MySQL@ExamScore@Final2021,MongoDB@StudentData@StudentPreferences',
                'TableIds' : '13,121',
                'Descriptions' : '分析學生成績與性向關係',
                'ViewCount' : '10',
                'UsedCount' : '3',
                'PopularTop3' : 'MongoDB@StudentData@StudentPreferences@Computer,MySQL@ExamScore@Final2020@MathExamScore'
            }
        ])
    except Exception as e:
        return str(e)

@app.route('/catalog')
def catalog():
    catalog_id  = request.args.get('catalog_id')
    catalog = mysql_query(f"SELECT * FROM CatalogManager WHERE ID = {catalog_id};")
    return json.dumps(catalog)

def get_table_info(tableid):
    result = mysql_query(f"SELECT * FROM TableInfo WHERE ID = {tableid};")
    return result[0]

@app.route('/table_preview')
def table_preview():
    try:
        limit     = request.args.get('limit', default=5, type=int)
        table_id  = request.args.get('table_id')

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
        return str(e)

@app.route('/mysql_test')
def mysql_test():
    try:
        result = mysql_query("SELECT * FROM CatalogManager;")
        return json.dumps(result)
    except Exception as e:
        return str(e)