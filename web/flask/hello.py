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
    if validate_user():
        print(f"[index] session['user_info'] = {json.dumps(session['user_info'])}", file=sys.stderr)
    else:
        print(f"[index] session['user_info'] not found, redirecting to login", file=sys.stderr)
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
    def get_user_info(username):
        return {
            'username' : username,
            'password' : password,
            'db_account' : {
                'mysql' : [{
                    'ip'       : '127.0.0.1',
                    'port'     : '3306',
                    'username' : 'root',
                    'password' : 'my-secret-pw'
                },
                {
                    'ip'       : '192.168.103.52',
                    'port'     : '3306',
                    'username' : 'brad',
                    'password' : '00000000'
                }]
            }
        }

    try:
        if request.method == 'GET':
            return render_template('login.html')
        elif request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')

            if validate_login(username, password):
                session['user_info'] = get_user_info(username)
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
    global mysql_db
    global redis_db

    if not validate_user():
        return redirect(url_for('login'))

    try:
        page        = request.args.get('page', default=1, type=int)
        search_text = request.args.get('text', default='', type=str)
        if search_text == '':
            return json.dumps([])

        query_id = f"user={session['user_info']['username']}&search={search_text}"
        if connect_redis() == True:
            if redis_db.exists(query_id):
                result = json.loads(redis_db.get(query_id).decode('utf-8'))
            else:
                if connect_mysql() == False:
                    raise "Failed to connect MySQL"
                cursor = mysql_db.cursor(pymysql.cursors.DictCursor)
                cursor.execute(f"SELECT * FROM CatalogManager WHERE ColumnMembers LIKE '%{search_text}%' LIMIT 50;")
                result = cursor.fetchall()
                redis_db.set(query_id, json.dumps(result))
        
        result_page = result[(page-1)*10:page*10]
        return json.dumps(result_page)
    except Exception as e:
        return str(e)

@app.route('/recommend')
def recommend():
    try:
        return json.dumps([
            {
                'CatalogName' : 'Recommend1: 期末考成績分析',
                'TableMembers' : 'MySQL@ExamScore@Final2020,MySQL@ExamScore@Final2021,MySQL@ExamScore@Final2022',
                'TableIds' : '12,13,14',
                'Descriptions' : '歷年期末考成績分析',
                'ViewCount' : '36',
                'UsedCount' : '27',
                'PopularTop3' : 'MySQL@ExamScore@Final2020@EnglishExamScore,MySQL@ExamScore@Final2020@ChineseExamScore,MySQL@ExamScore@Final2020@MathExamScore'
            },
            {
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
    return ""

@app.route('/preview')
def table_preview():
    try:
        dbms  = request.args.get('dbms')
        db    = request.args.get('db')
        table = request.args.get('table')
        return DBMSAccessor.hello()
    except Exception as e:
        return str(e)

@app.route('/mysql_test')
def mysql_test():
    try:
        if connect_mysql() == False:
            raise "Failed to connect MySQL."
        cursor = mysql_db.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT * FROM CatalogManager;")
        result = cursor.fetchall()
        return json.dumps(result)
    except Exception as e:
        return str(e)