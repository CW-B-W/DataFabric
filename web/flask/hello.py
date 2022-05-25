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
    def get_user_info():
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
    if not validate_user():
        return redirect(url_for('login'))

    try:
        page        = request.args.get('page', default=1, type=int)
        search_text = request.args.get('text')
        return json.dumps([
            {
                'catalog_name' : '期末考成績分析',
                'table_members' : 'MySQL@ExamScore@Final2020,MySQL@ExamScore@Final2021,MySQL@ExamScore@Final2022',
                'table_id' : '12,13,14',
                'description' : '歷年期末考成績分析',
                'view_count' : '36',
                'used_count' : '27',
                'popular_columns' : 'MySQL@ExamScore@Final2020@EnglishExamScore,MySQL@ExamScore@Final2020@ChineseExamScore,MySQL@ExamScore@Final2020@MathExamScore'
            },
            {
                'catalog_name' : '成績性向分析',
                'table_members' : 'MySQL@ExamScore@Final2020,MongoDB@StudentData@StudentPreferences',
                'table_id' : '13,121',
                'description' : '分析學生成績與性向關係',
                'view_count' : '10',
                'used_count' : '3',
                'popular_columns' : 'MongoDB@StudentData@StudentPreferences@Computer,MySQL@ExamScore@Final2020@MathExamScore'
            }
        ])
    except Exception as e:
        return str(e)

@app.route('/recommend')
def recommend():
    try:
        return json.dumps([
            {
                'catalog_name' : 'Recommend1: 期末考成績分析',
                'table_members' : 'MySQL@ExamScore@Final2020,MySQL@ExamScore@Final2021,MySQL@ExamScore@Final2022',
                'table_id' : '12,13,14',
                'description' : '歷年期末考成績分析',
                'view_count' : '36',
                'used_count' : '27',
                'popular_columns' : 'MySQL@ExamScore@Final2020@EnglishExamScore,MySQL@ExamScore@Final2020@ChineseExamScore,MySQL@ExamScore@Final2020@MathExamScore'
            },
            {
                'catalog_name' : 'Recommend2: 成績性向分析',
                'table_members' : 'MySQL@ExamScore@Final2021,MongoDB@StudentData@StudentPreferences',
                'table_id' : '13,121',
                'description' : '分析學生成績與性向關係',
                'view_count' : '10',
                'used_count' : '3',
                'popular_columns' : 'MongoDB@StudentData@StudentPreferences@Computer,MySQL@ExamScore@Final2020@MathExamScore'
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