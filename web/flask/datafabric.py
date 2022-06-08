#!/usr/bin/env python

import json
import sys, os, time
import random
import requests

from DatafabricManager import TableManager
from DatafabricManager import CatalogManager
from DatafabricManager import UserManager
from DatafabricManager.TransactionLogging import TransactionLogging
transaction_logging = TransactionLogging('TransactionLogs')

from DBMSAccessor import DBMSAccessor

from RecommenderService import RecommenderService

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


def validate_user() -> bool:
    '''
    To check whether user had login.
    '''
    if 'user_id' in session:
        return True
    else:
        return False

def validate_permission(action_info: dict) -> bool:
    '''
    To check whether user has the permission to access the requested data.
    '''

    user_id = session['user_id']
    try:
        if UserManager.get_action_permission(user_id, action_info['action']) == False:
            return False

        # simple example of permission check
        if action_info['action'] == 'catalog_page' or action_info['action'] == 'get_catalog':
            catalog_id = action_info['catalog_id']
            return UserManager.get_catalog_permission(user_id, catalog_id)
        elif action_info['action'] == 'table_preview':
            table_id = action_info['table_id']
            return UserManager.get_catalog_permission(user_id, table_id)

        return False
    except Exception as e:
        return False

@app.route('/')
def index():
    if not validate_user():
        transaction_logging.add_transaction('index', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    with open('./flask_config.json', 'r') as rf:
        flask_config = json.load(rf)

    transaction_logging.add_transaction('index', {}, session['user_id'], None, None)
    return render_template('index.html', flaskConfig = flask_config)

@app.route('/login', methods=['GET', 'POST'])
def login():
    try:
        if request.method == 'GET':
            transaction_logging.add_transaction('login.GET', {}, '', None, None)
            return render_template('login.html')
        elif request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')

            user_id = UserManager.login(username, password)
            if user_id >= 0:
                session['user_id'] = user_id
                transaction_logging.add_transaction('login.POST', request.form.to_dict(), session['user_id'], 'SUCCEEDED', None)
                return redirect(url_for('index'))
            else:
                transaction_logging.add_transaction('login.POST', request.form.to_dict(), '', 'FAILED', "Login Failed!")
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
        transaction_logging.add_transaction('search', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    try:
        page        = request.args.get('page', default=1, type=int)
        page_base   = int((page-1)/5)*5 # if page=1~5, page_base=0; page=6~10, page_base=5
        search_text = request.args.get('text', default='', type=str)
        if search_text == '':
            return json.dumps([])

        query_id = f"user={session['user_id']}&search={search_text}&page={page_base+1}~{page_base+5}"
        result = cache_db.get_json(query_id)
        if result is None:
            result = CatalogManager.search(search_text, page_base, 50)
            cache_db.set_json(query_id, result, 15*60)
        
        result_page = result[10*(page-page_base-1):10*(page-page_base)]
        
        transaction_logging.add_transaction('search', request.args.to_dict(), session['user_id'], 'SUCCEEDED', None)
        return json.dumps(result_page)
    except Exception as e:
        return str(e), 500

@app.route('/recommend')
def recommend():
    if not validate_user():
        transaction_logging.add_transaction('recommend', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    try:
        query_id = f"user={session['user_id']}&recommend=random"
        result = cache_db.get_json(query_id)
        if result is None:
            result = RecommenderService.recommend(session['user_id'])
            cache_db.set_json(query_id, result, 30*60)

        # return top10 results
        result = result[:10]

        transaction_logging.add_transaction('recommend', request.args.to_dict(), session['user_id'], 'SUCCEEDED', None)
        return json.dumps(result)
    except Exception as e:
        return str(e), 500

@app.route('/catalog_page')
def catalog_page():
    if not validate_user():
        transaction_logging.add_transaction('catalog_page', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    catalog_id = request.args.get('catalog_id', type=int)
    if not validate_permission({'action': 'catalog_page','catalog_id' : catalog_id}):
        transaction_logging.add_transaction('catalog_page', request.args.to_dict(), session['user_id'], 'FAILED', 'Permission denied.')
        return "Permission denied.", 403

    transaction_logging.add_transaction('catalog_page', request.args.to_dict(), session['user_id'], 'SUCCEEDED', None)
    return render_template('catalog.html', catalogId = catalog_id)

@app.route('/get_catalog')
def get_catalog():
    if not validate_user():
        transaction_logging.add_transaction('get_catalog', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    catalog_id = request.args.get('catalog_id', type=int)
    if not validate_permission({'action': 'get_catalog','catalog_id' : catalog_id}):
        transaction_logging.add_transaction('get_catalog', request.args.to_dict(), session['user_id'], 'FAILED', 'Permission denied.')
        return "Permission denied.", 403

    query_id = f"catalog={catalog_id}"
    catalog = cache_db.get_json(query_id)
    if catalog is None:
        catalog = CatalogManager.get_catalog(catalog_id)
        if catalog is None:
            return "Invalid catalog_id"
        cache_db.set_json(query_id, catalog, 60*60)
    
    transaction_logging.add_transaction('get_catalog', request.args.to_dict(), session['user_id'], 'SUCCEEDED', None)
    return json.dumps(catalog)

@app.route('/table_preview')
def table_preview():
    if not validate_user():
        transaction_logging.add_transaction('table_preview', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    try:
        limit     = request.args.get('limit', default=5, type=int)
        table_id  = request.args.get('table_id', type=int)
        if not validate_permission({'action': 'table_preview','table_id' : table_id}):
            transaction_logging.add_transaction('table_preview', request.args.to_dict(), session['user_id'], 'FAILED', 'Permission denied.')
            return "Permission denied.", 403

        table_info = TableManager.get_table_info(table_id)
        if table_info is None:
            return "Invalid table_id"

        ip, port = table_info['Connection'].split(':')
        dbms     = table_info['DBMS'].lower()
        db       = table_info['DB']
        table    = table_info['TableName']
        conn_username = UserManager.get_db_account(session['user_id'], dbms, f'{ip}:{port}')['username']
        conn_password = UserManager.get_db_account(session['user_id'], dbms, f'{ip}:{port}')['password']

        result = DBMSAccessor.preview_table(
            conn_username, conn_password,
            ip, port, dbms, db, table, limit
        )

        transaction_logging.add_transaction('table_preview', request.args.to_dict(), session['user_id'], 'SUCCEEDED', None)
        return json.dumps(result)
    except Exception as e:
        return str(e), 500

@app.route('/train_recommender')
def train_recommender():
    response = requests.get('http://datafabric-recommender:5000/train')
    return response.text