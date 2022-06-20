#!/usr/bin/env python

import json
import sys, os, time
import random
import requests
import pika
import socket

from DatafabricManager import TableManager
from DatafabricManager import CatalogManager
from DatafabricManager import UserManager
from DatafabricManager import SearchEngine
from DatafabricManager.TransactionLogging import TransactionLogging
transaction_logging = TransactionLogging('TransactionLogs')

from DatafabricTools import MetadataScanner

from InternalDB.RedisDB import RedisDB
cache_db = RedisDB(db=0)

from DBMSAccessor import DBMSAccessor

from RecommenderService import RecommenderService

from DataIntegrationService import DataIntegrationService
DataIntegrationService.start_monitor_task_status(background=True)

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
        if action_info['action'] == 'catalog_page' or action_info['action'] == 'catalog_get':
            catalog_id = action_info['catalog_id']
            return UserManager.get_catalog_permission(user_id, catalog_id)
        elif action_info['action'] == 'table_preview' or action_info['action'] == 'tableinfo_get':
            table_id = action_info['table_id']
            return UserManager.get_table_permission(user_id, table_id)
        elif action_info['action'] == 'management' or action_info['action'] == 'catalog_manage':
            return user_id == 0

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

@app.route('/logout')
def logout():
    if 'user_id' in session:
        del session['user_id']
    return redirect(url_for('login'))

@app.route('/management')
def management():
    if not validate_permission({'action': 'management'}):
        transaction_logging.add_transaction('management', request.args.to_dict(), session['user_id'], 'FAILED', 'Permission denied.')
        return "Permission denied.", 403
    
    return render_template('management.html')

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
            result = SearchEngine.search(search_text, page_base, 50)
            cache_db.set_json(query_id, result, 15*60)
        
        result_page = result[10*(page-page_base-1):10*(page-page_base)]
        
        transaction_logging.add_transaction('search', request.args.to_dict(), session['user_id'], 'SUCCEEDED', None)
        return json.dumps(result_page)
    except Exception as e:
        return str(e), 500

@app.route('/recommender/recommend')
def recommend():
    if not validate_user():
        transaction_logging.add_transaction('recommend', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    try:
        # query_id = f"user={session['user_id']}&recommend=random"
        # result = cache_db.get_json(query_id)
        # if result is None:
        #     result = RecommenderService.recommend(session['user_id'])
        #     cache_db.set_json(query_id, result, 30*60)

        result, ratings = RecommenderService.recommend(session['user_id'])
        # return top10 results
        result  = result[:10]
        ratings = ratings[:10]

        transaction_logging.add_transaction('recommend', request.args.to_dict(), session['user_id'], 'SUCCEEDED', None)
        return json.dumps({
            'items' : result,
            'ratings' : ratings
        })
    except Exception as e:
        return str(e), 500

@app.route('/catalog')
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

@app.route('/catalog/get')
def catalog_get():
    if not validate_user():
        transaction_logging.add_transaction('catalog_get', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    catalog_id = request.args.get('catalog_id', type=int)
    if not validate_permission({'action': 'catalog_get','catalog_id' : catalog_id}):
        transaction_logging.add_transaction('catalog_get', request.args.to_dict(), session['user_id'], 'FAILED', 'Permission denied.')
        return "Permission denied.", 403

    catalog = CatalogManager.get_catalog(catalog_id)
    if catalog is None:
        return "Invalid catalog_id"
    
    transaction_logging.add_transaction('catalog_get', request.args.to_dict(), session['user_id'], 'SUCCEEDED', None)
    return json.dumps(catalog)

@app.route('/catalog/manage')
def catalog_manage():
    if not validate_user():
        transaction_logging.add_transaction('catalog_manage', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    if not validate_permission({'action': 'catalog_manage'}):
        transaction_logging.add_transaction('catalog_manage', request.args.to_dict(), session['user_id'], 'FAILED', 'Permission denied.')
        return "Permission denied.", 403

    catalog_id = request.args.get('catalog_id', type=int)
    action = request.args.get('action', type=str)
    if action == 'add_table':
        table_id = request.args.get('table_id', type=int)
        try:
            status = CatalogManager.add_table_into_catalog(catalog_id, table_id)
            if status:
                return 'ok'
            else:
                return 'already exists in catalog'
        except Exception as e:
            return str(e), 500
    elif action == 'del_table':
        table_id = request.args.get('table_id', type=int)
        try:
            status = CatalogManager.del_table_from_catalog(catalog_id, table_id)
            if status:
                return 'ok'
            else:
                return 'not exists in catalog'
        except Exception as e:
            return str(e), 500
    
    return 'action not found', 400

@app.route('/catalog/search')
def catalog_search():
    if not validate_user():
        transaction_logging.add_transaction('catalog_search', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    try:
        search_text = request.args.get('text', default='', type=str)
        if search_text == '':
            return json.dumps([])

        result = CatalogManager.search(search_text, 0, 50)
        
        transaction_logging.add_transaction('catalog_search', request.args.to_dict(), session['user_id'], 'SUCCEEDED', None)
        return json.dumps(result)
    except Exception as e:
        return str(e), 500

@app.route('/tableinfo/get')
def tableinfo_get():
    if not validate_user():
        transaction_logging.add_transaction('tableinfo_get', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    table_id = request.args.get('table_id', type=int)
    if not validate_permission({'action': 'tableinfo_get','table_id' : table_id}):
        transaction_logging.add_transaction('tableinfo_get', request.args.to_dict(), session['user_id'], 'FAILED', 'Permission denied.')
        return "Permission denied.", 403

    table_info = TableManager.get_table_info(table_id)
    if table_info is None:
        return "Invalid table_id"
    
    transaction_logging.add_transaction('tableinfo_get', request.args.to_dict(), session['user_id'], 'SUCCEEDED', None)
    return json.dumps(table_info)

@app.route('/tableinfo/search')
def tableinfo_search():
    if not validate_user():
        transaction_logging.add_transaction('tableinfo_search', {}, '', 'FAILED', "Redirecting to login page")
        return redirect(url_for('login'))

    try:
        search_text = request.args.get('text', default='', type=str)
        if search_text == '':
            return json.dumps([])

        result = TableManager.search(search_text, 0, 50)
        
        transaction_logging.add_transaction('tableinfo_search', request.args.to_dict(), session['user_id'], 'SUCCEEDED', None)
        return json.dumps(result)
    except Exception as e:
        return str(e), 500

@app.route('/tableinfo/add', methods=['POST'])
def tableinfo_add():
    if request.method == 'POST':
        try:
            table_infos = json.loads(request.data.decode('utf-8'))
            added_ids = []
            for table_info in table_infos:
                added_ids.append(
                    TableManager.add_table_info(table_info['Connection'], table_info['DBMS'], table_info['DB'], table_info['TableName'], table_info['Columns'])
                )
            return json.dumps(added_ids)
        except Exception as e:
            return str(e), 500
    else:
        return 'Only support POST', 400

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

@app.route('/recommender/train')
def train_recommender():
    response = requests.get('http://datafabric-recommender:5000/train?' + request.query_string.decode())
    return response.text

@app.route('/data_integration', methods=['GET', 'POST'])
def data_integration():
    try:
        if request.method == 'GET':
            table_ids = request.args.get('table_ids', type=str)
            if table_ids is None:
                return 'table_ids must be specified.', 400
            table_ids = [int(table_id) for table_id in table_ids.split(',')]
            table_infos = [TableManager.get_table_info(table_id) for table_id in table_ids]

            user_info = UserManager.get_user_info(session['user_id'])
            for table_info in table_infos:
                dbms = table_info['DBMS'].lower()
                conn = table_info['Connection']
                table_info['Username'] = user_info['db_account'][dbms][conn]['username']
                table_info['Password'] = user_info['db_account'][dbms][conn]['password']

            if len(table_infos) == 1:
                table_infos.append(TableManager.none_table_info())

            return render_template('data_integration.html', tableInfos = table_infos)
        elif request.method == 'POST':
            task_info = json.loads(request.data.decode('utf-8'))
            try:
                DataIntegrationService.send_task(task_info)
            except Exception as e:
                return str(e)
            return task_info['task_id']
    except Exception as e:
        return str(e), 500

@app.route('/data_integration/status')
def data_integration_status():
    task_id     = request.args.get('task_id', type=str)
    task_status = DataIntegrationService.get_task_status(task_id)
    if task_status is not None:
        return json.dumps(task_status)
    else:
        return 'task_id is not found.', 400

@app.route('/data_integration/serving/<serving_name>')
def data_integration_serving(serving_name):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect_ex(('datafabric-data-integration', 5001))
    s.send(str.encode(serving_name))
    status = s.recv(2).decode('utf-8')
    if status == 'ok':
        def recv_file(s):
            while (True):
                try:
                    received_bytes = s.recv(16*1024*1024)
                    print(received_bytes)
                except:
                    return
                if received_bytes.decode() != '':
                    yield received_bytes
                else:
                    return
        return app.response_class(recv_file(s), mimetype='text/csv')
    else:
        return 'Not Found', 404

@app.route('/supported_dbms')
def supported_dbms():
    return json.dumps(DBMSAccessor.get_supported_dbms())

@app.route('/metadata_scanner')
def metadata_scanner_page():
    return render_template('metadata_scanner.html')

@app.route('/metadata_scanner/scan')
def metadata_scanner_scan():
    ip      = request.args.get('ip', type=str)
    port    = request.args.get('port', type=str)
    conn    = f'{ip}:{port}'
    dbms    = request.args.get('dbms', type=str)
    db      = request.args.get('db', default=None, type=str)
    tables  = request.args.get('tables', default=None, type=str)
    try:
        user_info = UserManager.get_user_info(session['user_id'])
        username = user_info['db_account'][dbms][conn]['username']
        password = user_info['db_account'][dbms][conn]['password']
        scanned = MetadataScanner.scan(username, password, ip, port, dbms, db, tables)
        return json.dumps(scanned)
    except Exception as e:
        return str(e), 500
