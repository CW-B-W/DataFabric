#!/usr/bin/env python

import json
import sys, os, time

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


@app.route('/')
def index():
    if not 'user_info' in session:
        print(f"[index] session['user_info'] not found, redirecting to login", file=sys.stderr)
        return redirect(url_for('login'))
    else:
        print(f"[index] session['user_info'] = {json.dumps(session['user_info'])}", file=sys.stderr)

    with open('./flask_config.json', 'r') as rf:
        flask_config = json.load(rf)
    return render_template('index.html', flask_config = flask_config)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        return render_template('login.html')
    elif request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        if validate_user(username, password):
            session['user_info'] = {
                'username' : username,
                'password' : password
            }
            return redirect(url_for('index'))
        else:
            return "Login Failed!"

def validate_user(username, password):
    if username == 'admin' and password == 'admin':
        return True
    else:
        return False