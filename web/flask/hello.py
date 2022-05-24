#!/usr/bin/env python

import json
import sys, os, time

''' ================ Flask ================ '''
from flask import Flask, request, render_template
from flask import render_template
from flask import session
from flask_cors import CORS
#You need to use following line [app Flask(__name__)]
app = Flask(__name__, template_folder='template')
class Config(object):
    SECRET_KEY = "cwbw"
app.config.from_object(Config())
CORS(app)

@app.route('/hello')
def hello():
    print(session.get('id'))
    session['id'] = "hello"
    return 'Hello, Flask'

@app.route('/about/')
def about():
    return 'about Flask'

@app.route('/')
def index():
    with open('./flask_config.json', 'r') as rf:
        flask_config = json.load(rf)
    return render_template('index.html', flask_config = flask_config)

''' ================ Flask ================ '''
