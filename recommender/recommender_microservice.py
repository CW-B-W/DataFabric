#!/usr/bin/env python

import json
import sys, os, time
import random

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


import pymongo
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.ml.recommendation import ALS 
from pyspark.sql.types import FloatType
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col
from pyspark.sql.functions import isnan, when, count
from pyspark.sql.functions import *
@app.route('/train')
def train():
    mongo_client = pymongo.MongoClient(
                            f'mongodb://root:example@datafabric-mongo',
                            serverSelectionTimeoutMS=3000)

    mongo_db  = mongo_client['datafabric']
    mongo_col = mongo_db['ratings']

    rating_list = [] # (user, item, rating)
    for entry in mongo_col.find({}, {'_id': 0, 'user': 1, 'catalog_rating': 1}):
        for item in entry['catalog_rating']:
            rating_list.append( (int(entry['user']), int(item), int(entry['catalog_rating'][item])) )

    conf = pyspark.SparkConf() \
                .setMaster("local") \
                .setAppName("ALS")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    df = spark.createDataFrame(
        rating_list,
        ["user", "item", "rating"]
    )

    # splitting into train and test sets
    X_train, X_test = df.randomSplit([0.6, 0.4])

    # training the model
    als = ALS(rank=5, maxIter=5, seed=0, nonnegative=True)
    model = als.fit(X_train.select(["user", "item", "rating"]))

    predictions = model.transform(X_test.select(["user", "item"]))
    # predictions = predictions.withColumn("prediction", when(isnan(col("prediction")), lit(random.uniform(2, 4))).otherwise(col("prediction")))
    predictions.show(1000)
    