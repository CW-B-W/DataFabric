#!/usr/bin/env python

import json
import sys, os, time
import json

''' ================ Flask Init ================ '''
from flask import Flask, request, render_template, redirect, url_for
from flask import render_template
from flask import session
from flask_cors import CORS
app = Flask(__name__, template_folder='template')
class Config(object):
    SECRET_KEY = "cwbw"
app.config.from_object(Config())
CORS(app)
''' ================ Flask Init ================ '''


''' ================ PySpark Init ================ '''
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
''' ================ PySpark Init ================ '''


@app.route('/train')
def train():
    try:
        max_iter = request.args.get('max_iter', default=10, type=int)
        rank     = request.args.get('rank', default=10, type=int)
        save_collection  = request.args.get('save_collection', default="default_recommendation", type=str)
        implicit_prefs   = request.args.get('implicit_prefs', default='false', type=str)
        implicit_prefs   = True if implicit_prefs.lower() == 'true' else False
    except Exception as e:
        print(str(e))
        return "Failed to parse arguments. " + str(e), 500

    try:
        mongo_client = pymongo.MongoClient(
                                f'mongodb://root:example@datafabric_mongo_1',
                                serverSelectionTimeoutMS=3000)
        mongo_db  = mongo_client['datafabric']
        mongo_col = mongo_db['ratings']

        rating_list = [] # (user, item, rating)
        rating_colname = 'catalog_views' if implicit_prefs else 'catalog_ratings'
        for entry in mongo_col.find({}, {'_id': 0, 'user': 1, rating_colname: 1}):
            for item in entry[rating_colname]:
                rating_list.append( (int(entry['user']), int(item), int(entry[rating_colname][item])) )
    except Exception as e:
        print(str(e))
        return "Failed to read from MongoDB. " + str(e), 500

    try:
        conf = pyspark.SparkConf() \
                    .setMaster("local") \
                    .setAppName("ALS") \
                    .set("spark.default.parallelism", "100") \
                    .set("spark.driver.memory", "30g")

        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        os.makedirs('checkpoint/', exist_ok=True)
        spark.sparkContext.setCheckpointDir('checkpoint/')

        df = spark.createDataFrame(
            rating_list,
            ["user", "item", "rating"]
        )

        # splitting into train and test sets
        # X_train, X_test = df.randomSplit([0.8, 0.2])
        X_train = df

        # training the model
        als = ALS(rank=rank, maxIter=max_iter, seed=0, nonnegative=True, implicitPrefs=implicit_prefs)
        model = als.fit(X_train.select(["user", "item", "rating"]))

        # predictions = model.transform(X_test.select(["user", "item"]))
        # predictions.show(1000)
        
        recommendations = model.recommendForAllUsers(100).toJSON().collect()
        recommendations = list(map(lambda s: json.loads(s), recommendations))
    except Exception as e:
        print(str(e))
        return "Some errors happen with pyspark. " + str(e), 500

    try:
        mongo_db  = mongo_client['recommendations']
        mongo_col = mongo_db[save_collection]
        mongo_col.drop()
        mongo_col = mongo_db[save_collection]
        mongo_col.insert_many(recommendations)
    except Exception as e:
        print(str(e))
        return "Failed to write to MongoDB. " + str(e), 500
    
    return 'ok'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
