import pandas as pd
import pymongo
import dateutil
import json
import bson

def preview_table_mongodb(username, password, ip, port, db, table, limit):
    auth = ''
    if username != '':
        auth = f'{username}:{password}@'
    mongo_client = pymongo.MongoClient(
            f'mongodb://{auth}{ip}:{port}',
            serverSelectionTimeoutMS=3000)

    mongo_db   = mongo_client[db]
    collection = mongo_db[table]
    docs    = []
    key_set = set()
    for doc in collection.find().limit(limit):
        docs.append(doc)
        for key in doc:
            if key == '_id':
                continue
            key_set.add(key)

    result = []
    for doc in docs:
        item = {}
        for key in key_set:
            if key in doc:
                if type(doc[key]) is list or type(doc[key]) is dict:
                    item[key] = json.dumps(doc[key])
                else:
                    item[key] = doc[key]
            else:
                item[key] = ''
        result.append(item)
    return result

def query_table_mongodb(username, password, ip, port, db, table, columns, start_time, end_time, time_column):
    auth = ''
    if username != '':
        auth = f'{username}:{password}@'
    mongo_client = pymongo.MongoClient(
            f'mongodb://{auth}{ip}:{port}',
            serverSelectionTimeoutMS=3000)

    mongo_db = mongo_client[db]
    filter     = dict.fromkeys(columns, 1)
    filter['_id'] = 0
    if start_time is not None and end_time is not None and time_column is not None:
        start_time = dateutil.parser.parse(start_time)
        end_time   = dateutil.parser.parse(end_time)
        myquery = { time_column: { '$gte': start_time, '$lt': end_time } }
        mongodb_cursor = mongo_db[table].find({myquery}, filter)
    else:
        mongodb_cursor = mongo_db[table].find({}, filter)
    return pd.DataFrame(list(mongodb_cursor))

def list_dbs_mongodb(username, password, ip, port):
    if username != '':
        mongo_client = pymongo.MongoClient(f'mongodb://{username}:{password}@{ip}:{port}/')
    else:
        mongo_client = pymongo.MongoClient(f'mongodb://{ip}:{port}/')
    return sorted(mongo_client.list_database_names())

def list_tables_mongodb(username, password, ip, port, db):
    if username != '':
        mongo_client = pymongo.MongoClient(f'mongodb://{username}:{password}@{ip}:{port}/')
    else:
        mongo_client = pymongo.MongoClient(f'mongodb://{ip}:{port}/')
    mongo_db = mongo_client[db]
    return sorted(mongo_db.list_collection_names())

def list_columns_mongodb(username, password, ip, port, db, table):
    if username != '':
        mongo_client = pymongo.MongoClient(f'mongodb://{username}:{password}@{ip}:{port}/')
    else:
        mongo_client = pymongo.MongoClient(f'mongodb://{ip}:{port}/')
    mongo_db = mongo_client[db]
    result = list(mongo_db[table].aggregate([
        {"$project":{"arrayofkeyvalue":{"$objectToArray":"$$ROOT"}}},
        {"$unwind":"$arrayofkeyvalue"},
        {"$group":{"_id":"null","allkeys":{"$addToSet":"$arrayofkeyvalue.k"}}}
    ]))
    return sorted(result[0]['allkeys'])