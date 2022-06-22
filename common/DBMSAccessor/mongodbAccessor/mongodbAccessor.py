import pandas as pd
import pymongo
import dateutil
import json
from bson import json_util

def parse_bson(data):
    return json.loads(json_util.dumps(data))

def preview_table_mongodb(username, password, ip, port, db, table, limit):
    auth = ''
    if username != '':
        auth = f'{username}:{password}@'
    mongo_client = pymongo.MongoClient(
            f'mongodb://{auth}{ip}:{port}',
            serverSelectionTimeoutMS=3000)

    mongo_db   = mongo_client[db]
    collection = mongo_db[table]
    
    results = list(map(parse_bson, list(collection.find().limit(limit))))
    for row in results:
        for k, v in row.items():
            if (type(v) is list) or (type(v) is dict):
                row[k] = json.dumps(v)
    return results

def query_table_mongodb(username, password, ip, port, db, table, columns, start_time, end_time, time_column):
    auth = ''
    if username != '':
        auth = f'{username}:{password}@'
    mongo_client = pymongo.MongoClient(
            f'mongodb://{auth}{ip}:{port}',
            serverSelectionTimeoutMS=3000)

    mongo_db = mongo_client[db]
    filter   = dict.fromkeys(columns, 1)
    filter['_id'] = 0
    if start_time is not None and end_time is not None and time_column is not None:
        start_time = dateutil.parser.parse(start_time)
        end_time   = dateutil.parser.parse(end_time)
        myquery = { time_column: { '$gte': start_time, '$lt': end_time } }
        mongodb_cursor = mongo_db[table].find({myquery}, filter)
    else:
        mongodb_cursor = mongo_db[table].find({}, filter)
        
    results = list(map(parse_bson, list(mongodb_cursor)))
    for row in results:
        for k, v in row.items():
            if (type(v) is list) or (type(v) is dict):
                row[k] = json.dumps(v)

    return pd.DataFrame(results)

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