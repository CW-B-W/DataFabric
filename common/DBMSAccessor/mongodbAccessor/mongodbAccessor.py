import pandas as pd
import pymongo
import dateutil
import json

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
