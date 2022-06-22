
from email.policy import default
from tracemalloc import start
from elasticsearch import Elasticsearch
import json
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
from elasticsearch import helpers
from elasticsearch_dsl import Search
def preview_table_elk(username, password, ip, port, db, table, limit):
    es = Elasticsearch(hosts=ip, port=port, http_auth=(username, password))
    resp = es.search(index=table, query={"match_all": {}}, size=limit)
    rows = []
    for hit in resp['hits']['hits']:
        rows.append(hit["_source"])
    return rows
    
   

def query_table_elk(username, password, ip, port, db, table, columns, start_time, end_time, time_column):
    es = Elasticsearch(hosts=ip, port=port, http_auth=(username, password))
    
    start_time = start_time.replace(" ", "T")
    end_time = end_time.replace(" ", "T")
    es_result = helpers.scan(
                        client = es,
                        query = {
                            "query" : {
                                "bool": {
                                    "filter":[
                                        {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}}
                                    ]
                                }
                            },
                        },
                        _source = list(columns),
                        index   = table,
                        scroll  ='10m',
                        timeout ="10m"
                    )
    rows = []
    for k in es_result:
        val_dict = k['_source']
        tmp_dict = {}
        for keyname in list(columns):
            if keyname.find('.') != -1:
                keys = keyname.split('.')
                val = val_dict
                for layer in range(len(keys)):
                    val = val[keys[layer]]
            else:
                val = val_dict[keyname]
                
            if type(val) == dict or type(val) == list:
                val = json.dumps(val)
                
            tmp_dict[keyname] = val
        rows.append(tmp_dict)
    return pd.DataFrame(rows)
print(query_table_elk("elastic", "00000000", "192.168.103.120", 9200, default, "test", {"id", "name"}, "2018-12-31 00:00", "2022-06-14 09:45", "@timestamp") )