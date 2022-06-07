import pymongo
from bson import json_util
from bson.objectid import ObjectId
import json

class MongoDB:
    def __init__(self, db):
        self.__mongo_client = None
        self.__mongo_db     = None
        self.__host         = 'datafabric-mongo'
        self.__username     = 'root'
        self.__password     = 'example'
        self.__db           = db

    def __connect_mongo(self, retry=3) -> bool:
        if self.__mongo_client is not None:
            return True
        while retry >= 0:
            try:
                mongo_client = pymongo.MongoClient(
                        f'mongodb://{self.__username}:{self.__password}@{self.__host}',
                        serverSelectionTimeoutMS=3000)
                mongo_client.server_info()
                mongo_db = mongo_client[self.__db]
                self.__mongo_client = mongo_client
                self.__mongo_db     = mongo_db
                return True
            except Exception as ex:
                if retry == 0:
                    raise ex
                retry -= 1
        return False
    
    def query(self, collection_name: str, query: dict = {}, filter: dict = {}) -> dict:
        if self.__connect_mongo() == False:
            raise "Failed to connect Mongo."
        collection = self.__mongo_db[collection_name]
        result = collection.find(query, filter)
        return result

    def update(self, collection_name: str, query: dict, value: dict) -> dict:
        if self.__connect_mongo() == False:
            raise "Failed to connect Mongo."
        collection = self.__mongo_db[collection_name]
        _id = value['_id']
        del value['_id']
        value = {
            "$set" : value
        }
        result = collection.update_one(query, value, upsert=False)
        value['_id'] = _id
        return result
    
    @staticmethod
    def parse_bson(data):
        return json.loads(json_util.dumps(data))

    @staticmethod
    def to_ObjectId(_id: str):
        return ObjectId(_id)