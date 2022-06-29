import redis
import json
class RedisDB:
    def __init__(self, db):
        self.__redis_db = None
        self.__host     = 'datafabric_redis_1'
        self.__port     = 6379
        self.__db       = db

    def __connect_redis(self, retry=3) -> bool:
        if self.__redis_db is not None:
            return True
        while retry >= 0:
            try:
                self.__redis_db = redis.Redis(host=self.__host, port=self.__port, db=self.__db)
                return True
            except Exception as ex:
                if retry == 0:
                    raise ex
                retry -= 1
        return False

    def set_json(self, key: str, obj: dict, expire: int = None) -> bool:
        if self.__connect_redis() == True:
            self.__redis_db.set(key, json.dumps(obj))
            if expire is not None:
                self.__redis_db.expire(key, expire)
            return True
        else:
            return False

    def get_json(self, key: str) -> dict:
        if self.__connect_redis() == True and self.__redis_db.exists(key):
            return json.loads(self.__redis_db.get(key).decode('utf-8'))
        else:
            return None
