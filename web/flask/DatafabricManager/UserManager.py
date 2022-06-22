from InternalDB.MongoDB import MongoDB

mongodb = MongoDB('datafabric')
user_info_cache = {} # user_info_cache[user_id(int)] = user_info

def login(username: str, password: str) -> int:
    """Try to login and return user_id if login is valid

    Args:
        username (str): username
        password (str): password

    Returns:
        int: user_id if login succeeded, otherwise -1.
    """
    result = mongodb.query('user_info', {"username": {"$eq": username}})
    try:
        if result[0]['password'] == password:
            result = MongoDB.parse_bson(result[0]) # To make sure it's serializable
            user_info_cache[result['id']] = result
            return result['id']
        else:
            return -1
    except Exception as e:
        return -1

def get_user_info(user_id: int) -> dict:
    """Get user info

    Args:
        user_id (int): user id

    Returns:
        dict: user_info if exists, otherwise None.
    """
    if user_id in user_info_cache:
        return user_info_cache[user_id]
    else:
        result = mongodb.query('user_info', {"id": {"$eq": user_id}})
        result = MongoDB.parse_bson(result[0]) # To make sure it's serializable
        user_info_cache[result['id']] = result
        return result

def set_user_info(user_id: int, user_info: dict):
    mongodb.update(
        'user_info',
        {'id' : user_id},
        user_info
    )

def add_user(user_info: dict):
    new_user_id = mongodb.collection_size('user_info')
    user_info['id'] = new_user_id
    mongodb.insert('user_info', user_info)
    rating_info = {
        'user' : new_user_id,
        'catalog_ratings' : {},
        'catalog_views' : {}
    }
    mongodb.insert('ratings', rating_info)
    return new_user_id

def del_user(user_id: int):
    user_info = get_user_info(user_id)
    rating_info = get_rating_info(user_id)
    mongodb.delete('user_info', query_by_mongoid(user_info))
    mongodb.delete('ratings', query_by_mongoid(rating_info))

def query_by_mongoid(obj: dict) -> dict:
    _id = MongoDB.to_ObjectId(obj['_id']['$oid'])
    return {'_id':_id}

def writeback_user_info(user_info: dict):
    mongodb.update(
        'user_info',
        query_by_mongoid(user_info),
        user_info
    )

def get_action_permission(user_id: int, action: str) -> bool:
    user_info = get_user_info(user_id)
    if '*' in user_info['action_permission'] \
            and user_info['action_permission']['*'] == True:
        return True
    elif action in user_info['action_permission'] \
            and user_info['action_permission'][action] == True:
        return True

    return False

def set_action_permission(user_id: int, action: str, accessible: bool):
    user_info = get_user_info(user_id)
    user_info['action_permission'][action] = accessible
    writeback_user_info(user_info)

def get_table_permission(user_id: int, table_id: int) -> bool:
    table_id = str(table_id)
    user_info = get_user_info(user_id)
    if '*' in user_info['data_permission']['table_id'] \
            and user_info['data_permission']['table_id']['*'] == True:
        return True
    elif table_id in user_info['data_permission']['table_id'] \
            and user_info['data_permission']['table_id'][table_id] == True:
        return True
    
    return False

def set_table_permission(user_id: int, table_id: int, accessible: bool):
    user_info = get_user_info(user_id)
    user_info['data_permission']['table_id'][table_id] = accessible
    writeback_user_info(user_info)

def get_catalog_permission(user_id: int, catalog_id: int) -> bool:
    catalog_id = str(catalog_id)
    user_info = get_user_info(user_id)
    if '*' in user_info['data_permission']['catalog_id'] \
            and user_info['data_permission']['catalog_id']['*'] == True:
        return True
    elif catalog_id in user_info['data_permission']['catalog_id'] \
            and user_info['data_permission']['catalog_id'][catalog_id] == True:
        return True
    
    return False

def set_catalog_permission(user_id: int, catalog_id: int, accessible: bool):
    user_info = get_user_info(user_id)
    user_info['data_permission']['table_id'][catalog_id] = accessible
    writeback_user_info(user_info)

def get_db_account(user_id: int, dbms: str, ip_port: str) -> dict:
    dbms = dbms.lower()
    user_info = get_user_info(user_id)
    return user_info['db_account'][dbms][ip_port]

def set_db_account(user_id: int, dbms: str, ip_port: str, username: str, password: str):
    dbms = dbms.lower()
    user_info = get_user_info(user_id)
    ip, port = ip_port.split(':')
    if dbms not in user_info['db_account']:
        user_info['db_account'][dbms] = {}
    user_info['db_account'][dbms][ip_port] = {
        'ip'       : ip,
        'port'     : port,
        'username' : username,
        'password' : password
    }
    writeback_user_info(user_info)

def set_username(user_id: int, username: str):
    user_info = get_user_info(user_id)
    user_info['db_account']['username'] = username
    writeback_user_info(user_info)

def set_password(user_id: int, password: str):
    user_info = get_user_info(user_id)
    user_info['db_account']['password'] = password
    writeback_user_info(user_info)


rating_info_cache = {} # rating_info_cache[user_id(int)] = rating_info
def get_rating_info(user_id: int) -> dict:
    """Get rating info of user

    Args:
        user_id (int): user id

    Returns:
        dict: rating_info if exists, otherwise None.
    """
    if user_id in rating_info_cache:
        return rating_info_cache[user_id]
    else:
        result = mongodb.query('ratings', {"user": {"$eq": user_id}})
        result = MongoDB.parse_bson(result[0]) # To make sure it's serializable
        rating_info_cache[result['user']] = result
        return result

def writeback_rating_info(rating_info: dict):
    mongodb.update(
        'ratings',
        query_by_mongoid(rating_info),
        rating_info
    )

def get_rating(user_id: int, catalog_id: int) -> float:
    rating_info = get_rating_info(user_id)
    return rating_info['catalog_ratings'][str(catalog_id)]

def set_rating(user_id: int, catalog_id: int, rating: float):
    rating_info = get_rating_info(user_id)
    rating_info['catalog_ratings'][str(catalog_id)] = rating
    writeback_rating_info(rating_info)

def get_viewcount(user_id: int, catalog_id: int) -> int:
    rating_info = get_rating_info(user_id)
    return rating_info['catalog_views'][str(catalog_id)]

def set_viewcount(user_id: int, catalog_id: int, count: int):
    rating_info = get_rating_info(user_id)
    rating_info['catalog_views'][str(catalog_id)] = count
    writeback_rating_info(rating_info)