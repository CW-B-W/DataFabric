from InternalDB.MongoDB import MongoDB

mongodb = MongoDB('datafabric')
user_info_cache = {} # user_info_cache[user_id] = user_info

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

def get_mongodb_query_for_user_info(user_info: dict) -> dict:
    _id = MongoDB.to_ObjectId(user_info['_id']['$oid'])
    return {'_id':_id}

def writeback_user_info(user_info):
    mongodb.update(
        'user_info',
        get_mongodb_query_for_user_info(user_info),
        user_info
    )

def get_action_permission(user_id: int, action: str):
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

def get_table_permission(user_id: int, table_id: int):
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

def get_catalog_permission(user_id: int, catalog_id: int):
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
    if user_id in user_info_cache:
        return user_info_cache[user_id]['db_account'][dbms][ip_port]
    else:
        return None

def set_db_account(user_id: int, dbms: str, ip_port: str, username: str, password: str):
    dbms = dbms.lower()
    user_info = get_user_info(user_id)
    ip, port = ip_port.split(':')
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

def set_rating(user, catalog, rating):
    pass

def inc_view(user, catalog):
    pass