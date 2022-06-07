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
        return None

def get_datafabric_permission(user_id: int, action: str) -> bool:
    return

def get_catalogs_permission(user_id: int, catalogs: list) -> list:
    """_summary_

    Args:
        user_id (int): _description_
        catalogs (list): _description_

    Returns:
        list: _description_
    """
    return

def get_tables_permission(user_id: int, tables: list) -> list:
    """Return whether the tables are accessible by user

    Args:
        user_id (int): The user id in datafabric
        tables (list): A list containing the table id of the requested tables

    Returns:
        list: A list containing the accessibility for the requested tables
    """
    
    if user_id not in []:
        raise ValueError("")

def get_action_permission(user_id: int, action: str):
    user_info = user_info_cache[user_id]
    if '*' in user_info['action_permission'] \
            and user_info['action_permission']['*'] == True:
        return True
    elif action in user_info['action_permission'] \
            and user_info['action_permission'][action] == True:
        return True

    return False

def set_action_permission():
    pass

def get_table_permission(user_id: int, table_id: int):
    table_id = str(table_id)
    user_info = user_info_cache[user_id]
    if '*' in user_info['data_permission']['table_id'] \
            and user_info['data_permission']['table_id']['*'] == True:
        return True
    elif table_id in user_info['data_permission']['table_id'] \
            and user_info['data_permission']['table_id'][table_id] == True:
        return True
    
    return False

def set_table_permission():
    pass

def get_catalog_permission(user_id: int, catalog_id: int):
    catalog_id = str(catalog_id)
    user_info = user_info_cache[user_id]
    if '*' in user_info['data_permission']['catalog_id'] \
            and user_info['data_permission']['catalog_id']['*'] == True:
        return True
    elif catalog_id in user_info['data_permission']['catalog_id'] \
            and user_info['data_permission']['catalog_id'][catalog_id] == True:
        return True
    
    return False

def set_catalog_permission():
    pass

def get_db_account(user_id: int, dbms: str, ip_port: str) -> dict:
    if user_id in user_info_cache:
        return user_info_cache[user_id]['db_account'][dbms][ip_port]
    else:
        return None

def set_db_account():
    pass

def set_username():
    pass

def set_password():
    pass

def set_rating(user, catalog, rating):
    pass

def inc_view(user, catalog):
    pass