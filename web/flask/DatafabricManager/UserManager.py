from InternalDB.MongoDB import MongoDB

mongodb = MongoDB('datafabric')

def get_user_info(username: str, password: str) -> dict:
    """Try to login and return user_info if login is valid

    Args:
        username (str): username
        password (str): password

    Returns:
        dict: user_info if login succeeded, otherwise None.
    """
    result = mongodb.query('user_info', {"username": {"$eq": username}})
    try:
        if result[0]['password'] == password:
            return MongoDB.parse_bson(result[0]) # To make sure it's serializable
        else:
            return None
    except Exception as e:
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