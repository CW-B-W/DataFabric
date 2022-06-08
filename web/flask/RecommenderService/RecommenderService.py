from InternalDB.MySQLDB import MySQLDB
from InternalDB.MongoDB import MongoDB

mysqldb = MySQLDB('datafabric')
mongodb = MongoDB('recommendations')

def random_recommend(n: int = 50) -> list:
    result = mysqldb.query(
        f"SELECT * FROM CatalogManager "
        f"ORDER BY RAND() LIMIT {n};"
    )
    return result

def get_catalogs_with_ids(ids: list) -> list:
    if len(ids) <= 0:
        return []
    ids_str = ''
    for id in ids:
        ids_str += str(id) + ','
    ids_str = ids_str[:-1]
    result = mysqldb.query(
        f"SELECT * FROM CatalogManager "
        f"WHERE ID IN ({ids_str});"
    )
    return result

def get_recommendation(user_id: int, n_item_max: int = 50, collection: str = 'default_recommendation') -> list:
    """Get recommendations from MongoDB.recommendations

    Args:
        user_id (int): user id
        n_item_max (int): max number of items to return
        collection (str): retrieve from which mongodb collection

    Returns:
        list: List of recommended catalogs (descending by rating)
    """
    result = mongodb.query(collection, {'user': user_id})
    try:
        recommendation = result[0]
        rec_ids     = []
        rec_ratings = {}
        for item in recommendation['recommendations']:
            if len(rec_ids) >= n_item_max:
                break
            rec_ids.append(item["item"])
            rec_ratings[str(item["item"])] = item["rating"]
        
        catalogs = get_catalogs_with_ids(rec_ids)

        # sort results by score
        catalogs.sort(key=lambda item: rec_ratings[str(item['ID'])])

        return catalogs
    except Exception as e:
        return []


def recommend(user_id: int, n_item_max: int = 50, fill_with_random = False) -> list:
    """Recommend n items for user_id

    Args:
        user_id (int): user id
        n_item_max (int, optional): Number of items to return. Defaults to 50.

    Returns:
        list: list containing the recommended catalog_info
    """
    result = get_recommendation(user_id)
    if fill_with_random and len(result) < n_item_max:
        result += random_recommend(n_item_max-len(result))
    return result