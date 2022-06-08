from InternalDB.MySQLDB import MySQLDB
from InternalDB.MongoDB import MongoDB

mysqldb = MySQLDB('datafabric')
mongodb = MongoDB('recommendations')

def random_recommend(n: int = 50) -> tuple:
    result = mysqldb.query(
        f"SELECT * FROM CatalogManager "
        f"ORDER BY RAND() LIMIT {n};"
    )
    ratings = [-1 for _ in range(n)]
    return result, ratings

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

def get_recommendation(user_id: int, n_item_max: int = 50, collection: str = 'default_recommendation') -> tuple:
    """Get recommendations from MongoDB.recommendations

    Args:
        user_id (int): user id
        n_item_max (int): max number of items to return
        collection (str): retrieve from which mongodb collection

    Returns:
        tuple:
            [0] (list): List of recommended catalogs (descending by rating)
            [1] (list): rating for items in [0]
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
        catalogs.sort(key=lambda item: rec_ratings[str(item['ID'])], reverse=True)

        return catalogs, [rec_ratings[str(item['ID'])] for item in catalogs]
    except Exception as e:
        return [],[]


def recommend(user_id: int, n_item_max: int = 50, fill_with_random: bool = False) -> tuple:
    """Recommend n items for user_id

    Args:
        user_id (int): user id
        n_item_max (int, optional): Number of items to return. Defaults to 50.
        fill_with_random (bool, optional): Whether to fill the result list with random if n_item < n_item_max. Defaults to False.

    Returns:
        tuple:
            [0] (list): list containing the recommended catalog_info
            [1] (list): rating for items in [0]
    """
    items, ratings = get_recommendation(user_id)
    if fill_with_random and len(items) < n_item_max:
        random_items, dummy_ratings = random_recommend(n_item_max-len(items))
        items   += random_items
        ratings += dummy_ratings
    return items, ratings