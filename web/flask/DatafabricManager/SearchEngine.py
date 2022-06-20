from InternalDB.MySQLDB import MySQLDB

mysqldb = MySQLDB('datafabric')

def search(search_text: str, start_page: int, n_items: int=50) -> list:
    """Search catalogs, tables, columns with search_text

    Args:
        search_text (str): search text
        start_page (int): start_page (start from 0). each page has 10 items
        n_items (int, optional): number of items to return. Defaults to 50.

    Returns:
        list: list of search result, elements are the catalogs
    """
    result = mysqldb.query(
        f"SELECT * FROM CatalogManager "
        f"WHERE LOWER(Keywords) LIKE '%{search_text.lower()}%' LIMIT {start_page*10},{n_items};"
    )
    return result