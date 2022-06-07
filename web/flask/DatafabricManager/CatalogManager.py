from InternalDB.MySQLDB import MySQLDB

mysqldb = MySQLDB('datafabric')

def search(search_text: str, start_page: int, n_items: int=50) -> list:
    """Search catalogs, tables, columns with search_text

    Args:
        search_text (str): search text
        start_page (int): start_page. each page has 10 items
        n_items (int, optional): number of items to return. Defaults to 50.

    Returns:
        list: list of search result, elements are the catalogs
    """
    result = mysqldb.query(
        f"SELECT * FROM CatalogManager "
        f"WHERE LOWER(ColumnMembers) LIKE '%{search_text.lower()}%' LIMIT {start_page*10},{n_items};"
    )
    return result

def get_catalog(catalog_id: int) -> dict:
    """Get the catalog via catalog_id

    Args:
        catalog_id (int): The id of the requested catalog

    Returns:
        dict: catalog if succeeded, else None
    """
    catalog = mysqldb.query(f"SELECT * FROM CatalogManager WHERE ID = {catalog_id};")
    
    try:
        return catalog[0]
    except Exception as e:
        return None