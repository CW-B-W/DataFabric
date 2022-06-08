from InternalDB.MySQLDB import MySQLDB
from DatafabricManager import TableManager

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
        f"WHERE LOWER(Keywords) LIKE '%{search_text.lower()}%' LIMIT {start_page*10},{n_items};"
    )
    return result

def parse_tableid_string(s : str) -> list:
    return [int(s_id) for s_id in s.split(',')]

def to_tableid_string(tableid_list: list) -> str:
    return ','.join([str(id) for id in tableid_list])

def parse_tablemember_string(s : str) -> list:
    return [tuple(t.split('@')) for t in s.split(',')]

def to_tablemember_string(tablemember_list: list) -> str:
    return ','.join([f'{t[0]}@{t[1]}@{t[2]}' for t in tablemember_list])

def parse_keyword_string(s : str) -> list:
    return [x for x in s.split(',')]

def to_keyword_string(keyword_list: list) -> str:
    return ','.join(keyword_list)

def get_catalog(catalog_id: int) -> dict:
    """Get the catalog with catalog_id from MySQL

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

def set_catalog(catalog_id: int, catalog: dict):
    """Write back the catalog with catalog_id into MySQL
    
    Args:
        catalog_id (int): The id of the requested catalog
        catalog (dict): The content of catalog
    """
    set_str = ','.join([f'{k}="{v}"' for k, v in catalog.items()])
    
    mysqldb.query(f"""
        UPDATE CatalogManager 
        SET {set_str}
        WHERE ID = {catalog_id}
    """)

def add_catalog( \
            catalog_name: str, \
            table_ids: str, table_members: str, \
            keywords:str, description: str, \
            view_count: int, used_count: int, \
            catalog_upvote: int, catalog_downvote: int):
    mysqldb.query(f"""
        INSERT INTO CatalogManager VALUES (
            NULL,
            "{catalog_name}",
            "{table_ids}",
            "{table_members}",
            "{keywords}",
            "{description}",
            {view_count},
            {used_count},
            {catalog_upvote},
            {catalog_downvote}
        );
    """)

def del_catalog(catalog_id: int):
    mysqldb.query(f"""
        DELETE FROM CatalogManager WHERE ID = {catalog_id};
    """)

def add_table_into_catalog(catalog_id: int, table_id: int) -> bool:
    """Add a table into catalog

    Args:
        catalog_id (int): The target catalog
        table_id (int): The table to be added

    Returns:
        bool: Whether catalog is written back into MySQL
    """
    catalog = get_catalog(catalog_id)
    table   = TableManager.get_table_info(table_id)

    table_ids = parse_tableid_string(catalog['TableIds'])
    if table_id in table_ids:
        return False
    table_ids.append(table_id)
    
    table_members = parse_tablemember_string(catalog['TableMembers'])
    keywords      = parse_keyword_string(catalog['Keywords'])

    table_members.append((table['DBMS'], table['DB'], table['TableName']))
    keywords.append(table['TableName'])
    for new_col in table['Columns'].split(','):
        keywords.append(new_col)

    catalog['TableIds']     = to_tableid_string(table_ids)
    catalog['TableMembers'] = to_tablemember_string(table_members)
    catalog['Keywords']     = to_keyword_string(keywords)

    set_catalog(catalog_id, catalog)
    return True

def del_table_from_catalog(catalog_id: int, table_id: int) -> bool:
    """Delete a table from catalog

    Args:
        catalog_id (int): The target catalog
        table_id (int): The table to be Delete

    Returns:
        bool: Whether catalog is written back into MySQL
    """
    catalog = get_catalog(catalog_id)
    table   = TableManager.get_table_info(table_id)

    table_ids = parse_tableid_string(catalog['TableIds'])
    if table_id not in table_ids:
        return False
    table_ids.remove(table_id)
    
    table_members = parse_tablemember_string(catalog['TableMembers'])
    keywords      = parse_keyword_string(catalog['Keywords'])

    table_members.remove((table['DBMS'], table['DB'], table['TableName']))
    keywords.remove(table['TableName'])
    for new_col in table['Columns'].split(','):
        keywords.remove(new_col)

    catalog['TableIds']     = to_tableid_string(table_ids)
    catalog['TableMembers'] = to_tablemember_string(table_members)
    catalog['Keywords']     = to_keyword_string(keywords)

    set_catalog(catalog_id, catalog)
    return True

def update_table_of_catalog(catalog_id: int, table_id: int) -> bool:
    del_table_from_catalog(catalog_id, table_id)
    return add_table_into_catalog(catalog_id, table_id)
