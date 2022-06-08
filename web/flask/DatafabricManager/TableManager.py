from InternalDB.MySQLDB import MySQLDB

mysqldb = MySQLDB('datafabric')

def get_table_info(table_id: int) -> dict:
    """Get the table info via table_id

    Args:
        tableid (int): The id of the requested table

    Returns:
        dict: table_info if succeeded, else None
    """
    result = mysqldb.query(f"SELECT * FROM TableInfo WHERE ID = {table_id};")

    try:
        return result[0]
    except Exception as e:
        return None

def set_table_info(table_id: int, table_info: dict):
    """Write back the table_info with table_id into MySQL
    
    Args:
        table_id (int): The id of the requested table
        table_info (dict): The content of table_info
    """
    set_str = ','.join([f'{k}="{v}"' for k, v in table_info.items()])
    
    mysqldb.query(f"""
        UPDATE TableInfo 
        SET {set_str}
        WHERE ID = {table_id}
    """)
    pass

def add_table_info(connection: str, dbms: str, db: str, table_name: str, columns: str):
    mysqldb.query(f"""
        INSERT INTO TableInfo VALUES (
            NULL,
            "{connection}",
            "{dbms}",
            "{db}",
            "{table_name}",
            "{columns}"
        );
    """)

def del_table_info(table_id: int):
    mysqldb.query(f"""
        DELETE FROM TableInfo WHERE ID = {table_id};
    """)