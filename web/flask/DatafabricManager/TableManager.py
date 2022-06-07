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