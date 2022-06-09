from sqlalchemy import create_engine

from .MySQLAccessor.MySQLAccessor import *


def preview_table(
    username: str, password: str, 
    ip: str, port: str, dbms: str, 
    db: str, table: str, limit: int = 5
) -> list:
    """The abstract function for calling functions of desired DBMS

    Args:
        username (str): Username of DBMS
        password (str): Password of DBMS
        ip (str): IP/Hostname of DBMS
        port (str): Port of DBMS. Note that for Oracle it can be like '1521/sid'
        dbms (str): Name of DBMS, e.g. 'MySQL', will be converted to lowercase
        db (str): Desired DB in DBMS
        table (str): Desired Table in DB
        limit (int, optional): Number of rows to return. Defaults to 5.

    Returns:
        list: List containing rows of requested data
    """
    dbms = dbms.lower()
    # Use reflection to call function (Better extensibility for adding new DBMS)
    # The target function name should be like such as 'preview_mysql(...)'
    return globals()[f'preview_{dbms}'](username, password, ip, port, db, table, limit)
