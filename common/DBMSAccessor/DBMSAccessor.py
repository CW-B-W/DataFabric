from pandas import DataFrame
from sqlalchemy import create_engine

from .MySQLAccessor.MySQLAccessor import *
from .MongoDBAccessor.MongoDBAccessor import *


def preview_table(
    username: str, password: str, 
    ip: str, port: str, dbms: str, 
    db: str, table: str, limit: int = 5
) -> list:
    """Request arbitrary records from DBMS for previewing.
    This is the abstract function for calling functions of desired DBMS.

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
    # The target function name should be like such as 'preview_table_mysql(...)'
    return globals()[f'preview_table_{dbms}'](username, password, ip, port, db, table, limit)

def query_table(
        username: str, password: str,
        ip: str, port, 
        db: str, table: str, key_names: str, 
        start_time: str, end_time: str, time_column: str) -> DataFrame:
    """_summary_

    Args:
        username (str): _description_
        password (str): _description_
        ip (str): _description_
        port (_type_): _description_
        db (str): _description_
        table (str): _description_
        key_names (str): _description_
        start_time (str): _description_
        end_time (str): _description_
        time_column (str): _description_

    Returns:
        DataFrame: _description_
    """
    dbms = dbms.lower()
    # Use reflection to call function (Better extensibility for adding new DBMS)
    # The target function name should be like such as 'query_table_mysql(...)'
    return globals()[f'query_table_{dbms}'](username, password, ip, port, db, table, key_names, start_time, end_time, time_column)
    