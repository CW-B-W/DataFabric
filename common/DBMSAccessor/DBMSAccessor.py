from importlib import import_module
from pandas import DataFrame
from sqlalchemy import create_engine

loaded_module = {}

def get_module_func(mod_name, func_name):
    if mod_name not in loaded_module:
        loaded_module[mod_name] = import_module(f'.{mod_name}.{mod_name}', f'DBMSAccessor')
    mod = loaded_module[mod_name]
    return getattr(mod, func_name)


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
    func = get_module_func(f'{dbms}Accessor', f'preview_table_{dbms}')
    return func(username, password, ip, port, db, table, limit)

def query_table(
        username: str, password: str,
        ip: str, port, dbms: str, 
        db: str, table: str, columns: list, 
        start_time: str = None, end_time: str = None, time_column: str = None) -> DataFrame:
    """Query data from DBMS. Return pandas.DataFrame

    Args:
        username (str): Username of DBMS
        password (str): Password of DBMS
        ip (str): IP/Hostname of DBMS
        port (str): Port of DBMS. Note that for Oracle it can be like '1521/sid'
        db (str): Desired DB in DBMS
        table (str): Desired Table in DB
        columns (list): Desired Keys(Columns) in Table
        start_time (str, optional): Query start time. Defaults to None.
        end_time (str, optional): Query end time. Defaults to None.
        time_column (str, optional): The column of start_time, end_time. Defaults to None.

    Returns:
        DataFrame: pandas.DataFrame containing requested data
    """
    dbms = dbms.lower()
    # Use reflection to call function (Better extensibility for adding new DBMS)
    # The target function name should be like such as 'query_table_mysql(...)'
    func = get_module_func(f'{dbms}Accessor', f'query_table_{dbms}')
    return func(username, password, ip, port, db, table, columns, start_time, end_time, time_column)
