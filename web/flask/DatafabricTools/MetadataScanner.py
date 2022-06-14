from DBMSAccessor import DBMSAccessor
from DatafabricManager import TableManager

def scan_and_import(username: str, password: str, ip: str, port: str, dbms: str) -> list:
    """Scan all the tables in dbms. Then import result to datafabric.TableInfo

    Args:
        username (str): Username of DBMS
        password (str): Password of DBMS
        ip (str): IP/Hostname of DBMS
        port (str): Port of DBMS. Note that for Oracle it can be like '1521/sid'
        dbms (str): Name of DBMS, e.g. 'MySQL', will be converted to lowercase

    Returns:
        list: The table_id of added table infos
    """

    added_ids = []

    try:
        dbs = DBMSAccessor.list_dbs(username, password, ip, port, dbms)
    except:
        return added_ids

    for db in dbs:
        try:
            tables = DBMSAccessor.list_tables(username, password, ip, port, dbms, db)
        except:
            continue
        for table in tables:
            try:
                columns = DBMSAccessor.list_columns(username, password, ip, port, dbms, db, table)
                columns = ','.join(columns)
                added_ids.append(
                    TableManager.add_table_info(f'{ip}:{port}', dbms, db, table, columns)
                )
            except:
                continue
    
    return added_ids
