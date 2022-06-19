from DBMSAccessor import DBMSAccessor
from DatafabricManager import TableManager
import traceback

def scan_and_import(username: str, password: str, ip: str, port: str, dbms: str, db: str = None, tables: list = None) -> list:
    """Scan all the tables in dbms. Then import result to datafabric.TableInfo

    Args:
        username (str): Username of DBMS
        password (str): Password of DBMS
        ip (str): IP/Hostname of DBMS
        port (str): Port of DBMS. Note that for Oracle it can be like '1521/sid' or '49161/xe'
        dbms (str): Name of DBMS, e.g. 'MySQL', will be converted to lowercase.
        db (str): Desired DB in DBMS. If set to None, then scan all possible DBs.
        tables (list): Desired Tables in DBs. If set to None, then scan all possible Tables.

    Returns:
        list: The table_id of added table infos
    """
    
    dbms = dbms.lower()

    added_ids = []

    if db is None:
        try:
            dbs = DBMSAccessor.list_dbs(username, password, ip, port, dbms)
        except:
            traceback.print_exc()
            return added_ids
    else:
        dbs = [db]

    if tables is None:
        for db in dbs:
            try:
                tables = DBMSAccessor.list_tables(username, password, ip, port, dbms, db)
            except:
                traceback.print_exc()
                continue
            for table in tables:
                try:
                    columns = DBMSAccessor.list_columns(username, password, ip, port, dbms, db, table)
                    columns = ','.join(columns)
                    added_ids.append(
                        TableManager.add_table_info(f'{ip}:{port}', dbms, db, table, columns)
                    )
                except:
                    traceback.print_exc()
                    continue
    else:
        for db in dbs:
            for table in tables:
                try:
                    columns = DBMSAccessor.list_columns(username, password, ip, port, dbms, db, table)
                    columns = ','.join(columns)
                    added_ids.append(
                        TableManager.add_table_info(f'{ip}:{port}', dbms, db, table, columns)
                    )
                except:
                    traceback.print_exc()
                    continue
    
    return added_ids
