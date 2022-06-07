import json
from InternalDB.MySQLDB import MySQLDB

class TransactionLogging(MySQLDB):
    def __init__(self, transaction_table: str):
        MySQLDB.__init__(self, 'datafabric_transaction')
        self.__transaction_table = transaction_table
        self.query(
            f"CREATE TABLE IF NOT EXISTS {self.__transaction_table} ("
            f"    ID         BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
            f"    Action     VARCHAR(100),"
            f"    Args       TEXT,"
            f"    User       VARCHAR(100),"
            f"    Status     VARCHAR(100),"
            f"    Result     TEXT,"
            f"    Timestamp  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
            f"    Datetime   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
            f");"
        )
        
    def add_transaction(self, action: str, args: dict, username: str, status: str = None, result: str = None):
        status_str = ("'" + status + "'") if status is not None else 'NULL'
        result_str = ("'" + result + "'") if result is not None else 'NULL'
        self.query(
            f"INSERT INTO {self.__transaction_table} (Action, Args, User, status, result) "
            f"VALUES ('{action}', '{json.dumps(args)}', '{username}', {status_str}, {result_str}"
            f");"
        )