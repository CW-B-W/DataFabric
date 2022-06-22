import pandas as pd
import happybase

def preview_table_hbase(username, password, ip, port, db, table, limit):
    connection = happybase.Connection(ip, port=int(port))
    happybase_table = happybase.Table(table, connection)
    data = happybase_table.scan(limit = limit)

    columns = list_columns_hbase(username, password, ip, port, db, table)
    b_columns = [str.encode(s) for s in columns]

    my_generator = ((tuple([d[col].decode('utf-8') for col in b_columns])) for k, d in data)
    my_list = list(my_generator)
    my_data = pd.DataFrame(my_list, columns=columns)
    result = my_data.to_dict('records')
    return result

def query_table_hbase(username, password, ip, port, db, table, columns, start_time, end_time, time_column):
    connection = happybase.Connection(ip, port=int(port))
    happybase_table = happybase.Table(table, connection)

    b_columns = [str.encode(s) for s in columns]

    query = generate_query(db, table, columns, start_time, end_time, time_column)
    if query is not None:
        data = happybase_table.scan(columns = b_columns, filter = query)
    else:
        data = happybase_table.scan(columns = b_columns)
    
    my_generator = ((tuple([d[col].decode('utf-8') for col in b_columns])) for k, d in data)
    my_list = list(my_generator)
    my_data = pd.DataFrame(my_list, columns=columns)
    return my_data

def generate_query(db, table, columns, start_time, end_time, time_column):
    if start_time is not None and end_time is not None and time_column is not None:
        family_qualifier = time_column.split(":")
        query = f"SingleColumnValueFilter('{family_qualifier[0]}', '{family_qualifier[1]}', >=, 'binary:{start_time}') AND SingleColumnValueFilter('{family_qualifier[0]}', '{family_qualifier[1]}', <=, 'binary:{end_time}')"
    return query

def list_dbs_hbase(username, password, ip, port):
    return ["Default"]

def list_tables_hbase(username, password, ip, port, db):
    connection = happybase.Connection(ip, port=int(port))
    l = []
    for x in connection.tables():
        l.append(x.decode("utf-8"))
    return sorted(l)

def list_columns_hbase(username, password, ip, port, db, table):
    connection = happybase.Connection(ip, port=int(port))
    table = happybase.Table(table, connection)
    qualifiersSet = set()
    for keyx, valuex in table.scan():
        for keyy,valuey in valuex.items():
            qualifiersSet.add(keyy.decode("utf-8"))
    return sorted(qualifiersSet)