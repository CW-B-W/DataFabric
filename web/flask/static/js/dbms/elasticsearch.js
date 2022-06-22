function generateSrcInfo_elk(ip, port, username, password, dbName, tableName, columns, namemapping, startTime, endTime, timeColumn) {
    let srcInfo = {
        'dbms'        : 'elasticsearch',
        'ip'          : ip,
        'port'        : port,
        'username'    : username,
        'password'    : password,
        'db'          : dbName,
        'table'       : tableName,
        'columns'     : columns,
        'namemapping' : namemapping,
        'start_time'  : startTime,
        'end_time'    : endTime,
        'time_column' : timeColumn
    };

    return srcInfo;
}