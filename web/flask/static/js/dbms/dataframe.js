function generateSrcInfo_dataframe(ip, port, username, password, dbName, tableName, columns, namemapping, startTime, endTime, timeColumn) {
    let srcInfo = {
        'dbms'        : 'dataframe',
        'ip'          : '',
        'port'        : '',
        'username'    : '',
        'password'    : '',
        'db'          : '',
        'table'       : '',
        'columns'     : columns,
        'namemapping' : namemapping,
        'start_time'  : startTime,
        'end_time'    : endTime,
        'time_column' : timeColumn
    };

    return srcInfo;
}