function gen_db_info_mysql(ip, port, username, password, dbname, tblname, keylist, namemapping, starttime, endtime, time_column) {
    db = {
        'dbms'        : 'mysql',
        'ip'          : ip,
        'port'        : port,
        'username'    : username,
        'password'    : password,
        'db'          : dbname,
        'table'       : tblname,
        'columns'     : keylist,
        'namemapping' : namemapping,
        'start_time'  : starttime,
        'end_time'    : endtime,
        'time_column' : time_column
    };

    return db;
}