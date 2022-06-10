function gen_db_info_mongodb(ip, port, username, password, dbname, tblname, keylist, namemapping, starttime, endtime, time_column) {
    db = {
        'dbms'        : 'mongodb',
        'ip'          : ip,
        'port'        : port,
        'username'    : username,
        'password'    : password,
        'db'          : dbname,
        'table'       : tblname,
        'columns'     : keylist,
        'namemapping' : namemapping,
        'starttime'   : starttime,
        'endtime'     : endtime,
        'time_column' : time_column
    };

    return db;
}