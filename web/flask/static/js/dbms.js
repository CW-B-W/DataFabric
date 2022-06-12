function generateSrcInfo(ip, port, username, password, dbms, dbName, tableName, columns, namemapping, startTime, endTime, timeColumn) {
    dbms = dbms.toLowerCase();

    let func_name = `generateSrcInfo_${dbms}`;

    if (typeof(window[func_name]) === 'undefined') {
        console.info(`Loading module ${dbms}.js`);
        let path = `/static/js/dbms/${dbms}.js`
        $("head").append($("<script></script>").attr("src", path));
    }

    if (startTime == '') {
        startTime = '1990-01-01 00:00';
    }
    if (endTime == '') {
        endTime   = '2099-12-31 23:55';
    }

    let srcInfo = eval(func_name)(ip, port, username, password, dbName, tableName, columns, namemapping, startTime, endTime, timeColumn);

    if (timeColumn == '') {
        delete srcInfo['time_column'];
        delete srcInfo['start_time'];
        delete srcInfo['end_time'];
    }

    return srcInfo;
}