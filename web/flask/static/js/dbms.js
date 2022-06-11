$(document).ready(function() {
    // load submodules
    var submodules = ['mysql.js', 'mongodb.js']
    for (var i in submodules) {
        var submodule = submodules[i];
        var path = `/static/js/dbms/${submodule}`
        $("head").append($("<script></script>").attr("src", path));
    }
});

function generateSrcInfo(ip, port, username, password, dbms, dbName, tableName, columns, namemapping, startTime, endTime, timeColumn) {
    dbms = dbms.toLowerCase();
    if (startTime == '') {
        startTime = '1990-01-01 00:00';
    }
    if (endTime == '') {
        endTime   = '2099-12-31 23:55';
    }

    let srcInfo = eval(`generateSrcInfo_${dbms}`)(ip, port, username, password, dbName, tableName, columns, namemapping, startTime, endTime, timeColumn);

    if (timeColumn == '') {
        delete srcInfo['time_column'];
        delete srcInfo['start_time'];
        delete srcInfo['end_time'];
    }

    return srcInfo;
}