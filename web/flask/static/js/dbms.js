$(document).ready(function() {
    // load submodules
    var submodules = ['mysql.js', 'mongodb.js']
    for (var i in submodules) {
        var submodule = submodules[i];
        var path = `/static/js/dbms/${submodule}`
        $("head").append($("<script></script>").attr("src", path));
    }
});

function gen_db_info(ip, port, username, password, dbms, dbname, tblname, keylist, namemapping, starttime, endtime, time_column) {
    return eval(`gen_db_info_${dbms}`)(ip, port, username, password, dbname, tblname, keylist, namemapping, starttime, endtime, time_column)
}