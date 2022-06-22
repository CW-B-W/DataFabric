$(document).ready(function (){
    initializePage();

    $("#ConnText").keypress(function(e) {
        var keyCode = e.keyCode || e.which;
        if (keyCode == 13) {
            e.preventDefault();
            let conn = $('#ConnText').val().split(':');
            let ip   = conn[0];
            let port = conn[1];
            let dbms = $('#DbmsSelect').val();
            scan(ip, port, dbms)
        }
    });

    $('#AddButton').click(function() {
        addToDatafabric();
    });
});

function initializePage() {
    $.ajax({
        "type": "GET",
        "dataType": "json",
        "contentType": "application/json",
        "url": `/supported_dbms`,
        "timeout": 30000,
        success: function(result) {
            let root = $('#DbmsSelect');
            for (i in result) {
                let dbms   = result[i];
                let newOpt = $(document.createElement('option'));
                newOpt.text(dbms);
                newOpt.val(dbms);
                root.append(newOpt);
            }
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("Connection Failed!");
        }
    });
}

function scan(ip, port, dbms)
{
    $('#ScannedTables').find('tbody').find('tr').empty();
    $.ajax({
        "type": "GET",
        "dataType": "json",
        "contentType": "application/json",
        "url": `/metadata_scanner/scan?ip=${ip}&port=${port}&dbms=${dbms}`,
        "timeout": 30000,
        success: function(result) {
            for (let i in result) {
                let tableInfo = result[i];
                appendToTable(tableInfo, i);
            }
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("Connection Failed!");
        }
    });
}

function appendToTable(tableInfo, idx) {
    let root   = $('#ScannedTables').find('tbody');
    let newRow = $($('#TableRowTemplate').html()).clone();
    root.append(newRow);
    newRow.children().eq(0).text(tableInfo['TableName']);
    newRow.children().eq(1).text(tableInfo['Connection']);
    newRow.children().eq(2).text(tableInfo['DBMS']);
    newRow.children().eq(3).text(tableInfo['DB']);
    newRow.children().eq(4).text(tableInfo['Columns']);
    newRow.children().eq(5).children().eq(0).val(idx+1); // children index of the table
}

function addToDatafabric() {
    let addList = [];
    let checked = $('#ScannedTables').find('tbody').find(':checked');
    checked.each(function() {
        let row = $(this).parent().parent();
        addList.push({
            'TableName' : row.children().eq(0).text(),
            'Connection' : row.children().eq(1).text(),
            'DBMS' : row.children().eq(2).text(),
            'DB' : row.children().eq(3).text(),
            'Columns' : row.children().eq(4).text()
        });
    });
    $.ajax({
        "type": "POST",
        "dataType": "json",
        "contentType": "application/json",
        "url": "/tableinfo/add",
        "data": JSON.stringify(addList),
        success: function(result) {
            alert(result);
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            alert('error');
        }
    });
}