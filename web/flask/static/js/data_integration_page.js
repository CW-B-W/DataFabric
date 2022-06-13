$(document).ready(function() {
    for (let i = 0; i < 2; ++i) {
        $(`#ColumnsSelect${i}`).dblclick(function() {
            let columnsSelected = [$('#ColumnsSelect0').val(), $('#ColumnsSelect1').val()];
            addJoinPairs(columnsSelected);            
        });
    }
});

function addJoinPairs(columnsSelected)
{
    if (columnsSelected[0].length == 1 && columnsSelected[1].length == 1) {
        insertJoinTable(columnsSelected[0][0], columnsSelected[1][0]);
    }
    else if (columnsSelected[0].length >= 1 && columnsSelected[1].length <= 1) {
        for (let i in columnsSelected[0]) {
            insertJoinTable(columnsSelected[0][i], '');
        }
    }
    else if (columnsSelected[0].length <= 1 && columnsSelected[1].length >= 1) {
        for (let i in columnsSelected[1]) {
            insertJoinTable('', columnsSelected[1][i]);
        }
    }
    else {
        throw "Undefined behavior";
    }
}

function insertJoinTable(leftKey, rightKey)
{
    if (leftKey == '' && rightKey == '')
        return;

    let entryCnt = $('#JoinTable tbody>tr').length;
    if (typeof(entryCnt) == 'undefined' || entryCnt <= 0) {
        entryCnt = 0;
    }
    $('#JoinTable tbody').append(`
        <tr>
            <th scope="row">${entryCnt+1}</th>
            <td>${leftKey}</td>
            <td>${rightKey}</td>
        </tr>`
    );
}

function popJoinTable()
{
    let entryCnt = $('#JoinTable tbody>tr').length;
    if (entryCnt <= 0)
        return false;
    $('#JoinTable tr:last').remove();
    return true;
}


function sendTaskRequest()
{
    let taskInfo = createTaskInfo();

    $.ajax({
        "type": "POST",
        "dataType": "json",
        "contentType": "application/json",
        "url": "/data_integration",
        "data": JSON.stringify(taskInfo),
        success: function(result) {
            
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            
        }
    });

    showTaskInfo(taskInfo);
}

function createTaskInfo()
{
    let taskInfo = {
        'task_id'   : _uuid(),
        'task_list' : [createJoinSubtask()]
    }
    return taskInfo;
}

function createJoinSubtask()
{
    let srcInfos = []
    for (let i = 0; i < 2; ++i) {
        srcInfos.push(getJoinSrcInfo(i));
    }
    let subtask = {
        'src'      : srcInfos,
        'join_sql' : generateJoinSql(),
        'results'  : {
            'column_order' : generateResultKeys()
        }
    }
    return subtask;
}

function getJoinSrcInfo(srcIdx) {
    let columnsInvolved = getColumnsInvolved(srcIdx);
    return generateSrcInfo(
        tableInfos[srcIdx]['Connection'].split(':')[0],
        tableInfos[srcIdx]['Connection'].split(':')[1],
        tableInfos[srcIdx]['Username'],
        tableInfos[srcIdx]['Password'],
        tableInfos[srcIdx]['DBMS'],
        tableInfos[srcIdx]['DB'],
        tableInfos[srcIdx]['TableName'],
        columnsInvolved,
        Object.fromEntries(columnsInvolved.map(x => [x, toFormattedKey(x)])),
        $(`#TimeStartInput${srcIdx}`).val(),
        $(`#TimeEndInput${srcIdx}`).val(),
        $(`#TimeColumnSelect${srcIdx}`).val()
    );
}

function getColumnsInvolved(srcIdx) {
    let involved = new Set()
    $('#JoinTable >tbody').children().each(function(idx) {
        involved.add($(this).find('td').eq(srcIdx).text());
    });
    involved.delete('');
    return Array.from(involved);
}

function generateJoinSql() {
    let leftKeys  = [];
    let rightKeys = [];
    $('#JoinTable >tbody').children().each(function(idx) {
        leftKeys.push($(this).find('td').eq(0).text());
        rightKeys.push($(this).find('td').eq(1).text());
    });

    let selStats = [];
    let onStats  = [];
    for (let i = 0; i < leftKeys.length; ++i) {
        let leftKey  = leftKeys[i];
        let rightKey = rightKeys[i];
        let newKey   = '';
        if (leftKey != '' && rightKey != '') {
            newKey = toFormattedKey(leftKey);
            selStats.push(`COALESCE(df0.${leftKey}, df1.${rightKey}) as ${newKey}`);
            onStats.push(`df0.${leftKey}=df1.${rightKey}`);
        }
        else if (leftKey != '' && rightKey == '') {
            newKey = toFormattedKey(leftKey);
            selStats.push(`df0.${leftKey} as ${newKey}`);
        }
        else if (leftKey == '' && rightKey != '') {
            newKey = toFormattedKey(rightKey);
            selStats.push(`df1.${rightKey} as ${newKey}`);
        }
        else {
            // DO NOTHING for this case
        }
    }

    let selFullStat = selStats.join(',');
    let onFullStat  = onStats.join(' AND ');

    let sql = `SELECT ${selFullStat} FROM df0 LEFT JOIN df1 ON ${onFullStat};`;

    return sql;
}

function generateResultKeys() {
    let leftKeys  = [];
    let rightKeys = [];
    $('#JoinTable >tbody').children().each(function(idx) {
        leftKeys.push($(this).find('td').eq(0).text());
        rightKeys.push($(this).find('td').eq(1).text());
    });

    let resultKeys = [];
    let keySet = new Set();
    for (let i = 0; i < leftKeys.length; ++i) {
        let leftKey  = leftKeys[i];
        let rightKey = rightKeys[i];
        let resultKey = '';
        if (leftKey != '' && rightKey != '') {
            resultKey = leftKey;
        }
        else if (leftKey != '' && rightKey == '') {
            resultKey = leftKey;
        }
        else if (leftKey == '' && rightKey != '') {
            resultKey = rightKey;
        }
        else {
            // DO NOTHING for this case
        }

        resultKey = toFormattedKey(resultKey);
        resultKeys.push(resultKey);
    }
    
    return resultKeys;
}

function showTaskInfo(taskInfo)
{
    $('#TaskInfoTextarea').text(JSON.stringify(taskInfo));
}

// src: https://cythilya.github.io/2017/03/12/uuid/
function _uuid() {
    let d = Date.now();
    if (typeof performance !== 'undefined' && typeof performance.now === 'function'){
        d += performance.now(); //use high-precision timer if available
    }
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        let r = (d + Math.random() * 16) % 16 | 0;
        d = Math.floor(d / 16);
        return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
    });
}

function toFormattedKey(key)
{
    return key.replace(/[:.]/g, "_").replace(/[@]/g, "").toUpperCase();
}