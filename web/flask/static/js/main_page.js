$(document).ready(function (){

    $("#SearchInput").keypress(function(e) {
        var keyCode = e.keyCode || e.which;
        if (keyCode == 13) {
            e.preventDefault();
            var text = $(this).val();
            querySearch(text);
        }
        else {
            var text = $(this).val() + String.fromCharCode(keyCode);
            querySearchHints(text);
        }
    });

    $("#SearchButton").click(function(e) {
        var searchText = $("#SearchInput").val();
        querySearch(searchText);
    });

    $("#NextPageButton").click(function(e) {
        querySearch(currentSearch, currentPage+1);
    });
    $("#PrevPageButton").click(function(e) {
        querySearch(currentSearch, currentPage-1);
    });

    $("#Logout").click(function (e) {
        logout();
    });

    queryRecommendations();
});

function queryRecommendations() {
    $.ajax({
        "type": "GET",
        "dataType": "json",
        "contentType": "application/json",
        "url": `/recommend`,
        "timeout": 1000,
        success: function(result) {
            showRecommendResults(result);
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("[querySearchHints] Connection Failed!");
        }
    });
}

function showRecommendResults(results) {
    $("#SearchResults").empty();
    items   = results['items']
    ratings = results['ratings']
    for (var i in items) {
        var item = items[i];
        var temp = document.getElementById("ResultItemTemplate");
        var clon = $(temp.content.cloneNode(true));
        clon.find("[name=CatalogName]").eq(0).text(`${item['CatalogName']} (score: ${ratings[i]})`);
        clon.find("[name=CatalogName]").eq(0).attr('href', `/catalog_page?catalog_id=${item['ID']}`)
        genTablePreviewHref(clon.find("[name=TableMembers]").eq(0), item['TableMembers'], item['TableIds']);
        clon.find("[name=Description]").eq(0).text(item['Description']);
        $("#SearchResults").append(clon);
    }
    $('#CurrentPageLabel').text(`Page: ${currentPage.toString()}`);
}

function querySearchHints(text) {
    $.ajax({
        "type": "GET",
        "dataType": "json",
        "contentType": "application/json",
        "url": `/searchhints?text=${text}`,
        "timeout": 1000,
        success: function(result) {
            showSearchHints(result);
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("[querySearchHints] Connection Failed!");
        }
    });
}

function showSearchHints(result) {
    $("#SearchInput").autocomplete({
        source: result
    });
}

currentSearch = '';
currentPage   = 0;
function querySearch(text, page=1) {
    if (text == '' || page < 1) {
        return;
    }
    $.ajax({
        "type": "GET",
        "dataType": "json",
        "contentType": "application/json",
        "url": `/search?text=${text}&page=${page}`,
        "timeout": 60000,
        success: function(result) {
            showSearchResults(result);
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("[querySearch] Connection Failed!");
        }
    });
    currentSearch = text;
    currentPage   = page;
}

function showSearchResults(results) {
    $("#SearchResults").empty();
    for (var i in results) {
        var result = results[i];
        var temp = document.getElementById("ResultItemTemplate");
        var clon = $(temp.content.cloneNode(true));
        clon.find("[name=CatalogName]").eq(0).text(result['CatalogName']);
        clon.find("[name=CatalogName]").eq(0).attr('href', `/catalog_page?catalog_id=${result['ID']}`)
        genTablePreviewHref(clon.find("[name=TableMembers]").eq(0), result['TableMembers'], result['TableIds']);
        clon.find("[name=Description]").eq(0).text(result['Description']);
        $("#SearchResults").append(clon);
    }
    $('#CurrentPageLabel').text(`Page: ${currentPage.toString()}`);
}

function genTablePreviewHref(tempElem, tableMembers, tableIds) {
    var parent  = tempElem.parent();
    var members = tableMembers.split(',');
    var ids     = tableIds.split(',');
    for (var i in members) {
        var tableId    = ids[i];
        var tablePath  = members[i];
        var tableInfo  = tablePath.split('@');
        var tableDbms  = tableInfo[0];
        var tableDb    = tableInfo[1];
        var tableTable = tableInfo[2];
        var clon = $(tempElem[0].cloneNode(true));
        clon.text(`[${tableDbms}]${tableTable}`);
        clon.attr('href', 'javascript: $( "#TablePreview" ).dialog();');
        clon.attr('href', `javascript: showTablePreview(${tableId});`);
        parent.append(clon);
    }
    tempElem.remove();
}

function showTablePreview(tableId) {
    $.ajax({
        "type": "GET",
        "dataType": "json",
        "contentType": "application/json",
        "url": `/table_preview?table_id=${tableId}`,
        "timeout": 60000,
        success: function(result) {
            var tableContent = result;
            var table = $('#TablePreview');
            setTableContent(table, tableContent);
            $('#TablePreviewDialog').dialog();
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("[querySearch] Connection Failed!");
        }
    });
}

function setTableContent(table, tableContent) {
    table.html("");
    if (tableContent.length == 0) {
        return;
    }
    
    var thead = $(document.createElement("thead"));
    var tbody = $(document.createElement("tbody"));

    // construct thead
    {
        let tr = $(document.createElement('tr'));
        thead.append(tr);
        let columnNames = Object.keys(tableContent[0]);
        columnNames.unshift('#');
        for (let i in columnNames) {
            let columnName = columnNames[i];
            let th = $(document.createElement('th'));
            tr.append(th);
            th.attr('scope', 'col');
            th.text(columnName);
        }
    }
    // construct tbody
    {
        for (let r = 0; r < tableContent.length; ++r) {
            let tr = $(document.createElement('tr'));
            tbody.append(tr);
            let th = $(document.createElement('th'));
            tr.append(th);
            th.attr('scope', 'row');
            th.text((r+1).toString());
            for (let c in tableContent[r]) {
                let columnVal = tableContent[r][c];
                let td = $(document.createElement('td'));
                tr.append(td);
                td.text(columnVal);
            }
        }
    }

    table.append(thead);
    table.append(tbody);
}

function logout() {
    $.ajax({
        "type": "GET",
        "dataType": "json",
        "contentType": "application/json",
        "url": `/logout`,
        "timeout": 60000,
        success: function(result) {
            location.href = '/';
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            location.href = '/';
        }
    });
}