$(document).ready(function (){

    $("#SearchInput").keypress(function(e) {
        var text = $(this).val() + String.fromCharCode(e.which);
        querySearchHints(text);
    });

    $("#SearchButton").click(function(e) {
        var searchText = $("#SearchInput").val();
        querySearch(searchText);
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
            showSearchResults(result);
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("[querySearchHints] Connection Failed!");
        }
    });
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

function querySearch(text) {
    $.ajax({
        "type": "GET",
        "dataType": "json",
        "contentType": "application/json",
        "url": `/search?text=${text}`,
        "timeout": 60000,
        success: function(result) {
            showSearchResults(result);
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("[querySearch] Connection Failed!");
        }
    });
}

function showSearchResults(results) {
    $("#SearchResults").empty();
    for (var i in results) {
        var result = results[i];
        var temp = document.getElementById("ResultItemTemplate");
        var clon = $(temp.content.cloneNode(true));
        clon.find("[name=CatalogName]").eq(0).text(result['CatalogName']);
        genTablePreviewHref(clon.find("[name=TableMembers]").eq(0), result['TableMembers'], result['TableIds']);
        clon.find("[name=Description]").eq(0).text(result['Description']);
        $("#SearchResults").append(clon);
    }
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
        clon.attr('href', `/preview?tblid=${tableId}`);
        parent.append(clon);
    }
    tempElem.remove();
}