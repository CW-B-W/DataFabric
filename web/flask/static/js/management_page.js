$(document).ready(function (){
    $("#CatalogSearchText").keypress(function(e) {
        var keyCode = e.keyCode || e.which;
        if (keyCode == 13) {
            e.preventDefault();
            var text = $(this).val();
            catalogSearch(text);
        }
    });

    $("#TableSearchText").keypress(function(e) {
        var keyCode = e.keyCode || e.which;
        if (keyCode == 13) {
            e.preventDefault();
            var text = $(this).val();
            tableSearch(text);
        }
    });

    $('#CatalogSelect').change(function() {
        let catalogId = $('#CatalogSelect').find(':selected').val();
        getCatalog(catalogId);
    });

    $('#TableAddButton').click(function() {
        let catalogId = $('#CatalogSelect').find(':selected').val();
        let tableId   = $('#TableSelect').find(':selected').val();
        addTableIntoCatalog(catalogId, tableId);
    });
});

function catalogSearch(searchText) {
    $.ajax({
        "type": "GET",
        "dataType": "json",
        "contentType": "application/json",
        "url": `/catalog/search?text=${searchText}`,
        "timeout": 30000,
        success: function(result) {
            showCatalogSearchResults(result);
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("Connection Failed!");
        }
    });
}

function showCatalogSearchResults(result) {
    let root = $('#CatalogSelect');
    root.empty();
    for (let i in result) {
        let catalog   = result[i];
        let newOption = $(document.createElement('option'));
        newOption.text(catalog['CatalogName']);
        newOption.val(catalog['ID']);
        root.append(newOption);
    }
    getCatalog(result[0]['ID']);
}

function getCatalog(catalogId) {
    $.ajax({
        "type": "GET",
        "dataType": "json",
        "contentType": "application/json",
        "url": `/catalog/get?catalog_id=${catalogId}`,
        "timeout": 30000,
        success: function(result) {
            showCatalog(result);
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("Connection Failed!");
        }
    });
}

function showCatalog(result) {
    clearTable();
    let tableIds = result['TableIds'].split(',');
    for (let i in tableIds) {
        let tableId = tableIds[i];
        $.ajax({
            "type": "GET",
            "dataType": "json",
            "contentType": "application/json",
            "url": `/tableinfo/get?table_id=${tableId}`,
            "timeout": 30000,
            success: function(result) {
                appendToTable(result);
            },
            error: function(jqXHR, JQueryXHR, textStatus) {
                console.warn("Connection Failed!");
            }
        });
    }
}

function clearTable() {
    let tableElem = $('#CatalogTables').find('tbody');
    tableElem.find('tr').remove();
}

function appendToTable(result) {
    let id   = result['ID'];
    let name = result['TableName'];
    let conn = result['Connection'];
    let dbms = result['DBMS'];
    let db   = result['DB'];

    let root = $('#CatalogTables').find('tbody');

    let newRow = $($('#TableRowTemplate').html()).clone();
    root.append(newRow);
    newRow.children().eq(0).text(id);
    newRow.children().eq(1).text(name);
    newRow.children().eq(2).text(conn);
    newRow.children().eq(3).text(dbms);
    newRow.children().eq(4).text(db);
    newRow.children().eq(5).val(id);
    newRow.children().eq(5).click(function() {
        let catalogId = $('#CatalogSelect').find(':selected').val();
        let tableId   = $(this).val();
        delTableFromCatalog(catalogId, tableId);
    });
}

function removeFromTable(tableId) {
    $('#CatalogTables').find('th').filter(function() {
        return $(this).text() == tableId;
    }).parent().remove();
}

function tableSearch(searchText) {
    $.ajax({
        "type": "GET",
        "dataType": "json",
        "contentType": "application/json",
        "url": `/tableinfo/search?text=${searchText}`,
        "timeout": 30000,
        success: function(result) {
            showTableSearchResults(result);
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("Connection Failed!");
        }
    });
}

function showTableSearchResults(result) {
    let root = $('#TableSelect');
    root.empty();
    for (let i in result) {
        let catalog   = result[i];
        let newOption = $(document.createElement('option'));
        newOption.text(catalog['TableName']);
        newOption.val(catalog['ID']);
        root.append(newOption);
    }
}

function addTableIntoCatalog(catalogId, tableId) {
    $.ajax({
        "type": "GET",
        "contentType": "text/plain",
        "url": `/catalog/manage?action=add_table&catalog_id=${catalogId}&table_id=${tableId}`,
        "timeout": 30000,
        success: function(result) {
            $.ajax({
                "type": "GET",
                "dataType": "json",
                "contentType": "application/json",
                "url": `/tableinfo/get?table_id=${tableId}`,
                "timeout": 30000,
                success: function(result) {
                    appendToTable(result);
                },
                error: function(jqXHR, JQueryXHR, textStatus) {
                    console.warn("Connection Failed!");
                }
            });
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("Connection Failed!");
        }
    });
}

function delTableFromCatalog(catalogId, tableId) {
    $.ajax({
        "type": "GET",
        "contentType": "text/plain",
        "url": `/catalog/manage?action=del_table&catalog_id=${catalogId}&table_id=${tableId}`,
        "timeout": 30000,
        success: function(result) {
            removeFromTable(tableId);
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("Connection Failed!");
        }
    });
}