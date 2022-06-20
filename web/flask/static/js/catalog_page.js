$(document).ready(function (){
    initializePage(catalogId);

    $('#IntegrateButton').click(function() {
        showIntegratePage();
    });
});

function initializePage(catalogId) {
    $.ajax({
        "type": "GET",
        "dataType": "json",
        "contentType": "application/json",
        "url": `/catalog/get?catalog_id=${catalogId}`,
        "timeout": 30000,
        success: function(result) {
            renderPage(result);
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("Connection Failed!");
        }
    });
}

function renderPage(catalogInfo) {
    $('#CatalogName').text(catalogInfo['CatalogName']);
    $('#CatalogDescription').text(catalogInfo['Description']);
    $('#CatalogKeywords').text(catalogInfo['Keywords']);

    renderTables(catalogInfo['TableIds'].split(','), catalogInfo['TableMembers'].split(','));
}

function renderTables(tableIds, tableMembers) {
    let root = $('#CatalogTables');
    let temp = $('#TableTemplate');
    for (let i in tableIds) {
        let tableId     = tableIds[i];
        let tableMember = tableMembers[i];
        
        let newElem = $(temp.html()).clone();
        newElem.find('[name=TableName]').text(`${tableId}: ${tableMember}`);
        newElem.find('input[type=checkbox]').val(tableId);
        root.append(newElem);
    }
}

function showIntegratePage() {
    let integrateList = [];
    $('#CatalogTables').find('input[type=checkbox]').each(function(){
        if ($(this).is(':checked')) {
            integrateList.push($(this).val());
        }
    });
    if (integrateList.length > 0) {
        let url = `/data_integration?table_ids=${integrateList.join(',')}`;
        window.open(url, '_blank').focus();
    }
}