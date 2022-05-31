$(document).ready(function (){
    // const catalogId = $("#CatalogID").text();
    initializePage(catalogId);
});

function initializePage(catalogId) {
    $.ajax({
        "type": "GET",
        "dataType": "json",
        "contentType": "application/json",
        "url": `/get_catalog?catalog_id=${catalogId}`,
        "timeout": 30000,
        success: function(result) {
            renderPage(result);
        },
        error: function(jqXHR, JQueryXHR, textStatus) {
            console.warn("[querySearchHints] Connection Failed!");
        }
    });
}

function renderPage(catalogInfo) {
    console.log(catalogInfo);

    $('#CatalogName').text(catalogInfo['CatalogName']);
}