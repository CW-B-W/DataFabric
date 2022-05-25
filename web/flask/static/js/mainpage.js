$(document).ready(function (){

    $("#search_bar").keypress(function(e) {
        var text = $(this).val() + String.fromCharCode(e.which);
        search_query(text);
    });

});

function search_query(text) {

}