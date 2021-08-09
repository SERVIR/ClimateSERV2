$(document).ready(function () {
    var metaid = getParameterByName("metaid");
    if (metaid) {
        console.log("metaid not null");
        getMetaData(metaid);
    }
    adjustCards();
});

function adjustCards() {
    $(".help-card").animate({height: "auto"}, function () {
        $(".help-card").height("auto");
        var maxHeight = Math.max.apply(
            null,
            $(".help-card.text-card")
                .map(function () {
                    return $(this).height();
                })
                .get()
        );
        $(".help-card").height(maxHeight);
        $(".help-image").height(maxHeight);
    });
}

$(window).resize(function () {
    $(".ui-dialog").css({
        width: $(window).width() - 100,
        height: $(window).height() - 140,
    });
    $("#dialog").css({
        height: "calc(100% - 60px)",
    });
    adjustCards();
});

let scroll_freeze = 0;

function openDialog(metaData) {
    // close dialog if open to ensure proper load and scroll
    if ($("#dialog").hasClass("ui-dialog-content") &&
        $("#dialog").dialog("isOpen")) {
        $('#dialog').dialog('close');
    }
    var html = '<div class="data-cards">';
    html +=
        '<h1 style="width:100%; text-align: center;">' +
        metaData.title +
        "</h1>";
    html += '<div class="col-md-6"> ';
    html += '<p class="abstract">' + metaData.abstract + "</p>";
    html += "<h4>Credit:</h4>";
    html += "<p>" + metaData.credit + "</p>";
    html += "</div>";
    html += '<div class="col-md-6">';
    html +=
        '<img src="' +
        metaData.thumbnail +
        '" style="max-width:100%; width:100%">';
    html += "</div>";
    html += "</div>";

    $("#dialog").html(html);
    $("#dialog").dialog({
        title: metaData.title,
        width: $(window).width() - 100,
        height: $(window).height() - 140,
        draggable: true,
        resizable: false,
        open: function () {
            $(".ui-dialog-content").scrollTop(0);
            scroll_freeze = document.documentElement.scrollTop;
            $('body').css('top', -(document.documentElement.scrollTop) + 'px')
                .addClass('noscroll');
        },
        close: function () {
            $('body').removeClass('noscroll');
            document.documentElement.scrollTop = scroll_freeze;
        }
    });
}

var holdme;

function getMetaData(which) {
    //sample http://gis1.servirglobal.net:8080/geonetwork/srv/api/records/ec5da150-d043-414a-80cd-1b750debd805/formatters/xml
    $.get(
        "https://gis1.servirglobal.net/geonetwork/srv/api/records/" +
        which +
        "/formatters/xml",
        function (xml) {
            var jsonObj = $.xml2json(xml);
            holdme = jsonObj;

            var title = "No Title implemented";
            try {
                title = jsonObj["#document"]["gmd:MD_Metadata"][
                    "gmd:identificationInfo"
                    ]["gmd:MD_DataIdentification"]["gmd:citation"]["gmd:CI_Citation"][
                    "gmd:title"
                    ]["gco:CharacterString"]["_"]
                    ? jsonObj["#document"]["gmd:MD_Metadata"][
                        "gmd:identificationInfo"
                        ]["gmd:MD_DataIdentification"]["gmd:citation"][
                        "gmd:CI_Citation"
                        ]["gmd:title"]["gco:CharacterString"]["_"]
                    : jsonObj["#document"]["gmd:MD_Metadata"][
                        "gmd:identificationInfo"
                        ]["gmd:MD_DataIdentification"]["gmd:citation"][
                        "gmd:CI_Citation"
                        ]["gmd:title"]["gco:CharacterString"];
            } catch (e) {
            }
            var abstract = "No Abstract implemented";
            try {
                abstract = jsonObj["#document"]["gmd:MD_Metadata"][
                    "gmd:identificationInfo"
                    ]["gmd:MD_DataIdentification"]["gmd:abstract"][
                    "gco:CharacterString"
                    ]["_"]
                    ? jsonObj["#document"]["gmd:MD_Metadata"][
                        "gmd:identificationInfo"
                        ]["gmd:MD_DataIdentification"]["gmd:abstract"][
                        "gco:CharacterString"
                        ]["_"]
                    : jsonObj["#document"]["gmd:MD_Metadata"][
                        "gmd:identificationInfo"
                        ]["gmd:MD_DataIdentification"]["gmd:abstract"][
                        "gco:CharacterString"
                        ];
            } catch (e) {
                console.log("why");
            }
            var thumbnail = "img/no_data_preview.png";
            try {
                thumbnail = jsonObj["#document"]["gmd:MD_Metadata"][
                    "gmd:identificationInfo"
                    ]["gmd:MD_DataIdentification"]["gmd:graphicOverview"][
                    "gmd:MD_BrowseGraphic"
                    ]["gmd:fileName"]["gco:CharacterString"]["_"]
                    ? jsonObj["#document"]["gmd:MD_Metadata"][
                        "gmd:identificationInfo"
                        ]["gmd:MD_DataIdentification"]["gmd:graphicOverview"][
                        "gmd:MD_BrowseGraphic"
                        ]["gmd:fileName"]["gco:CharacterString"]["_"]
                    : jsonObj["#document"]["gmd:MD_Metadata"][
                        "gmd:identificationInfo"
                        ]["gmd:MD_DataIdentification"]["gmd:graphicOverview"][
                        "gmd:MD_BrowseGraphic"
                        ]["gmd:fileName"]["gco:CharacterString"];
            } catch (e) {
            }
            var credit = "No Credits implemented";
            try {
                credit =
                    jsonObj["#document"]["gmd:MD_Metadata"][
                        "gmd:identificationInfo"
                        ]["gmd:MD_DataIdentification"]["gmd:credit"][
                        "gco:CharacterString"
                        ];
            } catch (e) {
            }

            openDialog({
                title: title.replaceAll("\\\\n", "<br />"),
                abstract: anchorme({
                    input: abstract
                        .replaceAll("\\\\n", "<br />"),
                    options: {
                        attributes: {
                            target: "_blank",
                            class: "carousel-control site-link",
                        },
                    }
                }),
                thumbnail: thumbnail,
                credit: anchorme({
                    input: credit,
                    options: {
                        attributes: {
                            target: "_blank",
                            class: "carousel-control site-link",
                        },
                    }
                })
                ,
            });
        }
    );
}

function activate(which) {
    $(".help-tabs").removeClass("active");
    $("#" + which).addClass("active");
    $("[id$=help-section]").hide()
    $("#" + which + "-help-section").show();
}


function toggleHelpImage(which) {
    let img = $(which);
    img.toggleClass("fullSize");
    img.parent().toggleClass("fullSize");


}

var debugme;
var debugme2;
$(function () {
    try {
        $(".collapse").on('show.bs.collapse', function (e) {
            console.log("show");
            debugme = this;
            debugme2 = e.target;
            $($(this).prev().first().children()[0]).removeClass("fa-angle-down").addClass("fa-angle-up");
        }).on('hide.bs.collapse', function (e) {
            console.log("hide");
            $($(this).prev().first().children()[0]).removeClass("fa-angle-up").addClass("fa-angle-down");
        });
    } catch (e) {
        console.log("aoiOptionToggle Failed");
    }
});