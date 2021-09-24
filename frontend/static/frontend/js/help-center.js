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
    const id = '"' + metaData.id + '"';
    if ($("#dialog").hasClass("ui-dialog-content") &&
        $("#dialog").dialog("isOpen")) {
        $('#dialog').dialog('close');
    }
    var html = '<div class="data-cards">';
    html +=
        '<h1 style="width:100%; font-size: 120%; text-align: center; font-weight: 600; margin: 50px;">' +
        metaData.title +
        "</h1>";
    html += '<div class="col-md-12"> ';
    html += '<div style="width:50%; float:right;margin-left: 20px;">';
    html +=
        '<img src="' +
        metaData.thumbnail +
        '" style="max-width:100%; width:100%" onerror="imgError(this)" class="meta-preview">';
    html += "<button onclick='goto_full_record(" + id +")' class='meta-btn'>View full record</button></div>";

    html += '<p class="abstract">' + metaData.abstract + "</p>";
    html += "<h4 style='font-size: 120%; font-weight: 600; margin-bottom: 25px;'>Credit:</h4>";
    html += "<p>" + metaData.credit + "</p>";
    html += "</div></div>";

    $("#dialog").html(html);
    $("#dialog").dialog({
        title: metaData.title,
        width: "200px",
        height: "200px",
        draggable: true,
        resizable: false,
        open: function () {
            //set position to top left
            $(".ui-dialog-content").scrollTop(0);
            scroll_freeze = document.documentElement.scrollTop;
            $('body').css('top', -(document.documentElement.scrollTop) + 'px')
                .addClass('noscroll');
            $("#dialog").parent().css({top:$("body").offset().top * -1 + 70, left:50})
            $("#dialog").parent().animate({
                height: $(window).height() - 140,
                width: $(window).width() - 100
              }, 1000 , "linear", function(){
console.log("in callback")

               fix_dialog();
                $(".ui-dialog-titlebar-close")[0].addEventListener("click", function(){
                   $('#dialog').dialog('close');
                });
            });
            $("#dialog").animate({
                height: $(window).height() - 140,
                width: $(window).width() - 100
              }, 1000 , "swing", function(){
console.log("in callback")
               fix_dialog()
            });



            //after animation do
            /*

             */
        },
        close: function () {
            $('body').removeClass('noscroll');
            document.documentElement.scrollTop = scroll_freeze;
        }
    });
}

function fix_dialog(){
     $("#dialog").dialog({
                  width: $(window).width() - 100,
                 height: $(window).height() - 140,
                })
               // $("#dialog").css({height:"unset"})
}

function goto_full_record(which){
    window.open("https://gis1.servirglobal.net/geonetwork/srv/eng/catalog.search#/metadata/" + which, "_meta_record");
}

function imgError(which){

    which.onerror=null;
    which.src=static_url + 'frontend/img/data_preview_unavailable.jpg';
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
                id: which,
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
    // try{
    //       $("#helpCarousel").carousel({
    //         interval: 5000,
    //         transition: "transform 4s ease, opacity 2s ease-out"
    //       });
    // } catch(e){}
    // try{
    //     $('#help-text').html($('.active > .media-content').html());
    //     $('.carousel').on('slide.bs.carousel', function () {
    //         $('#help-text').fadeOut( "slow", function() {
    //             // Animation complete.
    //           });
    //         });
    //     $('.carousel').on('slid.bs.carousel', function () {
    //         $('#help-text').html($('.active > .media-content').html()).show();
    //         });
    // } catch(e){}

     fix_sidebar();

    $(window).scroll(function(e){
     fix_sidebar();
    });
});

function fix_sidebar(){
     const $el = $('.sidebar');
      // const isPositionFixed = ($el.css('position') == 'fixed');
      // if ($(this).scrollTop() > 60 && !isPositionFixed){
      //   $el.css({'position': 'fixed', 'top': '0px'});
      // }
      // if ($(this).scrollTop() < 60 && isPositionFixed){
      //   $el.css({'position': 'absolute', 'top': '60px'});
      // }


      if((window.innerHeight + window.scrollY) >= (document.body.offsetHeight - 50)){
          $el.css({'bottom': '50px'})
      } else{
          $el.css({'bottom': 0})
      }
}

// jQuery(function ($) {
//     $('.carousel').carousel();
//     var caption = $('div.carousel-item:nth-child(1) .media-content');
//     $('.new-caption-area').html(caption.html());
//     caption.css('display', 'none');
//
//     $(".carousel").on('slide.bs.carousel', function (evt) {
//         var caption = $('div.carousel-item:nth-child(' + ($(evt.relatedTarget).index() + 1) + ') .media-content');
//         $('.new-caption-area').html(caption.html());
//         caption.css('display', 'none');
//     });
// });