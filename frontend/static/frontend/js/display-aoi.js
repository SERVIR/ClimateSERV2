let map;
let uploadLayer;
let adminHighlightLayer;
const admin_layer_url = location.hostname === "localhost" ||
location.hostname === "127.0.0.1" ||
location.hostname === "192.168.1.132"
    ? "https://climateserv2.servirglobal.net/servirmap_102100/?&crs=EPSG%3A102100"
    : window.location.origin + "/servirmap_102100/?&crs=EPSG%3A102100";

function initMap() {
    map = L.map("map", {
        zoom: 3,
        fullscreenControl: true,
        center: [38.0, 15.0],
    });
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(map);
    uploadLayer = L.geoJson().addTo(map);
    const cb = $(".leaflet-control-zoom.leaflet-bar.leaflet-control")[0];
    const show_json = $('<a/>',
        {
            style: "color:white",
            text: 'JSON',
            click: function () {
                open_json(true);
            }
        });
    $(cb).append(show_json);
    showAOI(passed_aoi);
}

function open_json(clicked){
    let dialog = $("#dialog");
    if ((dialog.hasClass("ui-dialog-content") && dialog.dialog('isOpen')) || clicked === true) {
         if (dialog.hasClass("ui-dialog-content") && dialog.dialog('isOpen')) {
             dialog.dialog("close");
         }
        dialog = $("#dialog");
                const wWidth = $(window).width();
                dialog.html(passed_aoi);
                dialog.dialog({
                    title: "AOI JSON",
                    resizable: {handles: "se"},
                    width: wWidth * 0.75,
                    height: "auto",
                    position: {
                        my: "center",
                        at: "center",
                        of: window
                    }
                });
                $("button.ui-button.ui-corner-all.ui-widget.ui-button-icon-only.ui-dialog-titlebar-close").text("");
    }
}

$(function () {
    initMap();
    $( window ).bind( "resize", open_json ); //Remove this if it's not needed. It will react when window changes size.

});

function htmlDecode(input) {
    const e = document.createElement('textarea');
    e.innerHTML = input;
    // handle case of empty input
    return e.childNodes.length === 0 ? "" : e.childNodes[0].nodeValue;
}

function showAOI(which) {
    let aoi;

    bob = which;
    try {
        uploadLayer.clearLayers();
        if (adminHighlightLayer) {
            adminHighlightLayer.remove();
        }
        aoi = JSON.parse(htmlDecode(which));
        if (aoi["Admin Boundary"]) {
            adminHighlightLayer = L.tileLayer.wms(
                admin_layer_url,
                {
                    layers: aoi["Admin Boundary"] + "_highlight",
                    format: "image/png",
                    transparent: true,
                    styles: "",
                    TILED: true,
                    VERSION: "1.3.0",
                    feat_ids: aoi["FeatureIds"].join(),
                }
            );
            map.addLayer(adminHighlightLayer);
        } else {
            uploadLayer.addData(aoi);
            try {
                map.fitBounds(uploadLayer.getBounds());
            } catch (e) {
                map.fitBounds([
                    [data.bbox[1], data.bbox[0]],
                    [data.bbox[3], data.bbox[2]],
                ]);
            }
        }
    } catch (e) {
        try {

        } catch (e) {

        }
    }
    console.log(aoi);
}