let bob;
let map;
let uploadLayer;
let adminHighlightLayer;
const admin_layer_url = location.hostname === "localhost" ||
location.hostname === "127.0.0.1" ||
location.hostname === "192.168.1.132"
    ? "https://climateserv2.servirglobal.net/servirmap_102100/?&crs=EPSG%3A102100"
    : window.location.origin + "/servirmap_102100/?&crs=EPSG%3A102100";
function RestartClimateSERV(which) {
    $.ajax({
        url: "/api/restartClimateSERV/",
        async: true,
        crossDomain: true
    }).fail(function (jqXHR, textStatus, errorThrown) {
        console.warn(jqXHR + textStatus + errorThrown);
    }).done(function (data) {
        alert("done restart");
    });
}

function showAOI(which) {
    let aoi;

    bob = which;
    try {
        uploadLayer.clearLayers();
        if (adminHighlightLayer) {
            adminHighlightLayer.remove();
        }
        aoi = JSON.parse(which.innerText);
        if(aoi["Admin Boundary"]){
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

function deleteRow(which) {
    console.log("Delete: " + which);
}

function enable_filter_view(which) {
    if (which === "time_requested") {
        $("#normal_filter").hide();
        $("#time_range_filter").show();
    } else {
        $("#time_range_filter").hide();
        $("#normal_filter").show();
    }
    console.log(which);
}

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
}

$(function () {
    initMap();
});