/** Global Variables */
let active_basemap = "Gsatellite";
let map;
let passedLayer;
let overlayMaps = {};
let adminLayer;
let adminHighlightLayer;
let highlightedIDs = [];
let uploadLayer;
let baseLayers;
let drawnItems;
let drawToolbar;
let styleOptions = [];
const api_url = "https://climateserv.servirglobal.net"; //"http://127.0.0.1:8000/"; //
const admin_layer_url = "https://climateserv2.servirglobal.net/servirmap_102100/?&crs=EPSG%3A102100";

/**
 * Evokes getLayerHtml, appends the result to the layer-list, then
 * creates the map layer and stores it in the overlayMaps array
 * @param {object} item  - layer json object
 */
function createLayer(item) {
    // Create actual layer and put in overlayMaps
    overlayMaps[item.id + "TimeLayer"] = L.timeDimension.layer.wms(
        L.tileLayer.wms(item.url + "&crs=EPSG%3A3857", {
            layers: item.layers,
            format: "image/png",
            transparent: true,
            colorscalerange: item.colorrange,
            abovemaxcolor: "transparent",
            belowmincolor: "transparent",
            numcolorbands: 100,
            styles: item.styles,
        }),
        {
            updateTimeDimension: true,
        }
    );
    overlayMaps[item.id + "TimeLayer"].id = item.id;

    if (item.id.includes(passedLayer)) {
        console.log(item.id);
        try {
            document.getElementById(item.id).checked = true;
            toggleLayer(item.id + "TimeLayer");
        } catch (e) {
        }
    }
}

/**
 * Helper function to get a layer out of globalLayerArray
 * @param {string} which - name of layer requesting
 * @returns layer json object
 */
function getLayer(which) {
    return client_layers.find(
        (item) => item.id === which.replace("TimeLayer", "")
    );
}

/**
 * Retrieves the current TDS styles available and stores them in the
 * styleOptions array, which will be used to load the styles dropdown box
 */
function buildStyles() {
    $.get(client_layers[0].url + "&request=GetCapabilities", function (xml) {
        const jsonObj = $.xml2json(xml);
        const styles =
            jsonObj["#document"]
                .WMS_Capabilities
                .Capability
                .Layer
                .Layer
                .Layer
                .Style
                .sort(function (a, b) {
                    const x = a.Name;
                    const y = b.Name;
                    return ((x < y) ? -1 : ((x > y) ? 1 : 0));
                });
        for (let i = 0; i < styles.length; i++) {
            styleOptions.push({
                val: styles[i].Name,
                text: styles[i].Name,
            });
        }
    });
}

/**
 * Populates the Settings box for the specific layer and opens the settings popup.
 * @param {string} which - Name of layer to open settings for
 */
function openSettings(which) {
    const active_layer = getLayer(which);

    let settingsHtml = "";
    if (active_layer.dataset === "model") {
        // need to get available ensembles then
        // add checkboxes for each to enable turning on and off
        // will likely have to adjust the apply button as well since
        // it currently works on overlayMaps[which]

        settingsHtml += "Get the Ens info to build the checkboxes";
    }

    settingsHtml += baseSettingsHtml();
    let dialog = $("#dialog");
    dialog.html(settingsHtml);
    dialog.dialog({
        title: "Settings",
        resizable: {handles: "se"},
        width: "auto",
        height: "auto",
        position: {
            my: "center",
            at: "center",
            of: window
        }
    });
    $(".ui-dialog-title").attr("title", "Settings");
    $(styleOptions).each(function () {
        $("#style_table").append(
            $("<option>").attr("value", this.val).text(this.text)
        );
    });

    $("#style_table").val(overlayMaps[which]._baseLayer.wmsParams.styles);

    const slider = document.getElementById("opacityctrl");
    slider.value = overlayMaps[which].options.opacity;
    slider.oninput = function () {
        overlayMaps[which].setOpacity(this.value);
    };

    const applyStylebtn = document.getElementById("applyStylebtn");

    applyStylebtn.onclick = function () {
        if (map.hasLayer(overlayMaps[which])) {
            map.removeLayer(overlayMaps[which]);
        }
        overlayMaps[which] = L.timeDimension.layer.wms(
            L.tileLayer.wms(active_layer.url + "&crs=EPSG%3A3857", {
                layers: active_layer.layers,
                format: "image/png",
                transparent: true,
                colorscalerange:
                    document.getElementById("range-min").value +
                    "," +
                    document.getElementById("range-max").value,
                abovemaxcolor: "transparent",
                belowmincolor: "transparent",
                numcolorbands: 100,
                styles: $("#style_table").val(),
            }),
            {
                updateTimeDimension: true,
            }
        );
        map.addLayer(overlayMaps[which]);
        document.getElementById(which.replace("TimeLayer", "")).checked = true;
        active_layer.styles = $("#style_table").val();
        active_layer.colorrange = document.getElementById("range-min").value +
            "," +
            document.getElementById("range-max").value
        overlayMaps[which].options.opacity = document.getElementById("opacityctrl").value;
        overlayMaps[which].setOpacity(overlayMaps[which].options.opacity);
    };
    // Update min/max
    document.getElementById("range-min").value =
        overlayMaps[which]._baseLayer.options.colorscalerange.split(",")[0];
    document.getElementById("range-max").value =
        overlayMaps[which]._baseLayer.options.colorscalerange.split(",")[1];
    $("button.ui-button.ui-corner-all.ui-widget.ui-button-icon-only.ui-dialog-titlebar-close").bind("touchstart", function () {
        $("#dialog").dialog('close');
    });
}

/**
 * Clones the base html settings and returns the html
 * @returns html
 */
function baseSettingsHtml() {
    return ($("#styletemplate:first").clone()).html();
}

/**
 * Opens the legend for the selected layer
 * @param {string} which - Name of layer to open legend for
 */
function openLegend(which) {
    //fix this, it's not getting the new style if the user changes, it's getting the default
    const active_layer = getLayer(which);
    const src =
        active_layer.url +
        "&REQUEST=GetLegendGraphic&LAYER=" +
        active_layer.layers +
        "&colorscalerange=" +
        active_layer.colorrange +
        "&PALETTE=" +
        active_layer.styles.substr(active_layer.styles.indexOf("/") + 1);
    $("#dialog").html(
        '<p style="text-align:center;"><img src="' + src + '" alt="legend"></p>'
    );
    $("#dialog").dialog({
        title: active_layer.title,
        resizable: {handles: "se"},
        width: 169,
        height: 322,
        position: {
            my: "center",
            at: "center",
            of: window
        }
    });
    $(".ui-dialog-title").attr("title", active_layer.title);
    $("button.ui-button.ui-corner-all.ui-widget.ui-button-icon-only.ui-dialog-titlebar-close").bind("touchstart", function () {
        $("#dialog").dialog('close');
    });
}

/**
 * Basic map setup and creates the basemap thumbnails in the basemaps tab
 */
function mapSetup() {
    map = L.map("servirmap", {
        zoom: 3,
        fullscreenControl: true,
        timeDimension: true,
        timeDimensionControl: true,
        center: [38.0, 15.0],
    });

    drawnItems = new L.FeatureGroup();
    map.addLayer(drawnItems);
    baseLayers = getCommonBaseLayers(map); // use baselayers.js to add, remove, or edit
    L.control.layers(baseLayers, overlayMaps).addTo(map);
    const sidebar = L.control.sidebar("sidebar").addTo(map);

    sidebar.open('chart');


    //create the basemap thumbnails in the panel
    for (let key of Object.keys(baseLayers)) {
        const img = $("<img>");
        img.attr("src", static_url + 'frontend/' + baseLayers[key].options.thumb);
        img.addClass("basemapbtn");
        img.attr("alt", baseLayers[key].options.displayName);
        img.attr("title", baseLayers[key].options.displayName);
        img.attr("datavalue", key);
        img.on("click", function (e) {
            handleBaseMapSwitch($(this)[0].getAttribute("datavalue"));
        });
        img.appendTo("#basemap");
    }
    map.on('layeradd', (e) => {
        adjustLayerIndex();
    });
}

/**
 * Switches the basemap to the user selected map.
 * @param {string} which - The Key of the basemap
 */
function handleBaseMapSwitch(which) {
    map.removeLayer(baseLayers[active_basemap]);
    active_basemap = which;
    baseLayers[active_basemap].addTo(map);
}

/**
 * Closes any open dialog and either adds or removes the selected layer.
 * @param {string} which - The id of the layer to toggle
 */
function toggleLayer(which) {
    if ($("#dialog").dialog()) {
        $("#dialog").dialog("close");
    }
    if (map.hasLayer(overlayMaps[which])) {
        map.removeLayer(overlayMaps[which]);
    } else {
        map.addLayer(overlayMaps[which]);
    }
    let available_times = [];
    for (let key in overlayMaps) {
        if (overlayMaps.hasOwnProperty(key)) {
            if (map.hasLayer(overlayMaps[key])) {
                available_times = available_times.concat(overlayMaps[key]._availableTimes).filter(onlyUnique);
            }
        }
    }
    map.timeDimension.setAvailableTimes(available_times, 'replace');
    map.timeDimension.prepareNextTimes(5, 1, false)
}

/**
 *
 *
 */
function onlyUnique(value, index, self) {
    return self.indexOf(value) === index;
}

/**
 * Opens the user selected method of selecting their AOI
 * @param {string} which - name of selection method to activate
 */
function selectAOI(which) {
    $("[id^=btnAOI]").removeClass("active");
    $("#btnAOI" + which).addClass("active");
    $(".selectAOI").hide();
    $("#" + which + "AOI").show();

    clearAOISelections();

    if (which === "draw") {
        enableDrawing();
    } else if (which === "upload") {
        enableUpload();
    }
}

/**
 * Removes all existing AOI selections and map click event
 */
function clearAOISelections() {
    if (drawToolbar) {
        drawToolbar.remove();
    }

    map.off("click");
    if (adminLayer) {
        adminLayer.remove();
    }
    if (adminHighlightLayer) {
        adminHighlightLayer.remove();
    }

    highlightedIDs = [];
    if (drawnItems) {
        drawnItems.clearLayers();
    }
    if (uploadLayer) {
        uploadLayer.clearLayers();
        uploadLayer.remove();
    }
    $("#nextStep1").prop("disabled", true);
    collect_review_data();
}

function triggerUpload(e) {
    e.preventDefault();
    $("#upload_files").trigger('click');
}

/**
 * Enables AOI upload capabilities by adding drop events to the drop zone
 */
function enableUpload() {
    uploadLayer = L.geoJson().addTo(map);

    const targetEl = document.getElementById("drop-container");
    targetEl.addEventListener("dragenter", function (e) {
        e.preventDefault();
    });
    targetEl.addEventListener("dragover", function (e) {
        e.preventDefault();
    });

    targetEl.addEventListener("drop", function (e) {
        handleFiles(e);
    });
}

function handleFiles(e) {
    e.preventDefault();
    const reader = new FileReader();
    reader.onloadend = function () {
        try {
            const data = JSON.parse(this.result);
            uploadLayer.clearLayers();
            uploadLayer.addData(data);
            map.fitBounds(uploadLayer.getBounds());
            if (uploadLayer.getLayers().length > 0) {
                $("#nextStep1").prop("disabled", false);
            } else {
                $("#nextStep1").prop("disabled", true);
            }
            $("#upload_error").hide();
            collect_review_data();
        } catch (e) {
            // When the section is built the url will need to add #pageanchorlocation
            $("#upload_error").html("* invalid file upload, please see the <a href='" + $("#menu-help").attr('href') + "#geojson'>Help Center</a> for more info about upload formats..")
            $("#upload_error").show();
        }
    };
    const files = e.target.files || e.dataTransfer.files || this.files;
    for (let i = 0, file; (file = files[i]); i++) {
        if (file.type === "application/json") {
            reader.readAsText(file);
        } else if (file.name.indexOf(".geojson") > -1) {
            reader.readAsText(file);
        } else if (file.type === "application/x-zip-compressed") {
            // https://gis.stackexchange.com/questions/368033/how-to-display-shapefiles-on-an-openlayers-web-mapping-application-that-are-prov
            if (uploadLayer) {
                uploadLayer.clearLayers();
            }
            loadshp(
                {
                    url: file,
                    encoding: "UTF-8",
                    EPSG: 4326,
                },
                function (data) {
                    let URL =
                            window.URL || window.webkitURL || window.mozURL || window.msURL,
                        url = URL.createObjectURL(
                            new Blob([JSON.stringify(data)], {type: "application/json"})
                        );
                    if (data.features.length > 10) {
                        data.features = data.features.splice(0, 10);
                    }
                    uploadLayer.addData(data);
                    map.fitBounds([
                        [data.bbox[1], data.bbox[0]],
                        [data.bbox[3], data.bbox[2]],
                    ]);
                    $(".dimmer").removeClass("active");
                    $("#preview").addClass("disabled");
                    $("#epsg").val("");
                    $("#encoding").val("");
                    $("#info").addClass("picInfo");
                    $("#option").slideUp(500);
                    if (uploadLayer.getLayers().length > 0) {
                        $("#nextStep1").prop("disabled", false);
                    } else {
                        $("#nextStep1").prop("disabled", true);
                    }
                    collect_review_data();
                }
            );
        }
    }
}

/**
 * Enables the drawing of an AOI on the map
 */
function enableDrawing() {
    clearAOISelections();
    drawToolbar = new L.Control.Draw({
        draw: {
            polyline: false,
            circle: false,
            circlemarker: false,
        },
        edit: {
            featureGroup: drawnItems,
        },
    });
    map.addControl(drawToolbar);

    map.on(L.Draw.Event.CREATED, function (e) {
        const type = e.layerType,
            layer = e.layer;
        if (type === "marker") {
            // Do marker specific actions
        }
        // Do whatever else you need to. (save to db; add to map etc)
        drawnItems.addLayer(layer);
        if (drawnItems.getLayers().length > 0) {
            $("#nextStep1").prop("disabled", false);
        } else {
            $("#nextStep1").prop("disabled", true);
        }
        collect_review_data();
    });

    map.on(L.Draw.Event.DELETED, function (e) {
        if (drawnItems.getLayers().length > 0) {
            $("#nextStep1").prop("disabled", false);
        } else {
            $("#nextStep1").prop("disabled", true);
        }
        collect_review_data();
    });

    map.on("draw:drawstart", function (e) {
        drawnItems.clearLayers();
    });
}

/**
 * Adds the selected admin features to the map for the user to select the AOI.
 * @param {string} which - name of admin layer to activate
 */
function enableAdminFeature(which) {
    $("[id^=btnAdminFeat]").removeClass("active");
    $("#btnAdminFeat" + which).addClass("active");
    clearAOISelections();
    adminLayer = L.tileLayer.wms(
        admin_layer_url,
        {
            layers: which,
            format: "image/png",
            transparent: true,
            styles: "",
            TILED: true,
            VERSION: "1.3.0",
        }
    );
    map.addLayer(adminLayer);
    adminLayer.setZIndex(
        Object.keys(baseLayers).length + client_layers.length + 5
    );

    // enable map click to show highlighted selections
    map.on("click", function (e) {
        const url = getFeatureInfoUrl(map, adminLayer, e.latlng, {
            info_format: "application/json",
            propertyName: "NAME,AREA_CODE,DESCRIPTIO",
        });

        $.ajax({
            type: "GET",
            async: true,
            url: url,
            crossDomain: true,
            jsonp: "callback",
            dataType: "jsonp",
            success: function (response) {
                if (response) {
                    if (adminHighlightLayer) {
                        adminHighlightLayer.remove();
                    }

                    const selectedID = response["data"];
                    if (highlightedIDs.includes(selectedID)) {
                        highlightedIDs = highlightedIDs.filter((e) => e !== selectedID);
                    } else {
                        highlightedIDs.push(selectedID);
                    }

                    adminHighlightLayer = L.tileLayer.wms(
                        admin_layer_url,
                        {
                            layers: which + "_highlight",
                            format: "image/png",
                            transparent: true,
                            styles: "",
                            TILED: true,
                            VERSION: "1.3.0",
                            feat_ids: highlightedIDs.join(),
                        }
                    );
                    // if(highlightedIDs.length > 0){
                    //   $("#btnstep2").prop("disabled", false);
                    // }
                    if (highlightedIDs.length > 0) {
                        $("#nextStep1").prop("disabled", false);
                    } else {
                        $("#nextStep1").prop("disabled", true);
                    }

                    map.addLayer(adminHighlightLayer);
                    adminHighlightLayer.setZIndex(
                        Object.keys(baseLayers).length + client_layers.length + 6
                    );
                    collect_review_data();
                }
            },
        });
    });
}

function gotostep(which) {
    $("[id^=step]").hide();
    $("[id^=btnstep]").removeClass("active");
    $("#step" + which).show();
    $("#btnstep" + which).addClass("active");
    $("#btnstep" + which).addClass("active");
    switch (which) {
        case 1:
            $("[id^=btnstep]").prop("disabled", true);
            break;
        case 2:
            $("#btnstep1").prop("disabled", false);
            // also disable any drawing ability, remove drawing bar
            // also disable any drawing ability, remove drawing bar
            if (drawToolbar) {
                drawToolbar.remove();
            }
            map.off("click");
            break
        case 3:
            $("#btnstep2").prop("disabled", false);
            collect_review_data();
            if ($("#requestTypeSelect").val() === "monthly_analysis") {
                // hide data review, show monthly review
                $("#dataset_review").hide();
                $("#monthly_rainfall_review").show();

            } else {
                $("#monthly_rainfall_review").hide();
                $("#dataset_review").show();
            }
            break;
    }
}

function enablestep3() {
    $("#btnstep3").prop("disabled", false);
}

/**
 *
 * @param {object} map - the map object
 * @param {object} layer - L.tileLayer.wms
 * @param {object} latlng - location clicked
 * @param {object} params - special parameters
 * @returns string url
 */
function getFeatureInfoUrl(map, layer, latlng, params) {
    const point = map.latLngToContainerPoint(latlng, map.getZoom());
    const size = map.getSize();
    const bounds = map.getBounds();
    let sw = bounds.getSouthWest();
    let ne = bounds.getNorthEast();
    sw = L.CRS.EPSG3857.project(new L.LatLng(sw.lat, sw.lng));
    ne = L.CRS.EPSG3857.project(new L.LatLng(ne.lat, ne.lng));

    const bb = sw.x + "," + sw.y + "," + ne.x + "," + ne.y;

    const defaultParams = {
        request: "GetFeatureInfo",
        service: "WMS",
        srs: "EPSG:102100",
        styles: "",
        version: layer._wmsVersion,
        format: layer.options.format,
        bbox: bb,
        height: size.y,
        width: size.x,
        layers: layer.options.layers,
        query_layers: layer.options.layers,
        info_format: "text/html",
    };

    params = L.Util.extend(defaultParams, params || {});

    params[params.version === "1.3.0" ? "i" : "x"] = point.x;
    params[params.version === "1.3.0" ? "j" : "y"] = point.y;

    return layer._url + L.Util.getParamString(params, layer._url, true);
}


/**
 * Sets up the sortable layers in the layer manager
 */
function sortableLayerSetup() {
    $("ol.layers").sortable({
        group: "simple_with_animation",
        handle: ".rst__moveHandle",
        pullPlaceholder: true,
        // animation on drop
        onEnd: function ($item, container, _super) {
            adjustLayerIndex();
        },
        onChange: function (/**Event*/evt) {
            adjustLayerIndex();
        },
        filter: ".ignore-elements",
        // Called when creating a clone of element
        onClone: function (/**Event*/evt) {
            let origEl = evt.item;
            let cloneEl = evt.clone;
        },
    });
}

function adjustLayerIndex() {
    let count = 10;
    for (let i = $("ol.layers li").length; i > 0; i--) {
        overlayMaps[
            $("ol.layers li")[i - 1].id.replace("_node", "TimeLayer")
            ].setZIndex(count);
        count++;
    }
}

/**
 * Page load functions, initializes all parts of application
 */
function initMap() {
    passedLayer = this.getParameterByName("data") || "none";
    mapSetup();
    client_layers.forEach(createLayer);
    sortableLayerSetup();
    try {
        buildStyles();
    } catch (e) {
    }
    adjustLayerIndex();
}

function isComplete() {
    //sDate_new_cooked
    //eDate_new_cooked
    let isReady = false;
    sDate_new_cooked = document.getElementById("sDate_new_cooked");
    eDate_new_cooked = document.getElementById("eDate_new_cooked");
    if (sDate_new_cooked.value && eDate_new_cooked.value) {
        isReady = $(sDate_new_cooked).valid({
            rules: {
                field: {
                    required: true,
                    dateISO: true
                }
            }
        }) && $(eDate_new_cooked).valid({rules: {field: {required: true, dateISO: true}}});
        if (isReady) {
            // Also should confirm s < e;
            if (moment(sDate_new_cooked.value) > moment(eDate_new_cooked.value)) {
                isReady = false;
                $("#compare-error").show();
            } else {
                $("#compare-error").hide();
            }
        }

    } else {
        $(sDate_new_cooked).valid({rules: {field: {required: true, dateISO: true}}});
        $(eDate_new_cooked).valid({rules: {field: {required: true, dateISO: true}}});
    }
    $("#nextStep2").prop("disabled", !isReady);
}

function collect_review_data() {
    //get all data and fill review

    $("#dataType").text($("#sourcemenu").val() + " - " + $("#sourcemenu option:selected").text());
    $("#begintime").text(moment(document.getElementById("sDate_new_cooked").value).format('MM/DD/YYYY'));
    $("#endtime").text(moment(document.getElementById("eDate_new_cooked").value).format('MM/DD/YYYY'));
    $("#operationtype").text($("#operationmenu").val() + " - " + $("#operationmenu option:selected").text());
    if (highlightedIDs.length > 0) {
        console.log("highlighted");
        const feature_label = highlightedIDs.length > 1 ? "Features" : "Feature"
        $("#geometry").text(adminHighlightLayer.options.layers.replace("_highlight", " - " + feature_label + ": ").replace("admin_2_af", "Admin #2").replace("admin_1_earth", "Admin #1").replace("country", "Country") + highlightedIDs.join());
    } else if (drawnItems.getLayers().length > 0) {
        console.log("drawn");
        $("#geometry").text(JSON.stringify(drawnItems.toGeoJSON()));
    } else if (uploadLayer) {
        console.log("uploaded");
        $("#geometry").text(JSON.stringify(uploadLayer.toGeoJSON()));
    } else {
        $("#geometry").text('{"type":"FeatureCollection","features":[]}');
        console.log("nothing");
    }

}

function getEnsDataType() {
    // will need to write this for the selected ensembles
    return
}

function handle_initial_request_data(data, isClimate) {
    let progress = '<div style="width:100%; height:100%; display: flex;\n' +
        '    align-items: center;\n' +
        '}">';
    progress += '<div class="progress">';
    progress += '<div class="progress-bar progress-bar-striped progress-bar-animated"\n' +
        ' role="progressbar" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100"\n' +
        ' style="width: 0%"><span><span class="percentage" id="txtpercent">0%</span></span></div>';
    progress += '</div></div>';
    $("#dialog").html(progress);
    $("#dialog").dialog({
        title: "Query Progress",
        resizable: false,
        width: $(window).width() / 2,
        height: 200,
        position: {
            my: "center",
            at: "center",
            of: window
        }
    });
    pollForProgress(data[0], isClimate);
}

function sendRequest() {
    $("#btnRequest").prop("disabled", true);
    const formData = new FormData();
    if ($("#requestTypeSelect").val() === "datasets") {
        formData.append(
            "datatype", $("#sourcemenu").val()
        );
        formData.append("begintime", $("#begintime").text()); // "01/01/2020");
        formData.append("endtime", $("#endtime").text()); //"06/30/2020");
        formData.append("intervaltype", 0);
        formData.append("operationtype", $("#operationmenu").val());
        formData.append("dateType_Category", "default");
        formData.append("isZip_CurrentDataType", false);
        if (highlightedIDs.length > 0) {
            formData.append("layerid", adminHighlightLayer.options.layers.replace("_highlight", ""));
            formData.append("featureids", highlightedIDs.join(","));
        } else if (drawnItems.getLayers().length > 0) {
            formData.append("geometry", JSON.stringify(drawnItems.toGeoJSON()));
        } else if (uploadLayer) {
            formData.append("geometry", JSON.stringify(uploadLayer.toGeoJSON()));
        }

        fetch(
            api_url + "/chirps/submitDataRequest/",
            {
                crossDomain: true,
                method: "POST",
                body: formData,
            }
        )
            .then((response) => response.json())
            .then((data) => {
                handle_initial_request_data(data, false);
            });
    } else {
        // this is climatology
        // this looks like it currently needs to be a get request not a post so we'll have to do it a bit different
        //example request with querystring
        //https://climateserv.servirglobal.net/chirps/submitMonthlyRainfallAnalysisRequest/?callback=successCallback&custom_job_type=monthly_rainfall_analysis&seasonal_start_date=2021_06_01&seasonal_end_date=2021_11_28&layerid=country&featureids=201&_=1627567378744

        let geometry_params;

        if (highlightedIDs.length > 0) {
            geometry_params = "&layerid=" + adminHighlightLayer.options.layers.replace("_highlight", "");
            geometry_params += "&featureids=" + highlightedIDs.join(",");
        } else if (drawnItems.getLayers().length > 0) {
            geometry_params = "&geometry=" + JSON.stringify(drawnItems.toGeoJSON());
        } else if (uploadLayer) {
            geometry_params = "&geometry=" + JSON.stringify(uploadLayer.toGeoJSON());
        }


        const csi = JSON.parse(climateModelInfo.climate_DataTypeCapabilities[0].current_Capabilities);
        let url = api_url + "/chirps/submitMonthlyRainfallAnalysisRequest/?custom_job_type=monthly_rainfall_analysis&";
        url += "seasonal_start_date=" + csi.startDateTime;
        url += "&seasonal_end_date=" + csi.endDateTime;
        url += geometry_params;

        fetch(url)
            .then(response => response.json())
            .then(data => handle_initial_request_data(data, true));


    }

}

function updateProgress(val) {
    $('.progress-bar').css('width', val + '%').attr('aria-valuenow', val);
    $("#txtpercent").text(parseInt(val) + '%');
}

function pollForProgress(id, isClimate) {
    fetch(
        api_url + "/chirps/getDataRequestProgress/?id=" +
        id,
        {
            crossDomain: true,
            method: "GET",
        }
    )
        .then((response) => response.json())
        .then((data) => {
            const val = data[0];
            if (val !== -1 && val !== 100) {
                updateProgress(val);
                pollForProgress(id, isClimate);
            } else if (val === 100) {
                getDataFromRequest(id, isClimate);
            } else {
                console.log("Server Error");
                $("#btnRequest").prop("disabled", false);
            }
        }); // this is the jobID to poll with and get data
}

function inti_chart_dialog() {
    $("#dialog").html(
        '<div id="chart_holder"></p>'
    );
    // $("#chart_holder").resize(function(){
    //   window.dispatchEvent(new Event('resize'));
    // });
    $("#dialog").dialog({
        title: "Statistical Query",
        resizable: {handles: "se"},
        width: $(window).width() - ($("#sidebar").width() + 100),
        height: $(window).height() - 140,
        resize: function () {
            window.dispatchEvent(new Event('resize'));
        },
        position: {
            my: "right",
            at: "right-25",
            of: window
        }
    });
}

function getIndex(which) {
    switch (which) {
        case 'LongTermAverage':
            return 0;
        case 'SeasonalFcstAvg':
            return 1;
        case '25thPercentile':
            return 2;
        default:
            return 3;
    }
}

var test_obj;
var rainfall_data;
var from_compiled;

function getDataFromRequest(id, isClimate) {
    let complete = '<div style="width:100%; height:100%; display: flex;\n' +
        '    align-items: center;\n' +
        '}">';
    complete += '<div style="width:100%">';
    complete += '<h1 class="step-marker">Download complete, downloading results.</h1>';
    complete += '</div>';
    $("#dialog").html(complete);
    $("#dialog").dialog({
        title: "Query Complete, Downloading Results",
        resizable: false,
        width: $(window).width() / 2,
        height: 200,
        position: {
            my: "center",
            at: "center",
            of: window
        }
    });
    fetch(
        api_url + "/chirps/getDataFromRequest/?id=" +
        id,
        {
            crossDomain: true,
            method: "GET",
        }
    )
        .then((response) => response.json())
        .then((data) => {
            if (isClimate) {
                test_obj = build_MonthlyRainFall_Analysis_Graphable_Object(data);
                // i will have to make an array of objects that look like
                /*
                seriesOptions[i] = {
                    name: name,
                    data: [[date, val], ...]
                  };
                 */
                rainfall_data = [];
                rainfall_data.push({
                    name: "LongTermAverage",
                    data: []
                });
                rainfall_data.push({
                    name: "SeasonalFcstAvg",
                    data: []
                });
                rainfall_data.push({
                    name: "25thPercentile",
                    data: []
                });
                rainfall_data.push({
                    name: "75thPercentile",
                    data: []
                });
                // create these from the object dates
                const xaxis = {
                    categories: []
                }
                test_obj.forEach(o => {
                    if (!xaxis.categories.includes(o.Month_Year)) {
                        xaxis.categories.push(o.Month_Year);
                    }
                    rainfall_data[getIndex(o.data_series_type)].data.push(o.Monthly_Rainfall_mm);

                });
                inti_chart_dialog();

                finalize_chart(rainfall_data, "mm", xaxis, "Monthly Rainfall Analysis", isClimate);

            } else {
                const compiledData = [];
                const otState = parseInt($("#operationmenu").val());
                if (otState === 6) {
                    // this is a download request form download link
                } else {
                    let min = 9999;
                    let max = -9999;
                    data.data.forEach((d) => {
                        let val = 0;

                        val =
                            otState === 0
                                ? d.value.max
                                : otState === 1
                                    ? d.value.min
                                    : otState === 5
                                        ? d.value.avg
                                        : -9191;

                        if (val > -9000) {
                            const darray = [];
                            darray.push(parseInt(d.epochTime) * 1000);
                            //fix this
                            if (val < min) {
                                min = val;
                            }
                            if (val > max) {
                                max = val;
                            }
                            darray.push(val);
                            compiledData.push(darray); // i can likely store min and max here
                        }
                    });
                    from_compiled = compiledData;
                    inti_chart_dialog();

                    const units = client_layers.find(
                        (item) => item.app_id === $("#sourcemenu").val()
                    ).units;

                    finalize_chart([{
                        color: "#758055",
                        type: "line",
                        name: $("#operationmenu option:selected").text(),
                        data: compiledData.sort((a, b) => a[0] - b[0])
                    }], units, {
                        type: "datetime"
                    }, $("#sourcemenu option:selected").text());
                }
            }
            $("#btnRequest").prop("disabled", false);
        });
};

function finalize_chart(compiled_series, units, xAxis_object, title, isClimate) {
    let chart_obj = {};
    chart_obj.title = {
        text: title
    };

    chart_obj.subtitle = {
        text: 'Source: climateserv.servirglobal.net'
    };
    chart_obj.xAxis = xAxis_object;
    chart_obj.yAxis = {
        title: {
            text: units
        }
    };

    chart_obj.legend = {
        layout: 'vertical',
        align: 'right',
        verticalAlign: 'middle'
    };

    chart_obj.plotOptions = {
        series: {
            connectNulls: false,
            marker: {
                radius: 3,
                fillColor: "#758055",
                states: {
                    hover: {
                        fillColor: '#758055',
                    },
                    halo: {
                        fillColor: '#758055',
                    }
                },
            },
            lineWidth: 2,
            states: {
                hover: {
                    lineWidth: 2
                },
                halo: {
                    fillColor: '#758055',
                }
            },
            threshold: null,
            allowPointSelect: true,
            point: {
                events: {
                    select: function (e) {
                        const full = new Date(e.target.x);
                        const date = full.getFullYear() + "-" + (full.getMonth() + 1) + "-" + full.getDate();

                        console.log(date);
                    }
                }
            }
        }
    };
    if (isClimate) {
        chart_obj.plotOptions.series.marker = {
            radius: 3
        };
    }

    chart_obj.exporting = {
        chartOptions: {
            chart: {
                events: {
                    load: function () {
                        const width = this.chartWidth - 105;
                        const height = this.chartHeight - 130;
                        this.renderer.image('https://servirglobal.net/images/servir_logo_full_color_stacked.jpg', width, height, 100, 82
                        ).add();
                    }
                }
            }
        }
    };
    chart_obj.chart = {
        events: {
            redraw: function (e) {
                img.translate(
                    this.chartWidth - originalWidth,
                    this.chartHeight - originalHeight
                );
            }
        }
    };
    chart_obj.series = compiled_series;
    chart_obj.tooltip = {
        pointFormat: "Value: {point.y:.2f} " + units,
        borderColor: "#758055",
    };
    chart_obj.responsive = {
        rules: [{
            condition: {
                maxWidth: 500
            },
            chartOptions: {
                legend: {
                    layout: 'horizontal',
                    align: 'center',
                    verticalAlign: 'bottom'
                }
            }
        }]
    };

    Highcharts.chart('chart_holder', chart_obj, function (chart) { // on complete

        originalWidth = chart.chartWidth;
        originalHeight = chart.chartHeight;
        const width = chart.chartWidth - 105;
        const height = chart.chartHeight - 130;
        img = chart.renderer
            .image('https://servirglobal.net/images/servir_logo_full_color_stacked.jpg', width, height, 100, 82)
            .add();
    });
}


function build_MonthlyRainFall_Analysis_Graphable_Object(raw_data_obj) {
    const ret_dataLines_List = [];
    let seasonal_end_month;
    let single_climate_model_capabiliites;
    try {
        single_climate_model_capabiliites = JSON.parse(climateModelInfo.climate_DataTypeCapabilities[0].current_Capabilities);
    } catch (err_Getting_Dates_From_Climate_Model_Capabilities) {  // something different needs to happen here if we don't have the capabilities object we can't continue this process
        return;
    }
    const seasonal_start_date = single_climate_model_capabiliites.startDateTime; //"2017_05_01";
    const seasonal_end_date = single_climate_model_capabiliites.endDateTime; //"2017_10_28";
    seasonal_end_month = parseInt(seasonal_end_date.split("_")[1]);
    let current_month_num = parseInt(seasonal_start_date.split("_")[1]);
    let current_year_num = parseInt(seasonal_start_date.split("_")[0]);

    do {
        current_month_num = current_month_num % 12 === 0 ? 12 : current_month_num % 12;
        const current_Month_Year_Value = moment().month(current_month_num - 1).format("MMM") + "-" + current_year_num;
        processData(ret_dataLines_List, raw_data_obj, current_month_num, "SEASONAL_FORECAST", "col02_MonthlyAverage", current_Month_Year_Value, "SeasonalFcstAvg");
        processData(ret_dataLines_List, raw_data_obj, current_month_num, "CHIRPS_REQUEST", "col02_MonthlyAverage", current_Month_Year_Value, "LongTermAverage");
        processData(ret_dataLines_List, raw_data_obj, current_month_num, "CHIRPS_REQUEST", "col03_25thPercentile", current_Month_Year_Value, "25thPercentile");
        processData(ret_dataLines_List, raw_data_obj, current_month_num, "CHIRPS_REQUEST", "col04_75thPercentile", current_Month_Year_Value, "75thPercentile");
        current_month_num === 12 && current_year_num++;
        current_month_num++;
    } while (current_month_num % 12 != (seasonal_end_month + 1) % 12);

    return ret_dataLines_List;
}

function processData(ret_dataLines_List, data_object, current_month_num, subtype, variable, current_Month_Year_Value, retrun_variable) {
    let data = get_values_By_month(data_object, current_month_num, subtype, variable)
    ret_dataLines_List.push(getDataLine(
        current_Month_Year_Value,
        retrun_variable,
        subtype === "SEASONAL_FORECAST" ? data.reduce((a, b) => a + b) / data.length : data[0]
    ));
}

function getDataLine(mmm_Y, type, data) {
    const data_line = [];
    data_line['Month_Year'] = mmm_Y;
    data_line['data_series_type'] = type;
    data_line['Monthly_Rainfall_mm'] = data;
    return data_line;
}

function openDataTypePanel(select_control) {
    if (select_control.value === "datasets") {
        $("#panel_monthly_rainfall").hide();
        $("#panel_dataset").show();
        isComplete();
    } else {
        $("#panel_dataset").hide();
        $("#panel_monthly_rainfall").show();
        $("#nextStep2").prop("disabled", false);
    }
}

let climateModelInfo;

function getClimateScenarioInfo() {
    fetch('https://climateserv.servirglobal.net/chirps/getClimateScenarioInfo/')
        .then(response => response.json())
        .then(data => climateModelInfo = data);
}

let img, originalWidth, originalHeight;

/**
 * Calls initMap
 *
 * @event map-ready
 */
$(function () {
    initMap();
    try {
        getClimateScenarioInfo();
    } catch (e) {
        console.log("ClimateScenarioInfo Failed");
    }
    try {
        const inputElement = document.getElementById("upload_files");
        inputElement.addEventListener("change", handleFiles, false);
    } catch (e) {
        console.log("upload handler Failed");
    }
    try {
        $(".collapse").on('show.bs.collapse', function () {
            $("#aoiOptionToggle").removeClass("fa-angle-down").addClass("fa-angle-up");
        }).on('hide.bs.collapse', function () {
            $("#aoiOptionToggle").removeClass("fa-angle-up").addClass("fa-angle-down");
        });
    } catch (e) {
        console.log("aoiOptionToggle Failed");
    }
});

(function ($) {

    $.support.touch = typeof Touch === 'object';

    if (!$.support.touch) {
        return;
    }

    var proto = $.ui.mouse.prototype,
        _mouseInit = proto._mouseInit;

    $.extend(proto, {
        _mouseInit: function () {
            this.element
                .bind("touchstart." + this.widgetName, $.proxy(this, "_touchStart"));
            _mouseInit.apply(this, arguments);
        },

        _touchStart: function (event) {
            if (event.originalEvent.targetTouches.length != 1) {
                return false;
            }

            this.element
                .bind("touchmove." + this.widgetName, $.proxy(this, "_touchMove"))
                .bind("touchend." + this.widgetName, $.proxy(this, "_touchEnd"));

            this._modifyEvent(event);

            $(document).trigger($.Event("mouseup")); //reset mouseHandled flag in ui.mouse
            this._mouseDown(event);

            return false;
        },

        _touchMove: function (event) {
            this._modifyEvent(event);
            this._mouseMove(event);
        },

        _touchEnd: function (event) {
            this.element
                .unbind("touchmove." + this.widgetName)
                .unbind("touchend." + this.widgetName);
            this._mouseUp(event);
        },

        _modifyEvent: function (event) {
            event.which = 1;
            var target = event.originalEvent.targetTouches[0];
            event.pageX = target.clientX;
            event.pageY = target.clientY;
        }

    });

})(jQuery);
