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
const admin_layer_url = location.hostname === "localhost" ||
location.hostname === "127.0.0.1" ||
location.hostname === "192.168.1.132"
    ? "https://climateserv2.servirglobal.net/servirmap_102100/?&crs=EPSG%3A102100"
    : "servirmap_102100/?&crs=EPSG%3A102100";
let retries = 0;
let sidebar;
let previous_chart;
let layer_limits = {min: null, max: null}
let queried_layers = [];
let control_layer;
let current_calculation;
let climateModelInfo;
let rainfall_data;
let from_compiled;
let too_fast = 0;
let polling_timeout;

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
            abovemaxcolor: "extend",
            belowmincolor: "extend",
            // abovemaxcolor: "transparent",
            // belowmincolor: "transparent",
            numcolorbands: 100,
            styles: item.styles,
            tileSize: 1024,
        }),
        {
            updateTimeDimension: true,
            cache: 3,
            cacheForward: 3,
            cacheBackward: 3,
            setDefaultTime: true,
        }
    );
    overlayMaps[item.id + "TimeLayer"].id = item.id;

    if (item.id.includes(passedLayer)) {
        queried_layers.push(item.id)
    } else {
        if (!control_layer) {
            control_layer = item.id;
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
    $.ajax({
        url: client_layers[0].url + "&request=GetCapabilities",
        type: "GET",
        async: true,
        crossDomain: true
    }).fail(function (jqXHR, textStatus, errorThrown) {
        console.warn(jqXHR + textStatus + errorThrown);
    }).done(function (data, _textStatus, _jqXHR) {
        if (data["errMsg"]) {
            console.info(data["errMsg"]);
        } else {
            try {
                const jsonObj = ($.xml2json(data))["#document"];
                const styles =
                    jsonObj
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
            } catch (e) {
                const backup = [
                    {
                        "Name": "boxfill/apcp_surface",
                    },
                    {
                        "Name": "boxfill/ensprob-spd10m-thresh50",
                    },
                    {
                        "Name": "boxfill/avg_temp",
                    },
                    {
                        "Name": "boxfill/ncview",
                    },
                    {
                        "Name": "boxfill/rel_humid",
                    },
                    {
                        "Name": "boxfill/ensprob-uphlcy25-thresh100",
                    },
                    {
                        "Name": "boxfill/tmp_2maboveground",
                    },
                    {
                        "Name": "boxfill/evap",
                    },
                    {
                        "Name": "boxfill/baseflow",
                    },
                    {
                        "Name": "boxfill/crimsonyellowgreen",
                    },
                    {
                        "Name": "boxfill/alg",
                    },
                    {
                        "Name": "boxfill/dpt_2maboveground",
                    },
                    {
                        "Name": "boxfill/ensprob-lfa-max-thresh5",
                    },
                    {
                        "Name": "boxfill/albedo",
                    },
                    {
                        "Name": "boxfill/par",
                    },
                    {
                        "Name": "boxfill/sst_36",
                    },
                    {
                        "Name": "boxfill/prob_refc_thresh50",
                    },
                    {
                        "Name": "boxfill/cdi",
                    },
                    {
                        "Name": "boxfill/prob_spd10m_thresh40",
                    },
                    {
                        "Name": "boxfill/prob_tcolg_thresh40",
                    },
                    {
                        "Name": "boxfill/rainbow",
                    },
                    {
                        "Name": "boxfill/occam",
                    },
                    {
                        "Name": "boxfill/pm25_india_ramp",
                    },
                    {
                        "Name": "boxfill/pm25",
                    },
                    {
                        "Name": "boxfill/prob_lfa_thresh5",
                    },
                    {
                        "Name": "boxfill/pm25_india",
                    },
                    {
                        "Name": "boxfill/alg2",
                    },
                    {
                        "Name": "boxfill/ensprob-spd10m-thresh40",
                    },
                    {
                        "Name": "boxfill/ensprob-refc-thresh50",
                    },
                    {
                        "Name": "boxfill/ensprob-tcolg-thresh30",
                    },
                    {
                        "Name": "boxfill/enspmm-prec1h",
                    },
                    {
                        "Name": "boxfill/crimsonyellowred",
                    },
                    {
                        "Name": "boxfill/occam_pastel-30"
                    },
                    {
                        "Name": "boxfill/ensprob-uphlcy25-thresh200",
                    },
                    {
                        "Name": "boxfill/prob_spd10m_thresh50",
                    },
                    {
                        "Name": "boxfill/prob_tcolg_thresh30",
                    },
                    {
                        "Name": "boxfill/cape_surface",
                    },
                    {
                        "Name": "boxfill/grace",
                    },
                    {
                        "Name": "boxfill/crimsonyellowblue",
                    },
                    {
                        "Name": "boxfill/dryspells",
                    },
                    {
                        "Name": "boxfill/temp",
                    },
                    {
                        "Name": "boxfill/crimsonbluegreen",
                    },
                    {
                        "Name": "boxfill/redblue",
                    },
                    {
                        "Name": "boxfill/avg_temp_rev",
                    },
                    {
                        "Name": "boxfill/ensprob-tcolg-thresh40",
                    },
                    {
                        "Name": "boxfill/blueyellowcrimson",
                    },
                    {
                        "Name": "boxfill/greyscale",
                    },
                    {
                        "Name": "boxfill/pm25ramp",
                    },
                    {
                        "Name": "boxfill/net_short_long",
                    },
                    {
                        "Name": "boxfill/cwg",
                    },
                    {
                        "Name": "boxfill/enspmm-prectot",
                    },
                    {
                        "Name": "boxfill/enspmm-refc",
                    },
                    {
                        "Name": "boxfill/crimsonbluewhite",
                    },
                    {
                        "Name": "boxfill/ensprob-lfa-max-thresh0.07",
                    },
                    {
                        "Name": "boxfill/prec_rainf",
                    },
                    {
                        "Name": "boxfill/ferret",
                    }
                ].sort(function (a, b) {
                    const x = a.Name;
                    const y = b.Name;
                    return ((x < y) ? -1 : ((x > y) ? 1 : 0));
                });
                for (let i = 0; i < backup.length; i++) {
                    styleOptions.push({
                        val: backup[i].Name,
                        text: backup[i].Name,
                    });
                }
            }
        }
    });
}

//In here i can add the options for transparent above and below
function apply_style_click(which, active_layer, bypass_auto_on) {
    let was_removed = false;
    const style_table = $("#style_table");
    if (map.hasLayer(overlayMaps[which])) {
        was_removed = true;
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
            abovemaxcolor: "extend",
            belowmincolor: "extend",
            // abovemaxcolor: document.getElementById("above_below").value,
            // belowmincolor: document.getElementById("above_below").value,
            numcolorbands: 100,
            styles: style_table.val(),
        }),
        {
            updateTimeDimension: true,
        }
    );
    if (!bypass_auto_on || was_removed) {
        map.addLayer(overlayMaps[which]);
    }
    if (!bypass_auto_on) {
        document.getElementById(which.replace("TimeLayer", "")).checked = true;
    }
    active_layer.styles = style_table.val();
    active_layer.colorrange = document.getElementById("range-min").value +
        "," +
        document.getElementById("range-max").value
    overlayMaps[which].options.opacity = document.getElementById("opacityctrl").value;
    overlayMaps[which].setOpacity(overlayMaps[which].options.opacity);
}

function apply_settings(which, active_layer, is_multi, multi_ids) {
    $("#style_table").val(overlayMaps[which]._baseLayer.wmsParams.styles);

    const slider = document.getElementById("opacityctrl");
    slider.value = overlayMaps[which].options.opacity;
    slider.oninput = function () {
        if (is_multi) {
            multi_ids.forEach(e => {
                overlayMaps[e + "TimeLayer"].setOpacity(this.value);
            });
        } else {
            overlayMaps[which].setOpacity(this.value);
        }
    };

    const applyStylebtn = document.getElementById("applyStylebtn");

    applyStylebtn.onclick = function () {
        if (is_multi) {
            // loop
            multi_ids.forEach(e => {
                apply_style_click(e + "TimeLayer", getLayer(multi_ids[0]), true);
            })
        } else {
            apply_style_click(which, active_layer);
        }
    };
    // Update min/max
    document.getElementById("range-min").value =
        overlayMaps[which]._baseLayer.options.colorscalerange.split(",")[0];
    document.getElementById("range-max").value =
        overlayMaps[which]._baseLayer.options.colorscalerange.split(",")[1];
}

/**
 * Populates the Settings box for the specific layer and opens the settings popup.
 * @param {string} which - Name of layer to open settings for
 */
close_dialog();

function openSettings(which) {
    let active_layer = getLayer(which);
    let multi = false;
    const multi_ids = [];
    if (!active_layer) {
        let id = which.replace("TimeLayer", "") + "ens";
        multi = true;
        $("[id^=" + id + "]").each(function () {
            multi_ids.push(this.id);
        });
    }


    let settingsHtml = "";
    // if (active_layer.dataset === "model") {
    //     // all ensembles are adjusted by single setting
    //     // if setting changes, they all change so dialog
    //     // should be the same.  The hooks will have to be
    //     // expanded to make all ensemble layers react in sync
    //
    //     //settingsHtml += "Get the Ens info to build the checkboxes";
    // }

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

    if (multi) {
        active_layer = getLayer(multi_ids[0]);
        apply_settings(multi_ids[0] + "TimeLayer", active_layer, true, multi_ids);

    } else {
        apply_settings(which, active_layer);
    }
}

/**
 * Clones the base html settings and returns the html
 * @returns html
 */
function baseSettingsHtml() {
    return ($("#style_template:first").clone()).html();
}

/**
 * Opens the legend for the selected layer
 * @param {string} which - Name of layer to open legend for
 */
function openLegend(which) {
    close_dialog();
    const dialog = $("#dialog");
    let id = which.replace("TimeLayer", "") + "ens";
    const active_layer = getLayer(which) || getLayer($("[id^=" + id + "]")[0].id);
    const src =
        active_layer.url +
        "&REQUEST=GetLegendGraphic&LAYER=" +
        active_layer.layers +
        "&colorscalerange=" +
        active_layer.colorrange +
        "&PALETTE=" +
        active_layer.styles.substr(active_layer.styles.indexOf("/") + 1);
    dialog.html(
        '<p style="text-align:center;"><img src="' + src + '" alt="legend"></p>'
    );
    dialog.dialog({
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
        timeDimensionControlOptions: {
            autoPlay: false,
            loopButton: true,
            timeSteps: 1,
            playReverseButton: true,
            displayDate: true,
            timeSlider: true,
            limitSliders: true,
            limitMinimumRange: 5,
            playerOptions: {
                buffer: 10,
                loop: true,
            }
        },
        center: [38.0, 15.0],
    });

    drawnItems = new L.FeatureGroup();
    map.addLayer(drawnItems);
    baseLayers = getCommonBaseLayers(map); // use baselayers.js to add, remove, or edit
    L.control.layers(baseLayers, overlayMaps).addTo(map);
    sidebar = L.control.sidebar("sidebar").addTo(map);

    sidebar.open('chart');


    //create the basemap thumbnails in the panel
    for (let key of Object.keys(baseLayers)) {
        const map_thumb = $("<div>");
        map_thumb.addClass("map-thumb");
        map_thumb.attr("datavalue", key);
        map_thumb.on("click", function () {
            handleBaseMapSwitch($(this)[0].getAttribute("datavalue"));
        });

        const thumb_cap = $("<div>");
        thumb_cap.addClass("caption-text");

        const thumb_text = $("<h2>");
        thumb_text.text(baseLayers[key].options.displayName);

        thumb_text.appendTo(thumb_cap);
        const img = $("<img src='"
            + static_url + "frontend/"
            + baseLayers[key].options.thumb
            + "' alt='"
            + baseLayers[key].options.displayName + "'>", {
            title: baseLayers[key].options.displayName,
            datavalue: key,
            click: function () {
                handleBaseMapSwitch($(this)[0].getAttribute("datavalue"));
            }
        });
        img.addClass("basemapbtn");


        img.appendTo(map_thumb);
        thumb_cap.appendTo(map_thumb);
        map_thumb.appendTo("#basemap");
    }
    map.on('layeradd', () => {
        adjustLayerIndex();
    });

    const search = new GeoSearch.GeoSearchControl({
        provider: new GeoSearch.OpenStreetMapProvider(),
        showMarker: false, // optional: true|false  - default true
        showPopup: false,
        autoClose: true,
    });
    map.addControl(search);
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
    close_dialog();
    if (map.hasLayer(overlayMaps[which])) {
        map.removeLayer(overlayMaps[which]);
    } else {
        map.addLayer(overlayMaps[which]);
    }
    let hasLayer = false;
    let available_times = [];
    for (let key in overlayMaps) {
        if (overlayMaps.hasOwnProperty(key)) {
            if (map.hasLayer(overlayMaps[key])) {
                hasLayer = true;
                available_times = available_times.concat(overlayMaps[key]._availableTimes).filter(onlyUnique);
            }
        }
    }
    layer_limits.min = available_times[0];
    layer_limits.max = available_times[available_times.length - 1];

    if (hasLayer) {
        map.timeDimension.setAvailableTimes(available_times, 'replace');
        //map.timeDimension.prepareNextTimes(5, 1, false)

        if (!map.timeDimension.getLowerLimit()) {
            map.timeDimension.setLowerLimit(moment.utc(layer_limits.min));
            map.timeDimension.setUpperLimit(moment.utc(layer_limits.max));
            map.timeDimension.setCurrentTime(moment.utc(layer_limits.max));
        }
        $("#slider-range-txt").text(moment(layer_limits.min).format('MM/DD/YYYY') +
            " to " + moment(layer_limits.max).format('MM/DD/YYYY'));
    } else {
        map.timeDimension.setAvailableTimes([null], "replace");
        $(".timecontrol-date").html("Time not available")
        $("#slider-range-txt").text('N/A to N/A');
    }
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
    collect_review_data();
    verify_ready();
}

function setPointAOI() {
    let valid_values = true;
    const point_lon = $("#point_lon").val();
    const point_lat = $("#point_lat").val();
    if (isNaN(point_lon) || point_lon < -180 || point_lon > 180) {
        valid_values = false;
    }
    if (isNaN(point_lat) || point_lat < -90 || point_lat > 90) {
        valid_values = false;
    }
    if (valid_values) {
        drawnItems.clearLayers();
        drawnItems.addLayer(L.marker([point_lat, point_lon]));
        $("#lat-lon-error").hide();
        point_lon.val("")
        point_lat.val("")
        $("#geometry").text(JSON.stringify(drawnItems.toGeoJSON()));
    } else {
        $("#lat-lon-error").show();
    }

}

function triggerUpload(e) {
    document.getElementById("upload_files").value = "";
    e.preventDefault();
    $("#upload_files").trigger('click');
}

/**
 * Enables AOI upload capabilities by adding drop events to the drop zone
 */
function enableUpload() {
    const targetEl = document.getElementById("drop-container");
    if (uploadLayer) {
        uploadLayer.clearLayers();
        uploadLayer.remove();
        uploadLayer = null;
        targetEl.removeEventListener("dragenter", prevent);
        targetEl.removeEventListener("dragover", prevent);

        targetEl.removeEventListener("drop", handleFiles);
    }
    targetEl.addEventListener("dragenter", prevent);
    targetEl.addEventListener("dragover", prevent);

    targetEl.addEventListener("drop", handleFiles);

    uploadLayer = L.geoJson().addTo(map);


}

function prevent(e) {
    e.preventDefault();
}

function verifyFeatures(data) {
    const allPoints = data.features.map(f => f.geometry.type.toLowerCase() === "point").every(v => v === true);
    let verifiedRequirements = false;
    if (!allPoints && data.features.length <= 20) {
        verifiedRequirements = true;
    } else if (allPoints && data.features.length === 1) {
        verifiedRequirements = true;
    }
    return verifiedRequirements;
}

function addDataToMap(data){
    try {
            // this handles all upload situations
            let verifiedRequirements = verifyFeatures(data);
            if(verifiedRequirements){
                uploadLayer.clearLayers();
                uploadLayer.addData(data);
                try {
                    map.fitBounds(uploadLayer.getBounds());
                } catch(e){
                    map.fitBounds([
                            [data.bbox[1], data.bbox[0]],
                            [data.bbox[3], data.bbox[2]],
                        ]);
                }
                $("#upload_error").hide();
                collect_review_data();
                verify_ready();
            } else{
                upload_file_error();
            }
        } catch (e) {
            upload_file_error();
        }
}

function handleFiles(e) {
    e.preventDefault();
    const reader = new FileReader();
    reader.onloadend = function () {
        try {
            addDataToMap(JSON.parse(this.result.toString()));
           } catch (e) {
            upload_file_error();
        }
    };
    const files = e.target.files || e.dataTransfer.files || this.files;
    for (let i = 0, file; (file = files[i]); i++) {
        if (file.type === "application/json") {
            reader.readAsText(file);
        } else if (file.name.indexOf(".geojson") > -1) {
            reader.readAsText(file);
        } else if (file.type === "application/x-zip-compressed" || file.type === "application/zip") {
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
                    // let URL =
                    //         window.URL || window.webkitURL || window.mozURL || window.msURL,
                    //     url = URL.createObjectURL(
                    //         new Blob([JSON.stringify(data)], {type: "application/json"})
                    //     );
                    // if (data.features.length > 10) {
                    //     data.features = data.features.splice(0, 10);
                    // }
                    // uploadLayer.addData(data);
                    addDataToMap(data);
                    // let verifiedRequirements = verifyFeatures(data);
                    // if(verifiedRequirements){
                    //     uploadLayer.clearLayers();
                    //     uploadLayer.addData(data);
                    //
                    //     map.fitBounds([
                    //         [data.bbox[1], data.bbox[0]],
                    //         [data.bbox[3], data.bbox[2]],
                    //     ]);
                    //     $("#upload_error").hide();
                    //     collect_review_data();
                    //     verify_ready();
                    // } else{
                    //     upload_file_error();
                    // }

                    $(".dimmer").removeClass("active");
                    $("#preview").addClass("disabled");
                    $("#epsg").val("");
                    $("#encoding").val("");
                    $("#info").addClass("picInfo");
                    $("#option").slideUp(500);
                    collect_review_data();
                    verify_ready();
                }
            );
            $("#upload_error").hide();
        } else {
            +upload_file_error();
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
            polygon: {
                icon: new L.DivIcon({
                    iconSize: new L.Point(8, 8),
                    className: 'leaflet-div-icon leaflet-editing-icon'
                }),
                touchIcon: new L.DivIcon({
                    iconSize: new L.Point(20, 20),
                    className: 'leaflet-div-icon leaflet-editing-icon leaflet-touch-icon'
                }),
            }
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
            drawnItems.addLayer(layer);
            drawnItems.addLayer(layer);
        } else {
            if (drawnItems.getLayers().length < 20) {
                drawnItems.addLayer(layer);
                if (drawnItems.getLayers().length === 20) {
                    alert("Maximum of 20 has been reached.  You may edit or remove shapes but you may not add more.");
                }
            } else {
                alert("You may not add this polygon because you have reached the limit of 20.");
            }
        }

        collect_review_data();
        verify_ready();
    });

    map.on('draw:edited', function () {
        collect_review_data();
        verify_ready();
    });

    map.on(L.Draw.Event.DELETED, function () {
        collect_review_data();
        verify_ready();
    });

    map.on("draw:drawstart", function (e) {
        if (e.layerType === "marker") {
            drawnItems.clearLayers();
        } else {
            let BreakException = {};
            // check to make sure drawnItems does not contain a marker
            try {
                drawnItems.eachLayer(function (layer) {
                    if (layer instanceof L.Marker) {
                        drawnItems.clearLayers();
                        throw BreakException;
                    }
                });
            } catch (e) {
                if (e !== BreakException) throw e;
            }
            // check to make sure drawnItems.length <= 20
        }
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
            propertyName: "NAME,AREA_CODE,DESCRIPTION",
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
                    if(highlightedIDs.length <= 20) {
                        if (highlightedIDs.length === 20) {
                            alert("Max selections has been reached");
                        }
                    } else{
                        alert("You may only select 20 features.");
                        highlightedIDs = highlightedIDs.filter((e) => e !== selectedID);
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

                        map.addLayer(adminHighlightLayer);
                        adminHighlightLayer.setZIndex(
                            Object.keys(baseLayers).length + client_layers.length + 6
                        );
                        collect_review_data();
                        verify_ready();
                }
            },
        });
    });
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
        onChange: function () {
            adjustLayerIndex();
        },
        filter: ".ignore-elements",
        // Called when creating a clone of element
        // onClone: function (/**Event*/evt) {
        //     let origEl = evt.item;
        //     let cloneEl = evt.clone;
        // },
    });
}

/**
 * Displays the error when the upload file is incorrect
 */
function upload_file_error() {
    const upload_error = $("#upload_error");
    upload_error.html("* invalid file upload, please see the <a href='" + $("#menu-help").attr('href') + "#geojson'>Help Center</a> for more info about upload formats..")
    upload_error.show();
}

/**
 * adjusts the layer indexes to match the order in the layer manager
 */
function adjustLayerIndex() {
    let count = 10;
    const ol_layers_li = $("ol.layers li");
    for (let i = ol_layers_li.length; i > 0; i--) {
        if (overlayMaps[
            ol_layers_li[i - 1].id.replace("_node", "TimeLayer")
            ]) {
            overlayMaps[
                ol_layers_li[i - 1].id.replace("_node", "TimeLayer")
                ].setZIndex(count);
            count++;
        } else {
            let id = ol_layers_li[i - 1].id.replace("_node", "") + "ens";
            let element = $("[id^=" + id + "]");
            for (let j = 0; j < element.length; j++) {
                overlayMaps[
                element[j].id + "TimeLayer"
                    ].setZIndex(count);
                count++;
            }
        }
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

    const slider_range = '<div id="slider-range" onclick="open_range_picker()"' +
        '><span id="slider-range-txt">N/A to N/A</span></div>'
    $(".leaflet-bar.leaflet-bar-horizontal.leaflet-bar-timecontrol.leaflet-control").prepend(slider_range);
}

/**
 * initiatives and opens the range picker
 */
function open_range_picker() {
    // open a dialog with 2 date fields, from and to (populated with current range) and
    // an update range button which calls setRange(from, to)
    // possibly a close button, but the [X] is likely enough
    // maybe a "full range" or "remove range" button as well
    close_dialog();

    let hasLayers = false;
    for (let key in overlayMaps) {
        if (overlayMaps.hasOwnProperty(key)) {
            if (map.hasLayer(overlayMaps[key])) {
                hasLayers = true;
                break;
            }
        }
    }
    let range_picker;
    if (hasLayers) {
        let current_min = "";
        let current_max = "";
        if (map.timeDimension.getLowerLimit()) {
            current_min = moment(map.timeDimension.getLowerLimit()).utc().format('YYYY-MM-DD');
            current_max = moment(map.timeDimension.getUpperLimit()).utc().format('YYYY-MM-DD');
        }
        const min_date = moment.utc(layer_limits.min).format('YYYY-MM-DD');
        const max_date = moment.utc(layer_limits.max).format('YYYY-MM-DD');
        range_picker = '<p class="picker-text">Select start date and end date of the desired animation loop</p>';
        range_picker += '<form id="range_picker_form" style="width:100%; height:100%; display: flex; align-items: center;" class="picker-text">';

        range_picker += '<div class="form-group panel-buffer">';
        range_picker += '<input type="date" class="form-control" placeholder="YYYY-MM-DD"';
        range_picker += 'id="begin_range_date" value="' + current_min + '" min="' + min_date + '" max="' + max_date + '"';
        range_picker += 'onchange="verify_range()">';
        range_picker += '<div class="input-group-addon">to</div>';
        range_picker += '<input type="date" class="form-control" placeholder="YYYY-MM-DD"';
        range_picker += 'id="end_range_date" value="' + current_max + '" min="' + min_date + ' " max="' + max_date + '"';
        range_picker += 'onchange="verify_range()">';
        range_picker += '<label id="range-error" for="end_range_date"';
        range_picker += 'style="color:#da2020; display: none;">End date must be equal or greater';
        range_picker += 'than the start date</label></div>';
        range_picker += '</form>';
        range_picker += '<div class="just-buttons">';
        range_picker += '<button style="width:45%" onclick="clearRange()">Clear Range</button>';
        range_picker += '<button style="width:45%" onclick="setRange()">Set Range</button>';
        range_picker += '</div>';
    } else {
        range_picker = '<p class="picker-text">You must add at least one layer to the map before you set an animation range</p>';
    }
    let dialog = $("#dialog");
    dialog.html(range_picker);
    dialog.dialog({
        title: "Range Picker",
        resizable: false,
        width: $(window).width() / 2,
        height: "auto",
        position: {
            my: "center",
            at: "center",
            of: window
        }
    });
    $('#range_picker_form').validate();
}

// function close_dialog_event() {
//     $('#dialog').dialog('close');
// }

/**
 * Sets the animation range
 */
function setRange() {
    const startTime = new Date($('#begin_range_date').val());
    const endTime = new Date($('#end_range_date').val());
    map.timeDimension.setLowerLimit(startTime);
    map.timeDimension.setUpperLimit(endTime);
    map.timeDimension.setCurrentTime(startTime);
    $("#slider-range-txt").text(moment(startTime).utc().format('MM/DD/YYYY') +
        " to " + moment(endTime).utc().format('MM/DD/YYYY'));

}

/**
 * Clears the range set by the user
 */
function clearRange() {
    map.timeDimension.setLowerLimit(false);
    map.timeDimension.setUpperLimit(false);
}

/**
 * downloads the user specified AOI
 */
function download_aoi() {
    const aoi = document.createElement('a');
    aoi.setAttribute(
        'href',
        'data:text/plain;charset=utf-8,' + encodeURIComponent($("#geometry").text().trim()));
    aoi.setAttribute('download', "climateserv_aoi.geojson");
    aoi.click();
}

/**
 * Helper function for completeness of the request dates
 * @returns {boolean}
 */
function isComplete() {
    // what if ensemble data with forecast dates
    // this will have to check those fields,
    // or assign dates to these when selected (think this is better)
    let isReady = false;
    const sDate_new_cooked = document.getElementById("sDate_new_cooked");
    const eDate_new_cooked = document.getElementById("eDate_new_cooked");
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
    return isReady;
}

/**
 * Verifies if the user has filled in enough information to send a request
 * When ready, enables the request button as well and the view API button
 */
function verify_ready() {
    let ready = true;
    const requestTypeSelect = $("#requestTypeSelect");
    if (requestTypeSelect.val() === "datasets") {
        ready = isComplete();
    }
    const geometry = $("#geometry");
    const download_aoi_holder = $("#download_aoi_holder");
    const disabled = !(geometry.text().trim() !== '{"type":"FeatureCollection","features":[]}' && ready);
    $("#btnRequest").prop("disabled", disabled);
    $("#btnViewAPI").prop("disabled", disabled);
    if (geometry.text().trim().indexOf('{"type"') > -1
        || geometry.text().trim().indexOf('{\"type\"') > -1) {
        download_aoi_holder.show();
    } else {
        download_aoi_holder.hide();
    }
    let api_host = window.location.hostname;
    if (window.location.port) {
        api_host += ":" + window.location.port
    }
    try {
        if (requestTypeSelect.val() === "datasets") {
            const formData = new FormData();
            buildForm(formData);
            $("#api_query").text(api_host + "/api/submitDataRequest/?" + new URLSearchParams(formData).toString());
        } else {
            $("#api_query").text(api_host + get_API_url());
        }
    } catch (e) {
        console.log(e);
    }
}

/**
 * Builds and returns the url needed to make an API call
 * @returns {string}
 */
function get_API_url() {
    let geometry_params;
    if (highlightedIDs.length > 0) {
        geometry_params = "&layerid=" + adminHighlightLayer.options.layers.replace("_highlight", "");
        geometry_params += "&featureids=" + highlightedIDs.join(",");
    } else if (drawnItems.getLayers().length > 0) {
        geometry_params = "&geometry=" + JSON.stringify(drawnItems.toGeoJSON());
    } else if (uploadLayer) {
        geometry_params = "&geometry=" + JSON.stringify(uploadLayer.toGeoJSON());
    }

    const csi = climateModelInfo.climate_DataTypeCapabilities[0].current_Capabilities;
    let url = "api/submitMonthlyRainfallAnalysisRequest/?custom_job_type=monthly_rainfall_analysis&";
    url += "seasonal_start_date=" + csi.startDateTime;
    url += "&seasonal_end_date=" + csi.endDateTime;
    url += geometry_params;
    return url
}

/**
 * Verifies range is within the data start and end range
 * This function is called from a dynamically created element
 * DO NOT REMOVE
 * @returns {boolean}
 */
function verify_range() {
    let isReady = false;
    let begin_range_date = document.getElementById("begin_range_date");
    let end_range_date = document.getElementById("end_range_date");
    if(begin_range_date && end_range_date) {
        if (begin_range_date.value && end_range_date.value) {
            isReady = $(begin_range_date).valid({
                rules: {
                    field: {
                        required: true,
                        dateISO: true
                    }
                }
            }) && $(end_range_date).valid({rules: {field: {required: true, dateISO: true}}});
            if (isReady) {
                // Also should confirm s < e;
                if (moment(begin_range_date.value) > moment(end_range_date.value)) {
                    isReady = false;
                    $("#range-error").show();
                } else {
                    $("#range-error").hide();
                }
            }

        } else {
            $(begin_range_date).valid({rules: {field: {required: true, dateISO: true}}});
            $(end_range_date).valid({rules: {field: {required: true, dateISO: true}}});
        }
    }
    return isReady;
}

/**
 * Collects all of the request data
 */
function collect_review_data() {
    const geometry = $("#geometry");
    const operation_max = $("#operation_max");
    const operation_min = $("#operation_min");
    const operation_average = $("#operation_average");
    if (highlightedIDs.length > 0) {
        const feature_label = highlightedIDs.length > 1 ? "Features" : "Feature"
        geometry.text(adminHighlightLayer.options.layers.replace("_highlight", " - " + feature_label + ": ").replace("admin_2_af", "Admin #2").replace("admin_1_earth", "Admin #1").replace("country", "Country") + highlightedIDs.join());
    } else if (drawnItems.getLayers().length > 0) {
        geometry.text(JSON.stringify(drawnItems.toGeoJSON()));
    } else if (uploadLayer) {
        geometry.text(JSON.stringify(uploadLayer.toGeoJSON()));
    } else {
        geometry.text('{"type":"FeatureCollection","features":[]}');
    }
    if (geometry.text().indexOf("Point") > -1) {
        operation_max.hide();
        operation_min.hide();
        operation_average.text("Timeseries");
    } else {
        operation_max.show();
        operation_min.show();
        operation_average.text("Average");
    }
}

// function getEnsDataType() {
//     // will need to write this for the selected ensembles
//     return
// }

/**
 * Handles the initial return from the submitRequest by setting up progress bar and
 * starting the polling process
 * @param data
 * @param isClimate
 */
function handle_initial_request_data(data, isClimate) {
    close_dialog();
    let progress = '<div style="width:100%; height:100%; display: flex;' +
        '    align-items: center;' +
        '}">';
    progress += '<div class="progress">';
    progress += '<div class="progress-bar progress-bar-striped progress-bar-animated"' +
        ' role="progressbar" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100"' +
        ' style="width: 0"><span><span class="percentage" id="text_percent">0%</span></span></div>';
    progress += '</div></div>';
    const dialog = $("#dialog");
    dialog.html(progress);
    dialog.dialog({
        title: "Query Progress",
        resizable: false,
        width: $(window).width() / 2,
        height: 200,
        close: function () {
            clearTimeout(polling_timeout);
        },
        position: {
            my: "center",
            at: "center",
            of: window
        }
    });
    pollForProgress(data[0], isClimate);
}

/**
 * Builds the form for the API request
 * @param formData
 */
function buildForm(formData) {

    current_calculation = {
        'value': parseInt($("#operationmenu").val()),
        'text': $("#operationmenu option:selected").text()
    };

    if ($("#ensemble_builder").is(":visible")) {
        formData.append(
            "datatype", parseInt($("#ensemblemenu").val()) + $("#ensemblevarmenu")[0].selectedIndex
        );
    } else {
        formData.append(
            "datatype", $("#sourcemenu").val()
        );
    }

    formData.append("begintime", moment(document.getElementById("sDate_new_cooked").value).format('MM/DD/YYYY')); // "01/01/2020");
    formData.append("endtime", moment(document.getElementById("eDate_new_cooked").value).format('MM/DD/YYYY')); //"06/30/2020");
    formData.append("intervaltype", 0);
    formData.append("operationtype", current_calculation.value);
    formData.append("dateType_Category", "default");  // ClimateModel shouldn't be needed. please confirm
    formData.append("isZip_CurrentDataType", false);
    if (highlightedIDs.length > 0) {
        formData.append("layerid", adminHighlightLayer.options.layers.replace("_highlight", ""));
        formData.append("featureids", highlightedIDs.join(","));
    } else if (drawnItems.getLayers().length > 0) {
        formData.append("geometry", JSON.stringify(drawnItems.toGeoJSON()));
    } else if (uploadLayer) {
        formData.append("geometry", JSON.stringify(uploadLayer.toGeoJSON()));
    }
}

/**
 * Initiates the processing request to ClimateSERV
 */
function sendRequest() {
    current_calculation = {
        'value': parseInt($("#operationmenu").val()),
        'text': $("#operationmenu option:selected").text()
    };
    //set the calculation info here
    clearTimeout(polling_timeout);
    $("#btnRequest").prop("disabled", true);
    const formData = new FormData();
    if ($("#requestTypeSelect").val() === "datasets") {
        buildForm(formData);
        let api_host = window.location.hostname;
        if (window.location.port) {
            api_host += ":" + window.location.port
        }
        $("#api_query").text(api_host + "/api/submitDataRequest/?" + new URLSearchParams(formData).toString());
        $.ajax({
            url: "/api/submitDataRequest/",
            type: "POST",
            processData: false,
            contentType: false,
            async: true,
            crossDomain: true,
            data: formData
        }).fail(function (jqXHR, textStatus, errorThrown) {
            console.warn(jqXHR + textStatus + errorThrown);
            close_dialog();
            let error_message = '<div style="width:100%; height:100%; display: flex;' +
                '    align-items: center;' +
                '}">';
            error_message += '<div style="width:100%; text-align: center;">';
            error_message += '<h1 class="step-marker" style="line-height: 2em;">Processing Error</h1>';
            error_message += '<p style="line-height: 2em;">There was am error processing this request';
            error_message += '.  If this persists, please contact us for assistance and reference the id.</p>'

            error_message += '</div>';
            const dialog = $("#dialog");
            dialog.html(error_message);
            dialog.dialog({
                title: "Processing Error",
                resizable: false,
                width: $(window).width() / 2,
                height: 200,
                position: {
                    my: "center",
                    at: "center",
                    of: window
                },
                close: function () {
                    $("#btnRequest").prop("disabled", false);
                    $("#btnViewAPI").prop("disabled", false);
                }
            });
        }).done(function (data, _textStatus, _jqXHR) {
            if (data.errMsg) {
                console.info(data.errMsg);
            } else {
                handle_initial_request_data(JSON.parse(data), false);
            }
        });

    } else {
        $.ajax({
            url: get_API_url(),
            type: "GET",
            async: true,
            crossDomain: true
        }).fail(function (jqXHR, textStatus, errorThrown) {
            console.warn(jqXHR + textStatus + errorThrown);
        }).done(function (data, _textStatus, _jqXHR) {
            if (data.errMsg) {
                console.info(data.errMsg);
            } else {
                handle_initial_request_data(JSON.parse(data), true);
            }
        });
    }
}

/**
 * Updates progress bar with value sent in
 * @param val
 */
function updateProgress(val) {
    $('.progress-bar').css('width', val + '%').attr('aria-valuenow', val);
    $("#text_percent").text(parseInt(val) + '%');
}

/**
 * Get the progress of a submitted job and sends teh returned value to updateProgress
 * @param id
 * @param isClimate
 */
function pollForProgress(id, isClimate) {
    $.ajax({
        url: "/api/getDataRequestProgress/?id=" +
            id,
        type: "GET",
        async: true,
        crossDomain: true
    }).fail(function (jqXHR, textStatus, errorThrown) {
        console.warn(jqXHR + textStatus + errorThrown);
    }).done(function (data, _textStatus, _jqXHR) {
        if (data.errMsg) {
            console.info(data.errMsg);
        } else {
            const val = JSON.parse(data)[0];
            if (val !== -1 && val !== 100) {
                retries = 0;
                updateProgress(val);
                polling_timeout = setTimeout(function () {
                    pollForProgress(id, isClimate);
                }, 500);

            } else if (val === 100) {
                retries = 0;
                if ($("#operationmenu").val() === "6") {
                    getDownLoadLink(id);
                } else {
                    getDataFromRequest(id, isClimate);
                }
            } else {
                if (retries < 5) {
                    console.log("Needed retry");
                    retries++;
                    setTimeout(function () {
                        pollForProgress(id, isClimate);
                    }, 500)
                } else {
                    retries = 0;
                    console.log("Server Error");
                    $("#btnRequest").prop("disabled", false);
                    $("#btnViewAPI").prop("disabled", false);
                    close_dialog();
                    let error_message = '<div style="width:100%; height:100%; display: flex;' +
                        '    align-items: center;' +
                        '}">';
                    error_message += '<div style="width:100%; text-align: center;">';
                    error_message += '<h1 class="step-marker" style="line-height: 2em;">Processing Error</h1>';
                    error_message += '<p style="line-height: 2em;">There was am error processing Job ID: ' + id;
                    error_message += '.  If this persists, please contact us for assistance and reference the id.</p>'

                    error_message += '</div>';
                    const dialog = $("#dialog");
                    dialog.html(error_message);
                    dialog.dialog({
                        title: "Processing Error",
                        resizable: false,
                        width: $(window).width() / 2,
                        height: 200,
                        position: {
                            my: "center",
                            at: "center",
                            of: window
                        },
                        close: function () {
                            $("#btnRequest").prop("disabled", false);
                        }

                    });
                }
            }
        }
    });
}

/**
 * Sets the UI to the correct state when a different source is selected
 * @param which
 */
function handleSourceSelected(which) {
    which = which.toString();
    let layer = client_layers.find(
        (item) => item.app_id === which
    )
    if (layer) {
        $("#ensemble_builder").hide();
        $("#non-multi-ensemble-dates").show();
        //show date range controls
    } else {
        // open and set ensemble section
        $("#ensemble_builder").show();
        $("#non-multi-ensemble-dates").hide();
        //hide date range controls

        $('#model_run_menu').find('option').remove();
        $('#ensemblemenu').find('option').remove();
        $("#forecastfrommenu").find('option').remove();
        $("#forecasttomenu").find('option').remove();
        // load the ensemble selection tools

        let id = which + "ens";

        $("[id^=" + id + "]").each(function (index, item) {
            const temp = getLayer(item.id);
            $("#ensemblemenu").append('<option value="' + temp.app_id + '">' + temp.title + '</option>');
        });

        $.ajax({
            url: "api/getClimateScenarioInfo/" +
                id,
            type: "GET",
            async: true,
            crossDomain: true
        }).fail(function (jqXHR, textStatus, errorThrown) {
            console.warn(jqXHR + textStatus + errorThrown);
        }).done(function (sdata, _textStatus, _jqXHR) {
            if (sdata.errMsg) {
                console.info(sdata.errMsg);
            } else {
                const data = JSON.parse(sdata);
                const cc = data.climate_DataTypeCapabilities[0].current_Capabilities;
                cc.startDateTime;

                $('#model_run_menu').append('<option value="' + cc.startDateTime + '">' + cc.startDateTime.replaceAll("-", "/").substr(0, cc.startDateTime.lastIndexOf("-")) + '</option>');

                // create date dropdowns
                const mformat = "YYYY-MM-DD"
                let sdate = moment(cc.startDateTime, mformat);
                let edate = moment(cc.endDateTime, mformat);
                let count = 1;

                $("#sDate_new_cooked").val(sdate.format('YYYY-MM-DD'));
                $("#eDate_new_cooked").val(sdate.format('YYYY-MM-DD'));

                do {
                    $("#forecastfrommenu")
                        .append
                        (
                            '<option value="' + sdate.format('YYYY-MM-DD') + '">' + "f" + count.toString().padStart(3, "0") + " " + sdate.format('YYYY-MM-DD') + '</option>');
                    $("#forecasttomenu")
                        .append
                        (
                            '<option value="' + sdate.format('YYYY-MM-DD') + '">' + "f" + count.toString().padStart(3, "0") + " " + sdate.format('YYYY-MM-DD') + '</option>');
                    count++;
                    sdate.add(1, "days");
                } while (sdate < edate)

                cc.endDateTime;
                cc.date_FormatString_For_ForecastRange;
                cc.number_Of_ForecastDays;
                $("#ensemblevarmenu").empty();
                data.climate_DatatypeMap[0].climate_DataTypes.forEach((variable) => {
                    // add variable with label to select

                    $("#ensemblevarmenu")
                        .append(
                            '<option value="' + variable.climate_Variable + '">' + variable.climate_Variable_Label + '</option>');
                });
            }
        });
    }
    $("#btnRequest").prop("disabled", false);
}

/**
 * synchronizes the dates between start date and forecast date
 */
function syncDates() {
    $("#sDate_new_cooked").val($("#forecastfrommenu").val());
    $("#eDate_new_cooked").val($("#forecasttomenu").val());
}

/**
 * Creates the chart dialog
 */
function inti_chart_dialog() {
    close_dialog();
    $("#btnPreviousChart").prop("disabled", true);
    const dialog = $("#dialog");
    const isMobile = $("#isMobile");
    dialog.html(
        '<div id="chart_holder"></p>'
    );
    dialog.dialog({
        title: "Statistical Query",
        resizable: isMobile.css("display") === "block" ? false : {handles: "se"},
        width: isMobile.css("display") === "block" ? $(window).width() : $(window).width() - ($("#sidebar").width() + 100),
        height: $(window).height() - 140,
        resize: function () {
            Highcharts.charts[0].reflow();
            window.dispatchEvent(new Event('resize'));
        },
        open: function () {
            window.dispatchEvent(new Event('resize'));
        },
        position: isMobile.css("display") === "block" ? {
            my: "center",
            at: "center",
            of: window
        } : {
            my: "right",
            at: "right-25",
            of: window
        }
    });

    dialog.on('dialogclose', function () {
        $("#btnPreviousChart").prop("disabled", false);
    });
}

/**
 * Opens the last chart that was closed.  Currently we only store one prior state
 */
function open_previous_chart() {
    if (previous_chart) {
        inti_chart_dialog();
        finalize_chart(previous_chart.compiled_series, previous_chart.units, previous_chart.xAxis_object, previous_chart.title, previous_chart.isClimate)
    } else {
        alert("you have no previous chart, please send a request.")
    }
}

/**
 * Helper function for monthly rainfall analysis graphing
 * @param which
 * @returns {number}
 */
function getIndex(which) {
    switch (which) {
        case 'col02_MonthlyAverage':
            return 0;
        case 'col05_50thPercentile':
            return 1;
        case 'col03_25thPercentile':
            return 2;
        default:
            return 3;
    }
}

/**
 * Creates the download link for a request ID and displays it in the dialog
 * @param id
 */
function getDownLoadLink(id) {
    close_dialog();
    let download = '<div style="width:100%; height:100%; display: flex;' +
        '    align-items: center;' +
        '}">';
    download += '<div style="width:100%; text-align: center;">';
    download += '<h1 class="step-marker" style="line-height: 2em;">File Download Ready</h1>';
    download += '<p style="line-height: 2em;">Job ID: ' + id + '</p>';
    const url = '/api/getFileForJobID/?id=' + id
    download += '<a href="' + url + '" class="step-marker" style="line-height: 2em;">Click Here to Download File</a>';
    download += '</div></div>';
    const dialog = $("#dialog");
    dialog.html(download);
    dialog.dialog({
        title: "Download Data",
        resizable: false,
        width: $(window).width() / 2,
        height: 200,
        position: {
            my: "center",
            at: "center",
            of: window
        }

    });
}

/**
 * Function to retrieve the processed data from the server with the id
 * that was created by the submitDataRequest
 * @param id
 * @param isClimate
 */
function getDataFromRequest(id, isClimate) {
    close_dialog();
    let complete = '<div style="width:100%; height:100%; display: flex;' +
        '    align-items: center;' +
        '}">';
    complete += '<div style="width:100%">';
    complete += '<h1 class="step-marker">Processing complete, downloading results.</h1>';
    complete += '</div>';
    const dialog = $("#dialog");
    dialog.html(complete);
    dialog.dialog({
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

    $.ajax({
        url: "/api/getDataFromRequest/?id=" +
            id,
        type: "GET",
        async: true,
        crossDomain: true
    }).fail(function (jqXHR, textStatus, errorThrown) {
        console.warn(jqXHR + textStatus + errorThrown);
    }).done(function (data, _textStatus, _jqXHR) {
        if (data.errMsg) {
            console.info(data.errMsg);
        } else {
            if (data === "need to send id") {
                if (too_fast < 5) {
                    getDataFromRequest(id, isClimate);
                }
            } else {
                too_fast = 0;

                if (isClimate) {

                    const graph_obj = JSON.parse(data).MonthlyAnalysisOutput.avg_percentiles_dataLines;

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
                    let start_month = 13;
                    graph_obj.forEach((o, i) => {
                        let month_year;
                        //new Date().getFullYear()
                        if (i === 0) {
                            start_month = parseInt(o.col01_Month);
                            month_year = o.col01_Month + "-" + new Date().getFullYear();
                        } else {
                            if (start_month < parseInt(o.col01_Month)) {
                                month_year = o.col01_Month + "-" + new Date().getFullYear();
                            } else {
                                const date = new Date();
                                date.setFullYear(date.getFullYear() + 1);
                                month_year = o.col01_Month + "-" + date.getFullYear();
                            }
                        }
                        if (!xaxis.categories.includes(month_year)) {
                            xaxis.categories.push(month_year);
                        }

                        rainfall_data[0].data.push(value_or_null(o.col05_50thPercentile));
                        rainfall_data[1].data.push(value_or_null(o.col02_MonthlyAverage));
                        rainfall_data[2].data.push(value_or_null(o.col03_25thPercentile));
                        rainfall_data[3].data.push(value_or_null(o.col04_75thPercentile));

                    });
                    inti_chart_dialog();

                    finalize_chart(rainfall_data, "mm", xaxis, "Monthly Rainfall Analysis", isClimate);

                } else {
                    const compiledData = [];
                    // change this, set it on send request
                    const otState = current_calculation.value;
                    if (otState === 6) {
                        // this is a download request form download link
                    } else {
                        let min = 9999;
                        let max = -9999;
                        JSON.parse(data).data.forEach((d) => {
                            let val;

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
                            } else {
                                const null_array = [];
                                null_array.push(parseInt(d.epochTime) * 1000);
                                null_array.push(null);
                                compiledData.push(null_array); // i can likely store min and max here
                            }
                        });
                        from_compiled = compiledData; // if this is empty, i need to let the user know there was no data
                        inti_chart_dialog();

                        if (compiledData.length === 0) {
                            //inti_chart_dialog
                            $("#chart_holder").html("<h1>No data available</h1>");
                        } else {
                            let layer = client_layers.find(
                                (item) => item.app_id === $("#sourcemenu").val()
                            ) || client_layers.find(
                                (item) => item.app_id === $("#ensemblemenu").val()
                            );
                            const units = layer.units.includes("|units|")
                                ? layer.units.split("|units|")[document.getElementById("ensemblevarmenu").selectedIndex]
                                : layer.units
                            //const units = layer.units;  // if layer units contains |units| split, then index

                            const yAxis_format = layer.yAxis_format || null;
                            const point_format = layer.point_format || null
                            finalize_chart([{
                                    color: "#758055",
                                    type: "line",
                                    name: current_calculation.text,
                                    data: compiledData.sort((a, b) => a[0] - b[0])
                                }], units, {
                                    type: "datetime"
                                }, $("#sourcemenu option:selected").text(),
                                false,
                                yAxis_format,
                                point_format);
                        }
                    }
                }
            }
        }
    });
}

/**
 * Fixes no data value issue by setting them to null
 * Updated from -9999 to > -9000 as i found some values
 * were returned not exactly as -9999 for whatever reason
 * @param value
 * @returns {null|number}
 */
function value_or_null(value) {
    if (value > -9000) {
        return Number.parseFloat(value);
    } else {
        return null;
    }
}

/**
 * Creates a graph with the supplied variables
 * @param compiled_series
 * @param units
 * @param xAxis_object
 * @param title
 * @param isClimate
 * @param yAxis_format
 * @param point_format
 */
function finalize_chart(compiled_series, units, xAxis_object, title, isClimate, yAxis_format, point_format) {
    previous_chart = {
        "compiled_series": compiled_series,
        "units": units,
        "xAxis_object": xAxis_object,
        "title": title,
        "isClimate": isClimate
    };

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

    if (yAxis_format) {
        chart_obj.yAxis.labels = yAxis_format
    }

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
                        // maybe set current time for layers to this date
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
        zoomType: 'xy',
        events: {
            redraw: function () {
                try {
                    img.translate(
                        this.chartWidth - originalWidth,
                        this.chartHeight - originalHeight
                    );
                } catch (e) {
                }
            }
        }
    };
    chart_obj.series = compiled_series;
    if (point_format) {
        chart_obj.tooltip = point_format;
        chart_obj.tooltip.borderColor = "#758055";
    } else {

        chart_obj.tooltip = {
            pointFormat: "Value: {point.y:.2f} " + units,
            borderColor: "#758055",
        };
    }
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

/**
 * Helper function for graphing the monthly rainfall analysis
 * @param mmm_Y
 * @param type
 * @param data
 * @returns {*[]}
 */
function getDataLine(mmm_Y, type, data) {
    const data_line = [];
    data_line['Month_Year'] = mmm_Y;
    data_line['data_series_type'] = type;
    data_line['Monthly_Rainfall_mm'] = data;
    return data_line;
}

/**
 * Opens the proper panel for the datatype selected
 * @param select_control
 */
function openDataTypePanel(select_control) {
    if (select_control.value === "datasets") {
        $("#panel_monthly_rainfall").hide();
        $("#panel_dataset").show();
    } else {
        $("#panel_dataset").hide();
        $("#panel_monthly_rainfall").show();
    }
    verify_ready();
}

/**
 * Calls the api to get the current Climate scenario properties
 */
function getClimateScenarioInfo() {
    $.ajax({
        url: "api/getClimateScenarioInfo/",
        type: "GET",
        async: true,
        crossDomain: true
    }).fail(function (jqXHR, textStatus, errorThrown) {
        console.warn(jqXHR + textStatus + errorThrown);
    }).done(function (data, _textStatus, _jqXHR) {
        if (data.errMsg) {
            console.info(data.errMsg);
        } else {
            climateModelInfo = JSON.parse(data);
        }
    });
}

/**
 * Toggles the About AOI section
 */
function toggleAOIHeight() {
    const el = $('#aoiOptions');
    const curHeight = el.height();
    if (curHeight === 0) {
        const autoHeight = el.css('height', 'auto').height();
        el.height(curHeight).animate({height: autoHeight}, 1000);
        el.css("marginBottom", '20px');
    } else {
        el.height(curHeight).animate({height: 0}, 1000);
        el.css("marginBottom", '0px');
    }
}

/**
 * Closes the dialog if it is open
 */
function close_dialog() {
    const dialog = $("#dialog");
    if (dialog.dialog()) {
        dialog.dialog("close");
    }
}

/**
 * Framework for the statistical query info box
 * @param which type of information requested
 */
function stats_info(which) {
    close_dialog();
    const title = which === "type"
        ? "Type of request"
        : which === "source"
            ? "Data Source info" : "Calculation info"
    let stat_info = '<div style="font-size:unset; width:100%; height:100%; display: flex;' +
        '    align-items: center;' +
        '}">';
    stat_info += '<div style="width:100%; text-align: left;">';
    stat_info += get_stat_body(which)
    stat_info += '</div>';
    const dialog = $("#dialog");
    dialog.html(stat_info);
    // const the_width = $(window).width() < 500 ? $(window).width() + "px" : "500px";
    dialog.dialog({
        title: title,
        resizable: false,
        width: '500px',
        height: 'auto',
        position: {
            my: "center",
            at: "center",
            of: window
        },
        open: function () {
            $(this).dialog('option', 'maxHeight', $(window).height());
            if ($(this).width() > $(window).width()) {
                $(this).dialog('option', 'width', $(window).width());
            }
        }
    });
}

/**
 * Gets the info body for specified type (which)
 * @param which the type of information requested
 * @returns {string}
 */
function get_stat_body(which) {
    let html = '';
    switch (which) {
        case 'type':
            html += "<div style='text-align:left'><p>ClimateSERV offers direct <b>Dataset</b> queries for your AOI and specific time period " +
                "which result offer results in graphs with " +
                "download options or downloadable raw data.</p><br>";
            html += "<p>In addition we offer a <b>Monthly Rainfall Analysis</b> which is derived from a combination" +
                " of CHIRPS historical data and current NMME seasonal forecast data. </p><br></div>";
            break;
        case 'source':
            html += "<div id=\"popup_SelectData_DataSource_ToolTip\" class=\"ui inverted popup servir_helper_width_215rem right center transition hidden\" style=\"max-height: 808px; inset: -325px auto auto 324px;\">    " +
                "  <div class=\"servir_tooltip_header\">CHIRPS Rainfall</div>" +
                "  <div class=\"servir_tooltip_body\">Climate Hazards group IR Precipitation with Stations (CHIRPS).</div>" +
                "  <!--<div class=\"servir_tooltip_link\" onclick=\"alert('hook me up to something!!');\">Learn more</div>-->" +
                "  <br>" +
                "  <div class=\"servir_tooltip_header\">eMODIS NDVI</div>" +
                "  <div class=\"servir_tooltip_body\">MODIS-derived Normalized Difference Vegetation Index (eMODIS NDVI).  NDVI datasets for the following regions are available: West Africa, East Africa, Southern Africa, and Central Asia</div>" +
                "  <!--<div class=\"servir_tooltip_link\" onclick=\"alert('hook me up to something!!');\">Learn more</div>-->" +
                "  <br>" +
                "  <div class=\"servir_tooltip_header\">Seasonal Forecast</div>" +
                "  <div class=\"servir_tooltip_body\">North American Multi-Model Ensemble (NMME).  Up to 180 day forecast models available.  This dataset supports download capabilities.</div>" +
                "  <!--<div class=\"servir_tooltip_link\" onclick=\"alert('hook me up to something!!');\">Learn more</div>-->" +
                "  <br>" +
                "  <div class=\"servir_tooltip_header\">IMERG 1 Day</div>" +
                "  <div class=\"servir_tooltip_body\">1 Day rainfall accumulations product from the Integrated Multi-satellitE Retrievals (IMERG) for Global Precipitation Mission (GPM).</div>" +
                "  <!--<div class=\"servir_tooltip_link\" onclick=\"alert('hook me up to something!!');\">Learn more</div>-->" +
                "  <br>" +
                "  <div class=\"servir_tooltip_header\">GEFS</div>" +
                "  <div class=\"servir_tooltip_body\">Global Ensemble Forecast System (GEFS) a weather forecast model made up of 21 separate forecasts, or ensemble members. Availability: January 1, 1985, to present.</div>" +
                "    <br>" +
                "    <div class=\"servir_tooltip_header\">Evaporative Stress Index  (ESI)</div>" +
                "    <div class=\"servir_tooltip_body\">ESI is a global dataset produced weekly at 5-kilometer resolution and reveals regions of drought where vegetation is stressed due to lack of water.</div>" +
                "    <br>" +
                "    <div class=\"servir_tooltip_header\">NASA-USDA SMAP</div>" +
                "    <div class=\"servir_tooltip_body\">The NASA-USDA Enhanced SMAP Global soil moisture data provides soil moisture information across the globe at 10-km spatial resolution.</div>" +
                "<br>" +
                "<div class=\"servir_tooltip_header\">For more information please visit the <a href='" + help_link + "' style='color:#3b6e22;'>Help Center</a></div>" +
                "</div>";
            break;
        case 'calculation':
            html += "<div id=\"popup_SelectData_Calculations_ToolTip\" class=\"ui inverted popup servir_helper_width_215rem right center transition hidden\" style=\"max-height: 808px; inset: -220.5px auto auto 324px;\">    " +
                "  <div class=\"servir_tooltip_header\">Min</div>" +
                "  <div class=\"servir_tooltip_body\">The minimum value found for all data in a given geographical selected area for each time interval in the date range.  Sometimes for large area selections, a value of 0 will be returned for every date.  If this happens, try selecting a smaller area.</div>" +
                "  <!--<div class=\"servir_tooltip_link\" onclick=\"alert('hook me up to something!!');\">Learn more</div>-->" +
                "  <br>" +
                "  <div class=\"servir_tooltip_header\">Max</div>" +
                "  <div class=\"servir_tooltip_body\">The maximum value found for all data in a given geographical selected area for each time interval in the selected date range.</div>" +
                "  <!--<div class=\"servir_tooltip_link\" onclick=\"alert('hook me up to something!!');\">Learn more</div>-->" +
                "  <br>" +
                "  <div class=\"servir_tooltip_header\">Average</div>" +
                "  <div class=\"servir_tooltip_body\">The average value for the entire geographical selected area for each time interval in the selected date range.</div>" +
                "  <!--<div class=\"servir_tooltip_link\" onclick=\"alert('hook me up to something!!');\">Learn more</div>-->" +
                "  <br>" +
                "  <div class=\"servir_tooltip_header\">Download</div>" +
                "  <div class=\"servir_tooltip_body\">Some datasets support the option to download a zip file of clipped raw data.  The format of data download is a single zip file which contains a set of geotif files for each time interval in the date selected range.</div>" +
                "  <!--<div class=\"servir_tooltip_link\" onclick=\"alert('hook me up to something!!');\">Learn more</div>-->" +
                "<br></div>";
            break;
    }
    return html;
}

/**
 * Tour variables
 */
let img, originalWidth, originalHeight;
const last_step_template = "<div class='popover tour'>" +
    "   <div class='arrow'></div>" +
    "   <h3 class='popover-title'></h3>" +
    "   <div class='popover-content'></div>" +
    "   <div class='popover-navigation'>" +
    "       <div class='btn-group'> " +
    "           <button class='btn btn-sm btn-default' data-role='prev'>« Prev</button>" +
    "           <button class='btn btn-sm btn-default' onclick='tour.goTo(0)'>Restart</button>" +
    "       </div>" +
    "       <button class='btn btn-sm btn-default' data-role='end'>End tour</button>" +
    "   </div>" +
    "</div>";

let tour;

/**
 * Initializes the tour steps and actions
 */
function init_tour() {
    tour = new Tour({
        smartPlacement: true,
        onEnd: function () {
            localStorage.setItem("hideTour", "true");
            document.querySelector(".tour_icon_blink").style.animationPlayState = 'running';
            document.querySelector(".tour_box_blink").style.animationPlayState = 'running';
        },
        autoscroll: false,
        backdrop: false,
        steps: [
            {
                element: "#menu-about",
                title: "Welcome to the ClimateSERV tour",
                content: "You may return to this tour anytime by clicking the <i class=\"fas fa-info-circle example-style\"></i> button at the bottom left of this page",
                placement: "bottom"

            },
            {
                element: "#btnAOIselect",
                title: "Statistical Query",
                content: "Start your query by either drawing, uploading, or selection the area of interest (AOI)",
                smartPlacement: true,
                onShow: function () {
                    sidebar.open('chart');
                    const el = $('#aoiOptions');
                    const curHeight = el.height();
                    const autoHeight = el.css('height', 'auto').height();
                    el.height(curHeight).animate({height: autoHeight}, 1000);
                    el.css("marginBottom", '20px');
                    const sidebar_content = $("#sidebar-content");
                    if (sidebar_content.scrollTop !== 0) {
                        sidebar_content.scrollTop(0);
                    }
                },
                onHide: function () {
                    const el = $('#aoiOptions');
                    const curHeight = el.height();
                    el.height(curHeight).animate({height: 0}, 1000);
                    el.css("marginBottom", '0px');
                }
            },
            {
                element: "#operationmenu",
                title: "Select Data",
                content: "Set the parameters of the data you would like to query.  Choose from our datasets or select monthly rainfall analysis as the type.  Select data source, calculation, start and end dates, the click Send Request.",
                onShow: function () {
                    const sidebar_content = $("#sidebar-content");
                    if (!(sidebar_content.scrollTop() + sidebar_content.innerHeight() >= sidebar_content[0].scrollHeight)) {
                        sidebar_content.animate({scrollTop: sidebar_content.prop("scrollHeight")}, 1000);
                    }
                }
            },
            {
                element: "#tab-layers",
                title: "Display Data",
                content: "Click here to show layer panel.  Check the layer you would like on the map. <br />To see that layers key, click the content stack below the name.  <br />To adjust the settings for the layer, click the gear.  <br />To animate the layer(s) use the animation controls at the bottom of the map.",
                onShow: function () {
                    sidebar.open('layers');
                },
            },
            {
                element: ".leaflet-control-timecontrol.timecontrol-play.play:first",
                title: "Layer Animation",
                content: "You animate the layers with these controls. The system will begin to cache the tiles for a smooth animation.  <br />While caching feel free to click the next button and caching will still continue in the background. <br />",
                // onShow: function (tour) {
                //     sidebar.open('layers');
                // },
            },
            {
                element: "#slider-range-txt",
                title: "Animation range",
                content: "You may select a specific animation range by clicking this button.  It will bring up a date range control for you to use.<br />",
                // onShow: function (tour) {
                //     sidebar.open('layers');
                // },
            },
            {
                element: "#basemap_link",
                title: "Change Basemap",
                content: "Click here to open basemaps, then click the one you would like to use.",
                onShow: function () {
                    sidebar.open('basemap');
                }
            },
            {
                element: "#menu-help",
                title: "Help Center",
                content: "Click here to answer any questions about the application or API",
                placement: "left"
            },
            {
                element: "#tour_link",
                title: "Tour",
                content: "Click here to open this tour anytime you need a refresher.",
                template: last_step_template
            }
        ],
        onHide: function () {
            sidebar.open('chart');
        }
    });
}

/**
 * Opens tour and removes the localStorage key
 */
function open_tour() {
    localStorage.removeItem("hideTour")
    tour.start(true);
}

/**
 * Calls initMap
 *
 * @event map-ready
 */
$(function () {
    initMap();
    try {
        init_tour();
        tour.init();
        /* This will have to check if they want to "not show" */
        if (!localStorage.getItem("hideTour")) {
            sidebar.close();
            tour.setCurrentStep(0);
            open_tour();
        }
    } catch (e) {
        console.log(e);
    }
    try {
        loadshp(
            {
                url: '/static/frontend/data/shape.zip',
                encoding: "UTF-8",
                EPSG: 4326,
            },
            function () {
                console.log("")
            });
    } catch (e2) {
        console.log(e2);
    }
    $(function () {
        $('[data-toggle="tooltip"]').tooltip({container: 'body', trigger: 'hover'})
    });
    const sourcemenu = $('#sourcemenu');
    sourcemenu.val(0);
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
        sourcemenu.change();
    } catch (e) {
    }

    try {
        verify_ready();
    } catch (e) {
    }
    try {
        $('html').on('mouseup', function (e) {
            if (!$(e.target).closest('.popover').length) {
                $('.popover').each(function () {
                    tour.end();
                });
            }
        });
    } catch (e) {
    }
    try {
        document.querySelector(".tour_icon_blink").addEventListener('animationend', () => {
            document.querySelector(".tour_icon_blink").style.animationPlayState = 'paused';
            const tour_icon = $("#tour_icon");
            tour_icon.removeClass("tour_icon_blink");
            tour_icon.addClass("tour_icon_blink");
        });
        document.querySelector(".tour_box_blink").addEventListener('animationend', () => {
            document.querySelector(".tour_box_blink").style.animationPlayState = 'paused';
            const tour_box_blink = $("#tour_box_blink");
            tour_box_blink.removeClass("tour_box_blink");
            tour_box_blink.addClass("tour_box_blink");
        });
    } catch (e) {
    }

    load_queried_layers();
    $("#dialog").dialog('close');
    $("button.ui-button.ui-corner-all.ui-widget.ui-button-icon-only.ui-dialog-titlebar-close").bind("touchstart", function () {
        $("#dialog").dialog('close');
    });
    verify_range();
});

/**
 * Support for layers activated via query string
 */
function load_queried_layers() {
    if (map.timeDimension._checkSyncedLayersReady() && !map.timeDimension.isLoading() && map.timeDimension._initHooksCalled) {
        for (let x = 0; x < queried_layers.length; x++) {
            try {
                document.getElementById(queried_layers[x]).checked = true;
                toggleLayer(queried_layers[x] + "TimeLayer");
                setTimeout(confirm_animation, 500);
            } catch (e) {
            }
        }
    } else {
        try {
            setTimeout(load_queried_layers, 500);
        } catch (e) {
        }
    }
}

/**
 * Support for layers activated via query string
 */
function confirm_animation() {
    if (!map.timeDimension.getUpperLimit()) {
        for (let x = 0; x < queried_layers.length; x++) {
            try {
                toggleLayer(queried_layers[x] + "TimeLayer");
                setTimeout(confirm_animation, 500);
            } catch (e) {
            }
        }
    } else {
        map.timeDimension.nextTime();
    }
}

/**
 * Helper function to filter layers by user input
 * in the text field layer_filter
 */
function layer_filter() {
    const input = document.getElementById('layer_filter');
    const filter = input.value.toUpperCase();
    const layer_list = document.getElementById("layer-list");
    const layers = layer_list.getElementsByTagName('li');

    for (let i = 0; i < layers.length; i++) {
        const label = layers[i].getElementsByClassName("cblabel")[0];
        const txtValue = label.textContent || label.innerText;
        if (txtValue.toUpperCase().indexOf(filter) > -1) {
            layers[i].style.display = "";
        } else {
            layers[i].style.display = "none";
        }
    }
}

/**
 * touch support
 */
(function ($) {

    $.support.touch = typeof Touch === 'object';

    if (!$.support.touch) {
        return;
    }

    const proto = $.ui.mouse.prototype,
        _mouseInit = proto._mouseInit;

    $.extend(proto, {
        _mouseInit: function () {
            this.element
                .bind("touchstart." + this.widgetName, $.proxy(this, "_touchStart"));
            _mouseInit.apply(this, arguments);
        },
        _touchStart: function (event) {
            if (event.originalEvent.targetTouches.length !== 1) {
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
            const target = event.originalEvent.targetTouches[0];
            event.pageX = target.clientX;
            event.pageY = target.clientY;
        }
    });

})(jQuery);