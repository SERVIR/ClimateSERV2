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
let polling_timeout = [];
let multi_progress_value = [];
const query_list = [];
const multiQueryData = [];
let simpleAxis = true;
let debug_data = [];
let multiChart;
const csrftoken = getCookie('csrftoken');

/**
 * createLayer
 * Evokes getLayerHtml, appends the result to the layer-list, then
 * creates the map layer and stores it in the overlayMaps object
 * with the key of the layers' id + TimeLayer.  This also checks to
 * see if the layer has been sent in the query string to be activated
 * if so it adds to queried_layers, which will be loaded later in the init.
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
    // Add to list to be activated if query string requested it
    if (item.id.includes(passedLayer)) {
        queried_layers.push(item.id)
    } else {
        if (!control_layer) {
            control_layer = item.id;
        }
    }
}

/**
 * getLayer
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
 * buildStyles
 * Retrieves the current TDS styles available and stores them in the
 * styleOptions array, which will be used to load the styles' dropdown box
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

/***
 * apply_style_click
 * Applies the selected style properties to the passed active_layer
 * @param which
 * @param active_layer
 * @param bypass_auto_on
 */
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
                document.getElementById("range-min").value
                + ","
                + document.getElementById("range-max").value,
            abovemaxcolor: document.getElementById("above_max").value,
            belowmincolor: document.getElementById("below_min").value,
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
    active_layer.colorrange = document.getElementById("range-min").value
        + ","
        + document.getElementById("range-max").value
    overlayMaps[which].options.opacity = document.getElementById("opacityctrl").value;
    overlayMaps[which].setOpacity(overlayMaps[which].options.opacity);
}

/**
 * apply_settings
 * Handles the setting changes and calls apply_style_click to complete the
 * application of the settings.  If the settings are for a multi layer
 * apply_style_click will be called for each layer
 * @param which
 * @param active_layer
 * @param is_multi
 * @param multi_ids
 */
function apply_settings(which, active_layer, is_multi, multi_ids) {
    $("#style_table").val(overlayMaps[which]._baseLayer.wmsParams.styles);
    $("#above_max").val(overlayMaps[which]._baseLayer.wmsParams.abovemaxcolor);
    $("#below_min").val(overlayMaps[which]._baseLayer.wmsParams.belowmincolor);

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
            multi_ids.forEach((e, i) => {
                apply_style_click(e + "TimeLayer", getLayer(multi_ids[i]), true);
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
 * openSettings
 * Populates the Settings box for the specific layer and opens the settings popup.
 * @param {string} which - Name of layer to open settings for
 */
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
 * baseSettingsHtml
 * Clones the base html settings and returns the html
 * @returns html
 */
function baseSettingsHtml() {
    return ($("#style_template:first").clone()).html();
}

/**
 * openLegend
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
 * mapSetup
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
 * handleBaseMapSwitch
 * Switches the basemap to the user selected map.
 * @param {string} which - The Key of the basemap
 */
function handleBaseMapSwitch(which) {
    map.removeLayer(baseLayers[active_basemap]);
    active_basemap = which;
    baseLayers[active_basemap].addTo(map);
}

/**
 * toggleLayer
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
        $("#slider-range-txt").text(moment.utc(layer_limits.min).format('MM/DD/YYYY') +
            " to " + moment.utc(layer_limits.max).format('MM/DD/YYYY'));
    } else {
        map.timeDimension.setAvailableTimes([null], "replace");
        $(".timecontrol-date").html("Time not available")
        $("#slider-range-txt").text('N/A to N/A');
    }
}

/**
 * onlyUnique
 * Used for a filter to ensure only unique values are returned
 * this is specifically used for the available_times of multiple time series layers
 * @param value
 * @param index
 * @param self
 * @returns {boolean}
 */
function onlyUnique(value, index, self) {
    return self.indexOf(value) === index;
}

/**
 * selectAOI
 * Opens the user selected method of selecting their AOI
 * @param {string} which - name of selection method to activate
 */
function selectAOI(which) {
    let new_aoi = true;
    if (query_list.length > 0) {
        let text = "Only one geometry allowed for multi-queries.\nClick ok to change AOI or Cancel to keep it.";
        new_aoi = confirm(text);
    }
    if (new_aoi) {
        $("[id^=btnAOI]").removeClass("active");
        $("#btnAOI" + which).addClass("active");
        $(".selectAOI").hide();
        $("#" + which + "AOI").show();

        clearAOISelections();

        if (which === "draw") {
            enableDrawing();
        } else if (which === "upload") {
            enableUpload();
        } else if (which === "select") {
            const adminLayerOptions = $("#adminLayerOptions");
            const firstOption = $("#adminLayerOptions option:first");
            if (adminLayerOptions.val() !== firstOption.val()) {
                adminLayerOptions.val(firstOption.val());
            }
            enableAdminFeature(firstOption.val());
        }
    }
}

/**
 * clearAOISelections
 * Removes any and all existing AOI selections and map click event
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

/**
 * setPointAOI
 * This is used to manually set the point AOI when the user has supplied the
 * values in the input boxes.  It adds the marker to the map and updates
 * the current AOI display
 */
function setPointAOI() {
    let valid_values = true;
    const lon_control = $("#point_lon");
    const lat_control = $("#point_lat");
    const point_lon = lon_control.val();
    const point_lat = lat_control.val();
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
        $("#geometry").text(JSON.stringify(drawnItems.toGeoJSON()));
        try {
            const draw_button = $(".leaflet-draw-actions.leaflet-draw-actions-bottom li a");
            if (draw_button[0]) {
                draw_button[0].click();
            }
        } catch (e) {
        }
    } else {
        $("#lat-lon-error").show();
    }

}

/**
 * triggerUpload
 * This opens the users file upload browser for them to select the
 * AOI file they want to upload
 * @param e
 */
function triggerUpload(e) {
    document.getElementById("upload_files").value = "";
    e.preventDefault();
    $("#upload_files").trigger('click');
}

/**
 * enableUpload
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

/**
 * prevent
 * This is a helper function to make adding and removing
 * the event listener easier
 * @param e
 */
function prevent(e) {
    e.preventDefault();
}

/**
 * verifyFeatures
 * Verifies that the uploaded features meet the requirements
 * @param data
 * @returns {boolean}
 */
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

/**
 * addDataToMap
 * Adds the data passed in to the map if it is valid data
 * @param data
 */
function addDataToMap(data) {
    try {
        // this handles all upload situations
        let verifiedRequirements = verifyFeatures(data);
        if (verifiedRequirements) {
            uploadLayer.clearLayers();
            uploadLayer.addData(data);
            try {
                map.fitBounds(uploadLayer.getBounds());
            } catch (e) {
                map.fitBounds([
                    [data.bbox[1], data.bbox[0]],
                    [data.bbox[3], data.bbox[2]],
                ]);
            }
            $("#upload_error").hide();
            collect_review_data();
            verify_ready();
        } else {
            upload_file_error();
        }
    } catch (e) {
        upload_file_error();
    }
}

/**
 * handleFiles
 * This is the file upload handler.  It handles json, geojson, and zip files
 * It will process based on the type and pass the data on to addDataToMap
 * @param e
 */
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
                    addDataToMap(data);

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
 * enableDrawing
 * Enables the drawing of an AOI on the map by adding the
 * draw toolbar which a user may select the draw type they
 * would like to use.  It also adds the draw handlers to
 * the map, which enforce the limit of 20 features.
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
            $("#point_manual_entry").show();
            drawnItems.clearLayers();
        } else {
            $("#point_manual_entry").hide();
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
        }
    });
}

/**
 * enableAdminFeature
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
                    if (highlightedIDs.length <= 20) {
                        if (highlightedIDs.length === 20) {
                            alert("Max selections has been reached");
                        }
                    } else {
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
 * getFeatureInfoUrl
 * Builds and returns the url needed to make the feature info call
 * for the selected admin layer
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
 * sortableLayerSetup
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
    });
}

/**
 * upload_file_error
 * Displays the error when the upload file is incorrect
 */
function upload_file_error() {
    const upload_error = $("#upload_error");
    upload_error.html(
        "* invalid file upload, please see the <a href='" + $("#menu-help").attr('href')
        + "#geojson'>Help Center</a> for more info about upload formats.")
    upload_error.show();
}

/**
 * adjustLayerIndex
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
 * initMap
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
 * open_range_picker
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
        range_picker += 'style="color:#da2020; display: none;">End date must be equal or ';
        range_picker += 'greater than the start date</label></div>';
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

/**
 * setRange
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
 * clearRange
 * Clears the range set by the user
 */
function clearRange() {
    map.timeDimension.setLowerLimit(moment.utc(layer_limits.min));
    map.timeDimension.setUpperLimit(moment.utc(layer_limits.max));
    document
        .getElementById("begin_range_date")
        .value = moment(map.timeDimension.getLowerLimit()).utc().format('YYYY-MM-DD');

    document
        .getElementById("end_range_date")
        .value = moment(map.timeDimension.getUpperLimit()).utc().format('YYYY-MM-DD');

    $("#slider-range-txt").text(moment.utc(layer_limits.min).format('MM/DD/YYYY') +
        " to " + moment.utc(layer_limits.max).format('MM/DD/YYYY'));
}

/**
 * download_aoi
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
 * isComplete
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
            // Also, should confirm s < e;
            $("#invalid-error").hide();
            if (moment(sDate_new_cooked.value) > moment(eDate_new_cooked.value)) {
                isReady = false;
                $("#compare-error").show();
            } else {
                $("#compare-error").hide();
            }
        } else {
            $("#invalid-error").show();
        }
    } else {
        $("#invalid-error").show();
        $(sDate_new_cooked).valid({rules: {field: {required: true, dateISO: true}}});
        $(eDate_new_cooked).valid({rules: {field: {required: true, dateISO: true}}});
    }
    return isReady;
}

/**
 * verify_ready
 * Verifies if the user has filled in enough information to send a request
 * When ready, enables the request button as well and the view API button
 */
function verify_ready() {
    const btnRequest = $("#btnRequest");
    let ready = true;
    const requestTypeSelect = $("#requestTypeSelect");
    if (requestTypeSelect.val() === "datasets") {
        ready = isComplete();
    }
    const geometry = $("#geometry");
    const download_aoi_holder = $("#download_aoi_holder");
    let disabled = !(geometry.text().trim() !== '{"type":"FeatureCollection","features":[]}' && ready);

    if (requestTypeSelect.val() === 'monthly_analysis'
        && (geometry.text().trim() !== '{"type":"FeatureCollection","features":[]}')) {
        btnRequest.prop("disabled", false);
    }
    if (requestTypeSelect.val() !== 'monthly_analysis') {
        const btnViewAPI = $("#btnViewAPI");
        btnViewAPI.prop("disabled", false);
        if (query_list.length >= 5) {
            disabled = true;
        }

        $("#btnAddToQuery").prop("disabled", disabled);
    }

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
        function get_AOI_String() {
            let aoi_string = "";
            if (geometry.text().trim().indexOf("- Feature:") > -1) {
                if (highlightedIDs.length > 0) {
                    aoi_string = "&layerid=" + adminHighlightLayer.options.layers.replace("_highlight", "");
                    aoi_string += "&featureids=" + highlightedIDs.join(",");
                }
            } else {
                aoi_string = "&geometry=" + encodeURI(geometry.text().trim());
            }
            return aoi_string;
        }

        const api_panel = $("#api_panel");
        if (requestTypeSelect.val() === "datasets") {
            api_panel.empty();
            query_list.forEach(buildAPIReference);

            function buildAPIReference(value) {
                api_panel.append("<span class='form-control' style='word-wrap: break-word; height: fit-content;'>"
                    + api_host + "/api/submitDataRequest/?" + new URLSearchParams(value).toString()
                    + get_AOI_String() + "</span>");
            }
        } else if (requestTypeSelect.val() === "download") {
            // get from panel
            let formData = new FormData();
            buildForm(formData);
            api_panel.empty();
            api_panel.append("<span class='form-control' style='word-wrap: break-word; height: fit-content;'>"
                + api_host + "/api/submitDataRequest/?"
                + new URLSearchParams(formData).toString() + get_AOI_String() + "</span>");
            btnRequest.prop("disabled", disabled);
        } else {
            api_panel.empty();
            api_panel.append("<span class='form-control' style='word-wrap: break-word; height: fit-content;'>"
                + api_host + get_API_url() + "</span>");
        }
    } catch (e) {
        console.log(e);
    }
}

/**
 * get_API_url
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
 * verify_range
 * Verifies range is within the data start and end range
 * This function is called from a dynamically created element
 * DO NOT REMOVE
 * @returns {boolean}
 */
function verify_range() {
    let isReady = false;
    let begin_range_date = document.getElementById("begin_range_date");
    let end_range_date = document.getElementById("end_range_date");
    if (begin_range_date && end_range_date) {
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
                // Also, should confirm s < e;
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
 * collect_review_data
 * Collects all the request data
 */
function collect_review_data() {
    const geometry = $("#geometry");
    const netCDF = $("#netcdf");
    const tif = $("#tif");
    const csv = $("#csv");

    if (highlightedIDs.length > 0) {
        const feature_label = highlightedIDs.length > 1 ? "Features" : "Feature"
        geometry.text(
            adminHighlightLayer
                .options
                .layers
                .replace("_highlight", " - " + feature_label + ": ")
                .replace("admin_2_af", "Admin #2")
                .replace("admin_1_earth", "Admin #1")
                .replace("country", "Country") + highlightedIDs.join()
        );
    } else if (drawnItems.getLayers().length > 0) {
        geometry.text(JSON.stringify(drawnItems.toGeoJSON()));
    } else if (uploadLayer) {
        geometry.text(JSON.stringify(uploadLayer.toGeoJSON()));
    } else {
        geometry.text('{"type":"FeatureCollection","features":[]}');
    }
    const selectedOption = $('#format-menu option:selected');
    selectedOption.removeAttr('selected');
    if ($("#requestTypeSelect").val() === "download" && geometry.text().indexOf("Point") > -1) {
        tif.hide();
        netCDF.hide();
        csv.show();
        $('#format-menu option[value=8]').attr('selected', 'selected');
    } else {
        tif.show();
        netCDF.show();
        csv.hide();
    }
}

/**
 * handle_initial_request_data
 * Handles the initial return from the submitRequest by setting up progress bar and
 * starting the polling process
 * @param data
 * @param isClimate
 * @param query_index
 */
function handle_initial_request_data(data, isClimate, query_index) {
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
            clearTimeout(polling_timeout[query_index]);
        },
        position: {
            my: "center",
            at: "center",
            of: window
        }
    });
    pollForProgress(data[0], isClimate, query_index);
}

/**
 * buildForm
 * Builds the form that is sent with the API request.  The form is passed
 * in as a reference variable, so it does not need to be returned.
 * @param formData
 */
function buildForm(formData) {

    const calc = $("#requestTypeSelect").val() === "datasets" ? $("#operationmenu").val() : $("#format-menu").val();
    current_calculation = {
        'value': parseInt(calc),
        'text': $("#operationmenu option:selected").text()
    };

    if ($("#ensemble_builder").is(":visible")) {
        formData.append(
            "datatype", parseInt($("#ensemblemenu").val()) + $("#ensemblevarmenu")[0].selectedIndex
        );
        formData.append(
            "ensemble", true
        );
        formData.append(
            "ensemble_data_source", $("#sourcemenu").val()
        );
    } else {
        formData.append(
            "datatype", $("#sourcemenu").val()
        );
        formData.append(
            "ensemble", false
        );
    }

    formData.append("begintime", moment(document.getElementById("sDate_new_cooked").value).format('MM/DD/YYYY')); // "01/01/2020");
    formData.append("endtime", moment(document.getElementById("eDate_new_cooked").value).format('MM/DD/YYYY')); //"06/30/2020");
    formData.append("intervaltype", 0);
    formData.append("operationtype", current_calculation.value);
    formData.append("dateType_Category", "default");  // ClimateModel shouldn't be needed. please confirm
    formData.append("isZip_CurrentDataType", false);
}

/**
 * update_number_queries
 * Helper function to handle the UI display of the number of
 * queries currently in the queue
 */
function update_number_queries() {
    const query_button_number_control = $("#query_button_number");
    query_button_number_control.text("(" + query_list.length + ")" + (query_list.length === 1 ? " Query" : " Queries"));
    $("#lblCartCount").text(query_list.length);
    if (query_list.length === 0) {
        $("#btnRequest").prop("disabled", true);
        $("#btnMultiQuerySubmit").prop("disabled", true);
        $("#btnViewAPI").prop("disabled", false);
    } else {
        $("#btnRequest").prop("disabled", false);
        $("#btnMultiQuerySubmit").prop("disabled", false);
        $("#btnViewAPI").prop("disabled", false);
    }
}

/**
 * add_multi_query
 * Adds the currently configured query to the list of queries
 */
function add_multi_query() {
    const formData = new FormData();
    buildForm(formData);
    query_list.push(formData);
    update_number_queries();
    if (drawToolbar) {
        drawToolbar.remove();
    }
    map.off('click');
    try {
        const targetEl = document.getElementById("drop-container");
        targetEl.removeEventListener("dragenter", prevent);
        targetEl.removeEventListener("dragover", prevent);

        targetEl.removeEventListener("drop", handleFiles);
    } catch (e) {
    }
    $(".selectAOI").hide();
    $("[id^=btnAOI]").removeClass("active");
    $("#btnAOIDraw").addClass("active");
    $("#drawAOI").show();
    if (query_list.length >= 5) {
        $("#btnAddToQuery").prop("disabled", true);
    }
    verify_ready();
}

function append_AOI_to_form(formData) {
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
 * sendRequest
 * Initiates the processing request to ClimateSERV
 */
function sendRequest() {
    current_calculation = {
        'value': parseInt($("#operationmenu").val()),
        'text': $("#operationmenu option:selected").text()
    };
    //set the calculation info here
    for (let t = 0; t < polling_timeout.length; t++) {
        clearTimeout(polling_timeout[t]);
    }
    multi_progress_value.length = 0;
    polling_timeout.length = 0;
    $("#btnRequest").prop("disabled", true);
    $("#btnMultiQuerySubmit").prop("disabled", true);

    const request_type_value = $("#requestTypeSelect").val();
    if (request_type_value === "datasets" || request_type_value === "download") {
        multiQueryData.length = 0;

        if (request_type_value === "download") {
            let formData = new FormData();
            buildForm(formData);
            query_list.push(formData);
        }
        for (let i = 0; i < query_list.length; i++) {
            let formData = query_list[i];

            append_AOI_to_form(formData);

            let api_host = window.location.hostname;
            if (window.location.port) {
                api_host += ":" + window.location.port
            }
            $("#api_query").text(api_host + "/api/submitDataRequest/?" + new URLSearchParams(formData).toString());
            $.ajax({
                url: "/api/submitDataRequest/",
                type: "POST",
                headers: {'X-CSRFToken': csrftoken},
                processData: false,
                contentType: false,
                async: true,
                crossDomain: true,
                data: formData,
                query_index: i,
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
                        $("#btnAddToQuery").prop("disabled", false);
                        //$("#btnViewAPI").prop("disabled", false);
                    }
                });
            }).done(function (data, _textStatus, _jqXHR) {
                if (data.errMsg) {
                    console.info(data.errMsg);
                } else {
                    handle_initial_request_data(JSON.parse(data), false, this.query_index);
                }
            });
        }

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
 * updateProgress
 * Updates progress bar with value sent in
 * @param val
 * @param index
 */
function updateProgress(val, index) {
    let final;
    if (query_list.length > 0) {

        multi_progress_value[index] = val;

        let combined = 0;
        multi_progress_value.forEach(combine);

        function combine(value) {
            combined += value;
        }

        final = (combined / query_list.length).toString();
    } else {
        final = val
    }

    $('.progress-bar').css('width', final + '%').attr('aria-valuenow', final);
    $("#text_percent").text(parseInt(final).toString() + '%');
}

/**
 * pollForProgress
 * Get the progress of a submitted job and sends teh returned value to updateProgress
 * @param id
 * @param isClimate
 * @param query_index
 */
function pollForProgress(id, isClimate, query_index) {
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
                updateProgress(val, query_index);
                polling_timeout.push(setTimeout(function () {
                    pollForProgress(id, isClimate, query_index);
                }, 500));

            } else if (val === 100) {
                retries = 0;
                const requestTypeValue = $("#requestTypeSelect").val();
                const request_operation_format = (requestTypeValue === "datasets"
                        ? $("#operationmenu").val()
                        : requestTypeValue === "download"
                            ? $("#format-menu").val()
                            : "987654"
                );
                if (
                    request_operation_format === "6"
                    || request_operation_format === "7"
                    || request_operation_format === "8"
                ) {
                    getDownLoadLink(id);
                } else {
                    getDataFromRequest(id, isClimate, query_index);

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
                    $("#btnAddToQuery").prop("disabled", false);
                    //$("#btnViewAPI").prop("disabled", false);
                    close_dialog();
                    let error_message = '<div style="width:100%; height:100%; display: flex;' +
                        '    align-items: center;' +
                        '}">';
                    error_message += '<div style="width:100%; text-align: center;">';
                    error_message += '<h1 class="step-marker" style="line-height: 2em;">Processing Error</h1>';
                    error_message += '<p style="line-height: 2em;">There was an error processing Job ID: ' + id;
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
                            $("#btnAddToQuery").prop("disabled", false);
                        }

                    });
                }
            }
        }
    });
}

/**
 * filter_datasets_by
 * This function filters the possible data sources by toggling
 * layer-on and layer-off classes, setting the selected value to the
 * first layer with the layer-on class, then handles any UI updates
 * necessary based on the newly selected data source.
 */
function filter_datasets_by() {
    $(".layer-on, .layer-off").toggleClass("layer-on layer-off");
    const firstLayerOn = $('#sourcemenu option.layer-on:first');
    $("#sourcemenu")[0].selectedIndex = firstLayerOn.index();
    handleSourceSelected(firstLayerOn.val());
}

function filter_edit_datasets_by() {
    console.log("doing it");
    $(".layer-on-edit, .layer-off-edit").toggleClass("layer-on-edit layer-off-edit");

    const firstLayerOn = $('#dataset-source-menu-edit option.layer-on-edit:first');
    console.log(firstLayerOn.val());
    $("#dataset-source-menu-edit")[0].selectedIndex = firstLayerOn.index();
    handleSourceSelected(firstLayerOn.val(), true);
}

/**
 * configure_nmme
 * Sets the variables needed to query the NMME dataset based on
 * the return value from the getClimateScenarioInfo call
 * which is passed in as the parameter
 * @param sdata
 * @param edit
 * @param edit_init_id
 */
function configure_nmme(sdata, edit, edit_init_id) {
    if (sdata.errMsg) {
        console.info(sdata.errMsg);
    } else {
        let edit_string = '';
        let sent_variable = 'precipitation';
        let start_date = '';
        let end_date = '';
        if (edit) {
            edit_string = '_edit';

            let start_date_array = $("#begin_time_review").text().split("/");
            start_date = start_date_array[2] + '-' + start_date_array[0] + '-' + start_date_array[1];
            let end_date_array = $("#end_time_review").text().split("/");
            end_date = end_date_array[2] + '-' + end_date_array[0] + '-' + end_date_array[1];
        }
        if (edit_init_id) {
            if (parseInt(edit_init_id) % 2 === 0) {
                sent_variable = 'air_temperature'
            }
        }
        const data = JSON.parse(sdata);
        const cc = data.climate_DataTypeCapabilities[0].current_Capabilities;
        cc.startDateTime;

        $('#model_run_menu' + edit_string)
            .append('<option value="' + cc.startDateTime + '">'
                + cc.startDateTime.replaceAll("-", "/").substr(0, cc.startDateTime.lastIndexOf("-"))
                + '</option>');

        // create date dropdowns
        const mformat = "YYYY-MM-DD"
        let sdate = moment(cc.startDateTime, mformat);
        let edate = moment(cc.endDateTime, mformat);
        let count = 1;
        if (edit) {
            $("#begin_date_edit").val(sdate.format('YYYY-MM-DD'));
            $("#eDate_new_cooked").val(edate.format('YYYY-MM-DD'));
        } else {
            $("#sDate_new_cooked").val(sdate.format('YYYY-MM-DD'));
            $("#end_date_edit").val(sdate.format('YYYY-MM-DD'));
        }

        do {
            let selected_start = '';
            let selected_end = '';
            //
            if (start_date === sdate.format('YYYY-MM-DD')) {
                selected_start = "selected";
            }
            if (end_date === sdate.format('YYYY-MM-DD')) {
                selected_end = "selected";
            }
            $("#forecastfrommenu" + edit_string)
                .append
                (
                    '<option value="' + sdate.format('YYYY-MM-DD') + '" ' + selected_start + '>'
                    + "f" + count.toString().padStart(3, "0")
                    + " " + sdate.format('YYYY-MM-DD') + '</option>');
            $("#forecasttomenu" + edit_string)
                .append
                (
                    '<option value="' + sdate.format('YYYY-MM-DD') + '" ' + selected_end + '>'
                    + "f" + count.toString().padStart(3, "0")
                    + " " + sdate.format('YYYY-MM-DD') + '</option>');
            count++;
            sdate.add(1, "days");
        } while (sdate < edate)

        cc.endDateTime;
        cc.date_FormatString_For_ForecastRange;
        cc.number_Of_ForecastDays;
        $("#ensemblevarmenu" + edit_string).empty();
        data.climate_DatatypeMap[0].climate_DataTypes.forEach((variable) => {
            // add variable with label to select
            let selected = '';
            if (variable.climate_Variable === sent_variable) {
                selected = 'selected';
            }
            $("#ensemblevarmenu" + edit_string)
                .append(
                    '<option value="' + variable.climate_Variable
                    + '" ' + selected + '>' + variable.climate_Variable_Label + '</option>');
        });
    }
}

/**
 * handleSourceSelected
 * Sets the UI to the correct state when a different source is selected
 * @param which
 * @param edit
 * @param edit_init_id
 */
function handleSourceSelected(which, edit, edit_init_id) {
    which = which.toString();
    let layer = client_layers.find(
        (item) => item.app_id === which
    )
    if (layer) {
        if (edit) {
            $("#ensemble_builder_edit").hide();
            $("#non-multi-ensemble-dates_edit").show();
            $("#panel_operation_edit").show();
            $(".observation-edit-hide").toggleClass("observation-edit observation-edit-hide");
        } else {
            $("#ensemble_builder").hide();
            $("#non-multi-ensemble-dates").show();
            $("#panel_operation_edit").hide();
        }

        //show date range controls
    } else {
        let id = which + "ens";
        if (edit) {
            $("#ensemble_builder_edit").show();
            $("#non-multi-ensemble-dates_edit").hide();
            $("#panel_operation_edit").show();
            $(".observation-edit").toggleClass("observation-edit observation-edit-hide");

            $('#model_run_menu_edit').find('option').remove();
            $('#ensemblemenu_edit').find('option').remove();
            $("#forecastfrommenu_edit").find('option').remove();
            $("#forecasttomenu_edit").find('option').remove();
            let init_id = '';
            if (edit_init_id) {
                if (parseInt(edit_init_id) % 2 === 0) {
                    init_id = edit_init_id;
                } else {
                    init_id = parseInt(edit_init_id) - 1;
                }

            }
            $("[id^=" + id + "]").each(function (index, item) {
                const temp = getLayer(item.id);
                let selected = '';
                if (temp.app_id == init_id) {
                    selected = 'selected'
                }
                $("#ensemblemenu_edit").append('<option value="' + temp.app_id + '" ' + selected + '>' + temp.title + '</option>');
            });

        } else {
            // open and set ensemble section

            $("#ensemble_builder").show();
            $("#non-multi-ensemble-dates").hide();
            $("#panel_operation_edit").hide();
            //hide date range controls
            $('#model_run_menu').find('option').remove();
            $('#ensemblemenu').find('option').remove();
            $("#forecastfrommenu").find('option').remove();
            $("#forecasttomenu").find('option').remove();
            // load the ensemble selection tools


            $("[id^=" + id + "]").each(function (index, item) {
                const temp = getLayer(item.id);
                $("#ensemblemenu").append('<option value="' + temp.app_id + '">' + temp.title + '</option>');
            });
        }
        $.ajax({
            url: "api/getClimateScenarioInfo/",
            type: "GET",
            async: true,
            crossDomain: true
        }).fail(function () {
            $.ajax({
                url: "https://climateserv.servirglobal.net/api/getClimateScenarioInfo/",
                type: "GET",
                async: true,
                crossDomain: true
            }).fail(function (jqXHR, textStatus, errorThrown) {
                console.warn(jqXHR + textStatus + errorThrown);
            }).done(function (data, _textStatus, _jqXHR) {
                if (data.errMsg) {
                    console.info(data.errMsg);
                } else {
                    if (edit) {
                        configure_nmme(data, true, edit_init_id);
                    } else {
                        configure_nmme(data);
                    }
                }
            });
            console.warn("NMME queries may not work if you are doing local development");
        }).done(function (sdata, _textStatus, _jqXHR) {
            if (edit) {
                configure_nmme(sdata, true, edit_init_id);
            } else {
                configure_nmme(sdata);
            }
        });
    }
    $("#btnAddToQuery").prop("disabled", false);
}

/**
 * syncDates
 * synchronizes the dates between start date and forecast date
 */
function syncDates(edit) {
    if (edit) {
        $("#begin_date_edit").val($("#forecastfrommenu_edit").val());
        $("#end_date_edit").val($("#forecasttomenu_edit").val());
    } else {
        $("#sDate_new_cooked").val($("#forecastfrommenu").val());
        $("#eDate_new_cooked").val($("#forecasttomenu").val());
    }
}

function rebuildGraph() {
    if ($("#axis_toggle").prop("checked")) {
        $("#multi_axis").prop("checked", true);
    } else {
        $("#simple_axis").prop("checked", true);
    }
    open_previous_chart();
}

/**
 * inti_chart_dialog
 * Creates the chart dialog
 */
function inti_chart_dialog() {
    close_dialog();
    $("#btnPreviousChart").prop("disabled", true);
    const dialog = $("#dialog");
    const isMobile = $("#isMobile");
    let dialog_html = '<div style="height:calc(100% - 32px)"><div id="chart_holder"></div></div>';
    const checked_text = $('input[name="axis_type"]:checked').val() === "simple" ? "" : "checked";
    dialog_html += '<div id="multi-switch-panel" style="visibility: hidden; ">';
    dialog_html += 'Simple Axis <label class="switch">'
    dialog_html += '<input id="axis_toggle" type="checkbox" ' + checked_text + ' onclick="rebuildGraph()">';
    dialog_html += '<span class="slider round"></span>';
    dialog_html += '</label> Multi-Axis';
    dialog_html += '</div>';
    dialog.html(
        dialog_html
    );
    dialog.dialog({
        title: "Statistical Query",
        resizable: isMobile.css("display") === "block" ? false : {handles: "se"},
        width: isMobile.css("display") === "block" ? $(window).width() : $(window).width() - ($("#sidebar").width() + 100),
        height: $(window).height() - 140,
        // resize: function () {
        //     Highcharts.charts[0].reflow();
        //     window.dispatchEvent(new Event('resize'));
        // },
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
 * open_previous_chart
 * Opens the last chart that was closed.
 */
function open_previous_chart() {
    if (previous_chart) {
        inti_chart_dialog();
        finalize_chart(previous_chart.compiled_series, previous_chart.units, previous_chart.xAxis_object, previous_chart.title, previous_chart.isClimate)
    } else if (multiQueryData.length !== 0) {
        inti_chart_dialog();
        multi_chart_builder();
    } else {
        alert("you have no previous chart, please send a request.")
    }
}

/**
 * getIndex
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
 * getDownLoadLink
 * Creates the download link for a request ID and displays it in the dialog
 * @param id
 */
function getDownLoadLink(id) {
    query_list.length = 0;
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

function format_data(i){
    const temp_data = multiQueryData[i].data;

                let breaks_gone = multiQueryData[i].yAxis_format.formatter.toString().replace(/(\r\n|\n|\r)/gm, "")
                let parsed_formula = breaks_gone.substring(breaks_gone.indexOf("return") + 6).replace("}", "").trim();

                temp_data.map(x =>{

                                    x[1] = eval(parsed_formula.replace("this.value", x[1]));
                                    return x;
                                });
                multiQueryData[i].point_format = null;
}

/**
 * multi_chart_builder
 * This will build the chart with the data that has been stored
 * in the multiQueryData list.  This should only be called after
 * data has been added to the list
 */
function multi_chart_builder() {
    console.log("building chart");
    simpleAxis = $('input[name="axis_type"]:checked').val() === "simple";
    const first_unit = multiQueryData[0].units;

    const chart_object = {};
    chart_object.title = {text: "ClimateSERV Statistical Query"};
    chart_object.subtitle = {
        text: 'Source: climateserv.servirglobal.net'
    };
    chart_object.tooltip = {shared: true,};
    chart_object.xAxis = {type: "datetime"};
    chart_object.yAxis = {
        id: "simple",
        title: {
            text: simpleAxis ? "values" : multiQueryData[0].units
        },
    };
    if (multiQueryData[0].yAxis_format) {
        //chart_object.yAxis.labels = multiQueryData[0].yAxis_format;
        format_data(0);
        multiQueryData[0].yAxis_format = null;
    }


    chart_object.legend = {
        layout: 'vertical',
        align: 'right',
        verticalAlign: 'middle'
    };
    let point_format = ''
    if (multiQueryData[0].point_format) {
        point_format = multiQueryData[0].point_format;
    } else {
        point_format = {
            pointFormatter: function () {
                return Highcharts.numberFormat(this.y, 2) + " " + first_unit + "<br>";
            }
        }
    }
    chart_object.series = [{
        color: "#758055",
        type: "line",
        name: multiQueryData[0].label,
        data: multiQueryData[0].data.sort((a, b) => a[0] - b[0]),
        tooltip: point_format

    }];

    chart_object.chart = {
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
    chart_object.exporting = {
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
    chart_object.responsive = {
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
    }
    console.log("init chart dialog");
    inti_chart_dialog();

    let dialog = $("#dialog");
    dialog.dialog({resize: function () {
            Highcharts.charts[0].reflow();
            window.dispatchEvent(new Event('resize'));
        }});

    console.log("first data");
    multiChart = Highcharts.chart('chart_holder', chart_object, function (chart) { // on complete
        originalWidth = chart.chartWidth;
        originalHeight = chart.chartHeight;
        const width = chart.chartWidth - 105;
        const height = chart.chartHeight - 160;
        img = chart.renderer
            .image('https://servirglobal.net/images/servir_logo_full_color_stacked.jpg', width, height, 100, 82)
            .add();
    });

    const colors = [
        "#bafc02",
        "#022cfc",
        "#02f4fc",
        "#283601"
    ]

    if (multiQueryData.length > 1) {
        for (let i = 1; i < multiQueryData.length; i++) {
            if (!simpleAxis) {
                multiChart.addAxis({
                    id: "yaxis-" + i,
                    opposite: i % 2 !== 0,
                    title: {
                        text: multiQueryData[i].units
                    },
                }, false, false);
            }
            if (multiQueryData[i].yAxis_format) {
                format_data(i);
            }
            let build_yAxis =  simpleAxis ? "simple" : "yaxis-" + i;



            let point_format = ''
            if (multiQueryData[i].point_format) {
                point_format = multiQueryData[i].point_format;
            } else {
                point_format = {
                    pointFormatter: function () {
                        return Highcharts.numberFormat(this.y, 2) + " " + multiQueryData[i].units + "<br>";
                    }
                }
            }
            multiQueryData[i].yAxis_format = null;
            multiChart.addSeries({
                yAxis: build_yAxis,
                color: colors[i - 1],
                type: "line",
                tooltip: point_format,
                name: multiQueryData[i].label,
                data: multiQueryData[i].data.sort((a, b) => a[0] - b[0]),
            });
        }
        $('#multi-switch-panel').css('visibility', 'visible');
    }
}
let passes = 0;
/**
 * getDataFromRequest
 * Function to retrieve the processed data from the server with the id
 * that was created by the submitDataRequest
 * @param id
 * @param isClimate
 * @param query_index
 */
function getDataFromRequest(id, isClimate, query_index) {
    // only do this for the final dataset that goes thru, othewise continue the progress bar
    passes ++;
    if (passes === query_list.length) {
        console.log("will show");
        let complete = '<div style="width:100%; height:100%; display: flex;' +
            '    align-items: center;' +
            '}">';
        complete += '<div style="width:100%">';
        complete += '<h1 class="step-marker">Processing complete, downloading results.</h1>';
        complete += '<p>If this window is stuck for a long period of time there may be an error.  To reset ';
        complete += 'the query status please click <button class="bread-crumb" onclick"reset_query()">Reset</button> ';
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
        passes = 0;
    }

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
                    getDataFromRequest(id, isClimate, query_index);
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

                    reset_query_panel();

                } else {
                    previous_chart = null;
                    const compiledData = [];
                    let min = 9999;
                    let max = -9999;
                    debug_data.push(data);
                    JSON.parse(data).data.forEach((d) => {
                        let val;
                        val = d.value.hasOwnProperty('max')
                            ? d.value.max
                            : d.value.hasOwnProperty('min')
                                ? d.value.min
                                : d.value.hasOwnProperty('avg')
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
                            compiledData.push(darray); // I can likely store min and max here
                        } else {
                            const null_array = [];
                            null_array.push(parseInt(d.epochTime) * 1000);
                            null_array.push(null);
                            compiledData.push(null_array); // I can likely store min and max here
                        }
                    });
                    from_compiled = compiledData; // if this is empty, I need to let the user know there was no data

                    let queried_data = JSON.parse(JSON.stringify(Object.fromEntries(query_list[query_index])))

                    let layer = client_layers.find(
                        (item) => item.app_id === queried_data.datatype
                    );
                    const units = layer.units.includes("|units|")
                        ? layer.units.split("|units|")[document.getElementById("ensemblevarmenu").selectedIndex]
                        : layer.units;

                    multiQueryData[query_index] = {
                        data: compiledData,
                        units: units,
                        yAxis_format: layer.yAxis_format || null,
                        point_format: layer.point_format || null,
                        label: layer.title + ": " + (queried_data.operationtype === "0"
                            ? "max"
                            : queried_data.operationtype === "1"
                                ? "min"
                                : queried_data.operationtype === "5"
                                    ? "avg"
                                    : "n/a")
                    };

                    if (multiQueryData.filter(Boolean).length === query_list.length) {
                        let hasData = false;
                        for (let i = 0; i < multiQueryData.length; i++) {
                            try {
                                if (multiQueryData[i].data.length > 0) {
                                    hasData = true;
                                    break;
                                }
                            } catch (e) {
                            }
                        }
                        if (hasData) {
                            close_dialog();

                            multi_chart_builder();
                        } else {
                            inti_chart_dialog();
                            $("#chart_holder").html("<h1>No data available</h1>");
                        }
                        query_list.length = 0;
                        update_number_queries();
                        $("#checkout_list").empty();
                        $("#checkout_number").text("0 Queries");
                        $("#chart-builder").show();
                        $("#query_list_checkout").hide();
                        reset_query_panel();
                    }
                }
            }
        }
    });
}

/**
 * value_or_null
 * Fixes no data value issue by setting them to null
 * Updated from -9999 to > -9000 as I found some values
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
 * finalize_chart
 * Creates a graph with the supplied variables.  Used for
 * monthly rainfall analysis
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

    multiChart = Highcharts.chart('chart_holder', chart_obj, function (chart) { // on complete
        originalWidth = chart.chartWidth;
        originalHeight = chart.chartHeight;
        const width = chart.chartWidth - 105;
        const height = chart.chartHeight - 160;
        img = chart.renderer
            .image('https://servirglobal.net/images/servir_logo_full_color_stacked.jpg', width, height, 100, 82)
            .add();
    });
}

/**
 * getDataLine
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
 * check_query_status
 * Prompts user about changing the query type if they have
 * already added one or more to the list.  The user may select to
 * abandon the list or keep the list.
 * @param control
 */
function check_query_status(control) {
    if (query_list.length > 0) {
        let text = "All multi-queries must be of type Time-series Analysis.\nClick ok to clear all added queries and " +
            "start over or Cancel to keep queries and continue.";
        if (confirm(text) === true) {
            query_list.length = 0;
            update_number_queries();
            clearAOISelections();
            // will need to reset the numbered UI things here as well as remove and geometry
        } else {
            $(control).blur();
        }
    }
}

/**
 * openDataTypePanel
 * Opens the proper panel for the datatype selected
 * @param select_control
 */
function openDataTypePanel(select_control) {
    const query_button_number_control = $("#query_button_number");
    if (select_control.value === "datasets") {
        $("#panel_monthly_rainfall").hide();
        $("#panel_download").hide();
        $("#panel_dataset").show();
        $("#panel_timeseries").show();
        $('.query_cart').show();
        $("#btnAddToQuery").css('visibility', 'visible');
        update_number_queries();
    } else if (select_control.value === "download") {
        $("#panel_monthly_rainfall").hide();
        $("#panel_timeseries").hide();
        $("#panel_dataset").show();
        $("#panel_download").show();
        $('.query_cart').hide();
        $("#btnAddToQuery").css('visibility', 'hidden');
        query_button_number_control.text("Query");
        // need to check the AOI state, if no AOI leave just NetCDF or TIF
        // if point type, show csv only
    } else {
        $("#panel_dataset").hide();
        $("#panel_download").hide();
        $("#panel_timeseries").hide();
        $("#panel_monthly_rainfall").show();
        $('.query_cart').hide();
        $("#btnAddToQuery").css('visibility', 'hidden');
        query_button_number_control.text("Query");
    }
    collect_review_data();
    verify_ready();
}

/**
 * getClimateScenarioInfo
 * Calls the api to get the current Climate scenario properties
 */
function getClimateScenarioInfo() {
    $.ajax({
        url: "api/getClimateScenarioInfo/",
        type: "GET",
        async: true,
        crossDomain: true
    }).fail(function () {
        $.ajax({
            url: "https://climateserv.servirglobal.net/api/getClimateScenarioInfo/",
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
        console.warn("NMME queries may not work if you are doing local development");
    }).done(function (data, _textStatus, _jqXHR) {
        if (data.errMsg) {
            console.info(data.errMsg);
        } else {
            climateModelInfo = JSON.parse(data);
        }
    });
}

/**
 * toggleUpDownIcon
 * Toggles the About AOI section
 */
function toggleUpDownIcon(which) {
    $('#' + which).toggleClass("fa-angle-up fa-angle-down");
}

/**
 * close_dialog
 * Closes the dialog if it is open
 */
function close_dialog() {
    const dialog = $("#dialog");
    if (dialog.dialog()) {
        dialog.dialog("close");
    }
}

/**
 * stats_info
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
 * get_stat_body
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
 * init_tour
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
 * open_tour
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
        const date = new Date();
        const firstDay = new Date(date.getFullYear(), date.getMonth() - 1, 1);
        const lastDay = new Date(date.getFullYear(), date.getMonth(), 0);
        $("#sDate_new_cooked").val(firstDay.toISOString().split('T')[0]);
        $("#eDate_new_cooked").val(lastDay.toISOString().split('T')[0]);
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
    close_dialog();
    $("button.ui-button.ui-corner-all.ui-widget.ui-button-icon-only.ui-dialog-titlebar-close").bind("touchstart", function () {
        $("#dialog").dialog('close');
    });
    verify_range();
});

/**
 * load_queried_layers
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
 * confirm_animation
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
 * layer_filter
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
 * review_query
 * Adds all selected queries to the UI for review by the user
 */
function review_query(no_toggle) {
    const checkout_list = $("#checkout_list");
    checkout_list.empty();
    $("#checkout_number").text(query_list.length + (query_list.length === 1 ? " Query" : " Queries"));
    let init = true;
    if (query_list.length > 0) {
        for (let [i, formData] of query_list.entries()) {
            const structured_data = JSON.parse(JSON.stringify(Object.fromEntries(formData)));
            if (init) {

                checkout_list.append('<br><h1 class="step-marker ten">Common Geometry</h1>');
                let geometry_element = '<span class="form-control panel-buffer" id="geometry_review" ';
                geometry_element += 'style="height: unset; word-break: break-all; max-height: 200px; overflow: auto;">';
                geometry_element += $("#geometry").text();
                geometry_element += '</span>';
                checkout_list.append(geometry_element);

                checkout_list.append('<br><h1 class="step-marker ten">Query Type</h1>');
                let request_type_element = '<span class="form-control panel-buffer" id="query_type_review" ';
                request_type_element += 'style="height: unset; word-break: break-all; max-height: 200px; overflow: auto;">';
                request_type_element += $("#requestTypeSelect option:selected").text();
                request_type_element += '</span>';
                checkout_list.append(request_type_element);

                checkout_list.append('<br><h1 class="step-marker ten">Queries</h1>');
                init = false;
            }
            const back_color = i % 2 === 0 ? "#909d6b94" : "transparent";
            const text_color = i % 2 === 0 ? "#000" : "#666666";
            let element_html = '<div class="checkout_list_elements" style="background-color: ' + back_color;
            element_html += '; color: ' + text_color + '">';
            let element_holder = $(element_html);
            let edit_element = '<p style="text-align: right;" class="form-group panel-buffer">';
            edit_element += '<span style="position: absolute; cursor:pointer; left: 10px; width: calc(100% - 80px); text-align: left;" data-toggle="collapse"';
            edit_element += ' href="#review-' + i + '" role="button" aria-expanded="false" ';
            edit_element += 'aria-controls="review-' + i + '" title="Show/Hide"';
            edit_element += 'onclick="toggleUpDownIcon(\'review-' + i + '-toggle\')">';
            edit_element += 'Query ' + (i + 1) + '</span>'
            edit_element += '<a class="z5px" onclick="edit_query(' + i + ')" title="Edit">';
            edit_element += '<i class="fas fa-edit" style="color:#758055" aria-hidden="true"></i></a>';
            edit_element += '<a class="z5px" onclick="delete_query(' + i + ')" title="Delete">';
            edit_element += '<i class="fas fa-trash" style="color:#758055" aria-hidden="true"></i></a>';

            edit_element += '<a type="button" class="bread-crumb collapsed" data-toggle="collapse"';
            edit_element += ' href="#review-' + i + '" role="button" aria-expanded="false" ';
            edit_element += 'aria-controls="review-' + i + '" title="Show/Hide"';
            edit_element += 'onclick="toggleUpDownIcon(\'review-' + i + '-toggle\')">';
            let toggle_arrow = "fa fa-angle-";
            let toggle_class = "collapse";
            if (i === 0) {
                toggle_class += " show";
                toggle_arrow += "up";
            } else {
                toggle_arrow += "down";
            }

            edit_element += '<i id="review-' + i + '-toggle" class="' + toggle_arrow + '"></i></a>';
            edit_element += '</p>';
            element_holder.append(edit_element)


            let layer = client_layers.find(
                (item) => item.app_id === structured_data.datatype
            )
            let element_panel = $('<div id="review-' + i + '" class="' + toggle_class + '">');
            element_panel.append(
                get_form_group(
                    'Datatype',
                    'datatype_review',
                    layer.title
                )
            );

            // see if it is ensemble, if so see which variable it is and display in a new form group

            if (structured_data["ensemble"] === "true") {
                element_panel.append(
                    get_form_group(
                        'Ensemble variable',
                        'ensemble_variable_review',
                        (parseInt(structured_data["datatype"]) % 2 === 0)
                            ? "Temperature"
                            : "Precipitation"
                    )
                );
            }


            element_panel.append(
                get_form_group(
                    'Begin time',
                    'begin_time_review',
                    structured_data["begintime"]
                )
            );
            element_panel.append(
                get_form_group(
                    'End time',
                    'end_time_review',
                    structured_data["endtime"]
                )
            );
            element_panel.append(
                get_form_group(
                    'Calculation',
                    'calculation_review',
                    $('#operationmenu option[value=' + structured_data["operationtype"] + ']').text()
                )
            );
            element_holder.append(element_panel);

            checkout_list.append(element_holder)
        }
    }
    if (!no_toggle) {
        toggle_query_tabs();
    }
}

let debug_this;

function edit_query(edit_index) {

    close_dialog();
    let structured_data = JSON.parse(JSON.stringify(Object.fromEntries(query_list[edit_index])));

    let layer = client_layers.find(
        (item) => item.app_id === structured_data.datatype
    )


    let edit_query_form = $('<div>');


    let data_type_group = '<div class="form-group panel-buffer" id="panel_timeseries_edit">';
    data_type_group += '<label for="data-type-menu-edit">Dataset Type</label></div>';

    let dataset_type_menu = $("#dataset-type-menu").clone().prop('id', 'dataset-type-menu-edit').off('change');

    dataset_type_menu.removeAttr("onchange");
    ;
    dataset_type_menu.on('change', function (event) {
        filter_edit_datasets_by();
    });
    if (structured_data.ensemble === 'true') {
        dataset_type_menu.val('model-forecast');
    } else {
        dataset_type_menu.val('observation');
    }
    // calculate how to set to selected data

    edit_query_form.append($(data_type_group).append(dataset_type_menu));

    let data_source_group = '<div class="form-group panel-buffer" id="panel_source_edit">';
    data_source_group += '<label for="data-source-menu-edit">Data Source</label></div>';

    let dataset_source_menu = $("#sourcemenu").clone().prop('id', 'dataset-source-menu-edit').off('change');


    if (structured_data.ensemble === 'true') {
        dataset_source_menu.val(structured_data.ensemble_data_source);
    } else {
        dataset_source_menu.val(structured_data.datatype);
    }

    dataset_source_menu.removeAttr("onchange");
    ;
    dataset_source_menu.on('change', function (event) {
        handleSourceSelected(this.value, true);
    });

    dataset_source_menu.find("option").each(function (i, obj) {
        let layer_class = 'layer-on-edit';
        if (isNaN(parseInt(obj.value))) { // turn off observation and on ens
            if (structured_data.ensemble !== 'true') { // turn off observation
                layer_class = 'layer-off-edit';
            }
        } else { // observation
            if (structured_data.ensemble === 'true') { // turn off observation
                layer_class = 'layer-off-edit';
            }
        }
        dataset_source_menu.find('option[value="' + obj.value + '"]').removeClass().addClass(layer_class);
    });

    edit_query_form.append($(data_source_group).append(dataset_source_menu));

    let operation_group = '<div class="form-group panel-buffer" id="panel_operation_edit"';
    // if (structured_data.ensemble === 'true') {
    //     operation_group += ' style="display:none;">';
    // } else {
    operation_group += ' style="display:block;">';
    // }
    operation_group += '<label for="operationmenu-edit">Calculation</label></div>';

    let operation_edit = $("#operationmenu").clone().prop('id', 'operationmenu-edit');
    const opts = operation_edit.find("option");
    for (let i = 0; i < opts.length; i++) {
        if (opts[i].value == structured_data.operationtype) {
            operation_edit.prop('selectedIndex', i);
            break;
        }
    }

    edit_query_form.append($(operation_group).append(operation_edit));

    const ensemble_builder = ($("#edit_ens_template:first").clone()).html();
    let replace_class = "ensemble-edit-hide";
    let observation_class = "observation-edit";
    if (structured_data.ensemble === 'true') {
        replace_class = "ensemble-edit";
        observation_class = "observation-edit-hide";
    }
    edit_query_form.append($(ensemble_builder.replace("replace_class", replace_class)));

    let date_range = '<div class="' + observation_class + '"><p class="picker-text">Date Range</p>';
    date_range += '<form id="range_picker_edit" style="width:100%; height:100%; display: flex; align-items: center;" class="picker-text">';

    date_range += '<div class="form-group panel-buffer">';
    date_range += '<input type="date" class="form-control" placeholder="YYYY-MM-DD"';
    date_range += 'id="begin_date_edit" value="' + moment(structured_data.begintime, "MM/DD/YYYY").format("YYYY-MM-DD") + '"';
    date_range += 'onchange="verify_range()">';
    date_range += '<div class="input-group-addon">to</div>';
    date_range += '<input type="date" class="form-control" placeholder="YYYY-MM-DD"';
    date_range += 'id="end_date_edit" value="' + moment(structured_data.endtime, "MM/DD/YYYY").format("YYYY-MM-DD") + '"';
    date_range += 'onchange="verify_range()">';
    date_range += '</div></div></form>';
    date_range += '<div class="just-buttons">';
    date_range += '<button style="width:45%" onclick="close_dialog()">Cancel</button>';
    date_range += '<button style="width:45%" onclick="apply_edits(' + edit_index + ')">Apply</button>';
    date_range += '</div><br>';


    edit_query_form.append(date_range);
    let dialog = $("#dialog");
    dialog.html(edit_query_form);
    dialog.dialog({
        title: "Edit Query",
        resizable: false,
        width: $(window).width() / 2,
        height: "auto",
        position: {
            my: "center",
            at: "center",
            of: window
        }
    });
    handleSourceSelected($("#dataset-source-menu-edit").val(), true, structured_data.datatype);
}

function apply_edits(edit_index) {
    let structured_data = JSON.parse(JSON.stringify(Object.fromEntries(query_list[edit_index])));
    const formData = new FormData();

    let datatype = '';
    if ($("#dataset-type-menu-edit").val() === 'model-forecast') {
        let edit_value = parseInt($("#ensemblemenu_edit").val());
        if ($("#ensemblevarmenu_edit").val() == "precipitation") {
            edit_value = edit_value + 1;
        }

        datatype = edit_value;
        formData.append("ensemble", true);
        formData.append("ensemble_data_source", $("#dataset-source-menu-edit").val());
    } else {
        datatype = $("#dataset-source-menu-edit").val();
    }
    formData.append("datatype", datatype);


    formData.append("operationtype", $("#operationmenu-edit").val());
    formData.append("begintime", moment(document.getElementById("begin_date_edit").value).format('MM/DD/YYYY'));
    formData.append("endtime", moment(document.getElementById("end_date_edit").value).format('MM/DD/YYYY'));
    formData.append("intervaltype", structured_data.intervaltype);
    formData.append("dateType_Category", "default");  // ClimateModel shouldn't be needed. please confirm
    formData.append("isZip_CurrentDataType", false);

    query_list[edit_index] = formData;
    review_query(true);
    close_dialog();
}

function delete_query(delete_index) {
    if (delete_index > -1) {
        query_list.splice(delete_index, 1); // 2nd parameter means remove one item only
    }

    review_query(true);
    update_number_queries();
}

/**
 * get_form_group
 * Helper function to build form groups
 * @param label
 * @param element_id
 * @param text
 * @returns {string}
 */
function get_form_group(label, element_id, text) {
    let form_group = '<div class="form-group panel-buffer">';
    form_group += '<label for="' + element_id + '">' + label + '</label>';
    form_group += '<p class="form-control" id="' + element_id + '">' + text + '</p>';
    return form_group;
}

/**
 * toggle_query_tabs
 * Toggles the tabs
 */
function toggle_query_tabs() {
    $("#query_list_checkout").toggle();
    $("#chart-builder").toggle();
    $("#sidebar-content").scrollTop(0);
}

function reset_query(){
    query_list.length = 0;
    update_number_queries();
    reset_query_panel();
}

function reset_query_panel() {
    const requestTypeSelect = $("#requestTypeSelect");
    if (requestTypeSelect[0].selectedIndex !== 0) {
        requestTypeSelect[0].selectedIndex = 0;
        requestTypeSelect.change();
    }


    const dataset_type_menu = $("#dataset-type-menu");
    if (dataset_type_menu[0].selectedIndex !== 0) {
        dataset_type_menu[0].selectedIndex = 0;
        dataset_type_menu.change();
    }


    const sourcemenu = $('#sourcemenu');
    if (sourcemenu.val() !== '0') {
        sourcemenu.val(0);
        sourcemenu.change();
    }


    const operationmenu = $('#operationmenu');
    if (operationmenu[0].selectedIndex !== 0) {
        operationmenu[0].selectedIndex = 0;
        operationmenu.change();
    }


    const date = new Date();
    const firstDay = new Date(date.getFullYear(), date.getMonth() - 1, 1);
    const lastDay = new Date(date.getFullYear(), date.getMonth(), 0);
    $("#sDate_new_cooked").val(firstDay.toISOString().split('T')[0]);
    $("#eDate_new_cooked").val(lastDay.toISOString().split('T')[0]);

}

/**
 * Checks to see if the cookie is set
 * @param name
 * @returns {null}
 */
function getCookie(name) {
    let cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        const cookies = document.cookie.split(';');
        for (let i = 0; i < cookies.length; i++) {
            const cookie = cookies[i].trim();
            // Does this cookie string begin with the name we want?
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

/**
 * touch support, this adds missing events to help enable
 * a mobile friendly UX
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