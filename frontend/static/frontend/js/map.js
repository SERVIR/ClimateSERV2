/** Global Variables */
var active_basemap = "OSM";
var map;
var passedLayer;
var overlayMaps = {};
var adminLayer;
var adminHighlightLayer;
var highlightedIDs = [];
var uploadLayer;
var baseLayers;
var drawnItems;
var drawtoolbar;
var styleOptions = [];

/**
 * Evokes getLayerHtml, appends the result to the layer-list, then
 * creates the map layer and stores it in the overlayMaps array
 * @param {object} item  - layer json object
 */
function createLayer(item) {
  // Create actual layer and put in overlayMaps
  var key = (overlayMaps[item.id + "TimeLayer"] = L.timeDimension.layer.wms(
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
  ));
    overlayMaps[item.id + "TimeLayer"].id = item.id;

    if (item.id.includes(passedLayer)) {
        console.log(item.id);
        try {
            document.getElementById(item.id).checked = true;
            toggleLayer(item.id + "TimeLayer");
        } catch (e) { }
    }
}

/**
 * Helper function to get a layer out of globalLayerArray
 * @param {string} which - name of layer requesting
 * @returns layer json object
 */
function getLayer(which) {
  return test_layers.find(
    (item) => item.id === which.replace("TimeLayer", "")
  );
}

/**
 * Retrieves the current TDS styles available and stores them in the
 * styleOptions array, which will be used to load the styles dropdown box
 */
function buildStyles() {
  $.get(test_layers[0].url + "&request=GetCapabilities", function (xml) {
    var jsonObj = $.xml2json(xml);
    var styles =
      jsonObj["#document"]
          .WMS_Capabilities
          .Capability
          .Layer
          .Layer
          .Layer
          .Style
          .sort(function(a, b){
            var x = a.Name;
            var y = b.Name;
            return ((x < y) ? -1 : ((x > y) ? 1 : 0));
          });
    for (i = 0; i < styles.length; i++) {
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
  var active_layer = getLayer(which);

  var settingsHtml = "";
  if (active_layer.dataset == "model") {
    // need to get available ensembles then
    // add checkboxes for each to enable turning on and off
    // will likely have to adjust the apply button as well since
    // it currently works on overlayMaps[which]

    settingsHtml += "Get the Ens info to build the checkboxes";
  }

  settingsHtml += baseSettingsHtml();

  $("#dialog").html(settingsHtml);
  $("#dialog").dialog({
    title: "Settings",
    resizable: { handles: "se" },
    width: "auto",
    height: "auto",
  });
  $(".ui-dialog-title").attr("title", "Settings");
  $(styleOptions).each(function () {
    $("#style_table").append(
      $("<option>").attr("value", this.val).text(this.text)
    );
  });

  $("#style_table").val(overlayMaps[which]._baseLayer.wmsParams.styles);

  var slider = document.getElementById("opacityctrl");
  slider.value = overlayMaps[which].options.opacity;
  slider.oninput = function () {
    overlayMaps[which].setOpacity(this.value);
  };

  var applyStylebtn = document.getElementById("applyStylebtn");

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
  $("button.ui-button.ui-corner-all.ui-widget.ui-button-icon-only.ui-dialog-titlebar-close").bind("touchstart", function(){
    $("#dialog").dialog('close');
  });
}

/**
 * Clones the base html settings and returns the html
 * @returns html
 */
function baseSettingsHtml() {
  var replica = $("#styletemplate:first").clone();
  return replica.html();
}

/**
 * Opens the legend for the selected layer
 * @param {string} which - Name of layer to open legend for
 */
function openLegend(which) {
  //fix this, it's not getting the new style if the user changes, it's getting the default
  var active_layer = getLayer(which);
  var src =
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
    resizable: { handles: "se" },
    width: 169,
    height: 322,
  });
  $(".ui-dialog-title").attr("title", active_layer.title);
  $("button.ui-button.ui-corner-all.ui-widget.ui-button-icon-only.ui-dialog-titlebar-close").bind("touchstart", function(){
    $("#dialog").dialog('close');
  });
}

/**
 * Basic map setup and creates the basemap thumbnails in the basemaps tab
 */
function mapSetup() {
  map = L.map("servirmap", {
    zoom: 5,
    fullscreenControl: true,
    timeDimension: true,
    timeDimensionControl: true,
    center: [38.0, 15.0],
  });

  drawnItems = new L.FeatureGroup();
  map.addLayer(drawnItems);
  baseLayers = getCommonBaseLayers(map); // use baselayers.js to add, remove, or edit
  L.control.layers(baseLayers, overlayMaps).addTo(map);
  L.control.sidebar("sidebar").addTo(map);

  //create the basemap thumbnails in the panel
  for (var key of Object.keys(baseLayers)) {
    var img = $("<img>");
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
  map.on('layeradd',(e)=>{
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
  for (var key in overlayMaps) {
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
  if (drawtoolbar) {
    drawtoolbar.remove();
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
    uploadLayer.remove();
  }
  $("#nextStep1").prop("disabled", true);
}

/**
 * Enables AOI upload capabilities by adding drop events to the drop zone
 */
function enableUpload() {
  uploadLayer = L.geoJson().addTo(map);

  var targetEl = document.getElementById("drop-container");
  targetEl.addEventListener("dragenter", function (e) {
    e.preventDefault();
  });
  targetEl.addEventListener("dragover", function (e) {
    e.preventDefault();
  });

  targetEl.addEventListener("drop", function (e) {
    e.preventDefault();
    var reader = new FileReader();
    reader.onloadend = function () {
      try {
        var data = JSON.parse(this.result);
        uploadLayer.clearLayers();
        uploadLayer.addData(data);
        map.fitBounds(uploadLayer.getBounds());
        if (uploadLayer.getLayers().length > 0) {
          $("#nextStep1").prop("disabled", false);
        } else {
          $("#nextStep1").prop("disabled", true);
        }
        $("#upload_error").hide();
      } catch(e){
        // When the section is built the url will need to add #pageanchorlocation
        $("#upload_error").html("* invalid file upload, please see the <a href='" + $("#menu-help").attr('href') + "'>Help Center</a> for more info about upload formats..")
        $("#upload_error").show();
      }
    };
    var files = e.target.files || e.dataTransfer.files;
    for (var i = 0, file; (file = files[i]); i++) {
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
            var URL =
                window.URL || window.webkitURL || window.mozURL || window.msURL,
              url = URL.createObjectURL(
                new Blob([JSON.stringify(data)], { type: "application/json" })
              );

            //$('#link').attr('href', url);
            //$('#link').html(file.name + '.geojson' + '<i class="download icon"></i>').attr('download', file.name + '.geojson');

            //$('#downloadLink').slideDown(400);

            //$('.shp-modal').toggleClass('effect');
            //$('.overlay').toggleClass('effect');
            //    $('#wrap').toggleClass('blur');
            if (data.features.length > 10) {
              data.features = data.features.splice(0, 10);
            }
            console.log(data);
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
            if(uploadLayer.getLayers().length > 0){
              $("#nextStep1").prop("disabled", false);
            } else {
              $("#nextStep1").prop("disabled", true);
            }
          }
        );
      }
    }
  });
}

/**
 * Enables the drawing of an AOI on the map
 */
function enableDrawing() {
  clearAOISelections();
  drawtoolbar = new L.Control.Draw({
    draw: {
      polyline: false,
      circle: false,
      circlemarker: false,
    },
    edit: {
      featureGroup: drawnItems,
    },
  });
  map.addControl(drawtoolbar);

  map.on(L.Draw.Event.CREATED, function (e) {
    var type = e.layerType,
      layer = e.layer;
    if (type === "marker") {
      // Do marker specific actions
    }
    // Do whatever else you need to. (save to db; add to map etc)
    drawnItems.addLayer(layer);
    if(drawnItems.getLayers().length > 0){
      $("#nextStep1").prop("disabled", false);
    } else {
      $("#nextStep1").prop("disabled", true);
    }
  });

  map.on(L.Draw.Event.DELETED, function (e) {
    if(drawnItems.getLayers().length > 0){
      $("#nextStep1").prop("disabled", false);
    } else {
      $("#nextStep1").prop("disabled", true);
    }
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
  clearAOISelections();

  adminLayer = L.tileLayer.wms(
    "https://climateserv2-ui.servirglobal.net/servirmap_102100/?&crs=EPSG%3A102100",
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
    Object.keys(baseLayers).length + test_layers.length + 5
  );

  // enable map click to show highlighted selections
  map.on("click", function (e) {
    var url = getFeatureInfoUrl(map, adminLayer, e.latlng, {
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

          var selectedID = response["data"];
          if (highlightedIDs.includes(selectedID)) {
            highlightedIDs = highlightedIDs.filter((e) => e !== selectedID);
          } else {
            highlightedIDs.push(selectedID);
          }

          adminHighlightLayer = L.tileLayer.wms(
            "https://climateserv2-ui.servirglobal.net/servirmap_102100/?&crs=EPSG%3A102100",
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
          if(highlightedIDs.length > 0){
            $("#nextStep1").prop("disabled", false);
          } else {
            $("#nextStep1").prop("disabled", true);
          }

          map.addLayer(adminHighlightLayer);
          adminHighlightLayer.setZIndex(
            Object.keys(baseLayers).length + test_layers.length + 6
          );
        }
      },
    });
  });
}

function gotostep(which){
  $("[id^=step]").hide();
  $("[id^=btnstep]").removeClass("active");
  $("#step" + which).show();
  $("#btnstep" + which).addClass("active");

}

function enablestep3(){
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
  var point = map.latLngToContainerPoint(latlng, map.getZoom()),
    size = map.getSize(),
    bounds = map.getBounds(),
    sw = bounds.getSouthWest(),
    ne = bounds.getNorthEast(),
    sw = L.CRS.EPSG3857.project(new L.LatLng(sw.lat, sw.lng)),
    ne = L.CRS.EPSG3857.project(new L.LatLng(ne.lat, ne.lng));

  var bb = sw.x + "," + sw.y + "," + ne.x + "," + ne.y;

  var defaultParams = {
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
var testme;
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
      var origEl = evt.item;
      var cloneEl = evt.clone;
    },
  });
}

function adjustLayerIndex() {
  var count = 10;
  for (var i = $("ol.layers li").length; i > 0; i--) {
    var name = $("ol.layers li")[i - 1].id.replace("_node", "TimeLayer");

    overlayMaps[name].setZIndex(count);
    count++;
  }
}

getParameterByName = (name, url) => {
    const regex = new RegExp(
        "[?&]" + name.replace(/[[\]]/g, "\\$&") + "(=([^&#]*)|&|#|$)"
    );
    const results = regex.exec(decodeURIComponent(url || window.location.href));
    return results
        ? results[2]
            ? decodeURIComponent(results[2].replace(/\+/g, " "))
            : ""
        : null;
};

/**
 * Page load functions, initializes all parts of application
 */
function initMap() {
  passedLayer = this.getParameterByName("data") || "none";
  mapSetup();
  test_layers.forEach(createLayer);
  sortableLayerSetup();
  try {
    buildStyles();
  } catch (e) {}
  adjustLayerIndex();
}

/**
 * Calls initMap
 *
 * @event map-ready
 */
$(function () {
  initMap();
});

(function( $ ) {

  $.support.touch = typeof Touch === 'object';

  if (!$.support.touch) {
    return;
  }

  var proto =  $.ui.mouse.prototype,
      _mouseInit = proto._mouseInit;

  $.extend( proto, {
    _mouseInit: function() {
      this.element
          .bind( "touchstart." + this.widgetName, $.proxy( this, "_touchStart" ) );
      _mouseInit.apply( this, arguments );
    },

    _touchStart: function( event ) {
      if ( event.originalEvent.targetTouches.length != 1 ) {
        return false;
      }

      this.element
          .bind( "touchmove." + this.widgetName, $.proxy( this, "_touchMove" ) )
          .bind( "touchend." + this.widgetName, $.proxy( this, "_touchEnd" ) );

      this._modifyEvent( event );

      $( document ).trigger($.Event("mouseup")); //reset mouseHandled flag in ui.mouse
      this._mouseDown( event );

      return false;
    },

    _touchMove: function( event ) {
      this._modifyEvent( event );
      this._mouseMove( event );
    },

    _touchEnd: function( event ) {
      this.element
          .unbind( "touchmove." + this.widgetName )
          .unbind( "touchend." + this.widgetName );
      this._mouseUp( event );
    },

    _modifyEvent: function( event ) {
      event.which = 1;
      var target = event.originalEvent.targetTouches[0];
      event.pageX = target.clientX;
      event.pageY = target.clientY;
    }

  });

})( jQuery );
