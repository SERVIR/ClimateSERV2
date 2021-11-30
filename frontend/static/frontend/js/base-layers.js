/*
 *    Return common layers used in different examples
 */
function getCommonBaseLayers(map) {
    var osmLayer = L.tileLayer("https://{s}.tile.osm.org/{z}/{x}/{y}.png", {
        attribution:
            '&copy; <a href="https://osm.org/copyright">OpenStreetMap</a> contributors',
        thumb: "img/osm.png",
        displayName: "OSM",
    });
    var bathymetryLayer = L.tileLayer.wms(
        "https://ows.emodnet-bathymetry.eu/wms",
        {
            layers: "emodnet:mean_atlas_land",
            format: "image/png",
            transparent: true,
            attribution: "EMODnet Bathymetry",
            opacity: 0.8,
        }
    );
    var coastlinesLayer = L.tileLayer.wms(
        "https://ows.emodnet-bathymetry.eu/wms",
        {
            layers: "coastlines",
            format: "image/png",
            transparent: true,
            attribution: "EMODnet Bathymetry",
            opacity: 0.8,
        }
    );
    var bathymetryGroupLayer = L.layerGroup([bathymetryLayer, coastlinesLayer]);
    bathymetryGroupLayer.options.thumb = "img/bath.png";
    bathymetryGroupLayer.options.displayName = "Bathymetry";
    //osmLayer.addTo(map);

    var topoLayer = L.tileLayer.wms(
        "https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}",
        {
            format: "image/png",
            transparent: true,
            attribution:
                'Tiles © <a href="https://services.arcgisonline.com/ArcGIS/' +
                'rest/services/World_Topo_Map/MapServer">ArcGIS</a>',
            opacity: 1,
            thumb: "img/topo.png",
            displayName: "Topo",
        }
    );

    var labelLayer = L.tileLayer.wms(
        "https://server.arcgisonline.com/arcgis/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}",
        {
            format: "image/png",
            transparent: true,
            attribution:
                'Tiles © <a href="https://services.arcgisonline.com/ArcGIS/' +
                'rest/services/Reference/World_Boundaries_and_Places/MapServer">ArcGIS</a>',
            opacity: 1,
        }
    );

    var satLayer = L.tileLayer.wms(
        "https://server.arcgisonline.com/arcgis/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        {
            format: "image/png",
            transparent: true,
            attribution:
                'Tiles © <a href="https://services.arcgisonline.com/ArcGIS/' +
                'rest/services/Reference/World_Imagery/MapServer">ArcGIS</a>',
            opacity: 1,
        }
    );

    var satGroupLayer = L.layerGroup([satLayer, labelLayer]);
    satGroupLayer.options.thumb = "img/satellite.png";
    satGroupLayer.options.displayName = "Satellite";


    var gSatLayer = L.tileLayer.wms(
        "https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
        {
            format: "image/png",
            transparent: true,
            attribution:
                'Tiles © Map data ©2019 Google',
            opacity: 1,
            thumb: "img/gsatellite.png",
            displayName: "Google Satellite",
        }
    );
    gSatLayer.addTo(map);

    var terrainLayer = L.tileLayer(
        "https://{s}.tile.jawg.io/jawg-terrain/{z}/{x}/{y}{r}.png?access-token={accessToken}",
        {
            attribution: '<a href="https://jawg.io" title="Tiles Courtesy of Jawg Maps" target="_blank">&copy; <b>Jawg</b>Maps</a> &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
            minZoom: 0,
            maxZoom: 22,
            subdomains: 'abcd',
            accessToken: 'rU9sOZqw2vhWdd1iYYIFqXxstyXPNKIp9UKC1s8NQkl9epmf0YpFF8a2HX1sNMBM',
            opacity: 1,
            thumb: "img/terrain.png",
            displayName: "Terrain",
        }
    );

    //   var labelLayer2 = L.tileLayer.wms(
    //     "https://server.arcgisonline.com/arcgis/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}",
    //     {
    //       format: "image/png",
    //       transparent: true,
    //       attribution:
    //         'Tiles © <a href="https://services.arcgisonline.com/ArcGIS/' +
    //         'rest/services/Reference/World_Boundaries_and_Places/MapServer">ArcGIS</a>',
    //       opacity: 1,
    //     }
    //   );

    //   var terrainGroup = L.layerGroup([terrainLayer, labelLayer2]);
    //   terrainGroup.options.thumb = "img/terrain.png";
    //   terrainGroup.options.displayName = "Terrain";

    var deLormeLayer = L.tileLayer.wms(
        "https://server.arcgisonline.com/arcgis/rest/services/Specialty/DeLorme_World_Base_Map/MapServer/tile/{z}/{y}/{x}",
        {
            format: "image/png",
            transparent: true,
            attribution:
                'Tiles © <a href="https://services.arcgisonline.com/ArcGIS/' +
                'rest/services/Reference/Specialty/DeLorme_World_Base_Map/MapServer">ArcGIS</a>',
            opacity: 1,
            thumb: "img/delorme.png",
            displayName: "DeLorme",
        }
    );

    return {
        OSM: osmLayer,
        Gsatellite: gSatLayer,
        Satellite: satGroupLayer,
        Topo: topoLayer,
        Terrain: terrainLayer,
        "EMODnet Bathymetry": bathymetryGroupLayer,
        DeLorme: deLormeLayer,
    };
}
