/*
 * predefined [EPSG:3821] projection
 * Please make sure your desired projection can find on http://epsg.io/
 *
 * Usage :
 *      load_shape({
 *          url: '/shp/test.zip', // path or your upload file
 *          encoding: 'big5' // default utf-8
 *          EPSG: 3826 // default 4326
 *      }, function(geojson) {
 *          // geojson returned
 *      });
 *
 * Created by Gipong <sheu781230@gmail.com>
 * Updated by BillyZ https://github.com/billyz313
 */

const inputData = {};
let EPSGUser;
let shape_url;
let encoding;
let EPSG;
const EPSG4326 = proj4('EPSG:4326');

function load_shape(config, returnData) {
    shape_url = config.url;
    encoding = config.encoding !== 'utf-8' ? config.encoding : 'utf-8';
    EPSG = typeof config.EPSG != 'undefined' ? config.EPSG : 4326;

    loadEPSG('https://epsg.io/' + EPSG + '.js', function () {
        if (EPSG === 3821)
            proj4.defs([
                ['EPSG:3821', '+proj=tmerc +ellps=GRS67 +towgs84=-752,-358,-179,-.0000011698,.0000018398,.0000009822,.00002329 +lat_0=0 +lon_0=121 +x_0=250000 +y_0=0 +k=0.9999 +units=m +no_defs']
            ]);

        EPSGUser = proj4('EPSG:' + EPSG);

        if (typeof shape_url != 'string') {
            const reader = new FileReader();
            reader.onload = function (e) {
                const URL = window.URL || window.webkitURL || window.mozURL || window.msURL,
                    zip = new JSZip(e.target.result),
                    shpString = zip.file(/.shp$/i)[0].name,
                    dbfString = zip.file(/.dbf$/i)[0].name,
                    prjString = zip.file(/.prj$/i)[0];
                console.log('dbfString ' + dbfString);
                if (prjString) {
                    proj4.defs('EPSGUSER', zip.file(prjString.name).asText());
                    try {
                        EPSGUser = proj4('EPSGUSER');
                    } catch (e2) {
                        console.error('Unsupported Projection: ' + e2);
                    }
                }

                SHPParser.load(URL.createObjectURL(new Blob([zip.file(shpString).asArrayBuffer()])), shpLoader, returnData);
                DBFParser.load(URL.createObjectURL(new Blob([zip.file(dbfString).asArrayBuffer()])), encoding, dbfLoader, returnData);
            };

            reader.readAsArrayBuffer(shape_url);
        } else {
            JSZipUtils.getBinaryContent(shape_url, function (err, data) {
                if (err) throw err;

                const URL = window.URL || window.webkitURL,
                    zip = new JSZip(data),
                    shpString = zip.file(/.shp$/i)[0].name,
                    dbfString = zip.file(/.dbf$/i)[0].name,
                    prjString = zip.file(/.prj$/i)[0];
                if (prjString) {
                    proj4.defs('EPSGUSER', zip.file(prjString.name).asText());
                    try {
                        EPSGUser = proj4('EPSGUSER');
                    } catch (e) {
                        console.error('Unsupported Projection: ' + e);
                    }
                }

                SHPParser.load(URL.createObjectURL(new Blob([zip.file(shpString).asArrayBuffer()])), shpLoader, returnData);
                DBFParser.load(URL.createObjectURL(new Blob([zip.file(dbfString).asArrayBuffer()])), encoding, dbfLoader, returnData);

            });
        }
    });
}

function loadEPSG(url, callback) {
    const script = document.createElement('script');
    script.src = url;
    script.onreadystatechange = callback;
    script.onload = callback;
    document.getElementsByTagName('head')[0].appendChild(script);
}

function TransCoord(x, y) {
    if (proj4) {
        const p = proj4(EPSGUser, EPSG4326, [parseFloat(x), parseFloat(y)]);
        return {x: p[0], y: p[1]};
    }
    return {x: x, y: y};
}

function shpLoader(data, returnData) {
    inputData.shp = data;
    if (inputData.shp && inputData.dbf)
        if (returnData) returnData(toGeojson(inputData));
}

function dbfLoader(data, returnData) {
    inputData.dbf = data;
    if (inputData.shp && inputData.dbf)
        if (returnData) returnData(toGeojson(inputData));
}

function toGeojson(geojsonData) {
    const geojson = {};
    const features = [];
    let feature;
    let geometry;
    let reprojection;

    const shpRecords = geojsonData.shp.records;
    const dbfRecords = geojsonData.dbf.records;

    geojson.type = "FeatureCollection";
    min = TransCoord(geojsonData.shp.minX, geojsonData.shp.minY);
    max = TransCoord(geojsonData.shp.maxX, geojsonData.shp.maxY);
    geojson.bbox = [
        min.x,
        min.y,
        max.x,
        max.y
    ];

    geojson.features = features;

    for (let i = 0; i < shpRecords.length; i++) {
        feature = {};
        feature.type = 'Feature';
        geometry = feature.geometry = {};
        feature.properties = dbfRecords[i];

        // point : 1 , polyline : 3 , polygon : 5, multipoint : 8
        switch (shpRecords[i].shape.type) {
            case 1:
                geometry.type = "Point";
                reprojection = TransCoord(shpRecords[i].shape.content.x, shpRecords[i].shape.content.y);
                geometry.coordinates = [
                    reprojection.x, reprojection.y
                ];
                break;
            case 3:
            case 8:
                geometry.type = (shpRecords[i].shape.type === 3 ? "LineString" : "MultiPoint");
                geometry.coordinates = [];
                for (let j = 0; j < shpRecords[i].shape.content.points.length; j += 2) {
                    reprojection = TransCoord(shpRecords[i].shape.content.points[j], shpRecords[i].shape.content.points[j + 1]);
                    geometry.coordinates.push([reprojection.x, reprojection.y]);
                }
                break;
            case 5:
                geometry.type = "Polygon";
                geometry.coordinates = [];

                for (let pts = 0; pts < shpRecords[i].shape.content.parts.length; pts++) {
                    const partsIndex = shpRecords[i].shape.content.parts[pts];
                    const part = [];

                    for (
                        let j2 = partsIndex * 2;
                        j2 < (shpRecords[i].shape.content.parts[pts + 1] * 2 || shpRecords[i].shape.content.points.length);
                        j2 += 2) {
                        const point = shpRecords[i].shape.content.points;
                        reprojection = TransCoord(point[j2], point[j2 + 1]);
                        part.push([reprojection.x, reprojection.y]);
                    }
                    geometry.coordinates.push(part);

                }
                break;
            default:
        }
        if ("coordinates" in feature.geometry) features.push(feature);
    }
    return geojson;
}
