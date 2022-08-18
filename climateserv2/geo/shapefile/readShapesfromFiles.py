import os
import sys
import json
from ast import literal_eval

from osgeo import ogr

from api.models import Parameters
import geopandas as gpd

module_path = os.path.abspath(os.getcwd())
if module_path not in sys.path:
    sys.path.append(module_path)


# To get path to shape file based on the name
def get_shapefile_path(name):
    params = Parameters.objects.first()
    for item in literal_eval(params.shapefileName):
        if item['id'] == name:
            return params.shapefilepath + item['shapefilename']


# To get the geometry of a shape file based on layer and feature
def get_polygon(shapefile_path, layer_id, fid):
    shapefile = ogr.Open(shapefile_path)
    lyr = shapefile.GetLayer(layer_id)
    poly = None
    for i in range(lyr.GetFeatureCount()):
        feature = lyr.GetFeature(i)
        if int(feature.geom_id) == int(fid):
            poly = feature

    if poly is None:
        poly = lyr.GetFeature(int(fid))
    return ogr.ForceToMultiPolygon(poly.GetGeometryRef().Clone())


# To get geometry of shape file based on layer and multiple features
def get_polygons(layer_id, feat_ids):
    path = get_shapefile_path(layer_id)
    output = {"type": "FeatureCollection", "features": []}
    for feature in feat_ids:
        geometry = get_polygon(path, str(layer_id), int(feature))
        for x in geometry:
            f = {"type": "Feature", "geometry": json.loads(x.ExportToJson())}
            output["features"].append(f)
    return json.dumps(output)


# To get geometry of shape file based on layer and multiple features
def get_aoi_area(layer_id, feat_ids):
    path = get_shapefile_path(layer_id)
    output = 0

    print(path)

    for feature in feat_ids:
        print(feature)
        polygon = get_polygon(path, str(layer_id), int(feature))
        print(polygon)
        test = gpd.read_file(path)
        selected = test.loc[test['geom_id'] == int(feature)]
        selected.crs = "EPSG:4326"

        output += sum(selected.geometry.to_crs(epsg=102100).area) / 10 ** 6

    return json.dumps(output)
