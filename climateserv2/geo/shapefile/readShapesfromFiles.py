import os
import sys
import json
from ast import literal_eval

from osgeo import ogr

from api.models import Parameters

module_path = os.path.abspath(os.getcwd())
if module_path not in sys.path:
    sys.path.append(module_path)


# To get path to shape file based on the name
def getShapefilePath(name):
    params = Parameters.objects.first()
    for item in literal_eval(params.shapefileName):
        if item['id'] == name:
            return params.shapefilepath + item['shapefilename']


# To get the geometry of a shape file based on layer and feature
def getPolygon(shapefile_path, layer_id, fid):
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
def getPolygons(layer_id, feat_ids):
    path = getShapefilePath(layer_id)
    output = {"type": "FeatureCollection", "features": []}
    for feature in feat_ids:
        geometry = getPolygon(path, str(layer_id), int(feature))
        for x in geometry:
            f = {"type": "Feature", "geometry": json.loads(x.ExportToJson())}
            output["features"].append(f)
    return json.dumps(output)
