import os
import sys
import json
from osgeo import ogr

module_path = os.path.abspath(os.getcwd())
if module_path not in sys.path:
    sys.path.append(module_path)
try:
    import climateserv2.parameters as param
except:
    from ... import parameters as param

# To get path to shape file based on the name
def getShapefilePath(name):
    for item in param.shapefileName:
        if (item['id'] == name):
            return param.shapefilepath+item['shapefilename']
    
# To get the geometry of a shape file based on layer and feature
def getPolygon(shapefilePath, layer_id, fid):
    shapef = ogr.Open(shapefilePath)
    lyr = shapef.GetLayer(layer_id)
    poly = lyr.GetFeature(int(fid))
    return ogr.ForceToMultiPolygon(poly.GetGeometryRef().Clone())

#To get geometry of shape file based on layer and multiple features
def getPolygons(layer_id, feat_ids):
    path = getShapefilePath(layer_id)
    output = {"type": "FeatureCollection", "features": []}
    for feature in feat_ids:
        geometry = getPolygon(path, str(layer_id), int(feature))
        for x in geometry:
            f = {"type": "Feature", "geometry": json.loads(x.ExportToJson())}
            output["features"].append(f)
    return json.dumps(output)

    
