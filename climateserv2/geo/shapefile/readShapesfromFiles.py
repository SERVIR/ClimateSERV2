'''
Created on Jun 4, 2014

@author: jeburks
'''
import CHIRPS.utils.configuration.parameters as param
from osgeo import ogr


def getShapefilePath(name):
    '''
    
    :param name:
    '''
    for item in param.shapefileName:
        if (item['id'] == name):
            return param.shapefilepath+item['shapefilename']
    
 
def getPolygon(shapefilePath, layer_id, fid): 
    '''
    
    :param name:
    :param fid:
    '''
    shapef=ogr.Open(shapefilePath)
    lyr = shapef.GetLayer( layer_id )
    poly = lyr.GetFeature(long(fid))
  
#     shape = poly.GetGeometryRef().exportToWKt()
#     print "Got shape",shape
#     print fid, shape.GetGeometryCount()
#     for i in range(0,shape.GetGeometryCount()):
#         output.append(shape.GetGeometryRef(i))
    return ogr.ForceToMultiPolygon(poly.GetGeometryRef().Clone())

def getPolygons(layer_id, feat_ids):
    path = getShapefilePath(layer_id)
    output = []
    for feature in feat_ids:
        output.append(getPolygon(path,str(layer_id),int(feature)))
    return output
    
    
