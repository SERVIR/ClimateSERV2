'''
Created on Aug 22, 2016

@author: BillyZ
'''
from osgeo import ogr

def getPolygon(shapefilePath, layer_id, fid):

    shapef=ogr.Open(shapefilePath)
    lyr = shapef.GetLayer( layer_id )
    numberFeatures = lyr.GetFeatureCount()
    feature = None
    for i in range(numberFeatures):
        feature = lyr.GetFeature(i)
        G2008_1_ID = feature.GetField("G2008_1_ID")
        if G2008_1_ID == fid:
            break
    print ogr.ForceToMultiPolygon(feature.GetGeometryRef().Clone())
print 'starting...'
getPolygon('/data/data/gis/mapfiles/admin_1_af.shp', 'admin_1_af', '13863')

