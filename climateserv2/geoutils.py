from osgeo import ogr

def decodeGeoJSON(input):
    '''
    :param input:
    '''
    ds = ogr.Open( input )
    lyr = ds.GetLayerByName( "OGRGeoJSON" )
    feat = lyr.GetNextFeature()
    shape = feat.GetGeometryRef()
    ring = shape.GetGeometryRef(0)
    return ogr.ForceToMultiPolygon(ring)

def decodeShapefile(input, layerName):
    '''
    :param input:
    :param layerName:
    '''
    shapef=ogr.Open(input)
    lyr = shapef.GetLayer( layerName )
    poly = lyr.GetNextFeature() 
    shape = poly.GetGeometryRef()
    ring = shape.GetGeometryRef(0)
    return ogr.ForceToMultiPolygon(ring)

def createShape():
    shapef=ogr.Open("/Users/jeburks/work/data/SERVIR/gis/Country_shp/kenya.shp")
    lyr = shapef.GetLayer( "kenya" )
    poly = lyr.GetNextFeature() 
    return poly.GetGeometryRef()
