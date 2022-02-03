from osgeo import ogr

# To get polygon string from geojson
def decodeGeoJSON(input):
    ds = ogr.Open( input )
    lyr = ds.GetLayerByName( "OGRGeoJSON" )
    feat = lyr.GetNextFeature()
    shape = feat.GetGeometryRef()
    ring = shape.GetGeometryRef(0)
    return ogr.ForceToMultiPolygon(ring)

# To get polygon string from shape file
def decodeShapefile(input, layerName):
    shapef=ogr.Open(input)
    lyr = shapef.GetLayer( layerName )
    poly = lyr.GetNextFeature() 
    shape = poly.GetGeometryRef()
    ring = shape.GetGeometryRef(0)
    return ogr.ForceToMultiPolygon(ring)