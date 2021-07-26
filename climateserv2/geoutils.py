'''
Created on Jan 23, 2014

@author: jeburks
'''
import copy

from osgeo import gdal, ogr


def decodeGeoJSON(input):
    '''
    
    :param input:
    '''
    ds = ogr.Open( input )
    ##print "open datastore"
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
    '''
    
    '''
    shapef=ogr.Open("/Users/jeburks/work/data/SERVIR/gis/Country_shp/kenya.shp")
    lyr = shapef.GetLayer( "kenya" )
    poly = lyr.GetNextFeature() 
    return poly.GetGeometryRef()

#decodeGeoJSON("/Users/jeburks/work/data/SERVIR/gis/GeoJSON/zaire.geojson")
#decodeGeoJSON("/Users/jeburks/work/data/SERVIR/gis/GeoJSON/kenya.geojson")
#output = decodeGeoJSON('{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[18.28125,17.978733095556183],[5.2734375,3.513421045640057],[25.6640625,0.7031073524364783],[28.4765625,10.14193168613103],[18.28125,17.978733095556183]]]}}"')
#print output