'''
Created on Dec 9, 2013
@author: jeburks

Modified starting from Sept 2015
@author: Kris Stanton 
'''
from gdalconst import *
from osgeo import gdal, gdalnumeric, ogr,osr
import numpy as np

def openGeoTiff_WithUpdateFlag(filename):
    '''
    
    :param filename:
    '''
    ds = gdal.Open(filename, GA_Update)#, GA_ReadOnly) 
    if ds is None:
        print 'Could not open ' + filename
        raise ValueError, "Problem opening file"
    return ds

def openGeoTiff(filename):
    '''
    
    :param filename:
    '''
    ds = gdal.Open(filename, GA_ReadOnly) 
    if ds is None:
        print 'Could not open ' + filename
        raise ValueError, "Problem opening file"
    return ds

def readBandFromFile(ds,bandnumber):
    '''
    
    :param ds:
    :param bandnumber:
    '''
    
    cols = ds.RasterXSize
    rows = ds.RasterYSize
    bands = ds.RasterCount  
    band = ds.GetRasterBand(bandnumber)
    ds = None
    return band.ReadAsArray(0, 0, cols, rows)

def imageToArray(i):
    '''
    
    :param i:
    '''
    """
    Converts a Python Imaging Library array to a 
    gdalnumeric image.
    """
    a=gdalnumeric.fromstring(i.tobytes(),'b')
    a.shape=i.im.size[1], i.im.size[0]
    return a
    
def createGeoTiff(filename,xsize,ysize,type,srs):
    '''
    
    :param filename:
    :param xsize:
    :param ysize:
    :param type:
    :param srs:
    '''
    format = "GTiff"
    driver = gdal.GetDriverByName( format )
    dst_ds = driver.Create(filename, xsize, ysize, 1, type )
    return dst_ds

    

def getTransformToWGS84(ds):
    '''
    
    :param ds:
    '''
    old_cs = osr.SpatialReference()
    old_cs.ImportFromWkt(ds.GetProjectionRef())

    # create the new coordinate system
    wgs84_wkt = """
        GEOGCS["WGS 84",
        DATUM["WGS_1984",
        SPHEROID["WGS 84",6378137,298.257223563,
        AUTHORITY["EPSG","7030"]],
        AUTHORITY["EPSG","6326"]],
        PRIMEM["Greenwich",0,
        AUTHORITY["EPSG","8901"]],
        UNIT["degree",0.01745329251994328,
        AUTHORITY["EPSG","9122"]],
        AUTHORITY["EPSG","4326"]]"""
    new_cs = osr.SpatialReference()
    new_cs.ImportFromWkt(wgs84_wkt)

    # create a transform object to convert between coordinate systems
    transform = osr.CoordinateTransformation(old_cs,new_cs) 
    return transform

def getTransformFromWGS84(ds):
    '''
    
    :param ds:
    '''
    old_cs = osr.SpatialReference()
    old_cs.ImportFromWkt(ds.GetProjectionRef())

    # create the new coordinate system
    wgs84_wkt = """
        GEOGCS["WGS 84",
        DATUM["WGS_1984",
        SPHEROID["WGS 84",6378137,298.257223563,
        AUTHORITY["EPSG","7030"]],
        AUTHORITY["EPSG","6326"]],
        PRIMEM["Greenwich",0,
        AUTHORITY["EPSG","8901"]],
        UNIT["degree",0.01745329251994328,
        AUTHORITY["EPSG","9122"]],
        AUTHORITY["EPSG","4326"]]"""
    new_cs = osr.SpatialReference()
    new_cs.ImportFromWkt(wgs84_wkt)

    # create a transform object to convert between coordinate systems
    transform = osr.CoordinateTransformation(new_cs,old_cs) 
    return transform

def getSpatialRefOfImage(ds):
    '''
    
    :param ds:
    '''
    old_cs = osr.SpatialReference()
    old_cs.ImportFromWkt(ds.GetProjectionRef())
    return old_cs

def getProjectionRef(ds):
    '''
    
    :param ds:
    '''
    return ds.GetProjectionRef()

def getLatLonArray(minx,miny,maxx,maxy,sizeX, sizeY, transform):
    '''
    
    :param minx:
    :param miny:
    :param maxx:
    :param maxy:
    :param sizeX:
    :param sizeY:
    :param transform:
    '''
    deltaX = (maxx-minx)/sizeX
    deltaY = (maxy-miny)/sizeY
    
    #print "deltaX="+str(deltaX)
    #print "deltaY="+str(deltaY)
    items = []
    
    for nx in range(0,sizeX):
        x = minx+nx*deltaX
        for ny in range(0,sizeY):
            y = miny+ny*deltaY
            items.append([x,y,0])
    return transform.TransformPoints(items)

def world2Pixel(geoMatrix, x, y):
    '''
    
    :param geoMatrix:
    :param x:
    :param y:
    '''
    """
      Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
      the pixel location of a geospatial coordinate 
    """
    ulX = geoMatrix[0]
    ulY = geoMatrix[3]
    xDist = geoMatrix[1]
    yDist = geoMatrix[5]
    rtnX = geoMatrix[2]
    rtnY = geoMatrix[4]
    pixel = int((x - ulX) / xDist)
    line = int((ulY - y) / xDist)
    return (pixel, line) 