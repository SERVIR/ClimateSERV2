# RasterClipper.py - clip a geospatial image using a shapefile

from PIL import Image
from PIL import ImageDraw
from osgeo import osr
import numpy as np
import sys
try:
    import climateserv2.locallog.locallogging as llog
except:
# Debug time!
    import locallog.locallogging as llog
logger = llog.getNamedLogger("request_processor")
# Debug time!

# Raster image to clip
def getShapeToGridTransform(ds,shapespatial_ref):
    '''
    
    :param ds:
    :param shapespatial_ref:
    '''
    old_cs = osr.SpatialReference()
    old_cs.ImportFromWkt(ds.GetProjectionRef())
    # create a transform object to convert between coordinate systems
    transform = osr.CoordinateTransformation(shapespatial_ref,old_cs) 
    return transform


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
    pixel = np.round((x - ulX) / xDist).astype(np.int)
    line = np.round((ulY - y) / xDist).astype(np.int)
    return (pixel, line)

def imageToArray(i):
    '''
    
    :param i:
    '''
    """
    Converts a Python Imaging Library array to a
    numpy array.
    """
    a=np.fromstring(i.tobytes(),'b')
    a.shape=i.im.size[1], i.im.size[0]
    return a


# KS Refactor 2015 // Need a function that is very simillar to 'rasterizePolygon' but only gets the bounds info.
def getPolyBoundsOnly(geoTrans,polygon):
    '''
    
    :param geoTrans:
    :param polygon:
    '''
    
    oSRS = osr.SpatialReference ()
    pts = polygon
    # # # pts is the polygon of interest

    points = []
    for p in range(pts.GetPointCount()):
        points.append((pts.GetX(p), pts.GetY(p)))
        pnts = np.array(points).transpose() 
    pixel, line = world2Pixel(geoTrans,pnts[0],pnts[1]) 
    minx = min(pixel)
    maxx = max(pixel)
    miny = min(line)
    maxy = max(line)
    
    ##print "minx ",minx
    #rasterPoly = Image.new("L", (nx,ny),1)
    #rasterize = ImageDraw.Draw(rasterPoly)
    #listdata = [(pixel[i],line[i]) for i in xrange(len(pixel))]
    #rasterize.polygon(listdata,0)
    
    #mask = 1 - imageToArray(rasterPoly)
    bounds = (minx,maxx,miny,maxy)
    return bounds #,mask


# This function deals in points..
def rasterizePolygon(geoTrans,nx,ny,polygon):
    '''
    
    :param geoTrans:
    :param nx:
    :param ny:
    :param polygon:
    '''
    
    
    oSRS = osr.SpatialReference ()
    pts = polygon
    # # # pts is the polygon of interest

    points = []
    for p in range(pts.GetPointCount()):
        points.append((pts.GetX(p), pts.GetY(p)))
        pnts = np.array(points).transpose() 
        # Debug time!
        #logger.debug("rasterizePolygon: =============================================")
        #logger.debug("rasterizePolygon: =REMOVE ME, LOGGER REF INSIDE CLIPPEDMASKGENERATOR.PY AND ALL THESE DEBUG LINES AS WELL!=")
        #logger.debug("rasterizePolygon: (inside for loop) points: " + str(points))
        #logger.debug("rasterizePolygon: (inside for loop) pnts: " + str(pnts))
        #logger.debug("rasterizePolygon: =============================================")
        # Debug time!
    
    pixel, line = world2Pixel(geoTrans,pnts[0],pnts[1]) 
    minx = min(pixel)
    maxx = max(pixel)
    miny = min(line)
    maxy = max(line)
    #print "minx ",minx
    rasterPoly = Image.new("L", (nx,ny),1)
    rasterize = ImageDraw.Draw(rasterPoly)
    listdata = [(pixel[i],line[i]) for i in xrange(len(pixel))]
    rasterize.polygon(listdata,0)
    
    mask = 1 - imageToArray(rasterPoly)
    if(minx == maxx):
         maxx = maxx +1
    if(miny == maxy):
         maxy = maxy +1
    bounds = (minx,maxx,miny,maxy)
    
    # Debug time!
    logger.debug("rasterizePolygon: =============================================")
    logger.debug("rasterizePolygon: =REMOVE ME, LOGGER REF INSIDE CLIPPEDMASKGENERATOR.PY AND ALL THESE DEBUG LINES AS WELL!=")
    logger.debug("rasterizePolygon: final: points: " + str(points))
    logger.debug("rasterizePolygon: final: pnts: " + str(pnts))
    logger.debug("minx: " + str(minx))
    logger.debug("maxx: " + str(maxx))
    logger.debug("miny: " + str(miny))
    logger.debug("maxy: " + str(maxy))
    logger.debug("rasterizePolygon: =============================================")
    # Debug time!
    
    
    return bounds,mask

def rasterizePolygons(geoTrans,nx,ny,polygons):
    '''
    
    :param geoTrans:
    :param nx:
    :param ny:
    :param polygon:
    '''
    
    # Debug time!
    #import CHIRPS.utils.locallog.locallogging as llog
    #logger = llog.getNamedLogger("request_processor")
    # Debug time!
    
    
    minx = sys.maxint
    miny = sys.maxint
    maxx = -sys.maxint - 1
    maxy = -sys.maxint - 1
    outputmask = np.zeros((ny,nx))
    for poly in polygons:
        for i in range(0,poly.GetGeometryCount()):
        
            bounds, mask = rasterizePolygon(geoTrans, nx, ny, poly.GetGeometryRef(i).GetGeometryRef(0))
            outputmask =np.logical_or(outputmask,mask)
        
            minx = min(bounds[0],minx)
            maxx = max(bounds[1],maxx)
            miny = min(bounds[2],miny)
            maxy = max(bounds[3],maxy)
            
            # Debug time!
            #logger.debug("=============================================")
            #logger.debug("=REMOVE ME, LOGGER REF INSIDE CLIPPEDMASKGENERATOR.PY AND ALL THESE DEBUG LINES AS WELL!=")
            #logger.debug("minx: " + str(minx))
            #logger.debug("maxx: " + str(maxx))
            #logger.debug("miny: " + str(miny))
            #logger.debug("maxy: " + str(maxy))
            #logger.debug("=============================================")
            # Debug time!
    
    
    # Return Bounds, Mask  (So the value: (minx,maxx,miny,maxy), is actually bounds, not cords).        
    return (minx,maxx,miny,maxy),outputmask

    
