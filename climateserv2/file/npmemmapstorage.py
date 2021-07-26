'''
Created on Feb 13, 2014

@author: jeburks
'''


import numpy as np
import os.path
import CHIRPS.utils.processtools.MathOperations as mo
import CHIRPS.utils.configuration.parameters as params

def openFileForWriting(dataType, year):
    outputfile = params.getFilename(dataType, year)
    size = params.getGridDimension(dataType)
    if (os.path.isfile(outputfile) !=  True) :
        memmap = np.core.memmap(outputfile,dtype=np.float32, mode='w+',shape=(372,size[0],size[1]))
        array= np.zeros((372,size[0],size[1]), dtype=np.float32)+params.getFillValue(dataType)
        memmap[:,:,:]=array
        return memmap
    else:
        memmap = np.core.memmap(outputfile,dtype=np.float32,mode='w+',shape=(372,size[0],size[1]))
        return memmap


def openFileForReading(dataType, year):
    outputfile = params.getFilename(dataType, year)
    size = params.getGridDimension(dataType)
    memmap = np.core.memmap(outputfile,dtype=np.float32,mode='r',shape=(372,size[0],size[1]))
    return memmap

# def createOrOpenYearFile(dataType, year):
#     '''
#     
#     :param dataType:
#     :param year:
#     '''
#     outputfile = params.getFilename(dataType, year)
#     size = params.getGridDimension(dataType)
#     if (os.path.isfile(outputfile) !=  True) :
#         
#         memmap = np.core.memmap(outputfile,dtype=np.float32, mode='w+',shape=(372,size[0],size[1]))
#         array= np.zeros((372,size[0],size[1]), dtype=np.float32)+params.getFillValue(dataType)
#         memmap[:,:,:]=array
#         return memmap
#     else:
#        
#         memmap = np.core.memmap(outputfile,dtype=np.float32,mode='r+',shape=(372,size[0],size[1]))
#         return memmap
        

def processFile(memmap, array, year, month, day):
    '''
    
    :param memmap:
    :param array:
    :param year:
    :param month:
    :param day:
    '''
   
    index =  (month-1)*31 + (day-1)
    memmap[index,:,:] = array
    
def writeSpatialInformation(outputdir, prjwkt, grid, year):
    '''
    
    :param outputdir:
    :param prjwkt:
    :param grid:
    :param year:
    '''
    outputFilePrj = os.path.join(outputdir,'wkt.prj')
    outputFileDef = os.path.join(outputdir,'grid.def')
    #print "Outputting to ",outputFile,wkt, year
    f = open(outputFilePrj, 'w')
    f.write(prjwkt)
    f.close()
    f2 = open(outputFileDef, 'w')
    f2.write(', '.join(str(x) for x in grid))
    f2.close()
  
def getDayValueByDictionary(dict):
    '''
    
    :param dict:
    '''
    return getDayValue(dict['year'],dict['month'],dict['day'],dict['bounds'],dict['clippedmask'],dict['datatype'],dict['operationtype'])

def getDayValue(year,month,day,bounds,clippedmask,dataType,operationsType):
    '''
    
    :param year:
    :param month:
    :param day:
    :param bounds:
    :param clippedmask:
    :param dataType:
    :param operationsType:
    '''
    # print "Getting Day value ",year,month,day
    #Single item in one dimension
    #Calculate index for the day using 31 days in every month
    dayindex = (month-1)*31+(day-1)
    
    fillValue = params.getFillValue(dataType)
    memmap  = openFileForReading(dataType, year)
    clippedmask=clippedmask[np.newaxis,:,:]
    clippedmask = np.repeat(clippedmask, 1, axis=0)
    ##Need to get just on
    array = memmap[dayindex:dayindex+1,bounds[2]:bounds[3],bounds[0]:bounds[1]]
    mask = np.where((array !=fillValue) & (clippedmask == 1))
    ####Need to check for no finds and the return fill value
    pointCount = len(mask)
    if (pointCount >= 0):
        value = mo.getValue(operationsType, array[mask])
    else:
        value = mo.setValue(operationsType,fillValue)
    value['numPoints'] = pointCount
    del array
    del mask
    del clippedmask
    del memmap
    return value

def getMonthValueByDictionary(dict):
    '''
    
    :param dict:
    '''
    return getMonthValue(dict['year'],dict['month'],dict['bounds'],dict['clippedmask'],dict['datatype'],dict['operationtype'])
    
def getMonthValue(year,month,bounds,clippedmask,dataType,operationsType):
    '''
    
    :param year:
    :param month:
    :param bounds:
    :param clippedmask:
    :param dataType:
    :param operationsType:
    '''
    fillValue = params.getFillValue(dataType)
    memmap  = openFileForReading(dataType, year)
    daystartindex = (month-1)*31
    
    clippedmask=clippedmask[np.newaxis,:,:]
    clippedmask = np.repeat(clippedmask, 31, axis=0)
    
    array = memmap[daystartindex:daystartindex+31,bounds[2]:bounds[3],bounds[0]:bounds[1]]
    mask = np.where((array != fillValue) & (clippedmask == 1))
    pointCount = len(mask)
    if (pointCount >= 0):
        value = mo.getValue(operationsType, array[mask])
    else:
        value = mo.setValue(operationsType,fillValue)
    value['numPoints'] = pointCount
    del array
    del memmap
    del mask
    del clippedmask
    return value

def getYearValueByDictionary(dict):
    '''
    
    :param dict:
    '''
    return getYearValue(dict['year'],dict['bounds'],dict['clippedmask'],dict['datatype'],dict['operationtype'])

def getYearValue(year,bounds,clippedmask,dataType,operationsType):
    '''
    
    :param year:
    :param bounds:
    :param clippedmask:
    :param dataType:
    :param operationsType:
    '''
    #  print "Getting Year value ",year
    fillValue = params.getFillValue(dataType)
    
    #entire slice in the z direction
    memmap  = openFileForReading(dataType, year)
    num = 372
    clippedmask=clippedmask[np.newaxis,:,:]
    clippedmask = np.repeat(clippedmask, num, axis=0)
    #Get whole array for year
    array = memmap[:,bounds[2]:bounds[3],bounds[0]:bounds[1]]
    del memmap
    mask = np.where((array !=fillValue) & (clippedmask == 1))
    del clippedmask
    pointCount = len(mask)
    if (pointCount >= 0):
        value = mo.getValue(operationsType, array[mask])
    else:
        value = mo.setValue(operationsType,fillValue)
    value['numPoints'] = pointCount
    del array
    del mask
    return value
  
def getArrayForYearMonthDay(year,month,day,dataType):
    '''
     
    :param year:
    :param month:
    :param day:
    :param dataType:
    '''
     
    dayindex = (month-1)*31+(day-1)
     
   
    memmap  =openFileForReading(dataType, year)
     
    ##Need to get just on
    array = memmap[dayindex,:,:]
    array = array.squeeze()
    del memmap
    return array

def getSpatialReference(dataType):
    '''
    
    :param dataType:
    '''
    # print "Getting Spatial Reference System"
    inputDir = params.dataTypes[int(dataType)]['directory']
    inputFilePrj = os.path.join(inputDir, 'wkt.prj')
    inputFileDef = os.path.join(inputDir, 'grid.def')
    fPrj = open(inputFilePrj,'r')
    wkt = fPrj.read()
    fPrj.close()
    inputFileDef = open(inputFileDef,'r')
    gridString = inputFileDef.read().split(",")
    #print gridString
    geoTransform = (float(gridString[0]),float(gridString[1]),float(gridString[2]),float(gridString[3]),float(gridString[4]),float(gridString[5]))
    inputFileDef.close()
    ##Need to create geotransform
    
    return geoTransform,wkt
      









