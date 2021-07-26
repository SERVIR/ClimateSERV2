import CHIRPS.utils.configuration.parameters as params
import numpy as np
import os
import CHIRPS.utils.processtools.dateIndexTools as dit

def getSpatialReference(self,dataType):
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
    
class datastorage(object):
    memmap = None
    dataType = None
    forWriting = False
    
    def __openFileForReading__(self,dataType, year):
        outputfile = params.getFilename(dataType, year)
        
        size = params.getGridDimension(dataType)
        indexLastDay = dit.convertEpochToJulianDay(dit.convertDayMonthYearToEpoch(31, 12, year))
        memmap = np.core.memmap(outputfile,dtype=np.float32,mode='r',shape=(indexLastDay,size[0],size[1]))
        return memmap
    
    def __openFileForWriting__(self,dataType, year):
        self.forWriting = True
        outputfile = params.getFilename(dataType, year)
        size = params.getGridDimension(dataType)
        if (os.path.isfile(outputfile) !=  True) :
            indexLastDay = dit.convertEpochToJulianDay(dit.convertDayMonthYearToEpoch(31, 12, year))
            memmap = np.core.memmap(outputfile,dtype=np.float32, mode='w+',shape=(indexLastDay,size[0],size[1]))
            memmap[:,:,:]=np.zeros((size[0],size[1]), dtype=np.float32)+params.getFillValue(dataType)
            return memmap
        else:
            indexLastDay = dit.convertEpochToJulianDay(dit.convertDayMonthYearToEpoch(31, 12, year))
            memmap = np.core.memmap(outputfile,dtype=np.float32,mode='w+',shape=(indexLastDay,size[0],size[1]))
            return memmap
    
    def __init__(self, dataType, year, forWriting=False):
        self.dataType = dataType
        if (forWriting == False):
            self.memmap = self.__openFileForReading__(dataType, year)
        else:
            self.memmap = self.__openFileForWriting__(dataType, year)
      
    def getData(self,index,bounds=None):
        if (bounds == None): 
            return self.memmap[index,:,:]
        else:
            return self.memmap[index,bounds[2]:bounds[3],bounds[0]:bounds[1]]
        
    def putData(self,index, array):
        if (self.forWriting == True):
            self.memmap[index,:,:] = array
            self.memmap.flush()
        else:
            print "File not open for writing"
        
    def close(self):
        if (self.forWriting == True):
            self.memmap.flush()
        del self.memmap
        
    
        
