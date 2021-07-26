import CHIRPS.utils.configuration.parameters as params
import os
import CHIRPS.utils.processtools.dateIndexTools as dit
import h5py
from CHIRPS.utils.file import fileutils

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
    f = open(outputFilePrj, 'w')
    f.write(prjwkt)
    f.close()
    f2 = open(outputFileDef, 'w')
    f2.write(', '.join(str(x) for x in grid))
    f2.close()
    try:
		os.chmod(outputFilePrj,0o777)
    except:
		pass
    try:
		os.chmod(outputFileDef,0o777)
    except:
		pass		
    
class datastorage(object):
    dset = None
    dataType = None
    forWriting = False
    f =None
    def __openFileForReading__(self,dataType, year):
        outputfile = params.getHDFFilename(dataType, year)
        self.f = h5py.File(outputfile,'r+')
        self.dset = self.f["data"]
        return self.dset
    
    def __openFileForWriting__(self,dataType, year):
        self.forWriting = True
        outputfile = params.getHDFFilename(dataType, year)
  
        size = params.getGridDimension(dataType)
        if not os.path.exists(params.dataTypes[self.dataType]['directory']):
            fileutils.makePath(params.dataTypes[self.dataType]['directory'])  
        if (os.path.isfile(outputfile) !=  True) :
            indexLastDay = dit.convertEpochToJulianDay(dit.convertDayMonthYearToEpoch(31, 12, year))
            self.f = h5py.File(outputfile,'a')
            try:
				os.chmod(outputfile, 0o777)
            except:
				pass
            return self.f.create_dataset("data", (indexLastDay,size[1],size[0]), dtype='float32', compression="lzf", fillvalue=params.getFillValue(dataType))
        else:
            print 'file exists'
     
            indexLastDay = dit.convertEpochToJulianDay(dit.convertDayMonthYearToEpoch(31, 12, year))
            self.f = h5py.File(outputfile,'a')
            return self.f["data"]
    
    def __init__(self, dataType, year, forWriting=False):
        self.dataType = dataType
        if (forWriting == False):
            self.dset = self.__openFileForReading__(dataType, year)
        else:
            self.dset = self.__openFileForWriting__(dataType, year)
      
    def getData(self,index,bounds=None):
        if (bounds == None): 
            return self.dset[index,:,:]
        else:
            return self.dset[index,bounds[2]:bounds[3],bounds[0]:bounds[1]]
        
    # Edit to try and fix a bug with h5py
    #def getData_AlternateH5PyFunc(self,index, newFunc, bounds=None):
    def getData_AlternateH5PyFunc(self,index, bounds=None):
        # Override the buggy function in h5py
        #h5py._hl.selections._translate_slice = newFunc  # That was a bad idea!!
        
        if (bounds == None): 
            return self.dset[index,:,:]
        else:
            # Switched these here.
            #return self.dset[index,bounds[2]:bounds[3],bounds[0]:bounds[1]]    # Original
            #return self.dset[index,bounds[3]:bounds[2],bounds[0]:bounds[1]]    # Nope
            #return self.dset[index,bounds[2]:bounds[3],bounds[1]:bounds[0]]     # Sort of works.. (Crops out the x axis part of the selection, and selects the inverse)
            
            # What needs to happen is the bounds get broken up into 2 sets of selections and then the datasets stitched together..
            return self.dset[index,bounds[2]:bounds[3],bounds[1]:bounds[0]]
         
    def putData(self,index, array):
        if (self.forWriting == True):
            #print("H5DataStorageDEBUG: array: " + str(array))
            #print("H5DataStorageDEBUG: index: " + str(index))
            self.dset[index,:,:] = array
        else:
            print "File not open for writing"
        
    def close(self):
        self.f.close()
        del self.dset
        
    
        
