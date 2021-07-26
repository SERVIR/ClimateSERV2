'''
Created on Mar 4, 2015
@author: jeburks

Modified from: Sept 2015 to: ????
@author: Kris Stanton
'''
import numpy as np
import CHIRPS.utils.configuration.parameters as params
import CHIRPS.utils.file.h5datastorage as dStore
import CHIRPS.utils.processtools.pMathOperations as pMath
import CHIRPS.utils.processtools.dateIndexTools as dit
import CHIRPS.utils.locallog.locallogging as llog
import CHIRPS.utils.file.ExtractTifFromH5 as extractTif


# KS Note 2015 : All these added for download job types only
import os
import sys
import datetime
import CHIRPS.utils.file.npmemmapstorage as rp
import CHIRPS.utils.geo.geoutils as geoutils

from gdalconst import *
from osgeo import gdal #, gdalnumeric, ogr, osr


logger = llog.getNamedLogger("request_processor")

# Some Hacky stuff here...  
# So getting a reverse order selections not allowed error at a certain point where h5 data is retrieved
# The main presentation of this is when user selects a polygon that crosses the Longitude 0 vertical line on the map
# Replacement function on fail
# Source:  h5py._hl.selections._translate_slice
# To replace the function: h5py._hl.selections._translate_slice = _ReplacementForFunc_translate_slice

# This did not work... and was actually kind of a bad idea!
#def _ReplacementForFunc_translate_slice(exp, length):
#    """ Given a slice object, return a 3-tuple
#        (start, count, step)
#        for use with the hyperslab selection routines
#    """
#    start, stop, step = exp.indices(length)
#        # Now if step > 0, then start and stop are in [0, length]; 
#        # if step < 0, they are in [-1, length - 1] (Python 2.6b2 and later; 
#        # Python issue 3004).
#
#    if step < 1:
#        raise ValueError("Step must be >= 1 (got %d)" % step)
#    #if stop < start:
#    #    raise ValueError("Reverse-order selections are not allowed")
#    print("No Turning back now!!!")
#
#    count = 1 + (stop - start - 1) // step
#
#    return start, count, step

def getDayValueByDictionary(dict):
    '''
    
    :param dict:
    '''
    #return getDayValue(dict['year'],dict['month'],dict['day'],dict['bounds'],dict['clippedmask'],dict['datatype'],dict['operationtype'])
    return getDayValue(dict['year'],dict['month'],dict['day'],dict['bounds'],dict['clippedmask'],dict['datatype'],dict['operationtype'], dict['polygon_Str_ToPass'], dict['uid']) #  dict['geometryToClip']

#def getDayValue(year,month,day,bounds,clippedmask,dataType,operationsType):
def getDayValue(year,month,day,bounds,clippedmask,dataType,operationsType, polygon_Str_ToPass, uid): # geometryToClip
    '''
    
    :param year:
    :param month:
    :param day:
    :param bounds:
    :param clippedmask:
    :param dataType: This is actually the datatype number (int)
    :param operationsType:
    '''
    # print "Getting Day value ",year,month,day
    #Single item in one dimension
    #Calculate index for the day using 31 days in every month
    logger.debug("getDay Value year="+str(year)+"  month="+str(month)+" day="+str(day)+" datatype="+str(dataType))
    
    # KS Refactor 2015 // This is where I'm intercepting the code to add the new 'download' operation at the worker thread level
    if(params.parameters[operationsType][1] == 'download'):
        # Do the download stuff
        #logger.debug("DataCalculator:getDayValue: TODO: Finish the code that creates a tif file from all the inputs we have here!")
        onErrorReturnValue = 0 # 0 for failures?  (555 is just a place holder to see if this all works!!)
        try:
            
            
            
            # Param Checking   (Compared to the test controller function in HDFDataToFile)
            theDataTypeNumber = dataType # formerly 'theDataType'
            size = params.getGridDimension(int(theDataTypeNumber))
            geotransform, wkt = rp.getSpatialReference(int(theDataTypeNumber))
            theBounds = bounds #mg.getPolyBoundsOnly(geoTrans,polygon):
            
            #polygon_Str_ToPass
            #geometry = geometryToClip # Had to pipe this one in as a new dictionary param from the head processor!!!
            geometry = geoutils.decodeGeoJSON(polygon_Str_ToPass)
        
            theYear = year  # Get this from param 'year'  (Passed in as part of a dictionary object)  (also applies for month, and day)
            theMonth = month
            theDay = day
            
            # Worker Section
            theStore = dStore.datastorage(theDataTypeNumber, theYear)
            theIndexer = params.dataTypes[theDataTypeNumber]['indexer']
            theFillValue = params.getFillValue(theDataTypeNumber)
            theIndex = theIndexer.getIndexBasedOnDate(theDay,theMonth,theYear)
            
            hdf_Data_Array = None
            try:
                hdf_Data_Array = theStore.getData(theIndex,bounds=theBounds)
                
            except:
                
                firstErrorMessage = str(sys.exc_info())
                logger.debug("DataCalculator: Download Job ERROR getting data from H5 to hdf_Data_Array: We are inside 2 try/except blocks.  firstErrorMessage:  " + str(firstErrorMessage) + ",  Trying something crazy before bailing out!")
                # Last ditch effort, lets replace the buggy h5py functions
                try:
                    # This did not work... it actually caused a crash that looked worse than the other one.
                    #h5py._hl.selections._translate_slice = _ReplacementForFunc_translate_slice
                    #hdf_Data_Array = theStore.getData_AlternateH5PyFunc(theIndex, _ReplacementForFunc_translate_slice, bounds=theBounds)
                    
                    # This did not work either, it ended up selecting inverse x range
                    #hdf_Data_Array = theStore.getData_AlternateH5PyFunc(theIndex, bounds=theBounds)
                    # Wrote a bit of code in my
                    # Next attempt is to get two sets of bounds and two sets of datasets.... and then stitch them together!!
                    # Here is the near final version of this
                    breakPoint = 0  # I seriously can't believe I just wrote this block of code without testing it, and it seemed to work the first try!!
                    theBounds_Part1 = (theBounds[0], (breakPoint - 1), theBounds[2], theBounds[3])
                    theBounds_Part2 = (breakPoint, theBounds[1], theBounds[2], theBounds[3])
                    hdf_Data_Array_Part1 = theStore.getData(theIndex,bounds=theBounds_Part1)
                    hdf_Data_Array_Part2 = theStore.getData(theIndex,bounds=theBounds_Part2)
                    theHeight_Of_New_Array = hdf_Data_Array_Part1.shape[0]
                    theWidth_Of_New_Array = hdf_Data_Array_Part1.shape[1] + hdf_Data_Array_Part2.shape[1]
                    stitchedData_Array = np.zeros(shape=(theHeight_Of_New_Array,theWidth_Of_New_Array), dtype=np.float32)
                    for currentRowIndex in range(0,theHeight_Of_New_Array):
                        tempRow = np.zeros(shape=(theWidth_Of_New_Array), dtype=np.float32)
                        for currValueIndex_1 in range(0, hdf_Data_Array_Part1.shape[1]):
                            currentValue = hdf_Data_Array_Part1[currentRowIndex][currValueIndex_1]
                            tempRow[currValueIndex_1] = currentValue
                        for currValueIndex_2 in range(0, hdf_Data_Array_Part2.shape[1]):
                            currentValueIndex_2_Adjusted = currValueIndex_2 + hdf_Data_Array_Part1.shape[1]
                            currentValue = hdf_Data_Array_Part2[currentRowIndex][currValueIndex_2]
                            tempRow[currentValueIndex_2_Adjusted] = currentValue
                        stitchedData_Array[currentRowIndex] = tempRow
                    
                    # here goes...
                    hdf_Data_Array = stitchedData_Array
                except:
                    #e = sys.exc_info()[0]
                    # If this error keeps happening and can't figure it out,, read HDFDataToFile line 138 to see some more detailed notes on this issue.
                    logger.debug("DataCalculator: Download Job ERROR getting data from H5 to hdf_Data_Array: We are inside 2 try/except blocks, and the second one failed..  firstErrorMessage:  " + str(firstErrorMessage) + " System Error Message: " + str(sys.exc_info()))
                    return onErrorReturnValue
            
            # Points processing from geometry value
            thePoints = geometry  # New Context for 'geometry'!
            theLats = []
            theLongs = []
            
            # Get the list of lats and longs from the geometry points
            for p in range(thePoints.GetPointCount()):
                theLats.append(thePoints.GetY(p))
                theLongs.append(thePoints.GetX(p))
            
            # Get the Min Longitude and Max Latitude (Top Left Corner)
            minLong = min(theLongs)
            maxLat = max(theLats)
            
            # Adjust the max lat and min long for negative values (Need to make sure this works for datatypes other than climate model outputs)
            adjusted_Min_Long = minLong
            adjusted_Max_Lat = maxLat
            if(minLong < 0):
                #adjusted_Min_Long = minLong + 360
                adjusted_Min_Long = minLong
            if(maxLat < 0):
                #adjusted_Max_Lat = abs(maxLat) + 90    # This line caused images selected below 0 lat to be in a very wrong position (off by 97 ish on one test)
                #adjusted_Max_Lat = abs(maxLat) - 90
                adjusted_Max_Lat = maxLat
             
            
            # This quick fix did not work well enough... need something better.   
            ## Quick Fix for 'bug 3 pixels off by half a degree' 
            #pixel_Resolution_X = 0.5   # grid[1]
            #if(adjusted_Min_Long < 180):
            #    adjusted_Min_Long = adjusted_Min_Long + ( - ( pixel_Resolution_X / 2) )
            #else:
            #    adjusted_Min_Long = adjusted_Min_Long + (   ( pixel_Resolution_X / 2) )
            #pixel_Resolution_Y = -0.5   # grid[5]
            #if(adjusted_Max_Lat > 0):
            #    adjusted_Max_Lat = adjusted_Max_Lat + ( - ( abs(pixel_Resolution_Y) / 2) )
            #else:
            #    adjusted_Max_Lat = adjusted_Max_Lat + (   ( abs(pixel_Resolution_Y) / 2) )
                    
            # Outfile transform x,y positions set using the adjusted min long and max lat
            outTransform_xPos = adjusted_Min_Long
            outTransform_yPos = adjusted_Max_Lat
            
            # Need this later 
            noData_Value = theFillValue
            bandName = 1
            
            fullDatset_GeoTransform = geotransform
            outFullGeoTransform = (outTransform_xPos, fullDatset_GeoTransform[1], fullDatset_GeoTransform[2], outTransform_yPos, fullDatset_GeoTransform[4], fullDatset_GeoTransform[5])
    
            fullDataset_Projection = wkt    
            
            uniqueID = uid  # Entire Job ID
            
            
            
            
            # Process the filename
            outFileName = extractTif.get_Tif_FileOutName(theDataTypeNumber, theYear, theMonth, theDay)
            outFileFolder = params.zipFile_ScratchWorkspace_Path + str(uid)+"/" 
            outFileFullPath = outFileFolder + outFileName
    
    
            #logger.debug("Alert: 1")
            
            
            
            
            #logger.debug("Alert: 2")
            
            # Get the output File size
            out_X_Size = hdf_Data_Array.shape[1]
            out_Y_Size = hdf_Data_Array.shape[0]
            
            # Get the gdal driver and create the a blank output file
            theDriverFormat = "GTiff"
            theDriver = gdal.GetDriverByName( theDriverFormat )
            
            #logger.debug("Alert: 3")
            
            outDS = theDriver.Create(outFileFullPath, out_X_Size, out_Y_Size, 1, GDT_Float32)        
            
            #logger.debug("Alert: 4")
            
            
            # Get the image band and write the data array values to it.  Flush the Cache and set the NoDataValue (This is the step that writes data to the output file)
            outDataArray = hdf_Data_Array
            outBand = outDS.GetRasterBand(bandName)
            outBand.WriteArray(outDataArray, 0, 0)
            outBand.SetNoDataValue(noData_Value)
            outBand.FlushCache()
            
            #logger.debug("Alert: 5")
            
            
            # Set the projection and transform
            outDS.SetGeoTransform(outFullGeoTransform)
            outDS.SetProjection(fullDataset_Projection)
            
            # closes the dataset (Very important!)
            outDS = None
            
            #logger.debug("Alert: 6")
            
            # That should be it... we should now have a tif file located in the zipfile scratch area... and many, for each time this is run!
            
            # If we got this far, return '1' as a way to signal that it all worked and the current Tif file should be created.
            return 1
        except:
            # Something went wrong.
            logger.debug("DataCalculator: Download Job ERROR: Not sure what went wrong... System Error Message: " + str(sys.exc_info()))
            return onErrorReturnValue
            pass
            
        # It's looking like we can use this return to be a 1 or 0 (if the tif file was generated or not?)
        return onErrorReturnValue  
    else:
        
        # Normal Statistical operations
        mathoper = pMath.mathOperations(operationsType,1,params.dataTypes[dataType]['fillValue'],None) 
        try:
            store = dStore.datastorage(dataType, year)
            
            #logger.debug("DataCalculator Alert A")
            
            indexer = params.dataTypes[dataType]['indexer']
            
            #logger.debug("DataCalculator Alert B")
            
            fillValue = params.getFillValue(dataType)
            
            #logger.debug("DataCalculator Alert C")
            
            index = indexer.getIndexBasedOnDate(day,month,year)
            
            #logger.debug("DataCalculator Alert D")
            
            # This fix worked for the downloads... lets see if it works here too!
            array_H5Data = None
            try:
                array_H5Data = store.getData(index,bounds=bounds)
                            except:
                firstErrorMessage = str(sys.exc_info())
                logger.debug("DataCalculator: Statistics Job ERROR getting data from H5 to array_H5Data: We are inside 2 try/except blocks.  firstErrorMessage:  " + str(firstErrorMessage) + ",  Trying something crazy before bailing out!")
                # Last ditch effort, lets replace the buggy h5py functions
                try:
                    # Vars we need in here..
                    theBounds = bounds
                    theStore = store
                    theIndex = index
                    
                    # Stitch the two arrays together
                    breakPoint = 0  
                    theBounds_Part1 = (theBounds[0], (breakPoint - 1), theBounds[2], theBounds[3])
                    theBounds_Part2 = (breakPoint, theBounds[1], theBounds[2], theBounds[3])
                    hdf_Data_Array_Part1 = theStore.getData(theIndex,bounds=theBounds_Part1)
                    hdf_Data_Array_Part2 = theStore.getData(theIndex,bounds=theBounds_Part2)
                    theHeight_Of_New_Array = hdf_Data_Array_Part1.shape[0]
                    theWidth_Of_New_Array = hdf_Data_Array_Part1.shape[1] + hdf_Data_Array_Part2.shape[1]
                    stitchedData_Array = np.zeros(shape=(theHeight_Of_New_Array,theWidth_Of_New_Array), dtype=np.float32)
                    for currentRowIndex in range(0,theHeight_Of_New_Array):
                        tempRow = np.zeros(shape=(theWidth_Of_New_Array), dtype=np.float32)
                        for currValueIndex_1 in range(0, hdf_Data_Array_Part1.shape[1]):
                            currentValue = hdf_Data_Array_Part1[currentRowIndex][currValueIndex_1]
                            tempRow[currValueIndex_1] = currentValue
                        for currValueIndex_2 in range(0, hdf_Data_Array_Part2.shape[1]):
                            currentValueIndex_2_Adjusted = currValueIndex_2 + hdf_Data_Array_Part1.shape[1]
                            currentValue = hdf_Data_Array_Part2[currentRowIndex][currValueIndex_2]
                            tempRow[currentValueIndex_2_Adjusted] = currentValue
                        stitchedData_Array[currentRowIndex] = tempRow
                    
                    # here goes...
                    array_H5Data = stitchedData_Array
                    logger.debug("DataCalculator stitchedData_Array has been built.")
                    #logger.debug("DataCalculator Value of 'stitchedData_Array': " + str(stitchedData_Array))
                    
                except:
                    logger.debug("DataCalculator: Download Job ERROR getting data from H5 to hdf_Data_Array: We are inside 2 try/except blocks, and the second one failed..The code will break shortly...  firstErrorMessage:  " + str(firstErrorMessage) + " System Error Message: " + str(sys.exc_info()))
            
            #logger.debug("DataCalculator Alert E")
            
            #logger.debug("DataCalculator.getDayValue : Value of 'index': " + str(index))
            
            # ks note // understanding whats in the 'array' object
            #logger.debug("DataCalculator.getDayValue : Value of 'index': " + str(index))
            #logger.debug("DataCalculator.getDayValue : Value of 'array': " + str(array))
            #logger.debug("DataCalculator.getDayValue : Value of 'array': " + str(array))
        
            
            #mask = np.where((array_H5Data != fillValue) & (clippedmask == True))
            #
            #logger.debug("DataCalculator Alert F")
            #
            #logger.debug("DataCalculator Alert F.debug: DataCalculator.getDayValue : Value of 'clippedmask': " + str(clippedmask))
            #logger.debug("DataCalculator Alert F.debug: DataCalculator.getDayValue : Value of 'mask': " + str(mask))
            #logger.debug("DataCalculator Alert F.debug: DataCalculator.getDayValue : Value of 'array_H5Data': " + str(array_H5Data))
            #logger.debug("DataCalculator Alert F.debug: DataCalculator.getDayValue : Value of 'str(len(mask[0]))': " + str(len(mask[0])))
            #logger.debug("DataCalculator Alert F.debug: DataCalculator.getDayValue : Value of 'str(len(mask[1]))': " + str(len(mask[1])))
            #logger.debug("DataCalculator Alert F.debug: DataCalculator.getDayValue : Value of 'str(array_H5Data.size)': " + str(array_H5Data.size))
            
            
            # Something in here breaks on Climate Datatypes that are found in the southern hemisphere
            #mathoper.addData(array_H5Data[mask])       # SOMETHING WRONG HERE!!
            mask = None
            try:
                #logger.debug("DataCalculator Alert FFFFF.debug: DataCalculator.getDayValue : Value of 'array_H5Data': " + str(array_H5Data))
                #logger.debug("DataCalculator Alert FF.debug: DataCalculator.getDayValue : Value of 'clippedmask': " + str(clippedmask))
                #logger.debug("fillValue" + fillValue)
                #logger.debug("Just about to build the mask!")
                mask = np.where((array_H5Data != fillValue) & (clippedmask == True))
            
                #logger.debug("Mask is built!")
            
                #logger.debug("DataCalculator Alert FF.debug: DataCalculator.getDayValue : Value of 'clippedmask': " + str(clippedmask))
                #logger.debug("DataCalculator Alert FFF.debug: DataCalculator.getDayValue : Value of 'mask': " + str(mask))
                #logger.debug("DataCalculator Alert FFFFF.debug: DataCalculator.getDayValue : Value of 'array_H5Data': " + str(array_H5Data))
                #logger.debug("DataCalculator Alert FFFFFF.debug: DataCalculator.getDayValue : Value of 'str(len(mask[0]))': " + str(len(mask[0])))
                #logger.debug("DataCalculator Alert FFFFFFF.debug: DataCalculator.getDayValue : Value of 'str(len(mask[1]))': " + str(len(mask[1])))
                #logger.debug("DataCalculator Alert FFFFFFFF.debug: DataCalculator.getDayValue : Value of 'str(array_H5Data.size)': " + str(array_H5Data.size))
            
                # If the Size of the mask is 0.... raise exception
                if len(mask[0]) == 0:
                    logger.debug("DataCalculator Alert F.debug.raise: DataCalculator.getDayValue : Issue With len(mask[0]).  It should NOT be equal to 0.  Raising the exception...': ")
                    raise
                
                
                mathoper.addData(array_H5Data[mask])       # SOMETHING WRONG HERE!!
            except:
                logger.debug("DataCalculator Alert F.except.debug: Something went wrong with the normal process..")
                # Make a mask that matches the existing data array but whose values are the result of a clipped mask that is always
                sizeOfH5Data = array_H5Data.size            # ex: 24
                numOf_H5_Rows = array_H5Data.shape[0]       # ex: 3
                numOf_H5_Cols = array_H5Data.shape[1]       # ex: 8
                maskArray_1 = np.zeros(shape=(sizeOfH5Data), dtype=int)
                maskArray_2 = np.zeros(shape=(sizeOfH5Data), dtype=int)
                # Set the values of the arrays (looks like using range does not include the last value.)
                for j in range(0, numOf_H5_Rows):
                    for i in range(0, numOf_H5_Cols):
                        current_Index =  i + (numOf_H5_Cols * j)    # currentColumnIndex + (numOfColumns * currentRowIndex)
                        current_Value_Part_1 = j    # Just put the Row Value (this gives the repeating pattern we want
                        current_Value_Part_2 = i    # Current Column should do it.. that pattern repeats for each row.
                        maskArray_1[current_Index] = current_Value_Part_1
                        maskArray_2[current_Index] = current_Value_Part_2
                fakeMask = (maskArray_1,maskArray_2)
                
                #logger.debug("DataCalculator Alert F.except.debug: DataCalculator.getDayValue : Value of 'fakeMask': " + str(fakeMask))
                
                # Lets try this again!!
                mathoper.addData(array_H5Data[fakeMask])
                

            #logger.debug("DataCalculator Alert G")
        
            del mask
            del array_H5Data
            store.close()
            #logger.debug("DataCalculator Alert H")
            value = mathoper.getOutput()
            #logger.debug("DataCalculator Alert I")
            mathoper.cleanup()
            logger.debug("DataCalculator Alert J")
            return value
        except:
            e = sys.exc_info()[0]
            logger.debug("DataCalculator.getDayValue : returning fill value.. 'mathoper.getFillValue()': " + str(mathoper.getFillValue()) + " System Error Message: " + str(e))
            return mathoper.getFillValue()
        
    

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
    logger.debug("getMonth Value year="+str(year)+"  month="+str(month)+" dataType="+str(dataType))
    lastDayOfMonth = dit.getLastDayOfMonth(month, year)
    mathoper = pMath.mathOperations(operationsType, lastDayOfMonth,params.dataTypes[dataType]['fillValue'],None) 
    try:
        store = dStore.datastorage(dataType, year)
        indexer = params.dataTypes[dataType]['indexer']
        fillValue = params.getFillValue(dataType)
        
        indexes = indexer.getIndexesBasedOnDate(1,month,year,lastDayOfMonth,month,year)
        
        for i in indexes:
            array = store.getData(i,bounds=bounds)
            mask = np.where((array != fillValue) & (clippedmask == True))
            if np.size(mask) >0:
                mathoper.addData(array[mask])
        del mask
        del array
        store.close()
        value = mathoper.getOutput()
        mathoper.cleanup()
        return value
    except: 
        return mathoper.getFillValue()
   
 
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
    logger.debug("getYear Value year="+str(year)+" datatype="+str(dataType))
    mathoper = pMath.mathOperations(operationsType,12,params.dataTypes[dataType]['fillValue'],None)  
    try:
        store = dStore.datastorage(dataType, year)
        indexer = params.dataTypes[dataType]['indexer']
        fillValue = params.getFillValue(dataType)
        indexes = indexer.getIndexesBasedOnDate(1,1,year,31,12,year)
       
        for i in indexes:
            array = store.getData(i,bounds=bounds)
            mask = np.where((array != fillValue) & (clippedmask == True))
            if np.size(mask) >0:
                mathoper.addData(array[mask])
            
        del mask
        del array
        store.close()
        value = mathoper.getOutput()
        mathoper.cleanup()
        return value
    except:
        return mathoper.getFillValue()
    
def getArrayForYearMonthDay(year,month,day,dataType):
    '''
      
    :param year:
    :param month:
    :param day:
    :param dataType:
    '''
    try:
        store = dStore.datastorage(dataType, year)
        indexer = params.dataTypes[dataType]['indexer']
        index = indexer.getIndexBasedOnDate(day,month,year)
        array = store.getData(index)
        return array
        store.close()
    except :
        return []
    return array