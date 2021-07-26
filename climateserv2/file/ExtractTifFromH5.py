'''
Created on Oct 11, 2015

@author: Kris Stanton
'''

import os
import zipfile
import sys
import datetime
import json

import CHIRPS.utils.configuration.parameters as params
import CHIRPS.utils.locallog.locallogging as llog
import CHIRPS.utils.db.bddbprocessing as bdp


# Utils
# Set of functions that when used together result in zipfile(s) which contain Tif file collections (datasets) that were extracted from H5 files.

logger = llog.getNamedLogger("request_processor")

# Zip a list of files
# Take in a list of full file paths and the full path to where a new zip file should be created.
# iterate through the list and place each file in the zip
def zip_List_Of_Files(listOf_FullFilePaths_ToZip, zipFile_FullPath):
    
    try:
        # Create the zip file
        theZipFile = zipfile.ZipFile(zipFile_FullPath, 'w')
    
        # Iterate through the list and add each file to the zip
        for currentFile_FullPath in listOf_FullFilePaths_ToZip:
            currentFile_FilenameOnly = os.path.basename(currentFile_FullPath)
            theZipFile.write(currentFile_FullPath, currentFile_FilenameOnly)
        
        # Close the zip file
        theZipFile.close()
    
        # If we made it this far, return the full path to the zip file that we just operated on.
        return zipFile_FullPath
    
    except:
        # On fail, return None
        return None

    
# returns the run YYYYMM capabilities
def get_RunYYYYMM_From_ClimateModel_Capabilities(theDataTypeNumber):
    try:
        conn = bdp.BDDbConnector_Capabilities()
        currentCapabilities_jsonString = conn.get_Capabilities(theDataTypeNumber)
        conn.close()
        currentCapabilities = json.loads(currentCapabilities_jsonString)
        startDateTime_Y_M_D = currentCapabilities['startDateTime']
        date_FormatString_For_ForecastRange = currentCapabilities['date_FormatString_For_ForecastRange']
        currentData_Date_datetime = datetime.datetime.strptime(startDateTime_Y_M_D,date_FormatString_For_ForecastRange)
        YYYYMM_String = currentData_Date_datetime.strftime("%Y%m")
        return YYYYMM_String
        
    except:
        return None
    
# Optimize, pass the YYYYMM String to the 
def get_Tif_FileOutName(theDataTypeNumber, theYear, theMonth, theDay):
    # Get ENS and Variable
    # Download requests (Place this code into 'ExtractTifFromH5' after done.
    #theIndexer = params.dataTypes[theDataTypeNumber]['indexer']
    theDataCategory = None
    theFileName = ""
    try:
        theDataCategory = params.dataTypes[theDataTypeNumber]['data_category']
    except:
        theDataCategory = None
        
    if (theDataCategory == 'ClimateModel'):
        
        current_YYYYMM_String = get_RunYYYYMM_From_ClimateModel_Capabilities(theDataTypeNumber)
        if (current_YYYYMM_String == None):
            # Stopgap, return the current YYYYMM
            current_YYYYMM_String = datetime.datetime.utcnow().strftime("%Y%m")
            
        # Get Ensemble and Variable
        currentEnsemble = params.dataTypes[theDataTypeNumber]['ensemble']
        currentVariable = params.dataTypes[theDataTypeNumber]['variable']
        
        # Process the filename
        currentData_Date_RawString = str(theYear) + "-" + str(theMonth) + "-" + str(theDay)
        currentData_Date_datetime = datetime.datetime.strptime(currentData_Date_RawString,"%Y-%m-%d")
        currentData_Date_OutString = currentData_Date_datetime.strftime("%Y%m%d")
            
        outFileName_Parts_RunYYYYMM = current_YYYYMM_String #"FIX__r201509"  # This is the year and month that the model ran, need a way to get this (from the original file somehow, or store this in a settings file somewhere else?)
        outFileName_Parts_Ensemble = currentEnsemble #"FIX__e01"
        outFileName_Parts_Variable = currentVariable #"FIX__prcp"    
        outFileName_Parts_Forecast_Date = "f" + currentData_Date_OutString #"fYYYY_MM_DD" # Date of the current file (can get this directly from the index)
        outFileName_Parts_Extension = ".tif"
        #outFileName = "testController_YYYY_MM_DD_1.tif"
        outFileName = outFileName_Parts_RunYYYYMM + "_" + outFileName_Parts_Ensemble + "_" + outFileName_Parts_Variable + "_" + outFileName_Parts_Forecast_Date + outFileName_Parts_Extension
        theFileName = outFileName
    else:
        # Placeholder
        theFileName = str(theYear) + "-" + str(theMonth) + "-" + str(theDay) + ".tif"
    
    return theFileName #"PlaceHolder_FileName.tif"  
        

# Takes all the files from the scratch workspace of a single dataset and zips them into a 
def zip_Extracted_Tif_Files_Controller(theJobID):
    
    errorMessage = None
    zipFilePath = None
    try:
        path_To_ScratchTifs = params.zipFile_ScratchWorkspace_Path
        path_To_Zip_MediumTerm_Storage = params.zipFile_MediumTermStorage_Path
    
        # append the job ID to the folder path.
        path_To_ScratchTifs = os.path.join(path_To_ScratchTifs,str(theJobID))
    
        # Make a list of files to be zipped
        list_Of_File_FullPaths_To_Zip = []
    
        # Gets a list of files from a folder and joins them into a list to create the full path.
        list_Of_Filenames_Only = [ f for f in os.listdir(path_To_ScratchTifs) if os.path.isfile(os.path.join(path_To_ScratchTifs, f))]
        for currFileName in list_Of_Filenames_Only:
            currFullPath = os.path.join(path_To_ScratchTifs, currFileName)
            list_Of_File_FullPaths_To_Zip.append(currFullPath)
        
        #list_Of_File_FullPaths_To_Zip = list_Of_Filenames_Only
    
        # Create the name of the zip file from the job ID
        zipFileName = str(theJobID) + ".zip"
    
        # create the final zipfile path
        zipFile_FullPath = os.path.join(path_To_Zip_MediumTerm_Storage, zipFileName)
    
        # If folder does not exist, create it.
        if not os.path.exists(path_To_Zip_MediumTerm_Storage):
            os.makedirs(path_To_Zip_MediumTerm_Storage)
        
        # only if we have atleast 1 file..
        if(len(list_Of_File_FullPaths_To_Zip) > 0):
            zipFilePath = zip_List_Of_Files(list_Of_File_FullPaths_To_Zip, zipFile_FullPath)
            os.chmod(zipFilePath, 0777)
        else:
            zipFilePath = None
            errorMessage = "Error, no files found to zip!"
    except:
        e = sys.exc_info()[0]
        errorMessage = "zip_Extracted_Tif_Files_Controller: Something went wrong while zipping files.. System Error Message: " + str(e)
        
    return zipFilePath, errorMessage
    
    


# Create the scratch folder if it does not exist.
def create_Scratch_Folder(pathToCreate):
    # If folder does not exist, create it.
    if not os.path.exists(pathToCreate):
        try:
            os.makedirs(pathToCreate)
            logger.info("ExtractTifFromH5: Created Folder at: " + str(pathToCreate))
        except:
            e = sys.exc_info()[0]
            errMsg = "ExtractTifFromH5: Failed to create folder path (it may have already been created by another thread).  Error Message: " + str(e)
            logger.debug(errMsg)
       

# Round Down
def get_Float_RoundedDown(theFloatNumber):
    rawInt = int(theFloatNumber)
    roundedDownFloat = float(rawInt)
    return roundedDownFloat
# Round Up
def get_Float_RoundedUp(theFloatNumber):
    roundedDownFloat = get_Float_RoundedDown(theFloatNumber)
    roundedUpFloat = roundedDownFloat + 1.0
    return roundedUpFloat
# Expand bbox to nearest Degree
def get_Expanded_BBox_OneDegree_Returns_MaxXLong_MinXLong_MaxYLat_MinYLat(maxXLong, minXLong, maxYLat, minYLat):
    
    maxX = get_Float_RoundedUp(maxXLong)
    minX = get_Float_RoundedDown(minXLong)
    maxY = get_Float_RoundedUp(maxYLat)
    minY = get_Float_RoundedDown(minYLat)
    
    
    return maxX,minX,maxY,minY   

# Filter to make sure Lats and Longs are not the same value.
def filter_BBox_To_Avoid_SinglePoint_Selections(maxXLong, minXLong, maxYLat, minYLat):
    
    maxX = maxXLong
    minX = minXLong
    maxY = maxYLat
    minY = minYLat
    
    # Check Longitudes
    if(maxXLong == minXLong):
        # Now change the values
        maxX = (maxXLong + 1.0)
        minX = (minXLong - 1.0)
    # Check Latitudes
    if(maxYLat == minYLat):
        # Now change values
        maxY = (maxYLat + 1.0)
        minY = (minYLat - 1.0)
    
    return maxX,minX,maxY,minY

# Get MaxX(Long), MinX(Long), MaxY(Lat), MinY(Lat) from single Geometry Object (GDAL)
def get_MaxXLong_MinXLong_MaxYLat_MinYLat_From_Geometry(ogr_SinglePoly_Obj):

    bbox = ogr_SinglePoly_Obj.GetEnvelope()
    maxX = bbox[1]
    minX = bbox[0]
    maxY = bbox[3]
    minY = bbox[2]
    
    # Easier way to do this above
    #pts = ogr_SinglePoly_Obj
    #points = []
    #for p in range(pts.GetPointCount()):
    #    currentX_Long = pts.GetX(p)
    #    currentY_Lat = pts.GetY(p)
    #    if()
    
    return maxX,minX,maxY,minY  

# Convert single geometry into Polygon string into expanded simple square shaped bounding box polygon
def get_ClimateDataFiltered_PolygonString_FromSingleGeometry(theGeometry):
    #thePolygonString
    
    maxXLong, minXLong, maxYLat, minYLat = get_MaxXLong_MinXLong_MaxYLat_MinYLat_From_Geometry(theGeometry)
    maxXLong, minXLong, maxYLat, minYLat = get_Expanded_BBox_OneDegree_Returns_MaxXLong_MinXLong_MaxYLat_MinYLat(maxXLong, minXLong, maxYLat, minYLat)
    maxXLong, minXLong, maxYLat, minYLat = filter_BBox_To_Avoid_SinglePoint_Selections(maxXLong, minXLong, maxYLat, minYLat)
    
    #box_TL = [maxYLat,minXLong]
    #box_TR = [maxYLat,maxXLong]
    #box_BR = [minYLat,maxXLong]
    #box_BL = [minYLat,minXLong]
    box_TL = [minXLong, maxYLat] #[maxYLat,minXLong]
    box_TR = [maxXLong, maxYLat] #[maxYLat,maxXLong]
    box_BR = [maxXLong, minYLat] #[minYLat,maxXLong]
    box_BL = [minXLong, minYLat] #[minYLat,minXLong]
    
    cords = []
    
    
    # Assuming that the order in which the points exist matters here.
    # Clockwise
    #cords.append(box_TL)
    #cords.append(box_TR)
    #cords.append(box_BR)
    #cords.append(box_BL)
    
    # Counterclockwise
    cords.append(box_BL)
    cords.append(box_BR)
    cords.append(box_TR)
    cords.append(box_TL)
    
    
    # For somereason, the coords are wrapped into a containing list that has 1 element which is the other list.
    coordinates = []
    coordinates.append(cords)
    
    
    # Breaking down the return type...
    #singleCord =        #[11.1 , 22.2]            # [Long,Lat]
    #cords =             #[singleCord , singleCord]
    #coordinates =       #[ cords ]
    
    # Build the return object.
    retObj = {
        "type":"Polygon",
        "coordinates":coordinates
    }
    
    # Expected output format
        #{
        #    "type":"Polygon",
        #    "coordinates":
        #    [    
        #        [
        #            [61.34765625,19.16015625],[91.23046875,18.80859375],[91.23046875,4.39453125],[62.05078125,6.85546875],[61.34765625,19.16015625]
        #        ]
        #    ]
        #}
    retObj_JSON = json.dumps(retObj)
    
    return retObj_JSON #retObj

def get_ClimateDataFiltered_PolygonString_FromMultipleGeometries(theGeometries):
    
    # Default 0'd values
    # I think this is the bug!!!! (Setting these values to 0)
    # Fixing by setting to radically large and small numbers (way out of range).. (note the ranges)
    maxXLong = -999.0 #0
    minXLong = 999.0 #0
    maxYLat = -99.0 #0 
    minYLat = 99.0 #0
    
    # Foreach geometry found.
    #for poly in polygons:
    for poly in theGeometries:
        for i in range(0,poly.GetGeometryCount()):
            objToSend = poly.GetGeometryRef(i).GetGeometryRef(0)  # Not sure what this is about but it seems to work!!
            current_maxX_Long, current_minX_Long, current_maxY_Lat, current_minY_Lat = get_MaxXLong_MinXLong_MaxYLat_MinYLat_From_Geometry(objToSend)
            
            # Now just compare, if the current values are larger/smaller than their respective positions, change the value of the maxes and mins.. 
            # 4 if statements... more or less!
            if current_maxX_Long > maxXLong:
                maxXLong = current_maxX_Long
            if current_maxY_Lat > maxYLat:
                maxYLat = current_maxY_Lat
            if current_minX_Long < minXLong:
                minXLong = current_minX_Long
            if current_minY_Lat < minYLat:
                minYLat = current_minY_Lat
            
            
    # Build the poly (Just like in the single function
    maxXLong, minXLong, maxYLat, minYLat = get_Expanded_BBox_OneDegree_Returns_MaxXLong_MinXLong_MaxYLat_MinYLat(maxXLong, minXLong, maxYLat, minYLat)
    maxXLong, minXLong, maxYLat, minYLat = filter_BBox_To_Avoid_SinglePoint_Selections(maxXLong, minXLong, maxYLat, minYLat)
 
    #box_TL = [maxYLat,minXLong]
    #box_TR = [maxYLat,maxXLong]
    #box_BR = [minYLat,maxXLong]
    #box_BL = [minYLat,minXLong]
    box_TL = [minXLong, maxYLat] #[maxYLat,minXLong]
    box_TR = [maxXLong, maxYLat] #[maxYLat,maxXLong]
    box_BR = [maxXLong, minYLat] #[minYLat,maxXLong]
    box_BL = [minXLong, minYLat] #[minYLat,minXLong]
    
    
    cords = []
    
    # Assuming that the order in which the points exist matters here.
    # Clockwise
    #cords.append(box_TL)
    #cords.append(box_TR)
    #cords.append(box_BR)
    #cords.append(box_BL)
    
    # Counterclockwise
    cords.append(box_BL)
    cords.append(box_BR)
    cords.append(box_TR)
    cords.append(box_TL)
    cords.append(box_BL)    # Close the polygon
    
    # For somereason, the coords are wrapped into a containing list that has 1 element which is the other list.
    coordinates = []
    coordinates.append(cords)
    
    # Build the return object.
    retObj = {
        "type":"Polygon",
        "coordinates":coordinates
    }
    
    retObj_JSON = json.dumps(retObj)
    
    return retObj_JSON #retObj


# These needs have already been addressed with the combination of above functions!    
# Convert polygon to polygon that contains rounded points for climatedatasets
# Convert geometry to a geometry that contains rounded points for climatedatasets

# Bounds and Stitching...
# (-8, 10, 174, 184)





            
# End