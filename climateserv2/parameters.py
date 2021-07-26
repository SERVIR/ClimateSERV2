'''
Created on Jun 3, 2014
@author: jeburks

Modified starting from Sept 2015
@author: Kris Stanton
'''
import socket

isDebug = True
isDev = False
# def checkDev():
#    if (socket.gethostname().startswith('chirps') == False):
#        return True
#    else :
#        return False
# isDev = checkDev()

# if (isDev == True):

# ks refactor 2015 // Alternative development configuration
# Add Alternative dev machines here
#    if ( (socket.gethostname().startswith('nswsmac-2315955') == True) or (socket.gethostname().startswith('nsstcpcp174485pcs.ndc.nasa.gov')) ):
#        import parameters_local_ks as params

#    else:
#        import parameters_local as params

# else:
from . import parameters_ops as params

DEBUG_LIVE = params.DEBUG_LIVE
logToConsole = params.logToConsole
serviringestroot = params.serviringestroot
maskstorage = params.maskstorage
dbfilepath = params.dbfilepath
newdbfilepath = params.newdbfilepath
capabilities_db_filepath = params.capabilities_db_filepath
requestLog_db_basepath = params.requestLog_db_basepath
zipFile_MediumTermStorage_Path = params.zipFile_MediumTermStorage_Path
zipFile_ScratchWorkspace_Path = params.zipFile_ScratchWorkspace_Path
logfilepath = params.logfilepath
workpath = params.workpath
shapefilepath = params.shapefilepath

# profileLocation = 'mycluster'
# profileClientLocation = None
parameters = params.parameters
dataTypes = params.dataTypes
dataTypeInfo = params.dataTypeInfo
shapefileName = params.shapefileName
operationTypes = params.operationTypes
intervals = params.intervals
ageInDaysToPurgeData = params.ageInDaysToPurgeData

resultsdir = params.resultsdir


# Climate Change Scenario Specific
# ks refactor 2015 // New Parameter Variables : New variable to match climate change variable names
# Climate_Variable_Names = params.Climate_Variable_Names

# ks refactor 2015 // New Parameter Variables : Object used by clientside code to assist in building client UI and client UI Validation.
# ClientSide_ClimateChangeScenario_Specs = params.ClientSide_ClimateChangeScenario_Specs


def getResultsFilename(id):
    '''

    :param id:
    '''
    filename = resultsdir + id + ".txt"
    return filename


##Regular grid. the grid x,y,day of year. But the day of the year assumes that each month has 31. This is done to make the grid regular
##All dates that are non represented are filled with fill value.
##This allows quick slicing of the grid using indexes
def getFilename(dataType, year):
    '''

    :param dataType:
    :param year:
    '''
    return dataTypes[int(dataType)]['directory'] + str(year) + '.np'


def getHDFFilename(dataType, year):
    '''

    :param dataType:
    :param year:
    '''
    return dataTypes[int(dataType)]['directory'] + str(year) + '.hdf'


def getFillValue(dataType):
    '''

    :param dataType:
    '''
    return dataTypes[int(dataType)]['fillValue']


def getGridDimension(dataType):
    '''

    :param dataType:
    '''
    return dataTypes[int(dataType)]['size']


def getMaskStorageName(uid):
    return maskstorage + uid + ".npy"


def getHMaskStorageName(uid):
    return maskstorage + uid + ".hdf"


# Get a list of all datatypesnumbers by their data_category property
def get_DataTypeNumber_List_By_DataCategory(dataCateogrySearchValue):
    resultList = []
    try:
        for currentDataType in params.dataTypes:
            try:
                current_DataCategory = currentDataType['data_category']
                if str(current_DataCategory).lower() == str(dataCateogrySearchValue).lower():
                    resultList.append(currentDataType['number'])
            except:
                # the current data type object probably is missing the data category?
                pass
    except:
        pass
    return resultList


# Get a list of all datatypesnumbers by a custom property    ("data_category", "climatemodel")
# Test :
# params.get_DataTypeNumber_List_By_Property("variable", "tref")
# params.get_DataTypeNumber_List_By_Property("data_category", "climatemodel")
def get_DataTypeNumber_List_By_Property(propertyName, propertySearchValue):
    resultList = []
    try:
        for currentDataType in params.dataTypes:
            try:
                current_PropValue = currentDataType[propertyName]
                if str(current_PropValue).lower() == str(propertySearchValue).lower():
                    resultList.append(currentDataType['number'])
            except:
                # Usually the datatype object is missing the property (not all datatypes have all properties)
                pass
    except:
        # Something in the whole loop broke
        pass
    return resultList


# Get a list of unique ensembles
def get_ClimateEnsemble_List():
    resultList = []
    try:
        for currentDataType in params.dataTypes:
            try:
                current_Ensemble = currentDataType['ensemble']
                resultList.append(current_Ensemble)
            except:
                pass
    except:
        pass
    # Now remove duplicates from the list
    tempSet = set(resultList)
    resultList = list(tempSet)
    return resultList


# Get a list of variables for an ensemble
# With the function below ( get_Climate_DatatypeMap ) There is really no need for this one.
# def get_ClimateVariable_List_For_Ensemble(ensembleValue):
#    resultList = []
#    return resultList


# Get Climate Datatype Map (A list of objects that contains a unique ensemble name and a list of variables for that ensemble)
# Current Version returns something that looks like this
# Returns object with these props
# obj : (List of objects)
# obj[n].climate_Ensemble : (string)
# obj[n].climate_DataTypes : (list of objects)
# obj[n].climate_DataTypes[m].climate_Ensemble : (string, should be == to obj[n].climate_Ensemble)
# obj[n].climate_DataTypes[m].climate_Variable : (string)
# obj[n].climate_DataTypes[m].dataType_Number : (int)
# obj[n].climate_DataTypes[m].OTHER_PROPERTIES : (Expand this function here to expose any other datatype properties the API/Client would need)
def get_Climate_DatatypeMap():
    resultList = []

    # Get the list of ensembles
    ensembleList = get_ClimateEnsemble_List()

    # Note: There is plenty of room for optimization here... probably don't need to
    # Iterate through each ensemble
    for currentEnsemble in ensembleList:
        currentEnsemble_DataTypeNumbers = get_DataTypeNumber_List_By_Property("ensemble", currentEnsemble)
        currentEnsembleObject_List = []
        for currentEnsemble_DataTypeNumber in currentEnsemble_DataTypeNumbers:
            currentVariable = params.dataTypes[currentEnsemble_DataTypeNumber]['variable']
            currentEnsembleLabel = params.dataTypes[currentEnsemble_DataTypeNumber]['ensemble_Label']
            currentVariableLabel = params.dataTypes[currentEnsemble_DataTypeNumber]['variable_Label']

            # obj[n].climate_DataTypes[m].OTHER_PROPERTIES : (Expand this function here to expose any other datatype properties the API/Client would need)

            # Create an object that maps the variable, ensemble with datatype number
            ensembleVariableObject = {
                "dataType_Number": currentEnsemble_DataTypeNumber,
                "climate_Variable": currentVariable,
                "climate_Ensemble": currentEnsemble,
                "climate_Ensemble_Label": currentEnsembleLabel,
                "climate_Variable_Label": currentVariableLabel
            }
            currentEnsembleObject_List.append(ensembleVariableObject)

        # Create an object that connects the current ensemble to the list of objects that map the variable with datatype number
        currentEnsembleObject = {
            "climate_Ensemble": currentEnsemble,
            "climate_DataTypes": currentEnsembleObject_List
        }
        resultList.append(currentEnsembleObject)

    return resultList


# This is environment independent (ops or local, doesn't matter)
# We have some of the data hardcoded in here as a fallback in
def get_HC_Fallback_ClimateCapabilities_Object():
    hardcoded_Capabilities_Object = {
        '11': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "prcp", "variable_Label": "Precipitation", "size": [720, 360], "name": "Climate Change Scenario: ens03 Precipitation", "description": "Climate Change Scenario: ens03 Variable: Precipitation", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens03"}',
        '24': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "tref", "variable_Label": "Temperature", "size": [720, 360], "name": "Climate Change Scenario: ens10 Temperature", "description": "Climate Change Scenario: ens10 Temperature", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens10"}',
        '13': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "prcp", "variable_Label": "Precipitation", "size": [720, 360], "name": "Climate Change Scenario: ens04 Precipitation", "description": "Climate Change Scenario: ens04 Precipitation", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens04"}',
        '12': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "tref", "variable_Label": "Temperature", "size": [720, 360], "name": "Climate Change Scenario: ens04 Temperature", "description": "Climate Change Scenario: ens04 Temperature", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens04"}',
        '15': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "prcp", "variable_Label": "Precipitation", "size": [720, 360], "name": "Climate Change Scenario: ens05 Precipitation", "description": "Climate Change Scenario: ens05 Precipitation", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens05"}',
        '14': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "tref", "variable_Label": "Temperature", "size": [720, 360], "name": "Climate Change Scenario: ens05 Temperature", "description": "Climate Change Scenario: ens05 Temperature", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens05"}',
        '17': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "prcp", "variable_Label": "Precipitation", "size": [720, 360], "name": "Climate Change Scenario: ens06 Precipitation", "description": "Climate Change Scenario: ens06 Precipitation", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens06"}',
        '23': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "prcp", "variable_Label": "Precipitation", "size": [720, 360], "name": "Climate Change Scenario: ens09 Precipitation", "description": "Climate Change Scenario: ens09 Precipitation", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens09"}',
        '19': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "prcp", "variable_Label": "Precipitation", "size": [720, 360], "name": "Climate Change Scenario: ens07 Precipitation", "description": "Climate Change Scenario: ens07 Precipitation", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens07"}',
        '18': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "tref", "variable_Label": "Temperature", "size": [720, 360], "name": "Climate Change Scenario: ens07 Temperature", "description": "Climate Change Scenario: ens07 Temperature", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens07"}',
        '16': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "tref", "variable_Label": "Temperature", "size": [720, 360], "name": "Climate Change Scenario: ens06 Temperature", "description": "Climate Change Scenario: ens06 Temperature", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens06"}',
        '22': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "tref", "variable_Label": "Temperature", "size": [720, 360], "name": "Climate Change Scenario: ens09 Temperature", "description": "Climate Change Scenario: ens09 Temperature", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens09"}',
        '21': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "prcp", "variable_Label": "Precipitation", "size": [720, 360], "name": "Climate Change Scenario: ens08 Precipitation", "description": "Climate Change Scenario: ens08 Precipitation", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens08"}',
        '20': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "tref", "variable_Label": "Temperature", "size": [720, 360], "name": "Climate Change Scenario: ens08 Temperature", "description": "Climate Change Scenario: ens08 Temperature", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens08"}',
        '25': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "prcp", "variable_Label": "Precipitation", "size": [720, 360], "name": "Climate Change Scenario: ens10 Precipitation", "description": "Climate Change Scenario: ens10 Precipitation", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens10"}',
        '7': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "prcp", "variable_Label": "Precipitation", "size": [720, 360], "name": "Climate Change Scenario: ens01 Precipitation", "description": "Climate Change Scenario: ens01 Precipitation", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens01"}',
        '6': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "tref", "variable_Label": "Temperature", "size": [720, 360], "name": "Climate Change Scenario: ens01 Temperature", "description": "Climate Change Scenario: ens01 Temperature", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens01"}',
        '9': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "prcp", "variable_Label": "Precipitation", "size": [720, 360], "name": "Climate Change Scenario: ens02 Precipitation", "description": "Climate Change Scenario: ens02 Precipitation", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens02"}',
        '8': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "tref", "variable_Label": "Temperature", "size": [720, 360], "name": "Climate Change Scenario: ens02 Temperature", "description": "Climate Change Scenario: ens02 Temperature", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens02"}',
        '10': '{"fillValue": -9999.0, "projection": "GEOGCS[\\"WGS 84\\",DATUM[\\"WGS_1984\\",SPHEROID[\\"WGS 84\\",6378137,298.257223563,AUTHORITY[\\"EPSG\\",\\"7030\\"]],AUTHORITY[\\"EPSG\\",\\"6326\\"]],PRIMEM[\\"Greenwich\\",0],UNIT[\\"degree\\",0.0174532925199433],AUTHORITY[\\"EPSG\\",\\"4326\\"]]", "startDateTime": "2015_10_01", "grid": [0.0, 0.5, 0.0, 90.0, 0.0, -0.5], "variable": "tref", "variable_Label": "Temperature", "size": [720, 360], "name": "Climate Change Scenario: ens03 Temperature", "description": "Climate Change Scenario: ens03 Temperature", "endDateTime": "2016_03_29", "date_FormatString_For_ForecastRange": "%Y_%m_%d", "data_category": "ClimateModel", "number_Of_ForecastDays": 180, "ensemble": "ens03"}'}
    return hardcoded_Capabilities_Object
#
#
# #print "Fill Value =",getFillValue(0)
# #print "filename =",getFilename(0,2010)
# #print "getGridDimension",getGridDimension(0)
# #print getFillValue(0)
