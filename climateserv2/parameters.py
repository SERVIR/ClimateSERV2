isDebug = True
isDev = False
import sys
import os

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

try:
    import climateserv2.parameters_ops as params
except:
    import parameters_ops as params

DEBUG_LIVE = params.DEBUG_LIVE
logToConsole = params.logToConsole
serviringestroot = params.serviringestroot
dbfilepath = params.dbfilepath
newdbfilepath = params.newdbfilepath
capabilities_db_filepath = params.capabilities_db_filepath
requestLog_db_basepath = params.requestLog_db_basepath
zipFile_ScratchWorkspace_Path = params.zipFile_ScratchWorkspace_Path
logfilepath = params.logfilepath
workpath = params.workpath
shapefilepath = params.shapefilepath
parameters = params.parameters
dataTypes = params.dataTypes
dataTypeInfo = params.dataTypeInfo
shapefileName = params.shapefileName
operationTypes = params.operationTypes
intervals = params.intervals
ageInDaysToPurgeData = params.ageInDaysToPurgeData
nmme_cfsv2_path=params.nmme_cfsv2_path
nmme_ccsm4_path=params.nmme_ccsm4_path
base_data_path=params.base_data_path
resultsdir = params.resultsdir
pythonPath = params.pythonPath
# To get the file name wth unique id
def getResultsFilename(id):
    filename = resultsdir + id + ".txt"
    return filename

# This allows quick slicing of the grid using indexes
def getFilename(dataType, year):
    return dataTypes[int(dataType)]['directory'] + str(year) + '.np'

# To get a list of all datatypes numbers by a custom property
def get_DataTypeNumber_List_By_Property(propertyName, propertySearchValue):
    resultList = []
    try:
        for currentDataType in params.dataTypes:
            try:
                current_PropValue = currentDataType[propertyName]
                if str(current_PropValue).lower() == str(propertySearchValue).lower():
                    if currentDataType['number']:
                        resultList.append(currentDataType['number'])
            except:
                pass
    except:
        pass
    return resultList

# To get a list of unique ensembles
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

# To get Climate Datatype Map (A list of objects that contains a unique ensemble name and a list of variables for that ensemble)
def get_Climate_DatatypeMap():
    resultList = []
    # Get the list of ensembles
    ensembleList = get_ClimateEnsemble_List()
    # Iterate through each ensemble
    for currentEnsemble in ensembleList:
        currentEnsemble_DataTypeNumbers = get_DataTypeNumber_List_By_Property("ensemble", currentEnsemble)
        currentEnsembleObject_List = []
        for currentEnsemble_DataTypeNumber in currentEnsemble_DataTypeNumbers:
            currentVariable = params.dataTypes[currentEnsemble_DataTypeNumber]['variable']
            currentEnsembleLabel = params.dataTypes[currentEnsemble_DataTypeNumber]['ensemble_Label']
            currentVariableLabel = params.dataTypes[currentEnsemble_DataTypeNumber]['variable_Label']
            # Create an object that maps the variable, ensemble with datatype number
            ensembleVariableObject = {
                "dataType_Number": currentEnsemble_DataTypeNumber,
                "climate_Variable": currentVariable,
                "climate_Ensemble": currentEnsemble,
                "climate_Ensemble_Label": currentEnsembleLabel,
                "climate_Variable_Label": currentVariableLabel
            }
            currentEnsembleObject_List.append(ensembleVariableObject)

        # An object that connects the current ensemble to the list of objects that map the variable with datatype number
        currentEnsembleObject = {
            "climate_Ensemble": currentEnsemble,
            "climate_DataTypes": currentEnsembleObject_List
        }
        resultList.append(currentEnsembleObject)

    return resultList