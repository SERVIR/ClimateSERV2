from wsgiref.util import FileWrapper

from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import json
import logging
import climateserv2.db.DBMDbprocessing as dbmDb
from . import parameters as params
from .geoutils import decodeGeoJSON as decodeGeoJSON
from .processtools import uutools as uutools
import zmq
import sys
import os
global_CONST_LogToken = "SomeRandomStringThatGoesHere"
#logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-8s [%(process)d] %(message)s', )
logger = logging.getLogger("request_processor")

def readResults(uid):
    '''
    Read a results file from the filesystem based on uuid
    :param uid: unique identifier to find the correct result.
    :rtype: loaded json data from file
    '''
    filename = params.getResultsFilename(uid)
    f = open(filename, "r")
    x = json.load(f)
    f.close()
    f = None
    return x

def readProgress(uid):
    '''
    Read a progress file from the filesystem
    :param uid: unique identifier to find the correct result.
    :rtype: the progress associated with the unique id.
    '''
    conn = dbmDb.DBMConnector()
    try:
        value = conn.getProgress(uid)
    except Exception as e:
        print(str(e))
    conn.close()
    return value

def processCallBack(request, output, contenttype):
    '''
    Creates the HTTP response loaded with the callback to allow javascript callback. Even for
    Non-same origin output
    :param request: Given request that formulated the intial response
    :param output: dictinoary that contains the response
    :param contenttype: output mime type
    :rtype: response wrapped in call back.
    '''

    # All requests get pumped through this function, so using it as the entry point of the logging all requests
    # Log the Request
    # dataThatWasLogged = set_LogRequest(request, get_client_ip(request))

    if request.method == 'POST':
        try:
            callback = request.POST["callback"]
            return HttpResponse(callback + "(" + output + ")", content_type=contenttype)
        except KeyError:
            return HttpResponse(output)

    if request.method == 'GET':
        try:
            callback = request.GET["callback"]
            return HttpResponse(callback + "(" + output + ")", content_type=contenttype)
        except KeyError:
            return HttpResponse(output)

@csrf_exempt
def getParameterTypes(request):
    '''
    Get a list of all of the parameter types.
    :param request: in coming request, but don't need anything from the request.
    '''
    print("Getting Parameter Types")
    logger.info("Getting Parameter Types")
    #ip = get_client_ip(request)
    return processCallBack(request, json.dumps(params.parameters), "application/javascript")

@csrf_exempt
def getFeatureLayers(request):
    '''
    Get a list of shaoefile feature types supported by the system
    :param request: Given request that formulated the intial response
    :param output: dictinoary that contains the response
    :param contenttype: output mime type
    '''
    logger.info("Getting Feature Layers")
    output = []
    for value in params.shapefileName:
        output.append({'id': value['id'], 'displayName': value['displayName'], 'visible': value['visible']})
    return processCallBack(request, json.dumps(output), "application/javascript")

@csrf_exempt
def getDataFromRequest(request):
    '''
    Get the actual data from the processing request.
    :param request:
    '''
    logger.debug("Getting Data from Request")

    ## Need to encode the data into json and send it.
    try:
        requestid = request.GET["id"]
        logger.debug("Getting Data from Request " + requestid)
        jsonresults = readResults(requestid)
        return processCallBack(request, json.dumps(jsonresults), "application/json")
    except Exception as e:
        logger.warning("problem getting request data for id: " + str(request))
        return processCallBack(request, "need to send id", "application/json")

def intTryParse(value):
    """Function to try to parse an int from a string.
         If the value is not convertible it returns the orignal string and False
        :param value: Value to be convertedfrom CHIRPS.utils.processtools import uutools as uutools
        :rtype: Return integer and boolean to indicate that it was or wasn't decoded properly.
    """
    try:
        return int(value), True
    except ValueError:
        return value, False
@csrf_exempt
def getDataRequestProgress(request):
    '''
    Get feedback on the request as to the progress of the request. Will return the float percentage of progress
    :param request: contains the id of the request you want to look up.
    '''

    logger.debug("Getting Data Request Progress")
    # print "Request for progress on id ",requestid
    ###Check request status and then respond with the
    try:
        requestid = request.GET["id"]
        progress = readProgress(requestid)
        logger.debug("Progress =" + str(progress))
        if (progress == -1.0):
            logger.warning("Problem with getDataRequestProgress: " + str(request))
            return processCallBack(request, json.dumps([-1]), "application/json")
        else:
            return processCallBack(request, json.dumps([float(progress)]), "application/json")
        ## return processCallBack(request,json.dumps([jsonresults['progress']]),"application/json")
    except (Exception, OSError) as e :
        logger.warning("Problem with getDataRequestProgress: " + str(request) + " " + str(e))
        return processCallBack(request, json.dumps([-1]), "application/json")
# def read_All_Climate_Capabilities(dataTypeNumberList):
def read_DataType_Capabilities_For(dataTypeNumberList):
    '''
    Gets the capabilities from the bddb storage for a given list of DataType Numbers
    :param request: list of data type numbers
    :rtype: List of objects
    returnList : (List)
    returnList[n].dataTypeNumber : (int) Current datatype number
    returnList[n].current_Capabilities : (string) (JSON Stringified Object) Current capabilities object, intention is that they are stored as JSON strings
    '''
    retList = []

    try:
        # Create a connection to the bddb
        conn = dbmDb.BDDbConnector_Capabilities()

        # try and get data
        try:
            for currentDataTypeNumber in dataTypeNumberList:
                currentValue = conn.get_Capabilities(currentDataTypeNumber)
                appendObj = {
                    "dataTypeNumber": currentDataTypeNumber,
                    "current_Capabilities": currentValue
                }
                retList.append(appendObj)
        except Exception as e:
            logger.warning(
                "Error here indicates trouble accessing data or possibly getting an individual capabilities item: " + str(
                    e))
            pass

        # Close the bddb connection
        conn.close()
    except Exception as e:
        # Catch an error?
        # Error here indicates trouble connecting to the BD Database
        # If trouble connect
        logger.warning("Error here indicates trouble connecting to the BD Database: " + str(e))
        pass
    logger.warning("No error was found, look some place else")
    # return the list!
    return retList

# getFileForJobID
@csrf_exempt
def getFileForJobID(request):
    '''
    Get the file for the completed Job ID.  Will return a file download (if it exists)
    :param request: contains the id of the request you want to look up.
    '''

    logger.debug("Getting File to download.")

    try:
        requestid = request.GET["id"]
        progress = readProgress(requestid)

        # Validate that progress is at 100%
        if (progress == 100.0):

            doesFileExist = False

            expectedFileLocation = ""  # Full path including filename
            expectedFileName = ""  # Just the file name
            try:
                # Lets find the file
                path_To_Zip_MediumTerm_Storage = params.zipFile_ScratchWorkspace_Path
                expectedFileName = requestid + ".zip"
                expectedFileLocation = os.path.join(params.zipFile_ScratchWorkspace_Path, expectedFileName)
                doesFileExist = os.path.exists(expectedFileLocation)
            except:
                doesFileExist = False

            # Validate that a file actually exists where we say it is
            if (doesFileExist == True):

                # If the above validation checks out, return the file contents
                # theFile = FileWrapper(expectedFileLocation)
                # FileWrapper()
                # response = HttpResponse(wrapper, content_type='application/zip')
                # theFileWrapper = FileWrapper.File
                # Open the file
                theFileToSend = open(expectedFileLocation)
                theFileWrapper = FileWrapper(theFileToSend)
                response = HttpResponse(theFileWrapper, content_type='application/zip')
                response['Content-Disposition'] = 'attachment; filename=' + str(
                    expectedFileName)  # filename=myfile.zip'

                # Log the data
                #dataThatWasLogged = set_LogRequest(request, get_client_ip(request))

                return response

                # theFileData = "Return_ZipFile_Data_TODO"
                #
                ## TODO, Write file stream server.
                ## Log the Request (This is normally done in the processCallBack function... however we won't be using that pipe to serve the file stream.
                # dataThatWasLogged = set_LogRequest(request, get_client_ip(request))
                # return processCallBack(request,json.dumps("_PLACEHOLDER THIS SHOULD BE THE FILE AND NOT THIS MESSAGE!! _PLACEHOLDER"),"application/json")
                ##return processCallBack(request,json.dumps(theFileData),"application/json")

            else:
                # File did not exist        // "File does not exist on server. Was this jobID associated with a server job that produces output as a file?"
                return processCallBack(request, json.dumps(
                    "File does not exist on server.  There was an error generating this file during the server job"),
                                       "application/json")
        elif (progress == -1.0):
            # File is not finished being created.
            retObj = {
                "msg": "File Not found.  There was an error validating the job progress.  It is possible that this is an invalid job id.",
                "fileProgress": progress
            }
            return processCallBack(request, json.dumps(retObj), "application/json")
        else:
            # File is not finished being created.
            retObj = {
                "msg": "File still being built.",
                "fileProgress": progress
            }
            return processCallBack(request, json.dumps(retObj), "application/json")

    # except Exception as e:
    except Exception as e:
        e = sys.exc_info()[0]
        logger.warning("Problem with getFileForJobID: System Error Message: " + str(
            e))  # +str(request.GET)+" "+str(e.errno)+" "+str(e.strerror))
        return processCallBack(request, json.dumps("Error Getting File"), "application/json")


# ks refactor 2015 // New API Hook getClimateScenarioInfo
# Note: Each individual capabilities entry is already wrapped as a JSON String
#        This means that those elements need to be individually unwrapped in client code.
#        Here is an example in JavaScript
#    // JavaScript code that sends the api return into an object called 'apiReturnData'
#    var testCapabilities_JSONString = apiReturnData.climate_DataTypeCapabilities[1].current_Capabilities
#    testCapabilities_Unwrapped = JSON.parse(testCapabilities_JSONString)
#    // At this point, 'testCapabilities_Unwrapped' should be a fully unwrapped JavaScript object.
@csrf_exempt
def getClimateScenarioInfo(request):
    '''
    Get a list of all climate change scenario info (capabilities).
    :param request: in coming request, but don't need anything from the request.
    returns an object
    '''

    # Error Tracking
    isError = False
    print('print 1')
    # Get list of datatype numbers that have the category of 'ClimateModel'
    climateModel_DataTypeNumbers = params.get_DataTypeNumber_List_By_Property("data_category", "climatemodel")
    print('print 2')


    # Get all info from the Capabilities Data for each 'ClimateModel' datatype number
    climateModel_DataType_Capabilities_List = read_DataType_Capabilities_For(climateModel_DataTypeNumbers)
    print('print 3')


    # 'data_category':'ClimateModel'
    climate_DatatypeMap = params.get_Climate_DatatypeMap()
    print('print 4')


    api_ReturnObject = {
        "RequestName": "getClimateScenarioInfo",
        "climate_DatatypeMap": climate_DatatypeMap,
        "climate_DataTypeCapabilities": climateModel_DataType_Capabilities_List,
        "isError": isError
    }

    # ip = get_client_ip(request)
    # return processCallBack(request,json.dumps(params.ClientSide_ClimateChangeScenario_Specs),"application/json")
    return processCallBack(request, json.dumps(api_ReturnObject), "application/javascript")
    # TODO!! Change the above return statement to return the proper object that the client can use to determine which options are available specifically for climate change scenario types.

@csrf_exempt
def submitDataRequest(request):
    '''
    Submit a data request for processing
    :param request: actual request that contains the data needed to put together the request for
    processing
    '''
    logger.debug("Submitting Data Request")
    error = []
    polygonstring = None
    datatype = None
    begintime = None
    endtime = None
    intervaltype = None
    layerid = None

    if request.method == 'POST':
        # Get datatype

        try:
            logger.debug("looking at getting datatype" + str(request))
            datatype = int(request.POST["datatype"])
        except KeyError:
            logger.warning("issue with datatype" + str(request))
            error.append("Datatype not supported")
        # Get begin and end times
        try:
            begintime = request.POST["begintime"]
        except KeyError:
            logger.warning("issue with begintime" + str(request))
            error.append("Error with begintime")
        try:
            endtime = request.POST["endtime"]
        except KeyError:
            logger.warning("issue with endtime" + str(request))
            error.append("Error with endtime")
        # Get interval type
        try:
            intervaltype = int(request.POST["intervaltype"])
        except KeyError:
            logger.warning("issue with intervaltype" + str(request))
            error.append("Error with intervaltype")
        # Get geometry from parameter
        # Or extract from shapefile
        geometry = None
        featureList = False
        if request.POST.get("layerid") is not None:
            print(request.POST.get("layerid"))
            try:
                layerid = str(request.POST["layerid"])
                fids = str(request.POST["featureids"]).split(',')
                featureids = []
                for fid in fids:
                    value, isInt = intTryParse(fid)
                    if (isInt == True):
                        featureids.append(value)

                featureList = True
                ##Go get geometry

                logger.debug("submitDataRequest: Loaded feature ids, featureids: " + str(featureids))

            except KeyError:
                logger.warning("issue with finding geometry")
                error.append("Error with finding geometry: layerid:" + str(layerid) + " featureid: " + str(featureids))

        else:
            try:
                polygonstring = request.POST["geometry"]
                #geometry = decodeGeoJSON(polygonstring);
            # create geometry
            except KeyError:
                logger.warning("Problem with geometry")
                error.append("problem decoding geometry " + polygonstring)

            if geometry is None:
                logger.warning("Problem in that the geometry is a problem")
            else:
                logger.warning(geometry)
        try:
            operationtype = int(request.POST["operationtype"])
        except KeyError:
            logger.warning("issue with operationtype" + str(request))
            error.append("Error with operationtype")

    if request.method == 'GET':
        # Get datatype
        try:
            logger.debug("looking at getting datatype" + str(request))
            datatype = int(request.GET["datatype"])
        except KeyError:
            logger.warning("issue with datatype" + str(request))
            error.append("Datatype not supported")
        # Get begin and end times
        try:
            begintime = request.GET["begintime"]
        except KeyError:
            logger.warning("issue with begintime" + str(request))
            error.append("Error with begintime")
        try:
            endtime = request.GET["endtime"]
        except KeyError:
            logger.warning("issue with endtime" + str(request))
            error.append("Error with endtime")
        # Get interval type
        try:
            intervaltype = int(request.GET["intervaltype"])
        except KeyError:
            logger.warning("issue with intervaltype" + str(request))
            error.append("Error with intervaltype")
        # Get geometry from parameter
        # Or extract from shapefile
        geometry = None
        featureList = False;
        if "layerid" in request.GET:
            try:
                layerid = str(request.GET["layerid"])
                fids = str(request.GET["featureids"]).split(',')
                featureids = []
                for fid in fids:
                    value, isInt = intTryParse(fid)
                    if (isInt == True):
                        featureids.append(value)

                featureList = True
                ##Go get geometry

                logger.debug("submitDataRequest: Loaded feature ids, featureids: " + str(featureids))

            except KeyError:
                logger.warning("issue with finding geometry")
                error.append("Error with finding geometry: layerid:" + str(layerid) + " featureid: " + str(featureids))

        else:
            try:
                polygonstring = request.GET["geometry"]
                geometry = decodeGeoJSON(polygonstring);
            # create geometry
            except KeyError:
                logger.warning("Problem with geometry")
                error.append("problem decoding geometry ")

            if geometry is None:
                logger.warning("Problem in that the geometry is a problem")
            else:
                logger.warning(geometry)
        try:
            operationtype = int(request.GET["operationtype"])
        except KeyError:
            logger.warning("issue with operationtype" + str(request))
            error.append("Error with operationtype")

    uniqueid = uutools.getUUID()
    logger.info("Submitting " + uniqueid)
    # Submit requests to the ipcluster service to get data
    if (len(error) == 0):
        dictionary = {'uniqueid': uniqueid,
                      'datatype': datatype,
                      'begintime': begintime,
                      'endtime': endtime,
                      'intervaltype': intervaltype,
                      'operationtype': operationtype
                      }
        if (featureList == True):
            dictionary['layerid'] = layerid
            dictionary['featureids'] = featureids

        else:
            dictionary['geometry'] = polygonstring

        ##logger.info("submitting ",dictionary)
        context = zmq.Context()
        sender = context.socket(zmq.PUSH)
        sender.connect("ipc:///home/tethys/tmp/servir/Q1/input") #sender.connect("ipc:///tmp/servir/Q1/input") (posix  - ipc, windows - inproc)
        sender.send_string(json.dumps(dictionary))

        return processCallBack(request, json.dumps([uniqueid]), "application/json")
    else:
        return processCallBack(request, json.dumps(error), "application/json")


@csrf_exempt
def submitMonthlyRainfallAnalysisRequest(request):
    '''
    In short, this is very simillar to submitDataRequest, but it is for a new type of server job.
    The plan is to use the same machinery that submitDataRequest uses.
    [requested Submitted via ZMQ] - [Job is processed by new code, but still updates a flat DB] - [request for data still returns data but in the shape that the client side needs for this feature.]
    :param request:   layerid, featureids, geometry,
    :return:
    '''

    # COMPLETELY ISOLATED SETUP FOR MONTHLY ANALYSIS TYPES - ALL REQUESTS ARE MONTHLY ANALYSIS UNTIL I GET IT ALL FIXED AND WORKING RIGHT..
    # print("RIGHT NOW, ALL JOBS ARE: MonthlyRainfallAnalysis TYPES.  NEED TO FIX THIS WHEN I WRITE THE JAVASCRIPT/AJAX code on the client AND THE API RECEIVER CODE HERE ")

    # if(custom_job_type == "MonthlyRainfallAnalysis"):
    #     uniqueid = uutools.getUUID()
    #
    # # END, ISOLATED MONTHLY ANALYSIS CODE
    #
    #
    # # ORGINAL, EXISTING, WORKING CODE BELOW.. (pre MonthlyAnalysisFeature)

    custom_job_type = "MonthlyRainfallAnalysis"  # String used as a key in the head processor to identify this type of job.
    logger.info("Submitting Data Request for Monthly Rainfall Analysis")
    error = []

    # Seasonal Forecast Start/End Dates (Pulled in from client) (allows greater request flexibility
    # &seasonal_start_date=2017_05_01&seasonal_end_date=2017_10_28
    seasonal_start_date = ""
    seasonal_end_date = ""
    if request.method == 'POST':
        try:
            seasonal_start_date = str(request.POST["seasonal_start_date"])
            seasonal_end_date = str(request.POST["seasonal_end_date"])
            # Force to only accept 10 character string // validation/sec
            seasonal_start_date = seasonal_start_date[0:10]
            seasonal_end_date = seasonal_end_date[0:10]
        except KeyError:
            logger.warning(
                "issue with getting start and end dates for seasonal forecast.  Expecting something like this: &seasonal_start_date=2017_05_01&seasonal_end_date=2017_10_28")
            error.append(
                "Error with getting start and end dates for seasonal forecast.  Expecting something like this: &seasonal_start_date=2017_05_01&seasonal_end_date=2017_10_28")

        # Get geometry from parameter
        # Or extract from shapefile
        geometry = None
        featureList = False
        if "layerid" in request.POST:
            try:
                layerid = str(request.POST["layerid"])
                fids = str(request.POST["featureids"]).split(',')
                featureids = []
                for fid in fids:
                    value, isInt = intTryParse(fid)
                    if (isInt == True):
                        featureids.append(value)

                featureList = True
                ##Go get geometry

                logger.debug("getMonthlyRainfallAnalysis: Loaded feature ids, featureids: " + str(featureids))

            except KeyError:
                logger.warning("issue with finding geometry")
                error.append("Error with finding geometry: layerid:" + str(layerid) + " featureid: " + str(featureids))

        else:
            try:
                polygonstring = request.POST["geometry"]
                geometry = decodeGeoJSON(polygonstring);
            # create geometry
            except KeyError:
                logger.warning("Problem with geometry")
                try:
                    error.append("problem decoding geometry " + polygonstring)
                except:
                    # Example Geometry param: {"type":"Polygon","coordinates":[[[24.521484374999996,19.642587534013032],[32.25585937500001,19.311143355064658],[32.25585937500001,14.944784875088374],[23.994140624999996,15.284185114076436],[24.521484374999996,19.642587534013032]]]}
                    example_geometry_param = '{"type":"Polygon","coordinates":[[[24.521484374999996,19.642587534013032],[32.25585937500001,19.311143355064658],[32.25585937500001,14.944784875088374],[23.994140624999996,15.284185114076436],[24.521484374999996,19.642587534013032]]]}'
                    error.append(
                        "problem decoding geometry.  Maybe missing param: 'geometry'.  Example of geometry param: " + example_geometry_param)

            if geometry is None:
                logger.warning("Problem in that the geometry is a problem")
            else:
                logger.warning(geometry)

        uniqueid = uutools.getUUID()
        logger.info("Submitting (getMonthlyRainfallAnalysis) " + uniqueid)

        # Submit requests to the ipcluster service to get data
        if (len(error) == 0):
            dictionary = {'uniqueid': uniqueid,
                          'custom_job_type': custom_job_type,
                          'seasonal_start_date': seasonal_start_date,
                          'seasonal_end_date': seasonal_end_date
                          }
            if (featureList == True):
                dictionary['layerid'] = layerid
                dictionary['featureids'] = featureids
            else:
                dictionary['geometry'] = polygonstring

            ##logger.info("submitting ",dictionary)
            context = zmq.Context()
            sender = context.socket(zmq.PUSH)
            sender.connect("ipc:///tmp/servir/Q1/input")
            sender.send_string(json.dumps(dictionary))

            return processCallBack(request, json.dumps([uniqueid]), "application/json")
        else:
            return processCallBack(request, json.dumps(error), "application/json")

    if request.method == 'GET':
        try:
            seasonal_start_date = str(request.GET["seasonal_start_date"])
            seasonal_end_date = str(request.GET["seasonal_end_date"])
            # Force to only accept 10 character string // validation/sec
            seasonal_start_date = seasonal_start_date[0:10]
            seasonal_end_date = seasonal_end_date[0:10]
        except KeyError:
            logger.warn(
                "issue with getting start and end dates for seasonal forecast.  Expecting something like this: &seasonal_start_date=2017_05_01&seasonal_end_date=2017_10_28")
            error.append(
                "Error with getting start and end dates for seasonal forecast.  Expecting something like this: &seasonal_start_date=2017_05_01&seasonal_end_date=2017_10_28")

        # Get geometry from parameter
        # Or extract from shapefile
        geometry = None
        featureList = False
        if "layerid" in request.GET:
            try:
                layerid = str(request.GET["layerid"])
                fids = str(request.GET["featureids"]).split(',')
                featureids = []
                for fid in fids:
                    value, isInt = intTryParse(fid)
                    if (isInt == True):
                        featureids.append(value)

                featureList = True
                ##Go get geometry

                logger.debug("getMonthlyRainfallAnalysis: Loaded feature ids, featureids: " + str(featureids))

            except KeyError:
                logger.warn("issue with finding geometry")
                error.append("Error with finding geometry: layerid:" + str(layerid) + " featureid: " + str(featureids))

        else:
            try:
                polygonstring = request.GET["geometry"]
                geometry = decodeGeoJSON(polygonstring);
            # create geometry
            except KeyError:
                logger.warning("Problem with geometry")
                try:
                    error.append("problem decoding geometry " + polygonstring)
                except:
                    # Example Geometry param: {"type":"Polygon","coordinates":[[[24.521484374999996,19.642587534013032],[32.25585937500001,19.311143355064658],[32.25585937500001,14.944784875088374],[23.994140624999996,15.284185114076436],[24.521484374999996,19.642587534013032]]]}
                    example_geometry_param = '{"type":"Polygon","coordinates":[[[24.521484374999996,19.642587534013032],[32.25585937500001,19.311143355064658],[32.25585937500001,14.944784875088374],[23.994140624999996,15.284185114076436],[24.521484374999996,19.642587534013032]]]}'
                    error.append(
                        "problem decoding geometry.  Maybe missing param: 'geometry'.  Example of geometry param: " + example_geometry_param)

            if geometry is None:
                logger.warning("Problem in that the geometry is a problem")
            else:
                logger.warning(geometry)

        uniqueid = uutools.getUUID()
        logger.info("Submitting (getMonthlyRainfallAnalysis) " + uniqueid)

        # Submit requests to the ipcluster service to get data
        if (len(error) == 0):
            dictionary = {'uniqueid': uniqueid,
                          'custom_job_type': custom_job_type,
                          'seasonal_start_date': seasonal_start_date,
                          'seasonal_end_date': seasonal_end_date
                          }
            if (featureList == True):
                dictionary['layerid'] = layerid
                dictionary['featureids'] = featureids
            else:
                dictionary['geometry'] = polygonstring

            ##logger.info("submitting ",dictionary)
            context = zmq.Context()
            sender = context.socket(zmq.PUSH)
            sender.connect("ipc:///home/tethys/tmp/servir/Q1/input")
            sender.send_string(json.dumps(dictionary))

            return processCallBack(request, json.dumps([uniqueid]), "application/json")
        else:
            return processCallBack(request, json.dumps(error), "application/json")
