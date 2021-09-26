from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import json
import logging
from . import parameters as params
from .geoutils import decodeGeoJSON as decodeGeoJSON
from .processtools import uutools as uutools
import zmq
import climateserv2.requestLog as requestLog
import sys
import pandas as pd
import os
import xarray as xr
from datetime import datetime,timedelta
from django.apps import apps
Request_Log = apps.get_model('api', 'Request_Log')
Request_Progress = apps.get_model('api', 'Request_Progress')

global_CONST_LogToken = "SomeRandomStringThatGoesHere"
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
    try:
        res = Request_Progress.objects.get(request_id=str(uid))
        value=res.progress
    except Exception as e:
        print(e)
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

# get the request logs from a given date range
# Returns a list
# def get_LogRequests_ByRange(earliest_DateTime, latest_DateTime):
# Usage Example
# theLogs = get_LogRequests_ByRange("2015", "10", "01", "2015", "10", "03")
# logger.info("DEBUG: Get Request Logs result, : (str(theLogs)): " + str(theLogs))
def get_LogRequests_ByRange(sYear, sMonth, sDay, eYear, eMonth, eDay):
    # Parse start and end times into datetimes
    dateTimeFormat = "%Y_%m_%d"
    if (len(str(sMonth)) == 1):
        sMonth = "0" + str(sMonth)
    if (len(str(eMonth)) == 1):
        eMonth = "0" + str(eMonth)
    if (len(str(sDay)) == 1):
        sDay = "0" + str(sDay)
    if (len(str(eDay)) == 1):
        eDay = "0" + str(eDay)
    sDateTimeString = str(sYear) + "_" + str(sMonth) + "_" + str(sDay)
    eDateTimeString = str(eYear) + "_" + str(eMonth) + "_" + str(eDay)
    dateTime_Early = datetime.datetime.strptime(sDateTimeString, dateTimeFormat)
    dateTime_Late = datetime.datetime.strptime(eDateTimeString, dateTimeFormat)

    # Get the logs
    retLogs = []  # Scoping
    try:
        # rLog = reqLog.requestLog()
        retLogs = requestLog.get_RequestData_ByRange(dateTime_Early, dateTime_Late)
    except:
        e = sys.exc_info()[0]
        errorMsg = "ERROR get_LogRequests_ByRange: There was an error trying to get the logs.  System error message: " + str(
            e)
        logger.error(errorMsg)
    return retLogs

# Logging
@csrf_exempt
def getRequestLogs(request):
    '''
    Get a list of all request logs within a specified date range.
    :param request: in coming request, Need to pull the following params: sYear, sMonth, sDay, eYear, eMonth, eDay, tn
    returns a list wrapped in JSON string
    '''
    theLogs = []

    try:
        # get tn (token)
        # global global_CONST_LogToken
        request_Token = request.GET["tn"]
        if (request_Token == global_CONST_LogToken):
            sYear = request.GET["sYear"]
            sMonth = request.GET["sMonth"]
            sDay = request.GET["sDay"]
            eYear = request.GET["eYear"]
            eMonth = request.GET["eMonth"]
            eDay = request.GET["eDay"]
            theLogs = get_LogRequests_ByRange(sYear, sMonth, sDay, eYear, eMonth, eDay)

    except:
        retObj = {
            "error": "Error Processing getRequestLogs (This error message has been simplified for security reasons.  Please contact the website owner for more information)"
        }
        theLogs.append(retObj)

    return processCallBack(request, json.dumps(theLogs), "application/json")


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
    try:
        requestid = request.GET["id"]
        progress = readProgress(requestid)
        logger.debug("Progress =" + str(progress))
        if (progress == -1.0):
            logger.warning("Problem with getDataRequestProgress: " + str(request))
            return processCallBack(request, json.dumps([-1]), "application/json")
        else:
            return processCallBack(request, json.dumps([float(progress)]), "application/json")
    except (Exception, OSError) as e :
        logger.warning("Problem with getDataRequestProgress: " + str(request) + " " + str(e))
        return processCallBack(request, json.dumps([-1]), "application/json")

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
        if (float(progress) == 100.0):
            expectedFileLocation = ""  # Full path including filename
            expectedFileName = ""  # Just the file name
            ext=""
            try:
                ext="zip"
                expectedFileName = requestid + ".zip"
                expectedFileLocation = os.path.join(params.zipFile_ScratchWorkspace_Path, expectedFileName)
                doesFileExist = os.path.exists(expectedFileLocation)
                if not doesFileExist:
                    ext = "csv"
                    expectedFileName = requestid + ".csv"
                    expectedFileLocation = os.path.join(params.zipFile_ScratchWorkspace_Path, expectedFileName)
                    doesFileExist = os.path.exists(expectedFileLocation)
            except:
                    doesFileExist=False

            # Validate that a file actually exists where we say it is
            if (doesFileExist == True):
                # Open the file
                theFileToSend = open(expectedFileLocation, 'rb')
                if ext=="csv":
                    response = HttpResponse(theFileToSend, content_type='text/csv')
                else:
                    response = HttpResponse(theFileToSend, content_type='application/zip')
                response['Content-Disposition'] = 'attachment; filename=' + str(
                    expectedFileName)

                return response

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
    except Exception as e:
        return processCallBack(request, json.dumps(str(e) ), "application/json")

@csrf_exempt
def getClimateScenarioInfo(request):
    '''
    Get a list of all climate change scenario info (capabilities).
    :param request: in coming request, but don't need anything from the request.
    returns an object
    '''
    nc_file = xr.open_dataset('/mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens001.nc4') # /mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/
    start_date = nc_file["time"].values.min()
    t = pd.to_datetime(str(start_date))
    start_date = t.strftime('%Y-%m-%d')
    ed = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=180)
    end_date = ed.strftime('%Y-%m-%d')
    isError = False
    climateModel_DataType_Capabilities_List=[
        {
            "current_Capabilities":{
                "startDateTime":start_date,
                "endDateTime": end_date
            }
        }
    ]
    climate_DatatypeMap = params.get_Climate_DatatypeMap()

    api_ReturnObject = {
        "RequestName": "getClimateScenarioInfo",
        "climate_DatatypeMap": climate_DatatypeMap,
        "climate_DataTypeCapabilities": climateModel_DataType_Capabilities_List,
        "isError": isError
    }

    return processCallBack(request, json.dumps(api_ReturnObject), "application/javascript")

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
        sender.connect("ipc:///cserv2/tmp/servir/Q1/input") #sender.connect("ipc:///tmp/servir/Q1/input") (posix  - ipc, windows - inproc)
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
    custom_job_type = "MonthlyRainfallAnalysis"  # String used as a key in the head processor to identify this type of job.
    logger.info("Submitting Data Request for Monthly Rainfall Analysis")
    error = []

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
            logger.warning(
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
                logger.warning("issue with finding geometry")
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

            context = zmq.Context()
            sender = context.socket(zmq.PUSH)
            sender.connect("ipc:///cserv2/tmp/servir/Q1/input")
            sender.send_string(json.dumps(dictionary))

            return processCallBack(request, json.dumps([uniqueid]), "application/json")
        else:
            return processCallBack(request, json.dumps(error), "application/json")