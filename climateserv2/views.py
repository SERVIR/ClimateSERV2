from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import json
import logging
import climateserv2.db.bddbprocessing as bdp
from . import parameters as params
from .processtools import uutools as uutools
from . import geoutils as decodeGeoJSON
import zmq
import dbm
global_CONST_LogToken = "SomeRandomStringThatGoesHere"
#logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-8s [%(process)d] %(message)s', )
logger = logging.getLogger(__name__)

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
    conn = bdp.BDDbConnector()

    value = conn.getProgress(uid)
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
        logger.warn("problem getting request data for id: " + str(request))
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
            logger.warn("Problem with getDataRequestProgress: " + str(request))
            return processCallBack(request, json.dumps([-1]), "application/json")
        else:
            return processCallBack(request, json.dumps([progress]), "application/json")
        ## return processCallBack(request,json.dumps([jsonresults['progress']]),"application/json")
    except (Exception, OSError) as e :
        logger.warn("Problem with getDataRequestProgress: " + str(request) + " " + str(e))
        return processCallBack(request, json.dumps([-1]), "application/json")

@csrf_exempt
def submitDataRequest(request):
    '''
    Submit a data request for processing
    :param request: actual request that contains the data needed to put together the request for
    processing
    '''
    print("from submit")
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
        print("from post")
        try:
            logger.debug("looking at getting datatype" + str(request))
            datatype = int(request.POST["datatype"])
        except KeyError:
            logger.warn("issue with datatype" + str(request))
            error.append("Datatype not supported")
        # Get begin and end times
        try:
            begintime = request.POST["begintime"]
        except KeyError:
            logger.warn("issue with begintime" + str(request))
            error.append("Error with begintime")
        try:
            endtime = request.POST["endtime"]
        except KeyError:
            logger.warn("issue with endtime" + str(request))
            error.append("Error with endtime")
        # Get interval type
        try:
            intervaltype = int(request.POST["intervaltype"])
        except KeyError:
            logger.warn("issue with intervaltype" + str(request))
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
                logger.warn("issue with finding geometry")
                error.append("Error with finding geometry: layerid:" + str(layerid) + " featureid: " + str(featureids))

        else:
            print('in elseee')
            try:
                polygonstring = request.POST["geometry"]
                geometry = decodeGeoJSON(polygonstring);
            # create geometry
            except KeyError:
                logger.warn("Problem with geometry")
                error.append("problem decoding geometry " + polygonstring)

            if geometry is None:
                logger.warn("Problem in that the geometry is a problem")
            else:
                logger.warn(geometry)
        try:
            operationtype = int(request.POST["operationtype"])
        except KeyError:
            logger.warn("issue with operationtype" + str(request))
            error.append("Error with operationtype")

    if request.method == 'GET':
        # Get datatype
        try:
            logger.debug("looking at getting datatype" + str(request))
            datatype = int(request.GET["datatype"])
        except KeyError:
            logger.warn("issue with datatype" + str(request))
            error.append("Datatype not supported")
        # Get begin and end times
        try:
            begintime = request.GET["begintime"]
        except KeyError:
            logger.warn("issue with begintime" + str(request))
            error.append("Error with begintime")
        try:
            endtime = request.GET["endtime"]
        except KeyError:
            logger.warn("issue with endtime" + str(request))
            error.append("Error with endtime")
        # Get interval type
        try:
            intervaltype = int(request.GET["intervaltype"])
        except KeyError:
            logger.warn("issue with intervaltype" + str(request))
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
                logger.warn("issue with finding geometry")
                error.append("Error with finding geometry: layerid:" + str(layerid) + " featureid: " + str(featureids))

        else:
            try:
                polygonstring = request.GET["geometry"]
                geometry = decodeGeoJSON(polygonstring);
            # create geometry
            except KeyError:
                logger.warn("Problem with geometry")
                error.append("problem decoding geometry ")

            if geometry is None:
                logger.warn("Problem in that the geometry is a problem")
            else:
                logger.warn(geometry)
        try:
            operationtype = int(request.GET["operationtype"])
        except KeyError:
            logger.warn("issue with operationtype" + str(request))
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
        sender.connect("inproc://D:/tmp") #sender.connect("ipc:///tmp/servir/Q1/input") (posix  - ipc, windows - inproc)
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
            logger.warn(
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
                logger.warn("issue with finding geometry")
                error.append("Error with finding geometry: layerid:" + str(layerid) + " featureid: " + str(featureids))

        else:
            try:
                polygonstring = request.POST["geometry"]
                geometry = decodeGeoJSON(polygonstring);
            # create geometry
            except KeyError:
                logger.warn("Problem with geometry")
                try:
                    error.append("problem decoding geometry " + polygonstring)
                except:
                    # Example Geometry param: {"type":"Polygon","coordinates":[[[24.521484374999996,19.642587534013032],[32.25585937500001,19.311143355064658],[32.25585937500001,14.944784875088374],[23.994140624999996,15.284185114076436],[24.521484374999996,19.642587534013032]]]}
                    example_geometry_param = '{"type":"Polygon","coordinates":[[[24.521484374999996,19.642587534013032],[32.25585937500001,19.311143355064658],[32.25585937500001,14.944784875088374],[23.994140624999996,15.284185114076436],[24.521484374999996,19.642587534013032]]]}'
                    error.append(
                        "problem decoding geometry.  Maybe missing param: 'geometry'.  Example of geometry param: " + example_geometry_param)

            if geometry is None:
                logger.warn("Problem in that the geometry is a problem")
            else:
                logger.warn(geometry)

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
                logger.warn("Problem with geometry")
                try:
                    error.append("problem decoding geometry " + polygonstring)
                except:
                    # Example Geometry param: {"type":"Polygon","coordinates":[[[24.521484374999996,19.642587534013032],[32.25585937500001,19.311143355064658],[32.25585937500001,14.944784875088374],[23.994140624999996,15.284185114076436],[24.521484374999996,19.642587534013032]]]}
                    example_geometry_param = '{"type":"Polygon","coordinates":[[[24.521484374999996,19.642587534013032],[32.25585937500001,19.311143355064658],[32.25585937500001,14.944784875088374],[23.994140624999996,15.284185114076436],[24.521484374999996,19.642587534013032]]]}'
                    error.append(
                        "problem decoding geometry.  Maybe missing param: 'geometry'.  Example of geometry param: " + example_geometry_param)

            if geometry is None:
                logger.warn("Problem in that the geometry is a problem")
            else:
                logger.warn(geometry)

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
