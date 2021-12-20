import json
import logging
import multiprocessing
import os
import socket
import sys
from datetime import datetime, timedelta

import pandas as pd
import xarray as xr
from django.apps import apps
from django.db import DatabaseError
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

import climateserv2.requestLog as requestLog
from api.models import Track_Usage
from . import parameters as params
from .geoutils import decodeGeoJSON as decodeGeoJSON
from .processDataRequest import start_processing
from .processtools import uutools as uutools
from django.middleware.csrf import CsrfViewMiddleware

Request_Log = apps.get_model('api', 'Request_Log')
Request_Progress = apps.get_model('api', 'Request_Progress')

global_CONST_LogToken = "SomeRandomStringThatGoesHere"
logger = logging.getLogger("request_processor")


# To read a results file from the filesystem based on uuid
def read_results(uid):
    filename = params.getResultsFilename(uid)
    f = open(filename, "r")
    x = json.load(f)
    f.close()
    return x


# To read progress from the database
def read_progress(uid):
    try:
        res = Request_Progress.objects.get(request_id=str(uid))
        return res.progress
    except Exception as e:
        print(e)
        return "-1"


# Creates the HTTP response loaded with the callback to allow javascript callback
def process_callback(request, output, content_type):
    http_response = None
    request_id = None
    if request.method == 'POST':
        try:
            if "id" in request.POST:
                request_id = request.POST["id"]
            else:
                request_id = json.loads(output)[0]
        except Exception:
            http_response = HttpResponse(output)
        try:
            callback = request.POST["callback"]
            http_response = HttpResponse(callback + "(" + output + ")", content_type=content_type)
        except KeyError:
            http_response = HttpResponse(output)
    if request.method == 'GET':
        request_id = request.GET["id"]
        try:
            callback = request.GET["callback"]
            http_response = HttpResponse(callback + "(" + output + ")", content_type=content_type)
        except KeyError:
            http_response = HttpResponse(output)

    try:
        if http_response.status_code == 200:
            track_usage = Track_Usage.objects.get(unique_id=request_id)
            track_usage.status = "Complete"
            track_usage.save()
        else:
            track_usage = Track_Usage.objects.get(unique_id=request_id)
            track_usage.status = "Fail"
            track_usage.save()
    except DatabaseError:
        pass
    except Exception:
        pass
    return http_response


# To get the request logs from a given date range
def get_log_requests_by_range(start_year, start_month, start_day, end_year, end_month, end_day):
    date_time_format = "%Y_%m_%d"
    # This should not be needed, after we confirm please remove
    # if len(str(start_month)) == 1:
    #     start_month = "0" + str(start_month)
    # if len(str(end_month)) == 1:
    #     end_month = "0" + str(end_month)
    # if len(str(start_day)) == 1:
    #     start_day = "0" + str(start_day)
    # if len(str(end_day)) == 1:
    #     end_day = "0" + str(end_day)
    # sDateTimeString = str(start_year) + "_" + str(start_month) + "_" + str(start_day)
    # eDateTimeString = str(end_year) + "_" + str(end_month) + "_" + str(end_day)
    date_time_early = datetime.datetime.strptime(str(start_year) + "_" + str(start_month) + "_" + str(start_day),
                                                 date_time_format)
    date_time_late = datetime.datetime.strptime(str(end_year) + "_" + str(end_month) + "_" + str(end_day),
                                                date_time_format)

    ret_logs = []
    try:
        ret_logs = requestLog.get_RequestData_ByRange(date_time_early, date_time_late)
    except:
        e = sys.exc_info()[0]
        error_msg = "ERROR get_LogRequests_ByRange: There was an error trying to get the logs.  System error message: " + str(
            e)
        logger.error(error_msg)
    return ret_logs


# To get a list of all request logs within a specified date range
@csrf_exempt
def getRequestLogs(request):
    theLogs = []
    try:
        request_Token = request.GET["tn"]
        if request_Token == global_CONST_LogToken:
            sYear = request.GET["sYear"]
            sMonth = request.GET["sMonth"]
            sDay = request.GET["sDay"]
            eYear = request.GET["eYear"]
            eMonth = request.GET["eMonth"]
            eDay = request.GET["eDay"]
            theLogs = get_log_requests_by_range(sYear, sMonth, sDay, eYear, eMonth, eDay)
    except:
        retObj = {
            "error": "Error Processing getRequestLogs (This error message has been simplified for security reasons.  "
                     "Please contact the website owner for more information) "
        }
        theLogs.append(retObj)
    return process_callback(request, json.dumps(theLogs), "application/json")


# To get a list of all of the parameter types
@csrf_exempt
def getParameterTypes(request):
    print("Getting Parameter Types")
    logger.info("Getting Parameter Types")
    return process_callback(request, json.dumps(params.parameters), "application/javascript")


# To get a list of shaoefile feature types supported by the system
@csrf_exempt
def getFeatureLayers(request):
    logger.info("Getting Feature Layers")
    track_usage = Track_Usage(unique_id=request.GET["id"], originating_IP=socket.gethostbyname(socket.gethostname()),
                              time_requested=datetime.now(), request_type=request.method, status="Submitted",
                              progress=100, API_call="getFeatureLayers", data_retrieved=False
                              )

    track_usage.save()
    output = []
    for value in params.shapefileName:
        output.append({'id': value['id'], 'displayName': value['displayName'], 'visible': value['visible']})
    return process_callback(request, json.dumps(output), "application/javascript")


# To get the actual data from the processing request
@csrf_exempt
def getDataFromRequest(request):
    logger.debug("Getting Data from Request")
    try:
        requestid = request.GET["id"]
        logger.debug("Getting Data from Request " + requestid)
        jsonresults = read_results(requestid)
        track_usage = Track_Usage.objects.get(unique_id=request.GET["id"])
        track_usage.data_retrieved = True
        track_usage.save()
        return process_callback(request, json.dumps(jsonresults), "application/json")
    except Exception as e:
        logger.warning("problem getting request data for id: " + str(request))
        track_usage = Track_Usage.objects.get(unique_id=request.GET["id"])
        track_usage.data_retrieved = False
        track_usage.save()
        return process_callback(request, "need to send id", "application/json")


# To parse an int from a string
def intTryParse(value):
    try:
        return int(value), True
    except ValueError:
        return value, False


# To get feedback on the request as to the progress of the request. Will return the float percentage of progress
@csrf_exempt
def getDataRequestProgress(request):
    logger.debug("Getting Data Request Progress")
    track_usage = Track_Usage.objects.get(unique_id=request.GET["id"])
    track_usage.progress = read_progress(request.GET["id"])
    track_usage.data_retrieved = True
    track_usage.save()
    try:
        requestid = request.GET["id"]
        progress = read_progress(requestid)

        logger.debug("Progress =" + str(progress))
        if progress == -1.0:
            logger.warning("Problem with getDataRequestProgress: " + str(request))
            return process_callback(request, json.dumps([-1]), "application/json")
        else:
            return process_callback(request, json.dumps([float(progress)]), "application/json")
    except (Exception, OSError) as e:
        logger.warning("Problem with getDataRequestProgress: " + str(request) + " " + str(e))
        return process_callback(request, json.dumps([-1]), "application/json")


# To get the file for the completed Job ID
@csrf_exempt
def getFileForJobID(request):
    logger.debug("Getting File to download.")

    try:
        requestid = request.GET["id"]
        progress = read_progress(requestid)
        # Validate that progress is at 100%
        if float(progress) == 100.0:
            track_usage = Track_Usage.objects.get(unique_id=request.GET["id"])
            track_usage.data_retrieved = True
            track_usage.save()
            expectedFileLocation = ""
            expectedFileName = ""
            ext = ""
            try:
                ext = "zip"
                expectedFileName = requestid + ".zip"
                expectedFileLocation = os.path.join(params.zipFile_ScratchWorkspace_Path, expectedFileName)
                doesFileExist = os.path.exists(expectedFileLocation)
                if not doesFileExist:
                    ext = "csv"
                    expectedFileName = requestid + ".csv"
                    expectedFileLocation = os.path.join(params.zipFile_ScratchWorkspace_Path, expectedFileName)
                    doesFileExist = os.path.exists(expectedFileLocation)
            except:
                doesFileExist = False

            if doesFileExist == True:
                track_usage = Track_Usage.objects.get(unique_id=requestid)
                track_usage.file_size = os.stat(expectedFileLocation).st_size
                track_usage.save()
                theFileToSend = open(expectedFileLocation, 'rb')
                if ext == "csv":
                    response = HttpResponse(theFileToSend, content_type='text/csv')
                else:
                    response = HttpResponse(theFileToSend, content_type='application/zip')
                response['Content-Disposition'] = 'attachment; filename=' + str(
                    expectedFileName)
                return response
            else:
                # File did not exist
                return process_callback(request, json.dumps(
                    "File does not exist on server.  There was an error generating this file during the server job"),
                                        "application/json")
        elif progress == -1.0:
            retObj = {
                "msg": "File Not found.  There was an error validating the job progress.  It is possible that this is "
                       "an invalid job id.",
                "fileProgress": progress
            }
            return process_callback(request, json.dumps(retObj), "application/json")
        else:
            retObj = {
                "msg": "File still being built.",
                "fileProgress": progress
            }
            return process_callback(request, json.dumps(retObj), "application/json")
    except Exception as e:
        return process_callback(request, json.dumps(str(e)), "application/json")


# To get list of all climate change scenario info
@csrf_exempt
def getClimateScenarioInfo(request):
    track_usage = Track_Usage.objects.get(unique_id=request.GET["id"])
    track_usage.progress = read_progress(request.GET["id"])
    track_usage.save()
    nc_file = xr.open_dataset(
        '/mnt/climateserv/process_tmp/fast_nmme_monthly/nmme-mme_bcsd.latest.global.0.5deg.daily.nc4',
        chunks={'time': 16, 'longitude': 128,
                'latitude': 128})  # /mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/

    start_date = nc_file["time"].values.min()
    t = pd.to_datetime(str(start_date))
    start_date = t.strftime('%Y-%m-%d')
    ed = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=180)
    end_date = ed.strftime('%Y-%m-%d')
    isError = False
    climateModel_DataType_Capabilities_List = [
        {
            "current_Capabilities": {
                "startDateTime": start_date,
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

    return process_callback(request, json.dumps(api_ReturnObject), "application/javascript")


# Submit a data request for processing
@csrf_exempt
def submitDataRequest(request):
    logger.debug("Submitting Data Request")
    from_ui = False
    reason = CsrfViewMiddleware().process_view(request, None, (), {})
    if not reason:
        from_ui = True

    error = []
    polygonstring = None
    datatype = None
    begintime = None
    endtime = None
    intervaltype = None
    layerid = None
    calculation = None

    if request.method == 'POST':
        try:
            logger.debug("looking at getting datatype" + str(request.POST["datatype"]))
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
        # Get geometry from parameter or from shapefile
        geometry = None
        featureList = False
        if request.POST.get("layerid") is not None:
            try:
                layerid = str(request.POST["layerid"])
                fids = str(request.POST["featureids"]).split(',')
                featureids = []
                for fid in fids:
                    value, isInt = intTryParse(fid)
                    if isInt == True:
                        featureids.append(value)

                featureList = True
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
        calculation = params.parameters[int(request.POST["operationtype"])][2]

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
        try:
            calculation =  operationtype = params.parameters[int(request.GET["operationtype"])][2]
        except KeyError:
            logger.warning("issue with operationtype" + str(request))
        # Get geometry from parameter Or extract from shapefile
        geometry = None
        featureList = False;
        if "layerid" in request.GET:
            try:
                layerid = str(request.GET["layerid"])
                fids = str(request.GET["featureids"]).split(',')
                featureids = []
                for fid in fids:
                    value, isInt = intTryParse(fid)
                    if isInt == True:
                        featureids.append(value)

                featureList = True
                logger.debug("submitDataRequest: Loaded feature ids, featureids: " + str(featureids))

            except KeyError:
                logger.warning("issue with finding geometry")
                error.append("Error with finding geometry: layerid:" + str(layerid) + " featureid: " + str(featureids))

        else:
            try:
                polygonstring = request.GET["geometry"]
                geometry = decodeGeoJSON(polygonstring)
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

    if len(error) == 0:
        dictionary = {'uniqueid': uniqueid,
                      'datatype': datatype,
                      'begintime': begintime,
                      'endtime': endtime,
                      'intervaltype': intervaltype,
                      'operationtype': operationtype
                      }
        if featureList == True:
            dictionary['layerid'] = layerid
            dictionary['featureids'] = featureids
        else:
            dictionary['geometry'] = polygonstring
            try:
                geom_str = dictionary['geometry']
                jsonn = json.loads(dictionary['geometry'])
                featuresexist = jsonn['features']
            except:
                dictionary['geometry'] = {"type": "FeatureCollection",
                                          "features": [{"type": "Feature", "properties": {}, "geometry": jsonn}]}

        # start multiprocessing here
        def print_my_results(my_results):
            print(my_results)
            logger.info("RESULTS " + str(my_results))

        # we could either start one process here from the dictionary
        # which in turn could split the work into yearly increments
        # then start a new process for each, or we could do the splitting here
        # and start all the processes, i think the first idea is better this
        # way we can give the user some progress feedback.  Inside of the
        # processes each of them would be able to update progress and when they
        # have updated it all the way to 100 we can merge their data and be ready for the
        # getDataFromRequest call where we could return it.

        p = multiprocessing.Process(target=start_processing, args=(dictionary,))
        log = Request_Progress(request_id=uniqueid, progress=0)
        log.save()
        logg = requestLog.Request_Progress.objects.get(request_id=uniqueid)
        if logg.progress == 100:
            status = "Success"
        else:
            status = "In Progress"
        if "geometry" in dictionary:
            aoi = dictionary['geometry']
        else:
            status = "In Progress"
            aoi = json.dumps({"Admin Boundary": layerid, "FeatureIds": featureids})
        track_usage = Track_Usage(unique_id=uniqueid, originating_IP=socket.gethostbyname(socket.gethostname()),
                                  time_requested=datetime.now(), AOI=aoi,
                                  dataset=params.dataTypes[int(datatype)]['name'],
                                  start_date=pd.to_datetime(begintime, format='%m/%d/%Y'),
                                  end_date=pd.to_datetime(endtime, format='%m/%d/%Y'),
                                  calculation=calculation, request_type=request.method,
                                  status=status, progress=logg.progress, API_call="submitDataRequest",
                                  data_retrieved=False, ui_request=from_ui)

        track_usage.save()
        p.start()

        return process_callback(request, json.dumps([uniqueid]), "application/json")
    else:
        status = "Fail"
        if "geometry" in request.POST:
            aoi = request.POST['geometry']
        else:
            aoi = json.dumps({"Admin Boundary": layerid, "FeatureIds": featureids})
        logg = requestLog.Request_Progress.objects.get(request_id=uniqueid)
        track_usage = Track_Usage(unique_id=uniqueid, originating_IP=socket.gethostbyname(socket.gethostname()),
                                  time_requested=datetime.now(), AOI=aoi,
                                  dataset=params.dataTypes[int(datatype)]['name'],
                                  start_date=pd.to_datetime(begintime, format='%m/%d/%Y'),
                                  end_date=pd.to_datetime(endtime, format='%m/%d/%Y'),
                                  request_type=request.method, status=status, progress=logg.progress,
                                  API_call="submitDataRequest", data_retrieved=False, ui_request=from_ui)

        track_usage.save()
        return process_callback(request, json.dumps(error), "application/json")


# To submit request for Monthly Analysis
@csrf_exempt
def submitMonthlyRainfallAnalysisRequest(request):
    custom_job_type = "MonthlyRainfallAnalysis"
    logger.info("Submitting Data Request for Monthly Rainfall Analysis")
    error = []
    seasonal_start_date = ""
    seasonal_end_date = ""
    if request.method == 'POST':
        try:
            seasonal_start_date = str(request.POST["seasonal_start_date"])
            seasonal_end_date = str(request.POST["seasonal_end_date"])
            seasonal_start_date = seasonal_start_date[0:10]
            seasonal_end_date = seasonal_end_date[0:10]
        except KeyError:
            logger.warning(
                "issue with getting start and end dates for seasonal forecast.  Expecting something like this: "
                "&seasonal_start_date=2017_05_01&seasonal_end_date=2017_10_28")
            error.append(
                "Error with getting start and end dates for seasonal forecast.  Expecting something like this: "
                "&seasonal_start_date=2017_05_01&seasonal_end_date=2017_10_28")

        # Get geometry from parameter Or extract from shapefile
        geometry = None
        featureList = False
        if "layerid" in request.POST:
            try:
                layerid = str(request.POST["layerid"])
                fids = str(request.POST["featureids"]).split(',')
                featureids = []
                for fid in fids:
                    value, isInt = intTryParse(fid)
                    if isInt == True:
                        featureids.append(value)
                featureList = True
                logger.debug("getMonthlyRainfallAnalysis: Loaded feature ids, featureids: " + str(featureids))

            except KeyError:
                logger.warning("issue with finding geometry")
                error.append("Error with finding geometry: layerid:" + str(layerid) + " featureid: " + str(featureids))
        else:
            try:
                polygonstring = request.POST["geometry"]
                geometry = decodeGeoJSON(polygonstring)
            # create geometry
            except KeyError:
                logger.warning("Problem with geometry")
                try:
                    error.append("problem decoding geometry " + polygonstring)
                except:
                    example_geometry_param = '{"type":"Polygon","coordinates":[[[24.521484374999996,' \
                                             '19.642587534013032],[32.25585937500001,19.311143355064658],' \
                                             '[32.25585937500001,14.944784875088374],[23.994140624999996,' \
                                             '15.284185114076436],[24.521484374999996,19.642587534013032]]]} '
                    error.append(
                        "problem decoding geometry.  "
                        "Maybe missing param: 'geometry'.  Example of geometry param: " + example_geometry_param)

            if geometry is None:
                logger.warning("Problem in that the geometry is a problem")
            else:
                logger.warning(geometry)

        uniqueid = uutools.getUUID()
        logger.info("Submitting (getMonthlyRainfallAnalysis) " + uniqueid)

        # Submit requests to the ipcluster service to get data
        if len(error) == 0:
            dictionary = {'uniqueid': uniqueid,
                          'custom_job_type': custom_job_type,
                          'seasonal_start_date': seasonal_start_date,
                          'seasonal_end_date': seasonal_end_date
                          }
            if featureList == True:
                dictionary['layerid'] = layerid
                dictionary['featureids'] = featureids
            else:
                dictionary['geometry'] = polygonstring
            return process_callback(request, json.dumps([uniqueid]), "application/json")
        else:
            return process_callback(request, json.dumps(error), "application/json")

    if request.method == 'GET':
        try:
            seasonal_start_date = str(request.GET["seasonal_start_date"])
            seasonal_end_date = str(request.GET["seasonal_end_date"])
            # Force to only accept 10 character string // validation/sec
            seasonal_start_date = seasonal_start_date[0:10]
            seasonal_end_date = seasonal_end_date[0:10]
        except KeyError:
            logger.warning(
                "issue with getting start and end dates for seasonal forecast.  Expecting something like this: "
                "&seasonal_start_date=2017_05_01&seasonal_end_date=2017_10_28")
            error.append(
                "Error with getting start and end dates for seasonal forecast.  Expecting something like this: "
                "&seasonal_start_date=2017_05_01&seasonal_end_date=2017_10_28")

        # Get geometry from parameter Or extract from shapefile
        geometry = None
        featureList = False
        if "layerid" in request.GET:
            try:
                layerid = str(request.GET["layerid"])
                fids = str(request.GET["featureids"]).split(',')
                featureids = []
                for fid in fids:
                    value, isInt = intTryParse(fid)
                    if isInt == True:
                        featureids.append(value)
                featureList = True
                logger.debug("getMonthlyRainfallAnalysis: Loaded feature ids, featureids: " + str(featureids))
            except KeyError:
                logger.warning("issue with finding geometry")
                error.append("Error with finding geometry: layerid:" + str(layerid) + " featureid: " + str(featureids))

        else:
            try:
                polygonstring = request.GET["geometry"]
                geometry = decodeGeoJSON(polygonstring)
            # create geometry
            except KeyError:
                logger.warning("Problem with geometry")
                try:
                    error.append("problem decoding geometry " + polygonstring)
                except:
                    example_geometry_param = '{"type":"Polygon","coordinates":[[[24.521484374999996,' \
                                             '19.642587534013032],[32.25585937500001,19.311143355064658],' \
                                             '[32.25585937500001,14.944784875088374],[23.994140624999996,' \
                                             '15.284185114076436],[24.521484374999996,19.642587534013032]]]} '
                    error.append(
                        "problem decoding geometry.  Maybe missing param: 'geometry'.  Example of geometry param: " + example_geometry_param)

            if geometry is None:
                logger.warning("Problem in that the geometry is a problem")
            else:
                logger.warning(geometry)

    uniqueid = uutools.getUUID()
    logger.info("Submitting (getMonthlyRainfallAnalysis) " + uniqueid)

    # Submit requests to the ipcluster service to get data
    if len(error) == 0:
        dictionary = {'uniqueid': uniqueid,
                      'custom_job_type': custom_job_type,
                      'seasonal_start_date': seasonal_start_date,
                      'seasonal_end_date': seasonal_end_date
                      }
        if featureList:
            dictionary['layerid'] = layerid
            dictionary['featureids'] = featureids
        else:
            dictionary['geometry'] = polygonstring
            try:
                geom_str = dictionary['geometry']
                jsonn = json.loads(dictionary['geometry'])
                featuresexist = jsonn['features']
            except:
                dictionary['geometry'] = {"type": "FeatureCollection",
                                          "features": [{"type": "Feature", "properties": {}, "geometry": jsonn}]}
        logger.info("Adding progress (getMonthlyRainfallAnalysis) " + uniqueid)

        log = Request_Progress(request_id=uniqueid, progress=0)
        logger.info("Added progress (getMonthlyRainfallAnalysis) " + uniqueid)

        log.save()

        p = multiprocessing.Process(target=start_processing, args=(dictionary,))

        logg = requestLog.Request_Progress.objects.get(request_id=uniqueid)
        if logg.progress == 100:
            status = "Success"
        else:
            status = "In Progress"

        if "geometry" in request.POST:
            aoi = request.POST['geometry']
        else:
            aoi = json.dumps({"Admin Boundary": layerid, "FeatureIds": featureids})
        track_usage = Track_Usage(unique_id=uniqueid, originating_IP=socket.gethostbyname(socket.gethostname()),
                                  time_requested=datetime.now(), AOI=aoi,
                                  dataset="MonthlyRainfallAnalysis",
                                  start_date=pd.to_datetime(seasonal_start_date, format='%Y-%m-%d'),
                                  end_date=pd.to_datetime(seasonal_end_date, format='%Y-%m-%d'),
                                  request_type=request.method,
                                  status=status, progress=logg.progress,
                                  API_call="submitMonthlyRainfallAnalysisRequest", data_retrieved=False)

        track_usage.save()
        p.start()
        return process_callback(request, json.dumps([uniqueid]), "application/json")
    else:
        status = "Fail"
        logg = requestLog.Request_Progress.objects.get(request_id=uniqueid)
        if "geometry" in request.POST:
            aoi = request.POST['geometry']
        else:
            aoi = json.dumps({"Admin Boundary": layerid, "FeatureIds": featureids})
        track_usage = Track_Usage(unique_id=uniqueid, originating_IP=socket.gethostbyname(socket.gethostname()),
                                  time_requested=datetime.now(),
                                  AOI=aoi, dataset="MonthlyRainfallAnalysis",
                                  start_date=pd.to_datetime(seasonal_start_date, format='%Y-%m-%d'),
                                  end_date=pd.to_datetime(seasonal_end_date, format='%Y-%m-%d'),
                                  request_type=request.method, status=status,
                                  progress=logg.progress, API_call="submitMonthlyRainfallAnalysisRequest",
                                  data_retrieved=False)

        track_usage.save()
        return process_callback(request, json.dumps(error), "application/json")
