import json
import logging
import multiprocessing
import os
import socket
import subprocess
from datetime import datetime, timedelta
import pandas as pd
import xarray as xr
from django.apps import apps
from django.db import DatabaseError
from django.http import HttpResponse
from django.utils.datastructures import MultiValueDictKeyError
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
import pytz
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
        except ValueError:
            error_msg = "ERROR loading json to get request_id"
            logger.error(error_msg)
        try:
            callback = request.POST["callback"]
            http_response = HttpResponse(callback + "(" + output + ")", content_type=content_type)
        except KeyError:
            http_response = HttpResponse(output)
    if request.method == 'GET':
        if "id" in request.GET:
            request_id = request.GET["id"]
        else:
            request_id = json.loads(output)[0]
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
        error_msg = "ERROR saving usage object to database"
        logger.error(error_msg)
    return http_response


# To get the request logs from a given date range
def get_log_requests_by_range(start_year, start_month, start_day, end_year, end_month, end_day):
    date_time_format = "%Y_%m_%d"
    date_time_early = datetime.strptime(str(start_year) + "_" + str(start_month) + "_" + str(start_day),
                                        date_time_format)
    date_time_late = datetime.strptime(str(end_year) + "_" + str(end_month) + "_" + str(end_day),
                                       date_time_format)

    ret_logs = requestLog.get_RequestData_ByRange(date_time_early, date_time_late)
    if len(ret_logs) > 0:
        error_msg = "ERROR get_LogRequests_ByRange: There was an error trying to get the logs."
        logger.error(error_msg)
    return ret_logs


# To get a list of all request logs within a specified date range
@csrf_exempt
def get_request_logs(request):
    the_logs = []
    try:
        request_token = request.GET["tn"]
        if request_token == global_CONST_LogToken:
            start_year = request.GET["sYear"]
            start_month = request.GET["sMonth"]
            start_day = request.GET["sDay"]
            end_year = request.GET["eYear"]
            end_month = request.GET["eMonth"]
            end_day = request.GET["eDay"]
            the_logs = get_log_requests_by_range(start_year, start_month, start_day, end_year, end_month, end_day)
    except MultiValueDictKeyError:
        ret_obj = {
            "error": "Error Processing getRequestLogs (This error message has been simplified for security reasons.  "
                     "Please contact the website owner for more information) "
        }
        the_logs.append(ret_obj)
    return process_callback(request, json.dumps(the_logs), "application/json")


# To get a list of all of the parameter types
@csrf_exempt
def get_parameter_types(request):
    print("Getting Parameter Types")
    logger.info("Getting Parameter Types")
    return process_callback(request, json.dumps(params.parameters), "application/javascript")


# To get a list of shaoefile feature types supported by the system
@csrf_exempt
def get_feature_layers(request):
    logger.info("Getting Feature Layers")
    track_usage = Track_Usage(unique_id=request.GET["id"], originating_IP=get_client_ip(request),
                              time_requested=timezone.now(), request_type=request.method, status="Submitted",
                              progress=100, API_call="getFeatureLayers", data_retrieved=False
                              )

    track_usage.save()
    output = []
    for value in params.shapefileName:
        output.append({'id': value['id'], 'displayName': value['displayName'], 'visible': value['visible']})
    return process_callback(request, json.dumps(output), "application/javascript")


# To get the actual data from the processing request
@csrf_exempt
def get_data_from_request(request):
    logger.debug("Getting Data from Request")
    try:
        request_id = request.GET["id"]
        logger.debug("Getting Data from Request " + request_id)
        json_results = read_results(request_id)
        track_usage = Track_Usage.objects.get(unique_id=request.GET["id"])
        track_usage.data_retrieved = True
        track_usage.save()
        return process_callback(request, json.dumps(json_results), "application/json")
    except DatabaseError:
        logger.warning("problem getting request data for id: " + str(request))
        track_usage = Track_Usage.objects.get(unique_id=request.GET["id"])
        track_usage.data_retrieved = False
        track_usage.save()
        return process_callback(request, "need to send id", "application/json")


# To parse an int from a string
def int_try_parse(value):
    try:
        return int(value), True
    except ValueError:
        return value, False


# To get feedback on the request as to the progress of the request. Will return the float percentage of progress
@csrf_exempt
def get_data_request_progress(request):
    logger.debug("Getting Data Request Progress")
    track_usage = Track_Usage.objects.get(unique_id=request.GET["id"])
    track_usage.progress = read_progress(request.GET["id"])
    track_usage.data_retrieved = True
    track_usage.save()
    try:
        request_id = request.GET["id"]
        progress = read_progress(request_id)

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
def get_file_for_job_id(request):
    logger.debug("Getting File to download.")

    try:
        request_id = request.GET["id"]
        progress = read_progress(request_id)
        # Validate that progress is at 100%
        if float(progress) == 100.0:
            track_usage = Track_Usage.objects.get(unique_id=request.GET["id"])
            track_usage.data_retrieved = True
            track_usage.save()
            expected_file_location = ""
            expected_file_name = ""
            ext = ""
            try:
                ext = "zip"
                expected_file_name = request_id + ".zip"
                expected_file_location = os.path.join(params.zipFile_ScratchWorkspace_Path, expected_file_name)
                does_file_exist = os.path.exists(expected_file_location)
                if not does_file_exist:
                    ext = "csv"
                    expected_file_name = request_id + ".csv"
                    expected_file_location = os.path.join(params.zipFile_ScratchWorkspace_Path, expected_file_name)
                    does_file_exist = os.path.exists(expected_file_location)
            except IOError:
                does_file_exist = False

            if does_file_exist:
                track_usage = Track_Usage.objects.get(unique_id=request_id)
                track_usage.file_size = os.stat(expected_file_location).st_size
                track_usage.save()
                the_file_to_send = open(expected_file_location, 'rb')
                if ext == "csv":
                    response = HttpResponse(the_file_to_send, content_type='text/csv')
                else:
                    response = HttpResponse(the_file_to_send, content_type='application/zip')
                response['Content-Disposition'] = 'attachment; filename=' + str(
                    expected_file_name)
                return response
            else:
                # File did not exist
                return process_callback(request, json.dumps(
                    "File does not exist on server.  There was an error generating this file during the server job"),
                                        "application/json")
        elif progress == -1.0:
            ret_obj = {
                "msg": "File Not found.  There was an error validating the job progress.  It is possible that this is "
                       "an invalid job id.",
                "fileProgress": progress
            }
            return process_callback(request, json.dumps(ret_obj), "application/json")
        else:
            ret_obj = {
                "msg": "File still being built.",
                "fileProgress": progress
            }
            return process_callback(request, json.dumps(ret_obj), "application/json")
    except Exception as e:
        return process_callback(request, json.dumps(str(e)), "application/json")


# To get list of all climate change scenario info
@csrf_exempt
def get_climate_scenario_info(request):
    try:
        unique_id = uutools.getUUID()
        track_usage = Track_Usage(unique_id=unique_id, originating_IP=get_client_ip(request),
                                  dataset="climateScenarioInfo",
                                  time_requested=timezone.now(), request_type=request.method, status="Submitted",
                                  progress=100, API_call="getClimateScenarioInfo", data_retrieved=False,
                                  AOI=json.dumps({})
                                  )
        track_usage.save()
    except MultiValueDictKeyError:
        error_msg = "ERROR get_climate_scenario_info: There was an error trying to get the logs."
        logger.error(error_msg)
    nc_file = xr.open_dataset(
        '/mnt/climateserv/process_tmp/fast_nmme_monthly/nmme-mme_bcsd.latest.global.0.5deg.daily.nc4',
        chunks={'time': 16, 'longitude': 128,
                'latitude': 128})  # /mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/

    start_date = nc_file["time"].values.min()
    t = pd.to_datetime(str(start_date))
    start_date = t.strftime('%Y-%m-%d')
    ed = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=180)
    end_date = ed.strftime('%Y-%m-%d')
    is_error = False
    climate_model_datatype_capabilities_list = [
        {
            "current_Capabilities": {
                "startDateTime": start_date,
                "endDateTime": end_date
            }
        }
    ]
    climate_datatype_map = params.get_Climate_DatatypeMap()
    api_return_object = {
        "unique_id": unique_id,
        "RequestName": "getClimateScenarioInfo",
        "climate_DatatypeMap": climate_datatype_map,
        "climate_DataTypeCapabilities": climate_model_datatype_capabilities_list,
        "isError": is_error
    }
    return process_callback(request, json.dumps(api_return_object), "application/javascript")


def get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


# Submit a data request for processing
@csrf_exempt
def submit_data_request(request):
    logger.debug("Submitting Data Request")
    from_ui = False
    reason = CsrfViewMiddleware().process_view(request, None, (), {})
    if not reason:
        from_ui = True

    error = []
    polygon_string = None
    datatype = None
    begin_time = None
    end_time = None
    interval_type = None
    layer_id = None
    calculation = None
    feature_ids_list = []
    feature_list = False

    if request.method == 'POST':
        datatype, begin_time, end_time, interval_type, error = validate_vars(request, error)
        # Get geometry from parameter or from shapefile
        geometry = None
        if request.POST.get("layerid") is not None:
            feature_ids_list = []
            try:
                layer_id = str(request.POST["layerid"])
                feature_ids_list = get_feature_ids_list(request)
                feature_list = True
                logger.debug("submitDataRequest: Loaded feature ids, feature_ids_list: " + str(feature_ids_list))
            except KeyError:
                logger.warning("issue with finding geometry")
                error.append(
                    "Error finding geometry: layerid:" + str(layer_id) + " feature_id: " + str(feature_ids_list))
        else:
            try:
                polygon_string = request.POST["geometry"]
            # create geometry
            except KeyError:
                logger.warning("Problem with geometry")
                error.append("problem decoding geometry " + polygon_string)

            if geometry is None:
                logger.warning("Problem in that the geometry is a problem")
            else:
                logger.warning(geometry)
        try:
            operation_type = int(request.POST["operationtype"])
        except KeyError:
            logger.warning("issue with operation_type" + str(request))
            error.append("Error with operation_type")
        calculation = params.parameters[int(request.POST["operationtype"])][2]

    if request.method == 'GET':
        datatype, begin_time, end_time, interval_type, error = validate_vars(request, error)
        try:
            calculation = operation_type = params.parameters[int(request.GET["operationtype"])][2]
        except KeyError:
            logger.warning("issue with operation_type" + str(request))

        # Get geometry from parameter Or extract from shapefile
        geometry = None
        if "layerid" in request.GET:
            try:
                layer_id = str(request.GET["layerid"])
                feature_ids_list = get_feature_ids_list(request)
                feature_list = True
                logger.debug("submitDataRequest: Loaded feature ids, featureids: " + str(feature_ids_list))

            except KeyError:
                logger.warning("issue with finding geometry")
                error.append(
                    "Error finding geometry: layerid:" + str(layer_id) + " feature_id: " + str(feature_ids_list))

        else:
            try:
                polygon_string = request.GET["geometry"]
                geometry = decodeGeoJSON(polygon_string)
            # create geometry
            except KeyError:
                logger.warning("Problem with geometry")
                error.append("problem decoding geometry ")

            if geometry is None:
                logger.warning("Problem in that the geometry is a problem")
            else:
                logger.warning(geometry)
        try:
            operation_type = int(request.GET["operationtype"])
        except KeyError:
            logger.warning("issue with operation_type" + str(request))
            error.append("Error with operation_type")

    unique_id = uutools.getUUID()
    logger.info("Submitting " + unique_id)
    # Submit requests to the ipcluster service to get data

    if len(error) == 0:
        json_obj = {}
        dictionary = {'uniqueid': unique_id,
                      'datatype': datatype,
                      'begintime': begin_time,
                      'endtime': end_time,
                      'intervaltype': interval_type,
                      'operationtype': operation_type
                      }
        if feature_list:
            dictionary['layerid'] = layer_id
            dictionary['featureids'] = feature_ids_list
        else:
            dictionary['geometry'] = polygon_string
            try:
                if dictionary['geometry'].index('FeatureCollection') > -1:
                    json_obj = json.loads(dictionary['geometry'])
            except ValueError:
                dictionary['geometry'] = json.dumps({"type": "FeatureCollection",
                                                     "features": [
                                                         {"type": "Feature", "properties": {},
                                                          "geometry": json.loads(polygon_string)}]})

        # start multiprocessing here

        # we could either start one process here from the dictionary
        # which in turn could split the work into yearly increments
        # then start a new process for each, or we could do the splitting here
        # and start all the processes, i think the first idea is better this
        # way we can give the user some progress feedback.  Inside of the
        # processes each of them would be able to update progress and when they
        # have updated it all the way to 100 we can merge their data and be ready for the
        # getDataFromRequest call where we could return it.

        p = multiprocessing.Process(target=start_processing, args=(dictionary,))
        log = Request_Progress(request_id=unique_id, progress=0)
        log.save()
        log_obj = requestLog.Request_Progress.objects.get(request_id=unique_id)
        if log_obj.progress == 100:
            status = "Success"
        else:
            status = "In Progress"
        if "geometry" in dictionary:
            aoi = dictionary['geometry']
        else:
            status = "In Progress"
            aoi = json.dumps({"Admin Boundary": layer_id, "FeatureIds": feature_ids_list})
        track_usage = Track_Usage(unique_id=unique_id, originating_IP=get_client_ip(request),
                                  time_requested=timezone.now(), AOI=aoi,
                                  dataset=params.dataTypes[int(datatype)]['name'],
                                  start_date=pd.Timestamp(begin_time, tz='UTC'),
                                  end_date=pd.Timestamp(end_time, tz='UTC'),
                                  calculation=calculation, request_type=request.method,
                                  status=status, progress=log_obj.progress, API_call="submitDataRequest",
                                  data_retrieved=False, ui_request=from_ui)

        track_usage.save()
        p.start()

        return process_callback(request, json.dumps([unique_id]), "application/json")
    else:
        status = "Fail"
        if "geometry" in request.POST:
            aoi = request.POST['geometry']
        else:
            aoi = json.dumps({"Admin Boundary": layer_id, "FeatureIds": feature_ids_list})
        log_obj = requestLog.Request_Progress.objects.get(request_id=unique_id)
        track_usage = Track_Usage(unique_id=unique_id, originating_IP=get_client_ip(request),
                                  time_requested=timezone.now(), AOI=aoi,
                                  dataset=params.dataTypes[int(datatype)]['name'],
                                  start_date=pd.Timestamp(begin_time, tz='UTC'),
                                  end_date=pd.Timestamp(end_time, tz='UTC'),
                                  request_type=request.method, status=status, progress=log_obj.progress,
                                  API_call="submitDataRequest", data_retrieved=False, ui_request=from_ui)

        track_usage.save()
        return process_callback(request, json.dumps(error), "application/json")


# To submit request for Monthly Analysis
@csrf_exempt
def submit_monthly_rainfall_analysis_request(request):
    custom_job_type = "MonthlyRainfallAnalysis"
    logger.info("Submitting Data Request for Monthly Rainfall Analysis")
    error = []
    seasonal_start_date = ""
    seasonal_end_date = ""
    geometry = None
    feature_list = False
    feature_ids_list = []
    polygon_string = None
    layer_id = ""
    if request.method == 'POST':
        seasonal_start_date, seasonal_end_date = validate_seasonal_dates(request, error)
        if "layerid" in request.POST:
            try:
                layer_id = str(request.POST["layerid"])
                feature_ids_list = get_feature_ids_list(request)
                feature_list = True
                logger.debug("getMonthlyRainfallAnalysis: Loaded feature ids, feature_ids: " + str(feature_ids_list))

            except KeyError:
                logger.warning("issue with finding geometry")
                error.append(
                    "Error with finding geometry: layer_id:" + str(layer_id) + " feature_id: " + str(feature_ids_list))
        else:
            try:
                polygon_string = request.POST["geometry"]
                geometry = decodeGeoJSON(polygon_string)
            # create geometry
            except KeyError:
                logger.warning("Problem with geometry")
                error.append("problem decoding geometry " + polygon_string)
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

        unique_id = uutools.getUUID()
        logger.info("Submitting (getMonthlyRainfallAnalysis) " + unique_id)

        # Submit requests to the ipcluster service to get data
        if len(error) == 0:
            dictionary = {'uniqueid': unique_id,
                          'custom_job_type': custom_job_type,
                          'seasonal_start_date': seasonal_start_date,
                          'seasonal_end_date': seasonal_end_date
                          }
            if feature_list:
                dictionary['layerid'] = layer_id
                dictionary['featureids'] = feature_ids_list
            else:
                dictionary['geometry'] = polygon_string
            return process_callback(request, json.dumps([unique_id]), "application/json")
        else:
            return process_callback(request, json.dumps(error), "application/json")

    if request.method == 'GET':
        seasonal_start_date, seasonal_end_date = validate_seasonal_dates(request, error)

        # Get geometry from parameter Or extract from shapefile
        geometry = None
        feature_list = False
        if "layerid" in request.GET:
            try:
                layer_id = str(request.GET["layerid"])
                feature_ids_list = get_feature_ids_list(request)
                feature_list = True
                logger.debug(
                    "getMonthlyRainfallAnalysis: Loaded feature ids, feature_ids_list: " + str(feature_ids_list))
            except KeyError:
                logger.warning("issue with finding geometry")
                error.append(
                    "Error finding geometry: layer_id:" + str(layer_id) + " feature_id: " + str(feature_ids_list))

        else:
            polygon_string = request.GET["geometry"]
            try:
                geometry = decodeGeoJSON(polygon_string)
            # create geometry
            except KeyError:
                logger.warning("Problem with geometry")
                error.append("problem decoding geometry " + polygon_string)
                example_geometry_param = '{"type":"Polygon","coordinates":[[[24.521484374999996,' \
                                         '19.642587534013032],[32.25585937500001,19.311143355064658],' \
                                         '[32.25585937500001,14.944784875088374],[23.994140624999996,' \
                                         '15.284185114076436],[24.521484374999996,19.642587534013032]]]} '
                error.append(
                    "problem decoding geometry. Maybe missing param: 'geometry'.  "
                    "Example of geometry param: " + example_geometry_param)

            if geometry is None:
                logger.warning("Problem in that the geometry is a problem")
            else:
                logger.warning(geometry)

    unique_id = uutools.getUUID()
    logger.info("Submitting (getMonthlyRainfallAnalysis) " + unique_id)

    # Submit requests to the ipcluster service to get data
    if len(error) == 0:
        json_geom = None
        dictionary = {'uniqueid': unique_id,
                      'custom_job_type': custom_job_type,
                      'seasonal_start_date': seasonal_start_date,
                      'seasonal_end_date': seasonal_end_date
                      }
        if feature_list:
            dictionary['layerid'] = layer_id
            dictionary['featureids'] = feature_ids_list
        else:
            dictionary['geometry'] = polygon_string
            try:
                json_geom = json.loads(dictionary['geometry'])
            except json.decoder.JSONDecodeError:
                dictionary['geometry'] = {"type": "FeatureCollection",
                                          "features": [{"type": "Feature", "properties": {}, "geometry": json_geom}]}
        logger.info("Adding progress (getMonthlyRainfallAnalysis) " + unique_id)

        log = Request_Progress(request_id=unique_id, progress=0)
        logger.info("Added progress (getMonthlyRainfallAnalysis) " + unique_id)

        log.save()

        p = multiprocessing.Process(target=start_processing, args=(dictionary,))

        log_obj = requestLog.Request_Progress.objects.get(request_id=unique_id)
        if log_obj.progress == 100:
            status = "Success"
        else:
            status = "In Progress"
        log_usage(request, layer_id, feature_ids_list, unique_id, seasonal_start_date, seasonal_end_date, status)
        p.start()
        return process_callback(request, json.dumps([unique_id]), "application/json")
    else:
        status = "Fail"
        log_usage(request, layer_id, feature_ids_list, unique_id, seasonal_start_date, seasonal_end_date, status)
        return process_callback(request, json.dumps(error), "application/json")


def log_usage(request, layer_id, featureids, uniqueid, seasonal_start_date, seasonal_end_date, status):
    logg = requestLog.Request_Progress.objects.get(request_id=uniqueid)
    if "geometry" in request.POST:
        aoi = request.POST['geometry']
    else:
        aoi = json.dumps({"Admin Boundary": layer_id, "FeatureIds": featureids})
    track_usage = Track_Usage(unique_id=uniqueid, originating_IP=get_client_ip(request),
                              time_requested=timezone.now(),
                              AOI=aoi, dataset="MonthlyRainfallAnalysis",
                              start_date=pd.Timestamp(seasonal_start_date, tz='UTC'),
                              end_date=pd.Timestamp(seasonal_end_date, tz='UTC'),
                              request_type=request.method, status=status,
                              progress=logg.progress, API_call="submitMonthlyRainfallAnalysisRequest",
                              data_retrieved=False)

    track_usage.save()


def validate_seasonal_dates(request, error):
    try:
        seasonal_start_date = str(request.POST["seasonal_start_date"])
        seasonal_end_date = str(request.POST["seasonal_end_date"])
        seasonal_start_date = seasonal_start_date[0:10]
        seasonal_end_date = seasonal_end_date[0:10]
        return seasonal_start_date, seasonal_end_date
    except KeyError:
        logger.warning(
            "issue with getting start and end dates for seasonal forecast.  Expecting something like this: "
            "&seasonal_start_date=2017_05_01&seasonal_end_date=2017_10_28")
        error.append(
            "Error with getting start and end dates for seasonal forecast.  Expecting something like this: "
            "&seasonal_start_date=2017_05_01&seasonal_end_date=2017_10_28")


def validate_vars(request, error):
    try:
        var = ""
        var = "datatype"
        datatype = int(request.GET["datatype"]) if request.method == 'GET' else int(request.POST["datatype"])
        var = "begintime"
        begin_time = request.GET["begintime"] if request.method == 'GET' else request.POST["begintime"]
        var = "endtime"
        end_time = request.GET["endtime"] if request.method == 'GET' else request.POST["endtime"]
        var = "intervaltype"
        interval_type = int(request.GET["intervaltype"]) if request.method == 'GET' \
            else int(request.POST["intervaltype"])
    except KeyError:
        logger.warning("issue with " + str(request))
        error.append("Error with " + var)
    return datatype, begin_time, end_time, interval_type, error


def get_feature_ids_list(request):
    if request.method == 'GET':
        feature_ids = str(request.GET["featureids"]).split(',')
    else:
        feature_ids = str(request.POST.get("featureids")).split(',')
    feature_ids_list = []
    for fid in feature_ids:
        value, is_int = int_try_parse(fid)
        if is_int:
            feature_ids_list.append(value)
    return feature_ids_list


def restart_climateserv(request):
    try:
        subprocess.call(['/bin/bash', '-i', '-c', "crestart"])
    except Exception as e:
        print(e)
