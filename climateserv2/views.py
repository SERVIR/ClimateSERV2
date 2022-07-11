import json
import logging
import multiprocessing
import threading
import os
import subprocess
import time
from ast import literal_eval
from datetime import datetime
import pandas as pd
import xarray as xr
from django.apps import apps
from django.db import DatabaseError
from django.http import HttpResponse
from django.utils.datastructures import MultiValueDictKeyError
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
from django import db
from geoip2.errors import AddressNotFoundError
import climateserv2.requestLog as requestLog
from api.models import Track_Usage, ETL_Dataset
from api.models import Parameters
from .geoutils import decodeGeoJSON as decodeGeoJSON
from .processDataRequest import start_processing
from .processtools import uutools as uutools
from django.middleware.csrf import CsrfViewMiddleware
from .file import TDSExtraction
from django.contrib.gis.geoip2 import GeoIP2
from django.forms.models import model_to_dict
import random

Request_Log = apps.get_model('api', 'Request_Log')
Request_Progress = apps.get_model('api', 'Request_Progress')
exempt = -1
global_CONST_LogToken = "SomeRandomStringThatGoesHere"
logger = logging.getLogger("request_processor")
g = GeoIP2()


# To read a results file from the filesystem based on uuid
def read_results(uid):
    params = Parameters.objects.first()
    filename = params.resultsdir + uid + ".txt"
    try:
        f = open(filename, "r")
        x = json.load(f)
        f.close()
    except Exception as e:
        x = {"errMsg": "error"}
    return x


# To read progress from the database
def read_progress(uid):
    try:
        db.connections.close_all()
        return (Request_Progress.objects.get(request_id=str(uid))).progress
    except Exception as e:
        print(e)
        return "-1"


def get_id_from_output(output):
    try:
        output_json = json.loads(output)
        if "unique_id" in output_json:
            return output_json["unique_id"]
        elif "uid" in output_json:
            return output_json["uid"]
        elif "id" in output_json:
            return output_json["id"]
        else:
            return json.loads(output)[0]
    except (json.decoder.JSONDecodeError, IndexError):
        return uutools.getUUID()


# Creates the HTTP response loaded with the callback to allow javascript callback
def process_callback(request, output, content_type):
    db.connections.close_all()
    request_id = request.POST.get("id", request.GET.get("id", None))

    if request_id is None:
        request_id = get_id_from_output(output)

    callback = request.POST.get("callback", request.GET.get("callback"))
    if callback:
        http_response = HttpResponse(callback + "(" + output + ")", content_type=content_type)
    else:
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
    except (DatabaseError, Track_Usage.DoesNotExist) as e:
        error_msg = "ERROR saving usage object to database"
        logger.error(error_msg)
        logger.error(e)
    return http_response


# To get the request logs from a given date range
def get_log_requests_by_range(start_year, start_month, start_day, end_year, end_month, end_day):
    date_time_format = "%Y_%m_%d"
    date_time_early = datetime.strptime(str(start_year) + "_" + str(start_month) + "_" + str(start_day),
                                        date_time_format)
    date_time_late = datetime.strptime(str(end_year) + "_" + str(end_month) + "_" + str(end_day),
                                       date_time_format)

    ret_logs = requestLog.Request_Log.get_RequestData_ByRange(date_time_early, date_time_late)
    if len(ret_logs) > 0:
        error_msg = "ERROR get_LogRequests_ByRange: There was an error trying to get the logs."
        logger.error(error_msg)
    return ret_logs


# To get a list of all the parameter types
@csrf_exempt
def get_parameter_types(request):
    params = Parameters.objects.first()
    print("Getting Parameter Types")
    logger.info("Getting Parameter Types")
    return process_callback(request, json.dumps(params.parameters), "application/javascript")


def get_country_code(r):
    try:
        return g.country_code(get_client_ip(r))
    except AddressNotFoundError:
        return "ZZ"


# To get a list of shapefile feature types supported by the system
@csrf_exempt
def get_feature_layers(request):
    params = Parameters.objects.first()
    logger.info("Getting Feature Layers")
    track_usage = Track_Usage(unique_id=request.GET["id"], originating_IP=get_client_ip(request),
                              country_ISO=get_country_code(request),
                              time_requested=timezone.now(), request_type=request.method, status="Submitted",
                              progress=100, API_call="getFeatureLayers", data_retrieved=False
                              )

    track_usage.save()
    output = []
    for value in params.shapefileName:
        output.append({'id': value['id'], 'displayName': value['displayName'], 'visible': value['visible']})
    return process_callback(request, json.dumps(output), "application/javascript")


# To get the actual data from the processing request
@never_cache
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
@never_cache
@csrf_exempt
def get_data_request_progress(request):
    logger.debug("Getting Data Request Progress")
    lock = multiprocessing.Lock()
    try:
        lock.acquire()
        request_id = request.GET["id"]
        progress = read_progress(request_id)
        # if float(progress) > 0:
        #     track_usage = Track_Usage.objects.get(unique_id=request_id)
        #     track_usage.progress = progress
        #     track_usage.save()
        lock.release()
    except (Exception, OSError) as e:
        logger.warning("Problem with getDataRequestProgress: initial part" + str(request) + " " + str(e))
    try:
        logger.debug("Progress =" + str(progress))
        lock.acquire()
        if progress == -5:
            request_progress = Request_Progress(request_id=request_id, progress=100)
            request_progress.save()
            progress = -1
        elif float(progress) < 0:
            # decrement progress
            request_progress = Request_Progress(request_id=request_id, progress=float(progress) - 1)
            request_progress.save()
            logger.warning("Problem with getDataRequestProgress: " + str(request))
            progress = -1
        lock.release()
        return process_callback(request, json.dumps([float(progress)]), "application/json")
    except (Exception, OSError) as e:
        logger.warning("Problem with getDataRequestProgress: " + str(request) + " " + str(e))
        return process_callback(request, json.dumps([-1]), "application/json")


# To get the file for the completed Job ID
@never_cache
@csrf_exempt
def get_file_for_job_id(request):
    logger.debug("Getting File to download.")
    params = Parameters.objects.first()
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


# To get a list of all datatypes numbers by a custom property
def get_data_type_number_list_by_property(property_name, property_search_value):
    data_types = ETL_Dataset.objects.all()
    result_list = []
    for currentDataType in data_types:
        if currentDataType.ensemble != '':
            if str(currentDataType.ensemble).lower() == str(property_search_value).lower():
                if currentDataType.number:
                    result_list.append(currentDataType.number)

    return result_list


# To get a list of unique ensembles
def get_climate_ensemble_list():
    data_types = ETL_Dataset.objects.all()
    result_list = []
    for current_data_type in data_types:
        try:
            if current_data_type.ensemble != '':
                current_ensemble = current_data_type.ensemble
                result_list.append(current_ensemble)
        except:
            pass
    # Now remove duplicates from the list
    temp_set = set(result_list)
    result_list = list(temp_set)
    return result_list


# To get Climate Datatype Map (A list of objects that contains a unique ensemble
# name and a list of variables for that ensemble)
def get_climate_datatype_map():
    result_list = []
    # Get the list of ensembles
    ensemble_list = get_climate_ensemble_list()
    # Iterate through each ensemble
    for current_ensemble in ensemble_list:
        current_ensemble_datatype_numbers = get_data_type_number_list_by_property("ensemble", current_ensemble)
        print(current_ensemble_datatype_numbers)
        current_ensemble_object_list = []
        for current_ensemble_datatype_number in current_ensemble_datatype_numbers:
            ds = ETL_Dataset.objects.filter(number=int(current_ensemble_datatype_number))[0]
            current_variable = ds.dataset_nc4_variable_name
            current_ensemble_label = ""  # ds.ensemble_Label
            current_variable_label = ""  # ds.variable_Label
            # Create an object that maps the variable, ensemble with datatype number
            ensemble_variable_object = {
                "dataType_Number": current_ensemble_datatype_number,
                "climate_Variable": current_variable,
                "climate_Ensemble": current_ensemble,
                "climate_Ensemble_Label": current_ensemble_label,
                "climate_Variable_Label": current_variable_label
            }
            current_ensemble_object_list.append(ensemble_variable_object)

        # An object that connects the current ensemble to the list of objects that map the variable with datatype number
        current_ensemble_object = {
            "climate_Ensemble": current_ensemble,
            "climate_DataTypes": current_ensemble_object_list
        }
        result_list.append(current_ensemble_object)

    return result_list


# To get list of all climate change scenario info
@never_cache
@csrf_exempt
def get_climate_scenario_info(request):
    from_ui = bool(request.POST.get("is_from_ui", request.GET.get("is_from_ui", False)))

    # reason = CsrfViewMiddleware().process_view(request, None, (), {})
    # if not reason:
    #     from_ui = True
    unique_id = uutools.getUUID()
    try:
        track_usage = Track_Usage(unique_id=unique_id, originating_IP=get_client_ip(request)
                                  , country_ISO=get_country_code(request),
                                  dataset="climateScenarioInfo",
                                  time_requested=timezone.now(), request_type=request.method, status="Submitted",
                                  progress=100, API_call="getClimateScenarioInfo", data_retrieved=False,
                                  AOI=json.dumps({}), metadata_request=True, ui_request=from_ui
                                  )
        track_usage.save()
    except MultiValueDictKeyError:
        error_msg = "ERROR get_climate_scenario_info: There was an error trying to get the logs."
        logger.error(error_msg)
    try:
        nc_file = xr.open_dataset(
            '/mnt/climateserv/process_tmp/fast_nmme_monthly/nmme-mme_bcsd.latest.global.0.5deg.daily.nc4',
            chunks={'time': 16, 'longitude': 128,
                    'latitude': 128})  # /mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/

        start_date, end_date = TDSExtraction.get_date_range_from_nc_file(nc_file)
        is_error = False
        climate_model_datatype_capabilities_list = [
            {
                "current_Capabilities": {
                    "startDateTime": start_date,
                    "endDateTime": end_date
                }
            }
        ]
        climate_datatype_map = get_climate_datatype_map()
        api_return_object = {
            "unique_id": unique_id,
            "RequestName": "getClimateScenarioInfo",
            "climate_DatatypeMap": climate_datatype_map,
            "climate_DataTypeCapabilities": climate_model_datatype_capabilities_list,
            "isError": is_error
        }
    except:
        with open('/cserv2/django_app/ClimateSERV2/climateserv2/sample_climate_scenario.json', 'r') as climate_scenario:
            api_return_object = json.loads(climate_scenario.read())
            api_return_object["unique_id"] = unique_id
    return process_callback(request, json.dumps(api_return_object), "application/javascript")


def get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


@csrf_exempt
def run_etl(request):
    params = Parameters.objects.first()
    if request.method == 'POST':
        uuid = request.POST["uuid"]
        start_year = request.POST["start_year"]
        end_year = request.POST["end_year"]
        start_month = request.POST["start_month"]
        end_month = request.POST["end_month"]
        start_day = request.POST["start_day"]
        end_day = request.POST["end_day"]
        from_last_processed = request.POST["from_last_processed"]
        merge = request.POST["merge"]
        etl = request.POST["etl"]
        merge_option = "nomerge"
        if merge == "true":
            if (str(etl.lower()) in ['chirp', 'chirps_gefs']) or ("emodis" in etl.lower()):
                merge_option = "monthly"
            else:
                merge_option = "yearly"
        if from_last_processed == "true":
            if merge_option == "monthly":
                print("chirp should be here!")
                p = subprocess.Popen(
                    [params.pythonPath, "/cserv2/django_app/ClimateSERV2/manage.py", "start_etl_pipeline",
                     "--etl_dataset_uuid", str(uuid), "--from_last_processed", "--merge_monthly"])
                p.wait()
            elif merge_option == "yearly":
                print("processing yearly merge loop")
                p = subprocess.Popen(
                    [params.pythonPath, "/cserv2/django_app/ClimateSERV2/manage.py", "start_etl_pipeline",
                     "--etl_dataset_uuid", str(uuid), "--from_last_processed", "--merge_yearly"])
                p.wait()
            else:
                subprocess.call([params.pythonPath, "/cserv2/django_app/ClimateSERV2/manage.py", "start_etl_pipeline",
                                 "--etl_dataset_uuid", str(uuid), "--from_last_processed"])
        elif merge_option == "monthly":
            p1 = subprocess.Popen([params.pythonPath, "/cserv2/django_app/ClimateSERV2/manage.py", "start_etl_pipeline",
                                   "--etl_dataset_uuid", str(uuid),
                                   "--START_YEAR_YYY", start_year, "--END_YEAR_YYY", end_year, "--START_MONTH_MM",
                                   start_month, "--END_MONTH_MM", end_month, "--START_DAY_DD", start_day,
                                   "--END_DAY_DD", end_day, "--merge_monthly"])
            p1.wait()
        elif merge_option == "yearly":
            p1 = subprocess.Popen([params.pythonPath, "/cserv2/django_app/ClimateSERV2/manage.py", "start_etl_pipeline",
                                   "--etl_dataset_uuid", str(uuid),
                                   "--START_YEAR_YYY", start_year, "--END_YEAR_YYY", end_year, "--START_MONTH_MM",
                                   start_month, "--END_MONTH_MM", end_month, "--START_DAY_DD", start_day,
                                   "--END_DAY_DD", end_day, "--merge_yearly"])
            p1.wait()
        else:
            proc = subprocess.Popen(
                [params.pythonPath, "/cserv2/django_app/ClimateSERV2/manage.py", "start_etl_pipeline",
                 "--etl_dataset_uuid", str(uuid),
                 "--START_YEAR_YYY", start_year, "--END_YEAR_YYY", end_year, "--START_MONTH_MM",
                 start_month,
                 "--END_MONTH_MM", end_month, "--START_DAY_DD", start_day, "--END_DAY_DD", end_day])
            proc.wait()

    return "success"


# Submit a data request for processing
@never_cache
@csrf_exempt
def submit_data_request(request):
    params = Parameters.objects.first()
    logger.debug("Submitting Data Request")
    from_ui = bool(request.POST.get("is_from_ui", request.GET.get("is_from_ui", False)))

    # reason = CsrfViewMiddleware().process_view(request, None, (), {})
    # if not reason:
    #     from_ui = True

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
    operation_type = None

    datatype, begin_time, end_time, interval_type, error = validate_vars(request, error)
    calculation_string = request.POST.get("operationtype", request.GET.get("operationtype"))
    ps = literal_eval(params.parameters)
    calculation = ps[int(calculation_string)][2]
    # Get geometry from parameter or from shapefile
    geometry = None
    layer_id_request = request.POST.get("layerid", request.GET.get("layerid", None))
    if layer_id_request is not None:
        feature_ids_list = []
        try:
            layer_id = str(layer_id_request)
            feature_ids_list = get_feature_ids_list(request)
            feature_list = True
            logger.debug("submitDataRequest: Loaded feature ids, feature_ids_list: " + str(feature_ids_list))
        except KeyError:
            logger.warning("issue with finding geometry")
            error.append(
                "Error finding geometry: layerid:" + str(layer_id) + " feature_id: " + str(feature_ids_list))
    else:
        try:
            polygon_string = request.POST.get("geometry", request.GET.get("geometry", None))
            geometry = decodeGeoJSON(polygon_string)
        except KeyError:
            logger.warning("Problem with geometry")
            error.append("problem decoding geometry " + polygon_string)

        if geometry is None:
            logger.warning("Problem in that the geometry is a problem")
        else:
            logger.warning(geometry)
    try:
        operation_type = int(request.POST.get("operationtype", request.GET.get("operationtype", None)))
    except KeyError:
        logger.warning("issue with operation_type" + str(request))
        error.append("Error with operation_type")

    unique_id = uutools.getUUID()
    logger.info("Submitting " + unique_id)
    # Submit requests to the ipcluster service to get data

    if datatype == 35 or datatype == 36:
        aoi = json.dumps({"Admin Boundary": layer_id, "FeatureIds": feature_ids_list})
        track_usage = Track_Usage(unique_id=unique_id, originating_IP=get_client_ip(request),
                                  country_ISO=get_country_code(request),
                                  time_requested=timezone.now(), AOI=aoi,
                                  dataset=ETL_Dataset.objects.get(number=int(datatype)).dataset_name_format,
                                  start_date=pd.Timestamp(begin_time, tz='UTC'),
                                  end_date=pd.Timestamp(end_time, tz='UTC'),
                                  calculation=calculation, request_type=request.method,
                                  status="Complete", progress=-1, API_call="submitDataRequest",
                                  data_retrieved=False, ui_request=from_ui)

        track_usage.save()
        return process_callback(request, json.dumps([unique_id]), "application/json")

    elif len(error) == 0:
        # json_obj = {}
        dictionary = {'uniqueid': unique_id, 'datatype': datatype, 'begintime': begin_time, 'endtime': end_time,
                      'intervaltype': interval_type, 'operationtype': operation_type,
                      'originating_IP': get_client_ip(request), 'request_type':request.method}
        if feature_list:
            dictionary['layerid'] = layer_id
            dictionary['featureids'] = feature_ids_list
        else:
            dictionary['geometry'] = polygon_string
            try:
                if dictionary['geometry'].index('FeatureCollection') > -1:
                    print("all is well")
                    # json_obj = json.loads(dictionary['geometry'])
            except ValueError:
                dictionary['geometry'] = json.dumps({"type": "FeatureCollection",
                                                     "features": [
                                                         {"type": "Feature", "properties": {},
                                                          "geometry": json.loads(polygon_string)}]})

        dictionary["params"] = model_to_dict(params)
        # I used to use multiprocessing to start the multiprocessing, i think a thread is better
        # p = multiprocessing.Process(target=start_processing, args=(dictionary,))
        t = threading.Thread(target=start_processing, args=(dictionary,))
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
        try:

            track_usage = Track_Usage(unique_id=unique_id, originating_IP=get_client_ip(request),
                                      country_ISO=get_country_code(request),
                                      time_requested=timezone.now(), AOI=aoi,
                                      dataset=ETL_Dataset.objects.filter(number=int(datatype))[0].dataset_name_format,
                                      start_date=pd.Timestamp(begin_time, tz='UTC'),
                                      end_date=pd.Timestamp(end_time, tz='UTC'),
                                      calculation=calculation, request_type=request.method,
                                      status=status, progress=log_obj.progress, API_call="submitDataRequest",
                                      data_retrieved=False, ui_request=from_ui)

            track_usage.save()
        except Exception as e:
            logger.error("THIS IS THE ISSUE!")
            logger.error(str(e))
        t.setDaemon(True)
        t.start()
        # p.start()
        # rest_time = random.uniform(0.5, 1.5)
        # time.sleep(rest_time)
        return process_callback(request, str(json.dumps([unique_id])), "application/json")
    else:
        status = "Fail"
        if "geometry" in request.POST:
            aoi = request.POST['geometry']
        else:
            aoi = json.dumps({"Admin Boundary": layer_id, "FeatureIds": feature_ids_list})
        log_obj = requestLog.Request_Progress.objects.get(request_id=unique_id)
        logger.error("****FAIL********* ")
        track_usage = Track_Usage(unique_id=unique_id, originating_IP=get_client_ip(request),
                                  country_ISO=get_country_code(request),
                                  time_requested=timezone.now(), AOI=aoi,
                                  dataset=ETL_Dataset.objects.get(number=int(datatype)).dataset_name_format,
                                  start_date=pd.Timestamp(begin_time, tz='UTC'),
                                  end_date=pd.Timestamp(end_time, tz='UTC'),
                                  request_type=request.method, status=status, progress=log_obj.progress,
                                  API_call="submitDataRequest", data_retrieved=False, ui_request=from_ui)

        track_usage.save()

        # rest_time = random.uniform(0.5, 1.5)
        # time.sleep(rest_time)

        return process_callback(request, json.dumps(error), "application/json")


# To submit request for Monthly Analysis
@csrf_exempt
def submit_monthly_rainfall_analysis_request(request):
    custom_job_type = "MonthlyRainfallAnalysis"
    logger.info("Submitting Data Request for Monthly Rainfall Analysis")
    error = []
    seasonal_start_date = ""
    seasonal_end_date = ""
    feature_list = False
    feature_ids_list = []
    polygon_string = None
    layer_id = ""

    seasonal_start_date, seasonal_end_date = validate_seasonal_dates(request, error)

    # Get geometry from parameter Or extract from shapefile
    geometry = None
    layer_id_request = request.POST.get("layerid", request.GET.get("layerid", None))
    if layer_id_request is not None:
        feature_ids_list = []
        try:
            layer_id = str(layer_id_request)
            feature_ids_list = get_feature_ids_list(request)
            feature_list = True
            logger.debug("submitDataRequest: Loaded feature ids, feature_ids_list: " + str(feature_ids_list))
        except KeyError:
            logger.warning("issue with finding geometry")
            error.append(
                "Error finding geometry: layerid:" + str(layer_id) + " feature_id: " + str(feature_ids_list))
    else:
        try:
            polygon_string = request.POST.get("geometry", request.GET.get("geometry", None))
            geometry = decodeGeoJSON(polygon_string)
        except KeyError:
            logger.warning("Problem with geometry")
            error.append("problem decoding geometry " + polygon_string)

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
                json_geom = json.loads(str(dictionary['geometry']))
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
                              country_ISO=get_country_code(request),
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
        seasonal_start_date = str(request.POST.get("seasonal_start_date", request.GET.get("seasonal_start_date")))
        seasonal_end_date = str(request.POST.get("seasonal_end_date", request.GET.get("seasonal_end_date")))
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
    var = ""
    datatype = None
    begin_time = None
    end_time = None
    interval_type = None
    try:
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
