from __future__ import absolute_import, unicode_literals

import calendar
import concurrent.futures
import json
import logging
import os
import shutil
import sys
import time
import uuid
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from os.path import basename
from zipfile import ZipFile

import celery
import numpy as np
import pandas as pd
import xarray as xr
import billiard as multiprocessing
from celery import shared_task
from celery.utils.log import get_task_logger
from django import db
from django.utils import timezone
from django.db import IntegrityError, transaction

import climateserv2.file.TDSExtraction as GetTDSData
import climateserv2.file.dateutils as dateutils
import climateserv2.geo.shapefile.readShapesfromFiles as sF
from api.models import Parameters as realParams
from api.models import Request_Progress
from api.models import Track_Usage  # , ETL_Dataset
import climateserv2.locallog.locallogging as llog

logger = get_task_logger('climateserv2.processDataRequest')
# logger.level = logging.DEBUG

dataTypes = None
params = realParams.objects.first()


def set_progress_to_100(uniqueid):
    try:
        with transaction.atomic():
            request_progress = Request_Progress.objects.get(request_id=uniqueid)
            request_progress.progress = 100
            request_progress.save()
    except IntegrityError as e:
        logger.error("Progress update issue, unable to set to 100%: " + str(e))


@shared_task(time_limit=3000)
def start_processing(statistical_query):
    merged_obj = None
    logger.debug("celery.current_task: " + str(celery.current_task.request.id))
    uniqueid = "Not assigned yet"
    operationtype = "None"
    try:
        date_range_list = []
        uniqueid = str(celery.current_task.request.id)
        logger.info("start_processing has begun for: " + uniqueid)
        operationtype = ""
        if 'geometry' in statistical_query:
            polygon_string = statistical_query["geometry"]
            logger.error("I had geometry in query")
        elif 'layerid' in statistical_query:
            polygon_string = sF.get_polygons(statistical_query['layerid'], statistical_query['featureids'])
            # logger.debug("poly string from feature(s) selected")
        else:
            raise Exception("Missing polygon_string")

        jobs = []
        if ('custom_job_type' in statistical_query.keys() and
                statistical_query['custom_job_type'] == 'MonthlyRainfallAnalysis'):
            operationtype = "Rainfall"
            dates, months, bounds = GetTDSData.get_monthly_analysis_dates_bounds(polygon_string)

            jobs.append({
                "uniqueid": uniqueid,
                "id": str(uuid.uuid4()),
                "bounds": bounds,
                "dates": dates,
                "months": months,
                "subtype": "chirps"})

            jobs.append({
                "uniqueid": uniqueid,
                "id": str(uuid.uuid4()),
                "bounds": bounds,
                "dates": dates,
                "months": months,
                "subtype": "nmme"})
        else:
            # here calculate the years and create a list of jobs
            logger.info("Regular query has been initiated for: " + uniqueid)
            operationtype = statistical_query["operationtype"]
            datatype = statistical_query['datatype']
            begin_time = statistical_query['begintime']
            end_time = statistical_query['endtime']
            first_date = datetime.strptime(begin_time, '%m/%d/%Y')
            first_date_string = datetime.strftime(first_date, '%Y-%m-%d')
            last_date = datetime.strptime(end_time, '%m/%d/%Y')
            last_date_string = datetime.strftime(last_date, '%Y-%m-%d')
            if first_date.year == last_date.year:
                date_range_list.append([first_date_string, last_date_string])
            else:
                years = [int(i.strftime("%Y")) for i in pd.date_range(start=begin_time, end=end_time, freq='MS')]
                year_list = np.unique(years)
                start_year = year_list[0]
                end_year = year_list[len(year_list) - 1]
                for year in year_list:
                    if year == start_year:
                        first_date_string = datetime.strftime(first_date, '%Y-%m-%d')
                        last_day = first_date.replace(month=12, day=31)
                        last_date_string = datetime.strftime(last_day, '%Y-%m-%d')
                    elif year == end_year:
                        first_day = last_date.replace(month=1, day=1)
                        first_date_string = datetime.strftime(first_day, '%Y-%m-%d')
                        last_date_string = datetime.strftime(last_date, '%Y-%m-%d')
                    else:
                        first_date_string = str(year) + "-01-01"
                        last_date_string = str(year) + "-12-31"
                    date_range_list.append([first_date_string, last_date_string])
            counter = 0
            # this breaks if data doesn't exist
            for dates in date_range_list:
                file_list, variable = GetTDSData.get_filelist(datatype, dates[0], dates[1])
                counter += 1
                if len(file_list) > 0:
                    jobs.append({
                        "uniqueid": uniqueid,
                        "id": str(uuid.uuid4()),
                        "start_date": dates[0],
                        "end_date": dates[1],
                        "variable": variable,
                        "geom": polygon_string,
                        "operation": literal_eval(params.parameters)[statistical_query["operationtype"]][1],
                        "file_list": file_list,
                        "subtype": None
                    })

        split_obj = []
        rest_time = len(jobs)
        for job in jobs:
            job['job_length'] = len(jobs)


            # split_obj.append((pool.apply_async(start_worker_process, args=[job], )).get())
            # To revert from multiprocessing, comment out line above and
            # uncomment the below lines.
            if len(jobs) > 3:
                split_obj.append(start_worker_process(job))
            #     syncronously
            else:
                with ThreadPoolExecutor(max_workers=None) as executor:
                    my_results = {executor.submit(start_worker_process, job)}

                    for _ in concurrent.futures.as_completed(my_results):
                        split_obj.append(_.result())
            # rest_time = rest_time - 1
            # if rest_time > 0:
            #     logger.debug("Sleeping for: " + str(rest_time))
            #     time.sleep(rest_time)

        if ('custom_job_type' in statistical_query.keys() and
                statistical_query['custom_job_type'] == 'MonthlyRainfallAnalysis'):
            opn = "avg"
            result_list = []
            for obj in split_obj:
                subtype = obj["subtype"]
                for dateIndex in range(len(obj["dates"])):
                    work_dict = {'uid': uniqueid, 'datatype_uuid_for_CHIRPS': str(uuid.uuid4()),
                                 'datatype_uuid_for_SeasonalForecast': str(uuid.uuid4()), 'sub_type_name': subtype,
                                 'derived_product': True, 'special_type': 'MonthlyRainfallAnalysis',
                                 "date": obj["dates"][dateIndex]}
                    if subtype == "chirps":
                        work_dict["value"] = {opn: [
                            obj["values"][dateIndex],
                            np.float64(obj["LTA"][dateIndex])
                        ]}
                    if subtype == "nmme":
                        work_dict['value'] = {opn: np.float64(obj["values"][dateIndex])}

                    result_list.append(work_dict)
            merged_obj = {"MonthlyAnalysisOutput": get_output_for_monthly_rainfall_analysis_from(result_list)}

        else:
            try:
                dates = []
                values = []
                nan = []
                for obj in split_obj:
                    try:
                        dates.extend(obj["dates"])
                        values.extend(obj["values"])
                        nan.extend(obj["NaN"])
                    except Exception as e:
                        logger.error("making result list failed: " + str(e))
                datatype = statistical_query['datatype']
                polygon_str_to_pass = polygon_string
                intervaltype = statistical_query['intervaltype']
                operationtype = statistical_query['operationtype']
                intervals = [
                    {'name': 'day', 'pattern': '%m/%d/%Y'},
                    {'name': 'month', 'pattern': '%m/%Y'},
                    {'name': 'year', 'pattern': '%Y'}
                ]
                opn = literal_eval(params.parameters)[operationtype][1]
                result_list = []
                for dateIndex in range(len(dates)):
                    gmt_midnight = calendar.timegm(
                        time.strptime(dates[dateIndex] + " 00:00:00 UTC", "%Y-%m-%d %H:%M:%S UTC"))
                    work_dict = {"year": int(dates[dateIndex][0:4]),
                                 "month": int(dates[dateIndex][5:7]),
                                 "day": int(dates[dateIndex][8:10]),
                                 "date": str(dates[dateIndex][5:7]) + "/" + str(dates[dateIndex][8:10]) + "/" + str(
                                     dates[dateIndex][0:4]), "epochTime": gmt_midnight,
                                 "value": {opn: np.float64(values[dateIndex])},
                                 "raw_value": np.float64(values[dateIndex]),
                                 "NaN": np.float64(nan[dateIndex])}
                    if intervaltype == 0:
                        date_object = dateutils.createDateFromYearMonthDay(work_dict["year"], work_dict["month"],
                                                                           work_dict["day"])
                    elif intervaltype == 1:
                        date_object = dateutils.createDateFromYearMonth(work_dict["year"], work_dict["month"])
                    elif intervaltype == 2:
                        date_object = dateutils.createDateFromYear(work_dict["year"])
                    else:
                        date_object = dateutils.createDateFromYear(work_dict["year"])
                    work_dict["isodate"] = date_object.strftime(intervals[0]["pattern"])
                    result_list.append(work_dict)
                merged_obj = {
                    'data': result_list,
                    'polygon_Str_ToPass': polygon_str_to_pass,
                    "uid": uniqueid,
                    "datatype": datatype,
                    "operationtype": operationtype,
                    "intervaltype": intervaltype,
                    "derived_product": False}
            except Exception as e:
                logger.error("Making merge_obj failed: " + str(e) + " for: " + uniqueid)

        if str(operationtype) in "6_7":
            if operationtype == 7:
                nc_list = os.listdir(params.zipFile_ScratchWorkspace_Path + uniqueid)

                ds = xr.open_mfdataset(params.zipFile_ScratchWorkspace_Path + uniqueid + '/*.nc')
                ds.to_netcdf(params.zipFile_ScratchWorkspace_Path + uniqueid + '/' + uniqueid + '.nc')
                for file in nc_list:
                    os.remove(params.zipFile_ScratchWorkspace_Path + uniqueid + '/' + file)
                time.sleep(0.5)
            with ZipFile(params.zipFile_ScratchWorkspace_Path + uniqueid + '.zip', 'w') as zipObj:
                for folderName, sub_folders, filenames in os.walk(
                        params.zipFile_ScratchWorkspace_Path + uniqueid + '/'):
                    for filename in filenames:
                        # create complete filepath of file in directory
                        if filename.index(".") > 0:
                            file_path = os.path.join(folderName, filename)
                            # Add file to zip
                            zipObj.write(file_path, basename(file_path))
                zipObj.close()
            logger.debug("Created zip at: " + params.zipFile_ScratchWorkspace_Path + uniqueid)

        logger.debug("preparing to write file for: " + uniqueid)
        filename = params.resultsdir + uniqueid + ".txt"
        f = open(filename, 'w+')
        if merged_obj:
            try:
                logger.debug("trying to dump")
                json.dump(merged_obj, f)
            except Exception as merge_error:
                logger.error(str(merge_error))
                logger.debug("trying to toList the dump")
                json.dump(list(merged_obj), f)
        else:
            json.dump({"Error": "There was an error processing your request."}, f)
        f.close()
        logger.debug("Processes joined, file written, and setting progress to 100 for: " + uniqueid)
        time.sleep(.5)
        set_progress_to_100(uniqueid)
        logger.debug("uniqueid: " + uniqueid)
        # if this
        track_usage, created = Track_Usage.objects.get_or_create(unique_id=uniqueid)
        logger.debug("got the object")
        track_usage.status = "Success"
        track_usage.save()
        if str(operationtype) == "6":
            zip_file_path = params.zipFile_ScratchWorkspace_Path + uniqueid + '.zip'
            if not os.path.exists(zip_file_path):
                track_usage = Track_Usage.objects.get(unique_id=uniqueid)
                track_usage.status = "Fail"
                track_usage.save()
            else:
                track_usage = Track_Usage.objects.get(unique_id=uniqueid)
                track_usage.file_size = os.stat(zip_file_path).st_size
                track_usage.save()
            try:
                shutil.rmtree(params.zipFile_ScratchWorkspace_Path + uniqueid)
            except OSError as e:
                print("Error: %s : %s" % (params.zipFile_ScratchWorkspace_Path + uniqueid, e.strerror))

        jobs.clear()
        # sys.exit(0)
    except Exception as e:
        logger.error("NEW ERROR ISSUE: " + str(e))
        try:
            # maybe need to create the appropriate file for extraction with error message
            try:
                track_usage = Track_Usage.objects.get_or_create(unique_id=uniqueid)
                logger.debug("creating the object here")
                track_usage.update(
                    time_requested=timezone.now(),
                    AOI=statistical_query["geometry"],
                    dataset="unknown",
                    calculation=statistical_query["operationtype"],
                    request_type=statistical_query["request_type"],
                    status="failed",
                    progress=100,
                    API_call="submitDataRequest",
                    originating_IP=statistical_query["originating_IP"]
                )
            #
            except Track_Usage.DoesNotExist:
                track_usage = Track_Usage(
                    time_requested=timezone.now(),
                    AOI=statistical_query["geometry"],
                    dataset="unknown",
                    calculation=statistical_query["operationtype"],
                    request_type=statistical_query["request_type"],
                    status="failed",
                    unique_id=uniqueid,
                    progress=100,
                    API_call="submitDataRequest",
                    originating_IP=statistical_query["originating_IP"]
                )

                track_usage.save()

            if str(operationtype) in "6_7_8":
                zip_file_path = params.zipFile_ScratchWorkspace_Path + uniqueid + '.zip'
                if not os.path.exists(zip_file_path):
                    try:
                        with ZipFile(params.zipFile_ScratchWorkspace_Path + uniqueid + '.zip', 'w') as zipObj:
                            zipObj.writestr(
                                "data.json",
                                data=json.dumps({"error": "Data download error"},
                                                ensure_ascii=False, indent=4))
                            zipObj.close()
                    except Exception as e:
                        print(e)
            try:
                with transaction.atomic():
                    request_progress, created = Request_Progress.objects.get_or_create(request_id=uniqueid)
                    logger.debug("created: " + str(created))
                    request_progress.progress = 100
                    request_progress.save()
            except IntegrityError:
                logger.error("Progress update issue")
        except Exception as e2:
            logger.debug("Failed updating progress")
            logger.debug(str(e2))
            pass

    finally:
        sys.exit(1)


def start_worker_process(job_item):
    db.connections.close_all()
    uniqueid = job_item["uniqueid"]
    dates = None
    values = None
    logger.debug("start_worker_process for: " + uniqueid)
    long_term_average = []
    nan_percentage = []
    if job_item["subtype"] == "chirps":
        dates = job_item["dates"]
        values, long_term_average = GetTDSData.get_chirps_climatology(job_item["months"],
                                                                      job_item["bounds"],
                                                                      job_item["uniqueid"])
    elif job_item["subtype"] == "nmme":
        dates = job_item["dates"]
        values, long_term_average = GetTDSData.get_nmme_data(job_item["bounds"], job_item["uniqueid"])
    else:
        if job_item['operation'] == 'download' or job_item['operation'] == 'netcdf' or job_item['operation'] == "csv":

            logger.debug("zip_file_path - GetTDSData.get_data_values")
            zip_file_path = GetTDSData.get_data_values(job_item["uniqueid"], job_item['start_date'],
                                                       job_item['end_date'], job_item['variable'], job_item['geom'],
                                                       job_item['operation'], job_item['file_list'],
                                                       job_item["job_length"])

            return {
                "uid": job_item["uniqueid"],
                'id': str(uuid.uuid4()),
                'dates': [],
                'values': [],
                'LTA': long_term_average,
                'subtype': job_item["subtype"],
                'zipfilepath': zip_file_path,
                'job_length': job_item["job_length"]
            }
        else:
            logger.debug("about to get_data_values for: " + uniqueid)
            logger.debug("Immmmmmmm dooooing it!!!")
            try:
                dates, values, nan_percentage = GetTDSData.get_data_values(job_item["uniqueid"],
                                                           job_item['start_date'],
                                                           job_item['end_date'],
                                                           job_item['variable'],
                                                           job_item['geom'],
                                                           job_item['operation'],
                                                           job_item['file_list'],
                                                           job_item["job_length"])

            except Exception as err:
                logger.debug("fail, fail, fail")
                logger.error(str(err))
                logger.error("We have an error getting NetCDF values for: " + uniqueid)

    logger.debug("completed start_worker_process for: " + uniqueid)
    try:
        uniqueid = job_item["uniqueid"]
        job_length = job_item["job_length"]
        if job_length > 0:
            logger.debug("job_length > 0 for: " + uniqueid)
            # try:
            #     update_progress({'progress': job_length, 'uniqueid': uniqueid})
            # except:
            #     logger.error("error getting or updating progress")
            #     pass
        else:
            try:
                with transaction.atomic():
                    logger.debug("job_length was an issue somehow for: " + uniqueid)
                    request_progress = Request_Progress.objects.get(request_id=uniqueid)
                    request_progress.progress = 99.5
                    request_progress.save()
            except IntegrityError:
                logger.error("Progress update issue")
    except Exception as e:
        logger.error("ISSUE" + str(e))

    return {
        "uid": job_item["uniqueid"],
        'id': str(uuid.uuid4()),
        'dates': dates,
        'values': values,
        'NaN': nan_percentage,
        'LTA': long_term_average,
        'subtype': job_item["subtype"],
        'zipfilepath': "",
        'job_length': job_item["job_length"]
    }


def get_output_for_monthly_rainfall_analysis_from(raw_items_list):
    avg_percentiles_data_lines = []
    month_avg = []
    chirps_25 = []
    chirps_50 = []  # LTA
    chirps_75 = []
    months = []
    for item in raw_items_list:
        if item["sub_type_name"] == 'chirps':
            chirps25 = item['value']['avg'][0][0]
            chirps50 = item['value']['avg'][1]
            chirps75 = item['value']['avg'][0][2]
            chirps_25.append(chirps25)
            chirps_50.append(chirps50)
            chirps_75.append(chirps75)
        if item["sub_type_name"] == 'nmme':
            current_full_date = item['date']
            months.append(current_full_date.split('-')[1])
            month_avg.append(item['value']['avg'])
    # Organize the values as key value pairs in a JSON object avg_percentiles_dataLine
    for i in range(len(chirps_25)):
        avg_percentiles_data_line = {
            'col01_Month': (months[i]),
            'col02_MonthlyAverage': (month_avg[i]),
            'col03_25thPercentile': np.float64(chirps_25[i]),
            'col04_75thPercentile': np.float64(chirps_75[i]),
            'col05_50thPercentile': np.float64(chirps_50[i])
        }
        # This is the object that has the processed monthly analysis values for both CHIRPS and NMME
        avg_percentiles_data_lines.append(avg_percentiles_data_line)
    final_output = {
        'avg_percentiles_dataLines': avg_percentiles_data_lines,
    }
    return final_output
