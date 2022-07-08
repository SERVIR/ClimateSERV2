import multiprocessing
import random
import shutil
import threading
import time
from ast import literal_eval
# from socket import socket

import climateserv2.file.TDSExtraction as GetTDSData
import sys
from datetime import datetime
import climateserv2.processtools.uutools as uu
import json
import calendar
import os
import climateserv2.file.dateutils as dateutils
import numpy as np
from django import db
from django.apps import apps
import pandas as pd
import climateserv2.geo.shapefile.readShapesfromFiles as sF
import logging
from api.models import Track_Usage  # , ETL_Dataset
from api.models import Parameters as realParams
from zipfile import ZipFile
from django.utils import timezone

Request_Log = apps.get_model('api', 'Request_Log')
Request_Progress = apps.get_model('api', 'Request_Progress')
logger = logging.getLogger("request_processor")
dataTypes = None
global jobs_object
jobs_object = {}
global results_object
results_object = {}


def set_progress_to_100(uniqueid):
    request_progress = Request_Progress.objects.get(request_id=uniqueid)
    request_progress.progress = 100
    request_progress.save()


def start_processing(statistical_query):
    logger.info("start_processing has begun")
    db.connections.close_all()
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    try:
        params = realParams.objects.first()
        date_range_list = []

        dataset = ""
        uniqueid = statistical_query["uniqueid"]
        operationtype = ""
        if 'geometry' in statistical_query:
            polygon_string = statistical_query["geometry"]
        elif 'layerid' in statistical_query:
            polygon_string = sF.getPolygons(statistical_query['layerid'], statistical_query['featureids'])
        else:
            raise Exception("Missing polygon_string")
        # lock = threading.Lock()
        # lock.acquire()

        # lock.release()
        jobs = []
        if ('custom_job_type' in statistical_query.keys() and
                statistical_query['custom_job_type'] == 'MonthlyRainfallAnalysis'):
            operationtype = "Rainfall"
            dates, months, bounds = GetTDSData.get_monthlyanalysis_dates_bounds(polygon_string)
            uu_id = uu.getUUID()
            # lock.acquire()
            jobs.append({
                "uniqueid": uniqueid,
                "id": uu_id,
                "bounds": bounds,
                "dates": dates,
                "months": months,
                "subtype": "chirps"})
            uu_id = uu.getUUID()
            jobs.append({
                "uniqueid": uniqueid,
                "id": uu_id,
                "bounds": bounds,
                "dates": dates,
                "months": months,
                "subtype": "nmme"})
            # lock.release()
        else:
            # here calculate the years and create a list of jobs
            logger.info("Regular query has been initiated")
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
            logger.debug("about to get the data for the range")
            for dates in date_range_list:
                uu_id = uu.getUUID()
                dataset = ""
                file_list, variable = GetTDSData.get_filelist(dataTypes, datatype, dates[0], dates[1], params)
                counter += 1
                if len(file_list) > 0:
                    # lock.acquire()
                    jobs.append({
                        "uniqueid": uniqueid,
                        "id": uu_id,
                        "start_date": dates[0],
                        "end_date": dates[1],
                        "variable": variable,
                        "geom": polygon_string,
                        "operation": literal_eval(params.parameters)[statistical_query["operationtype"]][1],
                        "file_list": file_list,
                        "subtype": None
                    })
                    # lock.release()

        my_results = []
        logger.debug("Got file list")

        def error_handler(exception):
            print(f'{exception} occurred, terminating pool.')
            try:
                set_progress_to_100(uniqueid)
                pool.join()
                pool.terminate()
            except Exception:
                try:
                    set_progress_to_100(uniqueid)
                except:
                    pass
                pool.terminate()

        for job in jobs:
            job['job_length'] = len(jobs)
            rest_time = random.uniform(0.5, 1.5)
            time.sleep(rest_time)
            my_results.append(pool.apply_async(start_worker_process,
                                               args=[job],
                                               callback=log_result,
                                               error_callback=error_handler
                                               ))
        logger.debug("should be back from start_worker_process")
        pool.close()
        pool.join()
        logger.debug("pool should be joined")
        split_obj = []

        for res in my_results:
            split_obj.append(res.get())

        dates = []
        values = []
        LTA = []
        subtype = ""
        if ('custom_job_type' in statistical_query.keys() and
                statistical_query['custom_job_type'] == 'MonthlyRainfallAnalysis'):
            opn = "avg"
            result_list = []
            uid = uu.getUUID()
            suid = uu.getUUID()
            for obj in split_obj:
                subtype = obj["subtype"]
                for dateIndex in range(len(obj["dates"])):
                    work_dict = {'uid': uniqueid, 'datatype_uuid_for_CHIRPS': uid,
                                 'datatype_uuid_for_SeasonalForecast': suid, 'sub_type_name': subtype,
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
            logger.debug("after join, preparing to create output")
            try:
                dates = []
                values = []
                for obj in split_obj:
                    db.connections.close_all()
                    try:
                        dates.extend(obj["dates"])
                        values.extend(obj["values"])
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
                                 "value": {opn: np.float64(values[dateIndex])}}
                    if intervaltype == 0:
                        date_object = dateutils.createDateFromYearMonthDay(work_dict["year"], work_dict["month"],
                                                                           work_dict["day"])
                    elif intervaltype == 1:
                        date_object = dateutils.createDateFromYearMonth(work_dict["year"], work_dict["month"])
                    elif intervaltype == 2:
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
                logger.error("Making merge_obj failed: " + str(e))
        logger.debug("preparing to write file")
        filename = params.resultsdir + uniqueid + ".txt"
        f = open(filename, 'w+')
        json.dump(merged_obj, f)
        f.close()
        db.connections.close_all()
        logger.error("Processes joined and setting progress to 100")
        set_progress_to_100(uniqueid)

        track_usage = Track_Usage.objects.get(unique_id=uniqueid)
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

        # Terminating main process
        # lock.acquire()
        # if uniqueid in jobs_object:
        #     del jobs_object[uniqueid]
        # if uniqueid in results_object:
        #     del results_object[uniqueid]
        # lock.release()
        jobs.clear()
        try:
            try:
                pool.join()
            finally:
                pool.terminate()
        finally:
            sys.exit(1)
    except Exception as e:
        logger.error("NEW ERROR ISSUE: " + str(e))
        try:
            # maybe need to create the appropriate file for extraction with error message
            try:
                track_usage = Track_Usage.objects.get(unique_id=uniqueid)
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
            request_progress, created = Request_Progress.objects.get_or_create(request_id=uniqueid)
            logger.debug("created: " + str(created))
            request_progress.progress = 100
            request_progress.save()
        except Exception as e2:
            logger.info("Failed updating progress")
            logger.info(str(e2))
            pass
        if pool is not None:
            pool.terminate()
        print(e)
    finally:
        try:
            try:
                pool.join()
            finally:
                pool.terminate()
        finally:
            # lock.acquire()
            # if uniqueid in jobs_object:
            #     del jobs_object[uniqueid]
            # if uniqueid in results_object:
            #     del results_object[uniqueid]
            # lock.release()
            job.clear()
            sys.exit(1)


def start_worker_process(job_item):
    # here is where you would open each netcdf
    # and do the processing and create the data
    # to return to the parent for said year.
    # I am using fake data so i'm just changing
    # it so we can see it is being "processed"
    logger.debug("start_worker_process")
    LTA = []
    if job_item["subtype"] == "chirps":
        dates = job_item["dates"]
        values, LTA = GetTDSData.get_chirps_climatology(job_item["months"], job_item["bounds"])
    elif job_item["subtype"] == "nmme":
        dates = job_item["dates"]
        values, LTA = GetTDSData.get_nmme_data(job_item["bounds"])
    else:
        if job_item['operation'] == 'download' or job_item['operation'] == 'netcdf':
            zip_file_path = GetTDSData.get_thredds_values(job_item["uniqueid"], job_item['start_date'],
                                                          job_item['end_date'], job_item['variable'], job_item['geom'],
                                                          job_item['operation'], job_item['file_list'])
            db.connections.close_all()
            return {
                "uid": job_item["uniqueid"],
                'id': uu.getUUID(),
                'dates': [],
                'values': [],
                'LTA': LTA,
                'subtype': job_item["subtype"],
                'zipfilepath': zip_file_path,
                'job_length': job_item["job_length"]
            }
        else:
            logger.debug("about to get_thredds_values")
            try:
                dates, values = GetTDSData.get_thredds_values(job_item["uniqueid"],
                                                          job_item['start_date'],
                                                          job_item['end_date'],
                                                          job_item['variable'],
                                                          job_item['geom'],
                                                          job_item['operation'],
                                                          job_item['file_list'])
            except Exception:
                logger.error("We have an error getting thredds values")

    db.connections.close_all()
    logger.debug("completed start_worker_process")
    return {
        "uid": job_item["uniqueid"],
        'id': uu.getUUID(),
        'dates': dates,
        'values': values,
        'LTA': LTA,
        'subtype': job_item["subtype"],
        'zipfilepath': "",
        'job_length': job_item["job_length"]
    }


def log_result(retval):
    lock = threading.Lock()
    try:
        uniqueid = retval["uid"]
        job_length = retval["job_length"]
        lock.acquire()

        if job_length > 0:

            db.connections.close_all()
            request_progress = Request_Progress.objects.get(request_id=uniqueid)

            update_value = (float(request_progress.progress) + (100/job_length)) - .5
            logger.info(str(update_value) + '% done')
            # this is so the progress is not set to 100 before the output files are saved to the drive
            # once saved it will update to 100.
            request_progress.progress = update_value
            request_progress.save()
            logger.debug("**********************************" + str(request_progress.progress))
            # log.progress = progress - .5
            # request_progress.save()
        lock.release()
    except Exception as e:
        logger.info("LOCK ISSUE" + str(e))


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
