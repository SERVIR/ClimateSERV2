import multiprocessing
import shutil
import time
from ast import literal_eval
from socket import socket

import climateserv2.file.TDSExtraction as GetTDSData
import sys
from datetime import datetime
import climateserv2.processtools.uutools as uu
import json
import calendar
import os
import climateserv2.file.dateutils as dateutils
import climateserv2.requestLog as reqLog
import numpy as np
from django import db
from django.apps import apps
import pandas as pd
import climateserv2.geo.shapefile.readShapesfromFiles as sF
import logging
from api.models import Track_Usage
from api.models import Parameters as real_params

Request_Log = apps.get_model('api', 'Request_Log')
Request_Progress = apps.get_model('api', 'Request_Progress')
logger = logging.getLogger("request_processor")
dataTypes = None


def start_processing(request):
    db.connections.close_all()
    try:
        params = real_params.objects.first()
    except Exception as e:
        print(e)
    date_range_list = []
    global jobs
    jobs = []
    global results
    results = []
    dataset = ""
    operationtype = ""
    if 'geometry' in request:
        polygon_string = request["geometry"]
    elif 'layerid' in request:
        layer_id = request['layerid']
        feature_ids = request['featureids']
        polygon_string = sF.getPolygons(layer_id, feature_ids)

    if 'custom_job_type' in request.keys() and request['custom_job_type'] == 'MonthlyRainfallAnalysis':

        dates, months, bounds = GetTDSData.get_monthlyanalysis_dates_bounds(polygon_string)
        id = uu.getUUID()
        jobs.append({"uniqueid": request["uniqueid"], "id": id, "bounds": bounds, "dates": dates, "months": months,
                     "subtype": "chirps"})
        id = uu.getUUID()
        jobs.append({"uniqueid": request["uniqueid"], "id": id, "bounds": bounds, "dates": dates, "months": months,
                     "subtype": "nmme"})
    else:
        # here calculate the years and create a list of jobs
        datatype = request['datatype']
        begin_time = request['begintime']
        end_time = request['endtime']
        first_date = datetime.strptime(begin_time, '%m/%d/%Y')
        first_date_string = datetime.strftime(first_date, '%Y-%m-%d')
        last_date = datetime.strptime(end_time, '%m/%d/%Y')
        last_date_string = datetime.strftime(last_date, '%Y-%m-%d')
        if first_date.year == last_date.year:
            date_range_list.append([first_date_string, last_date_string])
        else:
            years = [int(i.strftime("%Y")) for i in pd.date_range(start=begin_time, end=end_time, freq='MS')]
            # print(years)
            year_list = np.unique(years)
            # print(year_list)
            start_year = year_list[0]
            # print("start: ", str(start_year))
            end_year = year_list[len(year_list) - 1]
            # print("end: ", str(end_year))
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

        for dates in date_range_list:
            logger.error(str(",".join(dates)))
            id = uu.getUUID()
            dataset = ""
            file_list, variable = GetTDSData.get_filelist(dataTypes, datatype, dates[0], dates[1], params)
            logger.error(str(counter) + "*************file_list: " + str(",".join(file_list)))
            counter += 1
            if len(file_list) > 0:
                jobs.append({"uniqueid": request["uniqueid"], "id": id, "start_date": dates[0], "end_date": dates[1],
                             "variable": variable, "geom": polygon_string,
                             "operation": literal_eval(params.parameters)[request["operationtype"]][1],
                             "file_list": file_list,
                             "derivedtype": False, "subtype": None})
        logger.error("jobs length is: " + str(len(jobs)))
    pool = multiprocessing.Pool(os.cpu_count() * 2)
    for job in jobs:
        pool.apply_async(start_worker_process, args=[job], callback=log_result)
    pool.close()
    pool.join()

    # while len(results) / len(jobs) < 1:
    #     time.sleep(1)
    # this is the final list that would be returned by the jobs
    # you likely have to merge them, i'm guessing you had to do
    # similar with the results of zmq
    split_obj = results
    dates = []
    values = []
    LTA = []
    subtype = ""
    if 'custom_job_type' in request.keys() and request['custom_job_type'] == 'MonthlyRainfallAnalysis':
        uniqueid = request['uniqueid']
        opn = "avg"
        resultlist = []
        uid = uu.getUUID()
        suid = uu.getUUID()
        temp = []
        for d in dates:
            if d not in temp:
                temp.append(d)
        dates = temp
        for obj in split_obj:
            subtype = obj["subtype"]
            for dateIndex in range(len(obj["dates"])):
                workdict = {'uid': uniqueid, 'datatype_uuid_for_CHIRPS': uid,
                            'datatype_uuid_for_SeasonalForecast': suid, 'sub_type_name': subtype,
                            'derived_product': True, 'special_type': 'MonthlyRainfallAnalysis',
                            "date": obj["dates"][dateIndex]}
                if subtype == "chirps":
                    workdict["value"] = {opn: [obj["values"][dateIndex], np.float64(obj["LTA"][dateIndex])]}
                if subtype == "nmme":
                    workdict['value'] = {opn: np.float64(obj["values"][dateIndex])}

                resultlist.append(workdict)
        merged_obj = {"MonthlyAnalysisOutput": get_output_for_monthly_rainfall_analysis_from(resultlist)}

    else:
        dates = []
        values = []
        for obj in split_obj:
            dates.extend(obj["dates"])
            values.extend(obj["values"])
        uniqueid = request['uniqueid']
        datatype = request['datatype']
        polygon_Str_ToPass = polygon_string
        intervaltype = request['intervaltype']
        operationtype = request['operationtype']
        intervals = [
            {'name': 'day', 'pattern': '%m/%d/%Y'},
            {'name': 'month', 'pattern': '%m/%Y'},
            {'name': 'year', 'pattern': '%Y'}
        ]
        opn = literal_eval(params.parameters)[operationtype][1]
        resultlist = []
        for dateIndex in range(len(dates)):
            gmt_midnight = calendar.timegm(time.strptime(dates[dateIndex] + " 00:00:00 UTC", "%Y-%m-%d %H:%M:%S UTC"))
            workdict = {}
            workdict["year"] = int(dates[dateIndex][0:4])
            workdict["month"] = int(dates[dateIndex][5:7])
            workdict["day"] = int(dates[dateIndex][8:10])
            workdict["date"] = str(dates[dateIndex][5:7]) + "/" + str(dates[dateIndex][8:10]) + "/" + str(
                dates[dateIndex][0:4])
            workdict["epochTime"] = gmt_midnight
            workdict["value"] = {opn: np.float64(values[dateIndex])}
            if intervaltype == 0:
                dateObject = dateutils.createDateFromYearMonthDay(workdict["year"], workdict["month"], workdict["day"])
            elif intervaltype == 1:
                dateObject = dateutils.createDateFromYearMonth(workdict["year"], workdict["month"])
            elif intervaltype == 2:
                dateObject = dateutils.createDateFromYear(workdict["year"])
            workdict["isodate"] = dateObject.strftime(intervals[0]["pattern"])
            resultlist.append(workdict)
        merged_obj = {'data': resultlist, 'polygon_Str_ToPass': polygon_Str_ToPass, "uid": uniqueid,
                      "datatype": datatype, "operationtype": operationtype,
                      "intervaltype": intervaltype,
                      "derived_product": False}
    filename = params.resultsdir + request["uniqueid"] + ".txt"
    f = open(filename, 'w+')
    json.dump(merged_obj, f)
    f.close()
    db.connections.close_all()
    log = reqLog.Request_Progress.objects.get(request_id=request["uniqueid"])
    log.progress = 100
    log.save()
    if log.progress == 100:
        status = "Success"
    else:
        status = "In Progress"
    track_usage = Track_Usage.objects.get(unique_id=uniqueid)
    track_usage.status = status
    track_usage.save()
    if str(operationtype) == "6":
        zipFilePath = params.zipFile_ScratchWorkspace_Path + uniqueid + '.zip'
        if not os.path.exists(zipFilePath):
            track_usage = Track_Usage.objects.get(unique_id=uniqueid)
            track_usage.status = "Fail"
            track_usage.save()
        else:
            track_usage = Track_Usage.objects.get(unique_id=uniqueid)
            track_usage.file_size = os.stat(zipFilePath).st_size
            track_usage.save()
        try:
            shutil.rmtree(params.zipFile_ScratchWorkspace_Path + uniqueid)
        except OSError as e:
            print("Error: %s : %s" % (params.zipFile_ScratchWorkspace_Path + uniqueid, e.strerror))

    # Terminating main process
    jobs.clear()
    sys.exit(1)


def start_worker_process(job_item):
    logger.error("Starting worker for: "
                 + job_item['start_date']
                 + " to "
                 + job_item['end_date']
                 + " File: "
                 + str(",".join(job_item['file_list']))
                 + "job: "
                 + job_item["uniqueid"])
    # here is where you would open each netcdf
    # and do the processing and create the data
    # to return to the parent for said year.
    # I am using fake data so i'm just changing
    # it so we can see it is being "processed"
    LTA = []
    if job_item["subtype"] == "chirps":
        dates = job_item["dates"]
        values, LTA = GetTDSData.get_chirps_climatology(job_item["months"], job_item["bounds"])
    elif job_item["subtype"] == "nmme":
        dates = job_item["dates"]
        values, LTA = GetTDSData.get_nmme_data(job_item["bounds"])
    else:
        if job_item['operation'] == 'download' or job_item['operation'] == 'netcdf':
            zipfilepath = GetTDSData.get_thredds_values(job_item["uniqueid"], job_item['start_date'],
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
                'zipfilepath': zipfilepath
            }
        else:
            dates, values = GetTDSData.get_thredds_values(job_item["uniqueid"], job_item['start_date'],
                                                          job_item['end_date'], job_item['variable'], job_item['geom'],
                                                          job_item['operation'], job_item['file_list'])
            logger.error("got back for job: " + job_item["uniqueid"])
    db.connections.close_all()
    return {
        "uid": job_item["uniqueid"],
        'id': uu.getUUID(),
        'dates': dates,
        'values': values,
        'LTA': LTA,
        'subtype': job_item["subtype"],
        'zipfilepath': ""
    }


def log_result(retval):
    try:
        logger.error(retval["uid"] + ': len(jobs): ' + str(len(jobs)))
    except Exception as e:
        logger.error('len(jobs) error: ' + str(e))
        pass
    results.append(retval)
    try:
        progress = (len(results) / len(jobs)) * 100.0
        logger.info('{:.0%} done'.format(len(results) / len(jobs)))

        log = reqLog.Request_Progress.objects.get(request_id=retval["uid"])
        log.progress = progress - .5
        log.save()
    except Exception as e:
        logger.info(str(e))


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
