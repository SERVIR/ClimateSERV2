import multiprocessing
import random
import time
import climateserv2.file.TDSExtraction as GetTDSData
import climateserv2.parameters as params
import sys
from datetime import datetime,timedelta
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

Request_Log = apps.get_model('api', 'Request_Log')
Request_Progress = apps.get_model('api', 'Request_Progress')
def start_processing(request):
    # here calculate the years and create a list of jobs
    datatype = request['datatype']
    begintime = request['begintime']
    endtime = request['endtime']
    dataset = params.dataTypes[int(datatype)]['dataset_name'] + ".nc4"

    daterangelist=[]
    global jobs
    jobs=[]
    global results
    results = []
    firstdate = datetime.strptime(begintime, '%m/%d/%Y')
    firstdate_str= datetime.strftime(firstdate, '%Y-%m-%d')
    lastdate = datetime.strptime(endtime, '%m/%d/%Y')
    lastdate_str=datetime.strftime(lastdate, '%Y-%m-%d')
    if firstdate.year==lastdate.year:
        daterangelist.append([firstdate_str, lastdate_str])
    else:
        years = [int(i.strftime("%Y")) for i in pd.date_range(start=begintime, end=endtime, freq='MS')]
        yearlist = np.unique(years)
        startyear = yearlist[0]
        endyear = yearlist[len(yearlist) - 1]
        for year in yearlist:
            if year==startyear:
                firstdate_str = datetime.strftime(firstdate, '%Y-%m-%d')
                lastday = firstdate.replace(month=12, day=31)
                lastdate_str = datetime.strftime(lastday, '%Y-%m-%d')
            elif year==endyear:
                firstday = lastdate.replace(month=1, day=1)
                firstdate_str = datetime.strftime(firstday, '%Y-%m-%d')
                lastdate_str = datetime.strftime(lastdate, '%Y-%m-%d')
            else:
                firstdate_str=str(year)+"-01-01"
                lastdate_str=str(year)+"-12-31"
            daterangelist.append([firstdate_str, lastdate_str])
    for dates in daterangelist:
        id=uu.getUUID()
        file_list = GetTDSData.get_filelist(dataset,datatype,dates[0],dates[1])
        if len(file_list)>0:
            jobs.append({"uniqueid":request["uniqueid"],"id":id,"start_date":dates[0],"end_date":dates[1],"variable":params.dataTypes[int(datatype)]['variable'],"geom":request["geometry"],"operation":params.parameters[request["operationtype"]][1],"file_list":file_list})
    pool = multiprocessing.Pool(os.cpu_count())
    for job in jobs:
        pool.apply_async(start_worker_process, args=[job], callback=log_result)
    pool.close()
    pool.join()

    # this is the final list that would be returned by the jobs
    # you likely have to merge them, i'm guessing you had to do
    # similar with the results of zmq
    split_obj=results
    dates=[]
    values=[]
    for obj in split_obj:
        dates.extend(obj["dates"])
        values.extend(obj["values"])
    uniqueid = request['uniqueid']
    datatype = request['datatype']
    polygon_Str_ToPass=request['geometry']
    intervaltype = request['intervaltype']
    operationtype = request['operationtype']
    opn = params.parameters[operationtype][1]
    resultlist=[]
    for dateIndex in range(len(dates)):
        gmt_midnight = calendar.timegm(time.strptime(dates[dateIndex] + " 00:00:00 UTC", "%Y-%m-%d %H:%M:%S UTC"))
        workdict = {}
        workdict["year"] = int(dates[dateIndex][0:4])
        workdict["month"] = int(dates[dateIndex][5:7])
        workdict["day"] = int(dates[dateIndex][8:10])
        workdict["epochTime"] = gmt_midnight
        workdict["value"] = {opn: np.float64(values[dateIndex])}
        if (intervaltype == 0):
            dateObject = dateutils.createDateFromYearMonthDay(workdict["year"], workdict["month"], workdict["day"])
        elif (intervaltype == 1):
            dateObject = dateutils.createDateFromYearMonth(workdict["year"], workdict["month"])
        elif (intervaltype == 2):
            dateObject = dateutils.createDateFromYear(workdict["year"])
        workdict["isodate"] = dateObject.strftime(params.intervals[0]["pattern"])
        resultlist.append(workdict)
    merged_obj = {'data':resultlist,'polygon_Str_ToPass': polygon_Str_ToPass,"uid": uniqueid, "datatype": datatype, "operationtype": operationtype,
                    "intervaltype": intervaltype,
                    "derived_product": False}
    filename = params.getResultsFilename(request["uniqueid"])
    f = open(filename, 'w+')
    json.dump((merged_obj), f)
    f.close()

    # Terminating main process
    sys.exit(1)

def start_worker_process(job_item):
    # here is where you would open each netcdf
    # and do the processing and create the data
    # to return to the parent for said year.
    # I am using fake data so i'm just changing
    # it so we can see it is being "processed"
    dates, values = GetTDSData.get_aggregated_values(job_item['start_date'], job_item['end_date'],job_item['variable'], job_item['geom'], job_item['operation'], job_item['file_list'])
    db.connections.close_all()
    return  {
        "uid":job_item["uniqueid"],
        'id': uu.getUUID(),
        'dates': dates,
        'values':values
    }

def log_result(retval):
    results.append(retval)
    try:
        progress= (len(results) / len(jobs)) * 100.0
        print('{:.0%} done'.format(len(results) / len(jobs)))
        log = reqLog.Request_Progress.objects.get(request_id=retval["uid"])
        log.progress = progress
        log.save()
    except Exception as e:
        print(str(e))