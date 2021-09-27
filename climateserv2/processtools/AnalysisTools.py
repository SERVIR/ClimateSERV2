import calendar
import time
import os

try:
    import climateserv2.geoutils as geoutils
    import climateserv2.processtools.dateprocessor as dproc
    import climateserv2.parameters as params
    import climateserv2.processtools.uutools as uu
    import climateserv2.file.dateutils as dateutils
    import climateserv2.geo.shapefile.readShapesfromFiles as sf
    import climateserv2.file.TDSExtraction as GetTDSData
    import climateserv2.locallog.locallogging as llog
except:
    import file.TDSExtraction as GetTDSData
    import geoutils as geoutils
    import processtools.dateprocessor as dproc
    import parameters as params
    import processtools.uutools as uu
    import file.dateutils as dateutils
    import geo.shapefile.readShapesfromFiles as sf
    import locallog.locallogging as llog

chirps_dateRange_earlyYear = "1981"
chirps_dateRange_earlyMonth = "01"
chirps_dateRange_earlyDay = "01"
chirps_dateRange_lateYear = "2020"
chirps_dateRange_lateMonth = "12"
chirps_dateRange_lateDay = "31"
chirps_dataType = 0

logger = llog.getNamedLogger("request_processor")

# To get dates and values based on type
def getDatesAndValues(type,request):
    polygon_Str_ToPass = None
    # Get geometry and retrieve  data
    if ('geometry' in request):
        polygon_Str_ToPass = request['geometry']
    elif ('layerid' in request):
        layerid = request['layerid']
        featureids = request['featureids']
        polygon_Str_ToPass = sf.getPolygons(layerid, featureids)
    dates, values = GetTDSData.get_season_values(type, polygon_Str_ToPass)
    return polygon_Str_ToPass,dates, values

# To retrieve CHIRPS data for monthly analysis
def _MonthlyRainfallAnalysis__make_CHIRPS_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast):
    worklist = []
    sub_type_name = 'CHIRPS_REQUEST'
    datatype = chirps_dataType
    intervaltype = 0
    operation = "avg"
    polygon_Str_ToPass, dates, values = getDatesAndValues("chirps", request)
    # Build the worklist for each date in the dates
    for dateIndex in range(len(dates)):
        workid = uu.getUUID()
        gmt_midnight = calendar.timegm(time.strptime(dates[dateIndex] + " 00:00:00 UTC", "%Y-%m-%d %H:%M:%S UTC"))
        workdict = {"uid": uniqueid,
                    "workid": workid,
                    "datatype": datatype,
                    "operationtype": 5,
                    "intervaltype": intervaltype,
                    "polygon_Str_ToPass": polygon_Str_ToPass,
                    "datatype_uuid_for_CHIRPS": datatype_uuid_for_CHIRPS,
                    "datatype_uuid_for_SeasonalForecast": datatype_uuid_for_SeasonalForecast,
                    "current_mask_and_storage_uuid": datatype_uuid_for_CHIRPS, # Only one chirps type request needed so using same uuid
                    "sub_type_name": sub_type_name, "derived_product": True, "special_type": 'MonthlyRainfallAnalysis' }
        workdict["year"] = int(dates[dateIndex][0:4])
        workdict["month"] = int(dates[dateIndex][5:7])
        workdict["day"] = int(dates[dateIndex][8:10])
        workdict["epochTime"] = gmt_midnight
        workdict["value"] = {operation: values[dateIndex]}
        dateObject = dateutils.createDateFromYearMonthDay(workdict["year"], workdict["month"], workdict["day"])
        workdict["isodate"] = dateObject.strftime(params.intervals[0]["pattern"])
        worklist.extend([workdict])
    return worklist
	
def _MonthlyRainfallAnalysis__make_CHIRPS_GEFS_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast):
    worklist = []
    sub_type_name = 'SEASONAL_FORECAST'

    datatype = 32
    begintime = chirps_dateRange_earlyMonth + "/" + chirps_dateRange_earlyDay + "/" + chirps_dateRange_earlyYear
    endtime = chirps_dateRange_lateMonth + "/" + chirps_dateRange_lateDay + "/" + chirps_dateRange_lateYear
    intervaltype = 0    # Daily
    operation =   "avg"

    polygon_Str_ToPass = None
    if ('geometry' in request):
        # Get the polygon string
        polygonstring = request['geometry']

        polygon_Str_ToPass = polygonstring
        temp_file = os.path.join(params.netCDFpath, request['uniqueid'])  # name for temporary netcdf file

        dates, values = GetTDSData.get_aggregated_values(begintime, endtime,params.dataTypes[int(datatype)]['dataset_name']+'.nc4', params.dataTypes[int(datatype)]['variable'], polygonstring,

                                                                            operation,temp_file)
    # User Selected a Feature
    elif ('layerid' in request):
        layerid = request['layerid']
        featureids = request['featureids']
        geometries = sf.getPolygons(layerid, featureids)
        temp_file = os.path.join(params.netCDFpath, request['uniqueid'])  # name for temporary netcdf file

        dates, values = GetTDSData.get_aggregated_values(begintime, endtime,
                                                         'ucsb-chirps-gefs_global_0.05deg_10dy.nc4',
                                                         'precipitation_amount', geometries,

                                                         operation, temp_file)

    # Build the worklist for each date in the dates
    for dateIndex in range(len(dates)):
        gmt_midnight = calendar.timegm(time.strptime(dates[dateIndex] + " 00:00:00 UTC", "%Y-%m-%d %H:%M:%S UTC"))
        workid = uu.getUUID()
        workdict = {'uid': uniqueid,
                    'workid': workid,
                    'datatype': datatype,
                    'operationtype': 5,
                    'intervaltype': intervaltype,
                    #'bounds': bounds,
                    'polygon_Str_ToPass': polygon_Str_ToPass,
                    'datatype_uuid_for_CHIRPS': datatype_uuid_for_CHIRPS,
                    'datatype_uuid_for_SeasonalForecast': datatype_uuid_for_SeasonalForecast,
                    'current_mask_and_storage_uuid': datatype_uuid_for_CHIRPS,                  # Only one chirps type request needed so using same uuid
                    'sub_type_name': sub_type_name, 'derived_product': True, 'special_type': 'MonthlyRainfallAnalysis' }
        # Daily dates processing # if (intervaltype == 0):  # It is in this case, daily.
        workdict['year'] = int(dates[dateIndex][0:4])
        workdict['month'] = int(dates[dateIndex][5:7])
        workdict['day'] = int(dates[dateIndex][8:10])
        workdict['epochTime'] = gmt_midnight
        workdict['value'] = {operation: values[dateIndex]}
        dateObject = dateutils.createDateFromYearMonthDay(workdict['year'], workdict['month'], workdict['day'])
        workdict['isodate'] = dateObject.strftime(params.intervals[0]['pattern'])
        worklist.extend([workdict])
    return worklist


# To retrieve NMME data for monthly analysis
def _MonthlyRainfallAnalysis__make_SeasonalForecast_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast):
    worklist = []
    sub_type_name = 'SEASONAL_FORECAST'
    intervaltype = 0  # Daily
    operation = "avg"
    polygon_Str_ToPass, dates, values = getDatesAndValues("nmme", request)
    current_mask_uuid_for_SeasonalForecast = uu.getUUID()

    # Build the worklist for each date in the dates
    for dateIndex in range(len(dates)):
        gmt_midnight = calendar.timegm(time.strptime(dates[dateIndex] + " 00:00:00 UTC", "%Y-%m-%d %H:%M:%S UTC"))
        workid = uu.getUUID()
        workdict = {"uid": uniqueid,
                    "workid": workid,
                    "operationtype": 5,
                    "intervaltype": intervaltype,
                    "polygon_Str_ToPass": polygon_Str_ToPass,
                    "datatype_uuid_for_CHIRPS": datatype_uuid_for_CHIRPS,
                    "datatype_uuid_for_SeasonalForecast": datatype_uuid_for_SeasonalForecast,
                    "current_mask_and_storage_uuid": current_mask_uuid_for_SeasonalForecast,
                    "sub_type_name": sub_type_name,
                    "derived_product": True,
                    "special_type": 'MonthlyRainfallAnalysis'}
        workdict["year"] = int(dates[dateIndex][0:4])
        workdict["month"] = int(dates[dateIndex][5:7])
        workdict["day"] = int(dates[dateIndex][8:10])
        workdict["epochTime"] = gmt_midnight
        workdict["value"] = {operation: values[dateIndex]}
        dateObject = dateutils.createDateFromYearMonthDay(workdict["year"], workdict["month"], workdict["day"])
        workdict["isodate"] = dateObject.strftime(params.intervals[0]["pattern"])
        worklist.extend([workdict])
    return worklist

# To retrieve work list for GEFS Monthly Analyis
def get_workList_for_headProcessor_for_MonthlyGEFSRainfallAnalysis_types(uniqueid, request):
    worklist = []
    datatype_uuid_for_CHIRPS = uu.getUUID()
    datatype_uuid_for_SeasonalForecast = uu.getUUID()
    worklist_CHIRPS             = _MonthlyRainfallAnalysis__make_CHIRPS_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast)
    worklist_SeasonalForecast   = _MonthlyRainfallAnalysis__make_CHIRPS_GEFS_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast)
    worklist = worklist + worklist_CHIRPS
    worklist = worklist + worklist_SeasonalForecast
    return worklist

# To retrieve a work list (CHIRPS+NMME) that ZMQ Head Processor can use
def get_workList_for_headProcessor_for_MonthlyRainfallAnalysis_types(uniqueid, request):
    worklist = []
    datatype_uuid_for_CHIRPS = uu.getUUID()
    datatype_uuid_for_SeasonalForecast = uu.getUUID()
    worklist_CHIRPS             = _MonthlyRainfallAnalysis__make_CHIRPS_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast)
    worklist_SeasonalForecast   = _MonthlyRainfallAnalysis__make_SeasonalForecast_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast)
    worklist = worklist + worklist_CHIRPS
    worklist = worklist + worklist_SeasonalForecast
    return worklist

# To get CHIRPS_REQUEST and SEASONAL_FORECAST data from worklist raw values for Monthly Analyis
def get_output_for_MonthlyRainfallAnalysis_from(raw_items_list):
        avg_percentiles_dataLines = []
        monthavg=[]
        chirps_25=[]
        chirps_50 = []
        chirps_75 = []
        months=[]
        for item in raw_items_list:
            if item["sub_type_name"] == 'CHIRPS_REQUEST':
                chirps25 = item['value']['avg'][0]
                chirps50 = item['value']['avg'][1]
                chirps75 = item['value']['avg'][2]
                chirps_25.append(chirps25)
                chirps_50.append(chirps50)
                chirps_75.append(chirps75)
            if item["sub_type_name"] == 'SEASONAL_FORECAST':
                current_full_date = item['date']
                monthavg.append(item['value']['avg'])
                months.append( current_full_date.split('/')[0])
        # Organize the values as key value pairs in a JSON object avg_percentiles_dataLine
        for i in range(len(chirps_25)):
            avg_percentiles_dataLine = {
                'col01_Month': str(months[i]),
                'col02_MonthlyAverage': str(monthavg[i]),
                'col03_25thPercentile': str(chirps_25[i]),
                'col04_75thPercentile': str(chirps_75[i]),
                'col05_50thPercentile': str(chirps_50[i])
            }
            # This is the object that has the processed monthly analysis values for both CHIRPS and NMME
            avg_percentiles_dataLines.append(avg_percentiles_dataLine)
        final_output = {
            'avg_percentiles_dataLines': avg_percentiles_dataLines,
        }
        return final_output