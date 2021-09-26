import calendar
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
import time
import os

chirps_dateRange_earlyYear = "1981" # "2009" #"1981"  # 2009 is local test data   # 1981 is the live data
chirps_dateRange_earlyMonth = "01"
chirps_dateRange_earlyDay = "01"
chirps_dateRange_lateYear = "2021" # "2016"  #"2010" #"2016"   # 2010 is local test data  # 2016 is the live data
chirps_dateRange_lateMonth = "05"  # "10"
chirps_dateRange_lateDay = "31"    # "26"
chirps_dataType = 0
logger = llog.getNamedLogger("request_processor")

def _MonthlyRainfallAnalysis__make_CHIRPS_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast):
    worklist = []
    sub_type_name = 'CHIRPS_REQUEST'
    datatype = chirps_dataType
    intervaltype = 0
    operation = "avg"

    polygon_Str_ToPass = None

    if ('geometry' in request):
        polygonstring = request['geometry']
        polygon_Str_ToPass = polygonstring
        dates,values = GetTDSData.get_season_values('chirps',polygon_Str_ToPass, request['uniqueid'])
    elif ('layerid' in request):
        layerid = request['layerid']
        featureids = request['featureids']
        geometries = sf.getPolygons(layerid, featureids)
        dates,values = GetTDSData.get_season_values( 'chirps',geometries, request['uniqueid'])

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
                    "current_mask_and_storage_uuid": datatype_uuid_for_CHIRPS,                  # Only one chirps type request needed so using same uuid
                    "sub_type_name": sub_type_name, "derived_product": True, "special_type": 'MonthlyRainfallAnalysis' }
        # Daily dates processing # if (intervaltype == 0):  # It is in this case, daily.
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


# Need to make a request for all 10 ensembles of precipitation.
def _MonthlyRainfallAnalysis__make_SeasonalForecast_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast):
    worklist = []
    sub_type_name = 'SEASONAL_FORECAST'  # Choices for now are: 'CHIRPS_REQUEST' and 'SEASONAL_FORECAST'
    intervaltype = 0  # Daily
    operation = "avg"
    polygon_Str_ToPass = None

    if ('geometry' in request):
        # Get the polygon string
        polygonstring = request['geometry']
        polygon_Str_ToPass = polygonstring
        dates, values = GetTDSData.get_season_values('nmme',polygonstring, request['uniqueid'])

    # User Selected a Feature
    elif ('layerid' in request):
        layerid = request['layerid']
        featureids = request['featureids']
        geometries = sf.getPolygons(layerid, featureids)
        dates, values = GetTDSData.get_season_values( 'nmme',geometries, request['uniqueid'])
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
                    # Only one chirps type request needed so using same uuid
                    "sub_type_name": sub_type_name, "derived_product": True,
                    "special_type": 'MonthlyRainfallAnalysis'}
        # Daily dates processing # if (intervaltype == 0):  # It is in this case, daily.
        workdict["year"] = int(dates[dateIndex][0:4])
        workdict["month"] = int(dates[dateIndex][5:7])
        workdict["day"] = int(dates[dateIndex][8:10])
        workdict["epochTime"] = gmt_midnight
        workdict["value"] = {operation: values[dateIndex]}
        dateObject = dateutils.createDateFromYearMonthDay(workdict["year"], workdict["month"], workdict["day"])
        workdict["isodate"] = dateObject.strftime(params.intervals[0]["pattern"])
        worklist.extend([workdict])  # Basically adds the entire workdict object to the worklist (could also be written as, worklist.append(workdict)
    return worklist

def get_workList_for_headProcessor_for_MonthlyGEFSRainfallAnalysis_types(uniqueid, request):
    worklist = []
    datatype_uuid_for_CHIRPS = uu.getUUID()
    datatype_uuid_for_SeasonalForecast = uu.getUUID()

    # (A) Process incoming params
    worklist_CHIRPS             = _MonthlyRainfallAnalysis__make_CHIRPS_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast)
    worklist_SeasonalForecast   = _MonthlyRainfallAnalysis__make_CHIRPS_GEFS_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast)
    worklist = worklist + worklist_CHIRPS
    worklist = worklist + worklist_SeasonalForecast
    return worklist
	
# Alternate version of '__preProcessIncomingRequest__' code that gets the worklist for the Monthly Analysis
# The MonthlyRainfallAnalysis types require a set of primary (normal type of) reuqests that generate data.
# The data is then used to make the derrived product dataset which is what is returned to the client and then placed on a graph.
# # For Workers to be able to differentiate between requests, the worklist gets new parameters.
# # # See, 'sub_type_name' # Currently, the choices forthis are: 'CHIRPS_REQUEST' and 'SEASONAL_FORECAST'
# # # # Descriptions of types: Chirps types are flat (single geometry, single mask, single dataset, LOTS of dates, all we need is a separate UUID to distinguish this part of the request from the rest of the parent request.
# # # # Descriptions of types: Seasonal Forecast types are a request for data on ALL 10 ensembles.  So this means we need a secondary layer of UUIDs for each mask, for each ensemble.
# # # # # For seasonal forecast, check this UUID to get at the given mask's UUID current_mask_and_storage_uuid_for_SeasonalForecast
# # At the end of the day, data is stored in a hierarchy like this
# # # JobUUID  - Seen by the API to get the final data set and progress
# # # datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast          - Holds information related to each datatype category
# # # current_mask_and_storage_uuid     - This is the UUID to use for accessing mask file AND storage progress of each type.

def get_workList_for_headProcessor_for_MonthlyRainfallAnalysis_types(uniqueid, request):
    worklist = []
    datatype_uuid_for_CHIRPS = uu.getUUID()
    datatype_uuid_for_SeasonalForecast = uu.getUUID()
    # Process incoming params
    worklist_CHIRPS             = _MonthlyRainfallAnalysis__make_CHIRPS_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast)

    worklist_SeasonalForecast   = _MonthlyRainfallAnalysis__make_SeasonalForecast_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast)

    worklist = worklist + worklist_CHIRPS
    worklist = worklist + worklist_SeasonalForecast
    return worklist

def get_output_for_MonthlyRainfallAnalysis_from(raw_items_list):

        avg_percentiles_dataLines = []
        monthavg=[]
        chirps_25=[]
        chirps_50 = []
        chirps_75 = []
        months=[]
        for item in raw_items_list:
            if item["sub_type_name"] == 'CHIRPS_REQUEST':
                current_full_date = item['date']
                chirps25 = item['value']['avg'][0]
                chirps50 = item['value']['avg'][1]
                chirps75 = item['value']['avg'][2]
                chirps_25.append(chirps25)
                chirps_50.append(chirps50)
                chirps_75.append(chirps75)
                current_month = current_full_date.split('/')[0]
            if item["sub_type_name"] == 'SEASONAL_FORECAST':
                current_full_date = item['date']
                monthavg.append(item['value']['avg'])
                months.append( current_full_date.split('/')[0])

        for i in range(len(chirps_25)):
            avg_percentiles_dataLine = {
                'col01_Month': str(months[i]),
                'col02_MonthlyAverage': str(monthavg[i]),
                'col03_25thPercentile': str(chirps_25[i]),
                'col04_75thPercentile': str(chirps_75[i]),
                'col05_50thPercentile': str(chirps_50[i])
            }
            avg_percentiles_dataLines.append(avg_percentiles_dataLine)


        final_output = {
            'avg_percentiles_dataLines': avg_percentiles_dataLines,
        }

        return final_output