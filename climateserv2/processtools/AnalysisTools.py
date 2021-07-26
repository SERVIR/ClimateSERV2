# Kris Stanton (Cris Squared)
# 2017

import CHIRPS.utils.geo.geoutils as geoutils
import CHIRPS.utils.processtools.dateprocessor as dproc
import CHIRPS.utils.configuration.parameters as params
import CHIRPS.utils.processtools.uutools as uu
import CHIRPS.utils.file.dateutils as dateutils
import CHIRPS.utils.geo.clippedmaskgenerator as mg
import CHIRPS.utils.geo.shapefile.readShapesfromFiles as sf
import CHIRPS.utils.file.MaskTempStorage  as mst
import CHIRPS.utils.file.npmemmapstorage as rp
import CHIRPS.utils.file.ExtractTifFromH5 as extractTif
import numpy as np
#import CHIRPS.utils.file.ExtractTifFromH5 as extractTif
import time





# Monthly analysis section (stateless)  START
# Monthly analysis section (stateless)  START
# Monthly analysis section (stateless)  START


# #  PHASE I SUPPORT - Head Processor, setting up incomming request..     START
# #  PHASE I SUPPORT - Head Processor, setting up incomming request..     START
# #  PHASE I SUPPORT - Head Processor, setting up incomming request..     START


# Big picture Procedural Process
# (1) Do some stuff for Chirps
# (2) Do some stuff for Seasonal Forecast Models
# (4) Combine it all together in a new dataset with multiple arrays (because we will have multiple graphs on the server later)
# (5) Store it all in a way that can be easily retrieved (JSON, in a BSD) and sent back to the client

# Settings - Dates for Chirps are hardcoded because we don't have a dynamic system for getting the dates.
# # Also note, the date is a historical compilation, so this means adding another year won't change it by much
# # To add another year, make sure we have the dataset and then just change the 'late' dates
# # If we implement a system for keeping track of the temporal bounds of the datasets, then this can be dynamic as it should be.
# Early == Start,   Late == End
chirps_dateRange_earlyYear = "1981" # "2009" #"1981"  # 2009 is local test data   # 1981 is the live data
chirps_dateRange_earlyMonth = "01"
chirps_dateRange_earlyDay = "01"
chirps_dateRange_lateYear = "2016" # "2016"  #"2010" #"2016"   # 2010 is local test data  # 2016 is the live data
chirps_dateRange_lateMonth = "02"  # "10"
chirps_dateRange_lateDay = "01"    # "26"
chirps_dataType = 0
seasonalForecast_dataType_list = [7, 9, 11, 13, 15, 17, 19, 21, 23, 25]  # ens01 - ens10 for Tempurature variables

# Decided to pull these in from the client (for flexibility)
# def get_seasonalForecast_StartDate():
#     pass
# def get_seasonalForecast_EndDate():
#     pass

def _MonthlyRainfallAnalysis__make_CHIRPS_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast):
    worklist = []
    sub_type_name = 'CHIRPS_REQUEST'  # Choices for now are: 'CHIRPS_REQUEST' and 'SEASONAL_FORECAST'

    datatype = chirps_dataType  # Much of the copy/paste code already references this as 'datatype'
    begintime = chirps_dateRange_earlyMonth + "/" + chirps_dateRange_earlyDay + "/" + chirps_dateRange_earlyYear
    endtime = chirps_dateRange_lateMonth + "/" + chirps_dateRange_lateDay + "/" + chirps_dateRange_lateYear
    intervaltype = 0    # Daily
    operationtype = 5   # 5 == average, 0 == max, 1 == min

    size = params.getGridDimension(int(datatype))
    dates = dproc.getListOfTimes(begintime, endtime, intervaltype)

    if (intervaltype == 0):
        dates = params.dataTypes[datatype]['indexer'].cullDateList(dates)

    # PROCESS GEOMETRY STUFF NOW
    geotransform, wkt = rp.getSpatialReference(int(datatype))
    # User Drawn Polygon
    #bounds = None
    #mask = None
    polygon_Str_ToPass = None
    if ('geometry' in request):
        # Get the polygon string
        polygonstring = request['geometry']
        # Process input polygon string
        geometry = geoutils.decodeGeoJSON(polygonstring)
        # # this is not a download type or a climate model type  --START
        polygon_Str_ToPass = polygonstring
        bounds, mask = mg.rasterizePolygon(geotransform, size[0], size[1], geometry)
    # # this is not a download type or a climate model type  --END
    # User Selected a Feature
    elif ('layerid' in request):
        layerid = request['layerid']
        featureids = request['featureids']
        geometries = sf.getPolygons(layerid, featureids)

        # If we MUST have a polygon_Str_ToPass, uncomment the next two lines.
        #polygonstring = extractTif.get_ClimateDataFiltered_PolygonString_FromMultipleGeometries(geometries)
        #polygon_Str_ToPass = polygonstring

        # # this is not a download type or a climate model type --START
        bounds, mask = mg.rasterizePolygons(geotransform, size[0], size[1], geometries)
        # # this is not a download type or a climate model type --END
    # if no cached polygon exists rasterize polygon
    clippedmask = mask[bounds[2]:bounds[3], bounds[0]:bounds[1]]

    # TODO, Create System of multiple masks for the Monthly Analysis process.
    # self.__writeMask__(uniqueid, clippedmask, bounds)  # mst.writeHMaskToTempStorage(uid,array,bounds)
    #mst.writeHMaskToTempStorage(uniqueid,clippedmask,bounds)        # NEED TO FIND OUT HOW AND WHERE THIS IS USED IN THE DEEPER PROCESSING CODE, AND MAKE A SYSTEM THAT WILL ALLOW MORE THAN JUST ONE MASK..
    mst.writeHMaskToTempStorage(datatype_uuid_for_CHIRPS, clippedmask, bounds)
    del mask
    del clippedmask


    # Build the worklist for each date in the dates
    for date in dates:
        workid = uu.getUUID()
        workdict = {'uid': uniqueid,
                    'workid': workid,
                    'datatype': datatype,
                    'operationtype': operationtype,
                    'intervaltype': intervaltype,
                    'bounds': bounds,
                    'polygon_Str_ToPass': polygon_Str_ToPass,
                    'datatype_uuid_for_CHIRPS': datatype_uuid_for_CHIRPS,
                    'datatype_uuid_for_SeasonalForecast': datatype_uuid_for_SeasonalForecast,
                    'current_mask_and_storage_uuid': datatype_uuid_for_CHIRPS,                  # Only one chirps type request needed so using same uuid
                    'sub_type_name': sub_type_name, 'derived_product': True, 'special_type': 'MonthlyRainfallAnalysis' }
        # Daily dates processing # if (intervaltype == 0):  # It is in this case, daily.
        workdict['year'] = date[2]
        workdict['month'] = date[1]
        workdict['day'] = date[0]
        dateObject = dateutils.createDateFromYearMonthDay(date[2], date[1], date[0])
        workdict['isodate'] = dateObject.strftime(params.intervals[0]['pattern'])
        workdict['epochTime'] = dateObject.strftime("%s")
        worklist.extend([workdict])  # Basically adds the entire workdict object to the worklist (could also be written as, worklist.append(workdict)

    return worklist
	
def _MonthlyRainfallAnalysis__make_CHIRPS_GEFS_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast):
    worklist = []
    sub_type_name = 'SEASONAL_FORECAST'  # Choices for now are: 'CHIRPS_REQUEST' and 'SEASONAL_FORECAST'

    datatype = 32  # Much of the copy/paste code already references this as 'datatype'
    begintime = chirps_dateRange_earlyMonth + "/" + chirps_dateRange_earlyDay + "/" + chirps_dateRange_earlyYear
    endtime = chirps_dateRange_lateMonth + "/" + chirps_dateRange_lateDay + "/" + chirps_dateRange_lateYear
    intervaltype = 0    # Daily
    operationtype = 5   # 5 == average, 0 == max, 1 == min

    size = params.getGridDimension(int(datatype))
    dates = dproc.getListOfTimes(begintime, endtime, intervaltype)

    if (intervaltype == 0):
        dates = params.dataTypes[datatype]['indexer'].cullDateList(dates)

    # PROCESS GEOMETRY STUFF NOW
    geotransform, wkt = rp.getSpatialReference(int(datatype))
    # User Drawn Polygon
    #bounds = None
    #mask = None
    polygon_Str_ToPass = None
    if ('geometry' in request):
        # Get the polygon string
        polygonstring = request['geometry']
        # Process input polygon string
        geometry = geoutils.decodeGeoJSON(polygonstring)
        # # this is not a download type or a climate model type  --START
        polygon_Str_ToPass = polygonstring
        bounds, mask = mg.rasterizePolygon(geotransform, size[0], size[1], geometry)
    # # this is not a download type or a climate model type  --END
    # User Selected a Feature
    elif ('layerid' in request):
        layerid = request['layerid']
        featureids = request['featureids']
        geometries = sf.getPolygons(layerid, featureids)

        # If we MUST have a polygon_Str_ToPass, uncomment the next two lines.
        #polygonstring = extractTif.get_ClimateDataFiltered_PolygonString_FromMultipleGeometries(geometries)
        #polygon_Str_ToPass = polygonstring

        # # this is not a download type or a climate model type --START
        bounds, mask = mg.rasterizePolygons(geotransform, size[0], size[1], geometries)
        # # this is not a download type or a climate model type --END
    # if no cached polygon exists rasterize polygon
    clippedmask = mask[bounds[2]:bounds[3], bounds[0]:bounds[1]]

    # TODO, Create System of multiple masks for the Monthly Analysis process.
    # self.__writeMask__(uniqueid, clippedmask, bounds)  # mst.writeHMaskToTempStorage(uid,array,bounds)
    #mst.writeHMaskToTempStorage(uniqueid,clippedmask,bounds)        # NEED TO FIND OUT HOW AND WHERE THIS IS USED IN THE DEEPER PROCESSING CODE, AND MAKE A SYSTEM THAT WILL ALLOW MORE THAN JUST ONE MASK..
    mst.writeHMaskToTempStorage(datatype_uuid_for_CHIRPS, clippedmask, bounds)
    del mask
    del clippedmask


    # Build the worklist for each date in the dates
    for date in dates:
        workid = uu.getUUID()
        workdict = {'uid': uniqueid,
                    'workid': workid,
                    'datatype': datatype,
                    'operationtype': operationtype,
                    'intervaltype': intervaltype,
                    'bounds': bounds,
                    'polygon_Str_ToPass': polygon_Str_ToPass,
                    'datatype_uuid_for_CHIRPS': datatype_uuid_for_CHIRPS,
                    'datatype_uuid_for_SeasonalForecast': datatype_uuid_for_SeasonalForecast,
                    'current_mask_and_storage_uuid': datatype_uuid_for_CHIRPS,                  # Only one chirps type request needed so using same uuid
                    'sub_type_name': sub_type_name, 'derived_product': True, 'special_type': 'MonthlyRainfallAnalysis' }
        # Daily dates processing # if (intervaltype == 0):  # It is in this case, daily.
        workdict['year'] = date[2]
        workdict['month'] = date[1]
        workdict['day'] = date[0]
        dateObject = dateutils.createDateFromYearMonthDay(date[2], date[1], date[0])
        workdict['isodate'] = dateObject.strftime(params.intervals[0]['pattern'])
        workdict['epochTime'] = dateObject.strftime("%s")
        worklist.extend([workdict])  # Basically adds the entire workdict object to the worklist (could also be written as, worklist.append(workdict)

    return worklist


# Need to make a request for all 10 ensembles of precipitation.
def _MonthlyRainfallAnalysis__make_SeasonalForecast_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast):
    worklist = []
    sub_type_name = 'SEASONAL_FORECAST'  # Choices for now are: 'CHIRPS_REQUEST' and 'SEASONAL_FORECAST'
    seasonal_start_date = request['seasonal_start_date']
    seasonal_end_date = request['seasonal_end_date']
    begintime = str(seasonal_start_date.split('_')[1]) + "/" + str(seasonal_start_date.split('_')[2]) + "/" + str(seasonal_start_date.split('_')[0])
    endtime = str(seasonal_end_date.split('_')[1]) + "/" + str(seasonal_end_date.split('_')[2]) + "/" + str(seasonal_end_date.split('_')[0])
    intervaltype = 0  # Daily
    operationtype = 5  # 5 == average, 0 == max, 1 == min
    placeholder_datatype = seasonalForecast_dataType_list[0]
    size = params.getGridDimension(int(placeholder_datatype))
    dates = dproc.getListOfTimes(begintime, endtime, intervaltype)
    if (intervaltype == 0):
        dates = params.dataTypes[placeholder_datatype]['indexer'].cullDateList(dates)


    # Iterate through all seasonalForecast dataTypes
    for seasonalForecast_dataType in seasonalForecast_dataType_list:
        datatype = seasonalForecast_dataType  # Much of the copy/paste code already references this as 'datatype'

        # PROCESS GEOMETRY STUFF NOW
        polygon_Str_ToPass = None
        geotransform, wkt = rp.getSpatialReference(int(datatype))
        if ('geometry' in request):
            # Get the polygon string
            polygonstring = request['geometry']
            # Process input polygon string
            geometry = geoutils.decodeGeoJSON(polygonstring)

            # # this IS a climate model type  --START
            polygon_Str_ToPass = extractTif.get_ClimateDataFiltered_PolygonString_FromSingleGeometry(geometry)
            geometry = geoutils.decodeGeoJSON(polygon_Str_ToPass)
            bounds, mask = mg.rasterizePolygon(geotransform, size[0], size[1], geometry)
            # # this IS a climate model type  --END

            # # this is not a download type or a climate model type  --START
            #polygon_Str_ToPass = polygonstring
            #bounds, mask = mg.rasterizePolygon(geotransform, size[0], size[1], geometry)
            # # this is not a download type or a climate model type  --END

            # Fail.. remove this code!
            # This IS a ClimateModel Type (Modeling code after existing code here)
            #polygon_Str_ToPass = polygonstring
            #polygon_Str_ToPass = extractTif.get_ClimateDataFiltered_PolygonString_FromSingleGeometry(geometry)
            #geometry = geoutils.decodeGeoJSON(polygon_Str_ToPass)
            #bounds, mask = mg.rasterizePolygon(geotransform, size[0], size[1], geometry)

        # User Selected a Feature
        elif ('layerid' in request):
            layerid = request['layerid']
            featureids = request['featureids']
            geometries = sf.getPolygons(layerid, featureids)

            # If we MUST have a polygon_Str_ToPass, uncomment the next two lines.
            # polygonstring = extractTif.get_ClimateDataFiltered_PolygonString_FromMultipleGeometries(geometries)
            # polygon_Str_ToPass = polygonstring

            # # this IS a download type or a climate model type --START
            polygonstring = extractTif.get_ClimateDataFiltered_PolygonString_FromMultipleGeometries(geometries)
            polygon_Str_ToPass = polygonstring
            geometry = geoutils.decodeGeoJSON(polygonstring)
            bounds, mask = mg.rasterizePolygon(geotransform, size[0], size[1], geometry)
            # # this IS a download type or a climate model type --END

            # # this is not a download type or a climate model type --START
            # The 'else' where it is NOT a seasonal forecast type.
            #bounds, mask = mg.rasterizePolygons(geotransform, size[0], size[1], geometries)
            # # this is not a download type or a climate model type --END

        # if no cached polygon exists rasterize polygon
        clippedmask = mask[bounds[2]:bounds[3], bounds[0]:bounds[1]]

        # TODO, Create System of multiple masks for the Monthly Analysis process.
        # self.__writeMask__(uniqueid, clippedmask, bounds)  # mst.writeHMaskToTempStorage(uid,array,bounds)
        # mst.writeHMaskToTempStorage(uniqueid,clippedmask,bounds)        # NEED TO FIND OUT HOW AND WHERE THIS IS USED IN THE DEEPER PROCESSING CODE, AND MAKE A SYSTEM THAT WILL ALLOW MORE THAN JUST ONE MASK..
        current_mask_uuid_for_SeasonalForecast = uu.getUUID()
        mst.writeHMaskToTempStorage(current_mask_uuid_for_SeasonalForecast, clippedmask, bounds)
        del mask
        del clippedmask

        # Build the worklist for each date in the dates
        for date in dates:
            workid = uu.getUUID()
            workdict = {'uid': uniqueid,
                        'workid': workid,
                        'datatype': datatype,
                        'operationtype': operationtype,
                        'intervaltype': intervaltype,
                        'bounds': bounds,
                        'polygon_Str_ToPass': polygon_Str_ToPass,
                        'datatype_uuid_for_CHIRPS': datatype_uuid_for_CHIRPS,
                        'datatype_uuid_for_SeasonalForecast': datatype_uuid_for_SeasonalForecast,
                        'current_mask_and_storage_uuid': current_mask_uuid_for_SeasonalForecast,
                        'sub_type_name': sub_type_name, 'derived_product': True, 'special_type': 'MonthlyRainfallAnalysis' }
            # Daily dates processing # if (intervaltype == 0):  # It is in this case, daily.
            workdict['year'] = date[2]
            workdict['month'] = date[1]
            workdict['day'] = date[0]
            dateObject = dateutils.createDateFromYearMonthDay(date[2], date[1], date[0])
            workdict['isodate'] = dateObject.strftime(params.intervals[0]['pattern'])
            workdict['epochTime'] = dateObject.strftime("%s")
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


    # (B) Get the CHIRPS and Monthly Seasonal Forecast Worklists

    # (C) Do any additional stuff needed at the pre-processing level right here?


    # Procedural process
    # Focus on making the tasks for all the workers for (1), and (2).

    # CHIRPS
    # # Needs to be all historical Chirps dates.  We need the following items from each chirps date
    # # # FOR JUST THIS FIRST PART (we only need the CHIRPS AVERAGE dataset)
    # # # FOR ALL THE REST OF THE PARTS (POST PROCESSING)
    # # # # We need to create a collection of all the monthly averages, and then get the 25, 50, 75 percentile

    # SEASONAL FORECAST
    # # FOR JUST THIS FIRST PART
    # # # Need the 10 ensembles, Average
    # # FOR ALL THE REST OF THE PARTS (POST PROCESSING)
    # # # For Each Month
    # # # # Combine the 10 into a min
    # # # # Combine the 10 into an Average
    # # # # Combine the 10 into a max


    # OLD NOTES - MAYBE IGNORE?
    # # # Get The averages of all chirps, then create a collection of all the monthly averages
    # # # LongTerm Average (Average over entire date range?


    #worklist.append("WHASSUP..")
    #worklist.append("TODO.. FINISH THIS SECTION!")

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

    # (A) Process incoming params
    worklist_CHIRPS             = _MonthlyRainfallAnalysis__make_CHIRPS_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast)
    worklist_SeasonalForecast   = _MonthlyRainfallAnalysis__make_SeasonalForecast_workList(uniqueid, request, datatype_uuid_for_CHIRPS, datatype_uuid_for_SeasonalForecast)
    worklist = worklist + worklist_CHIRPS
    worklist = worklist + worklist_SeasonalForecast


    # (B) Get the CHIRPS and Monthly Seasonal Forecast Worklists

    # (C) Do any additional stuff needed at the pre-processing level right here?


    # Procedural process
    # Focus on making the tasks for all the workers for (1), and (2).

    # CHIRPS
    # # Needs to be all historical Chirps dates.  We need the following items from each chirps date
    # # # FOR JUST THIS FIRST PART (we only need the CHIRPS AVERAGE dataset)
    # # # FOR ALL THE REST OF THE PARTS (POST PROCESSING)
    # # # # We need to create a collection of all the monthly averages, and then get the 25, 50, 75 percentile

    # SEASONAL FORECAST
    # # FOR JUST THIS FIRST PART
    # # # Need the 10 ensembles, Average
    # # FOR ALL THE REST OF THE PARTS (POST PROCESSING)
    # # # For Each Month
    # # # # Combine the 10 into a min
    # # # # Combine the 10 into an Average
    # # # # Combine the 10 into a max


    # OLD NOTES - MAYBE IGNORE?
    # # # Get The averages of all chirps, then create a collection of all the monthly averages
    # # # LongTerm Average (Average over entire date range?


    #worklist.append("WHASSUP..")
    #worklist.append("TODO.. FINISH THIS SECTION!")

    return worklist



# #  PHASE I SUPPORT - Head Processor, setting up incomming request..     END
# #  PHASE I SUPPORT - Head Processor, setting up incomming request..     END
# #  PHASE I SUPPORT - Head Processor, setting up incomming request..     END

# #  PHASE II SUPPORT - Worker.__dowork__ function, Handling various UUID Hierarchy and doing the same thing worker.__dowork__ does     START
# #  PHASE II SUPPORT - Worker.__dowork__ function, Handling various UUID Hierarchy and doing the same thing worker.__dowork__ does     START
# #  PHASE II SUPPORT - Worker.__dowork__ function, Handling various UUID Hierarchy and doing the same thing worker.__dowork__ does     START

# Nope, nothing really needed here..

# #  PHASE II SUPPORT - Worker.__dowork__ function, Handling various UUID Hierarchy and doing the same thing worker.__dowork__ does     END
# #  PHASE II SUPPORT - Worker.__dowork__ function, Handling various UUID Hierarchy and doing the same thing worker.__dowork__ does     END
# #  PHASE II SUPPORT - Worker.__dowork__ function, Handling various UUID Hierarchy and doing the same thing worker.__dowork__ does     END


# #  PHASE III SUPPORT - Head Processor, Read 'self.finished_items' array and convert it to actionable data to be output and ready to be graphed.     START
# #  PHASE III SUPPORT - Head Processor, Read 'self.finished_items' array and convert it to actionable data to be output and ready to be graphed.     START
# #  PHASE III SUPPORT - Head Processor, Read 'self.finished_items' array and convert it to actionable data to be output and ready to be graphed.     START

# datatype, sub_type_name, current_mask_and_storage_uuid

# Example of: self.finished_items[0]:
# # {
# #     u'epochTime': u'1230786000',
# #     u'datatype': 0,
# #     u'sub_type_name': u'CHIRPS_REQUEST',
# #     u'workid': u'd25ae615-550d-4925-b630-165e3933980f',
# #     u'current_mask_and_storage_uuid': u'20fb9e16-066c-44a7-83c8-f617ef70c85b',
# #     u'value': {u'avg': 0.0},
# #     u'date': u'1/1/2009',
# #     u'derived_product': True
# # }
def get_output_for_MonthlyRainfallAnalysis_from(raw_items_list):

    # Data Buckets
    # Need to resort the way this data is contained, so we can perform operations on various parts separately.
    organized_container = {}
    # First Pass, separate all datasets.
    for raw_item in raw_items_list:
        current_dataset_uuid = raw_item['current_mask_and_storage_uuid']
        # Append the current item into the bucket, or create a new bucket and then append the current item into that bucket.
        try:
            organized_container[current_dataset_uuid].append(raw_item)
        except:
            organized_container[current_dataset_uuid] = []
            organized_container[current_dataset_uuid].append(raw_item)

    # List of strings that tell us which buckets exist so we can go over them one by one.
    keys_for_organized_container = organized_container.keys()

    return_dataset_info_list = []
    # Iterate over each dataset and do the processing to them.
    for dataset_key in keys_for_organized_container:

        current_dataset = organized_container[dataset_key]  # current_dataset is an array of raw_items

        # Let's go inside one item and grab some of the meta info.
        out_datatype = ""
        out_subTypeName = ""
        out_storageUUID = ""
        try:
            #datatype, sub_type_name, current_mask_and_storage_uuid
            out_datatype = current_dataset[0]['datatype']
            out_subTypeName = current_dataset[0]['sub_type_name']
            out_storageUUID = current_dataset[0]['current_mask_and_storage_uuid']
        except:
            pass

        # Now set up Ashutosh's Numpy work right here.
        # np

        # SeasonalFcstAnalysis.py Line 42 - 46 translated/ported
        #  set arrays
        npList_mon = np.zeros(len(current_dataset), 'i')  # npList_mon was just mon # np was N # 'current_dataset' was 'datax'
        npList_day = np.zeros(len(current_dataset), 'i')
        npList_year = np.zeros(len(current_dataset), 'i')
        npList_xtemps = np.zeros(len(current_dataset), 'd')

        # SeasonalFcstAnalysis.py Line 53 - 59 translated/ported
        for i in xrange(len(current_dataset)):
            current_dataset_item = current_dataset[i]
            current_full_date = current_dataset_item['date']
            current_avg_value = current_dataset_item['value']['avg']
            current_month = current_full_date.split('/')[0]
            current_day = current_full_date.split('/')[1]
            current_year = current_full_date.split('/')[2]

            #data2 = datax[i].split('/')
            npList_mon[i] = current_month #data2[0]
            npList_day[i] = current_day #data2[1]
            #data3 = data2[2].split(',')
            npList_year[i] = current_year # data3[0]
            npList_xtemps[i] = current_avg_value #data3[1]

        # SeasonalFcstAnalysis.py Line 62 - 64 translated/ported
        # find the min and max years
        minyr = int(np.min(npList_year)) # minyr = int(N.min(year))
        maxyr = int(np.max(npList_year)) # maxyr = int(N.max(year))

        # SeasonalFcstAnalysis.py Line 67 - 73 translated/ported
        # ________________________
        #  Monthly summary statistics for EVERY year
        # Set the statistics arrays. Set it for number of years (maxyr-minyr+1) and 12 months
        temps = np.zeros(((maxyr - minyr + 1), 12), dtype='d')
        msum = np.zeros(((maxyr - minyr + 1), 12), dtype='d')
        mhits = np.zeros(((maxyr - minyr + 1), 12), dtype='i')
        xline = np.zeros(12, dtype='i')

        # SeasonalFcstAnalysis.py Line 74 - 78 translated/ported
        #  Do the sums for the months of data for each month of each year
        # for k in xrange(len(datax)):
        #     temps[(year[k] - minyr), mon[k] - 1] = xtemps[k]
        #     msum[(year[k] - minyr), mon[k] - 1] = msum[(year[k] - minyr), mon[k] - 1] + xtemps[k]
        #     mhits[(year[k] - minyr), mon[k] - 1] = mhits[(year[k] - minyr), mon[k] - 1] + 1
        for k in xrange(len(current_dataset)):
            temps[(npList_year[k] - minyr), npList_mon[k] - 1] = npList_xtemps[k]
            msum[(npList_year[k] - minyr), npList_mon[k] - 1] = msum[(npList_year[k] - minyr), npList_mon[k] - 1] + npList_xtemps[k]
            mhits[(npList_year[k] - minyr), npList_mon[k] - 1] = mhits[(npList_year[k] - minyr), npList_mon[k] - 1] + 1

        # SeasonalFcstAnalysis.py Line 80 - 86 translated/ported
        # print/write the data out
        # for j in xrange(maxyr - minyr + 1):
        #     for k in xrange(12):
        #         #    print minyr+j,k+1, msum[j,k],mhits[j,k]
        #         xprint = str(minyr + j) + ' ' + str(k + 1) + ' ' + str(msum[j, k]) + ' ' + str(mhits[j, k])
        #         f.write(xprint)
        #         f.write('\n')
        # USED TO WRITE TO _SUMMARY.TXT
        year_month_dataLines = []
        for j in xrange(maxyr - minyr + 1):
            for k in xrange(12):
                minyr_j_col01 = str(minyr + j)
                k_1_col02 = str(k + 1)
                msum_j_k_col03 = str(msum[j, k])
                mhits_j_k_col04 = str(mhits[j, k])
                dataLine = {
                    'minyr_j_col01':minyr_j_col01,
                    'k_1_col02': k_1_col02,
                    'msum_j_k_col03': msum_j_k_col03,
                    'mhits_j_k_col04': mhits_j_k_col04
                }
                year_month_dataLines.append(dataLine)
                # #    print minyr+j,k+1, msum[j,k],mhits[j,k]
                # xprint = str(minyr + j) + ' ' + str(k + 1) + ' ' + str(msum[j, k]) + ' ' + str(mhits[j, k])
                # f.write(xprint)
                # f.write('\n')

        # SeasonalFcstAnalysis.py Line 88 - 97 translated/ported
        # ___________________
        # Monthy average Summary Statistics
        # set up arrays
        msum2 = np.zeros(12, dtype='d')
        mhits2 = np.zeros(12, dtype='i')
        p75 = np.zeros(12, dtype='d')
        p25 = np.zeros(12, dtype='d')
        x45 = np.zeros(18, dtype='d')
        x45 = msum[:, 1]
        # print x45


        # SeasonalFcstAnalysis.py Line 99 - 104 translated/ported
        # Compute percentiles
        for k in xrange(12):
            x45 = msum[:, k]
            p75[k] = np.percentile(x45, 75)
            p25[k] = np.percentile(x45, 25)
            xline[k] = k + 1

        # SeasonalFcstAnalysis.py Line 108 - 112 translated/ported
        # Do the sums over the same month in all years
        for j in xrange(12):
            for k in xrange(maxyr - minyr + 1):
                msum2[j] = msum2[j] + msum[k, j]
                mhits2[j] = mhits2[j] + 1

        # SeasonalFcstAnalysis.py Line 114 - 129 translated/ported
        #         # print/write the data out
        # f3.write('Month  MonthlyAverage    25thPercentile  75thPercentile  #YearsInAnalysis')
        # f3.write('\n')
        #
        # for k in xrange(12):
        #     if mhits2[k] > 0:
        #         print k + 1, msum2[k] / (mhits2[k]), mhits2[k], p25[k], p75[k]
        #     else:
        #         print k + 1, 0.0, 0.0, 0.0, 0.0
        #
        #     if mhits2[k] > 0:
        #         xprint2 = str(k + 1) + ' ' + str(msum2[k] / (mhits2[k])) + ' ' + ' ' + str(p25[k]) + ' ' + str(
        #             p75[k]) + ' ' + str(mhits2[k])
        #     else:
        #         xprint2 = str(k + 1) + ' ' + str(0.0) + ' ' + str(0.0) + ' ' + str(0.0) + ' ' + str(0.0)
        #     f3.write(xprint2)
        #     f3.write('\n')
        #        # print/write the data out
        avg_percentiles_dataLines = []
        avg_percentiles_Headings = 'Month, MonthlyAverage, 25thPercentile, 75thPercentile, #YearsInAnalysis'
        #f3.write('Month  MonthlyAverage    25thPercentile  75thPercentile  #YearsInAnalysis')
        #f3.write('\n')
        for k in xrange(12):
            col01_Month = ""
            col02_MonthlyAverage = ""
            col03_25thPercentile = ""
            col04_75thPercentile = ""
            col05_YearsInAnalysis = ""
            # Looks like this bit was just debugging (print statments) on the original version of the code.
            # if mhits2[k] > 0:
            #     #print k + 1, msum2[k] / (mhits2[k]), mhits2[k], p25[k], p75[k]
            #     col01_Month = k + 1
            #     col02_MonthlyAverage = msum2[k] / (mhits2[k])
            #     col03_25thPercentile = mhits2[k]
            #     col04_75thPercentile = p25[k]
            #     col05_YearsInAnalysis = p75[k]
            # else:
            #     #print k + 1, 0.0, 0.0, 0.0, 0.0
            #     col01_Month = k + 1
            #     col02_MonthlyAverage = 0.0
            #     col03_25thPercentile = 0.0
            #     col04_75thPercentile = 0.0
            #     col05_YearsInAnalysis = 0.0
            if mhits2[k] > 0:
                #xprint2 = str(k + 1) + ' ' + str(msum2[k] / (mhits2[k])) + ' ' + ' ' + str(p25[k]) + ' ' + str(p75[k]) + ' ' + str(mhits2[k])
                col01_Month = k + 1
                col02_MonthlyAverage = msum2[k] / (mhits2[k])
                col03_25thPercentile = p25[k]
                col04_75thPercentile = p75[k]
                col05_YearsInAnalysis = mhits2[k]
            else:
                #xprint2 = str(k + 1) + ' ' + str(0.0) + ' ' + str(0.0) + ' ' + str(0.0) + ' ' + str(0.0)
                col01_Month = k + 1
                col02_MonthlyAverage = 0.0
                col03_25thPercentile = 0.0
                col04_75thPercentile = 0.0
                col05_YearsInAnalysis = 0.0

            #f3.write(xprint2)
            #f3.write('\n')
            avg_percentiles_dataLine = {
                'col01_Month':str(col01_Month),
                'col02_MonthlyAverage':str(col02_MonthlyAverage),
                'col03_25thPercentile':str(col03_25thPercentile),
                'col04_75thPercentile':str(col04_75thPercentile),
                'col05_YearsInAnalysis':str(col05_YearsInAnalysis)
            }
            avg_percentiles_dataLines.append(avg_percentiles_dataLine)


        dataset_info_obj = {
            'year_month_dataLines':year_month_dataLines,
            'avg_percentiles_dataLines':avg_percentiles_dataLines,
            'avg_percentiles_Headings':avg_percentiles_Headings,
            'dataset_key':dataset_key,
            'out_datatype':out_datatype,
            'out_subTypeName':out_subTypeName,
            'out_storageUUID':out_storageUUID
            #'what_else':'more goes here!'
        }
        return_dataset_info_list.append(dataset_info_obj)


        # for current_dataset_item in current_dataset:
        #     # THIS IS WHERE TO PROCESS THE DATA.
        #     pass

    # def __sortData__(self,array):
    #     newlist = sorted(array, key=itemgetter('epochTime'))
    #     return newlist



    # # Iterate through all the items
    # for raw_item in raw_items_list:
    #     current_full_date = raw_item['date']
    #     current_avg_value = raw_item['value']['avg']
    #     current_month = current_full_date.split('/')[0]
    #     current_day = current_full_date.split('/')[1]
    #     current_year = current_full_date.split('/')[2]
    #     current_dataset_uuid = raw_item['current_mask_and_storage_uuid']
    #
    #     # Append the data item to the list, or create a new item
    #     try:
    #         organized_container[current_dataset_uuid].append()
    #     except:
    #         pass


    # We are going to need to convert the raw_items_list into
    # num_of_data_entries_for_current_dataset = 55 # Need to

    final_output = {
        'dataset_info_list':return_dataset_info_list,
        #'separated_datasets':organized_container
        #'output_key':'output_value'
    }

    return final_output

# #  PHASE III SUPPORT - Head Processor, Read 'self.finished_items' array and convert it to actionable data to be output and ready to be graphed.     END
# #  PHASE III SUPPORT - Head Processor, Read 'self.finished_items' array and convert it to actionable data to be output and ready to be graphed.     END
# #  PHASE III SUPPORT - Head Processor, Read 'self.finished_items' array and convert it to actionable data to be output and ready to be graphed.     END


# Monthly analysis section (stateless)  END
# Monthly analysis section (stateless)  END
# Monthly analysis section (stateless)  END




# OLDER STUFF (around March 2017 time frame) BELOW  (MAYBE SOME OF IT WILL BE USABLE?)


# So, here is how this is all going to work..

# (SINGLE THREAD)
# First, The head worker reads in the ZMQ data from the API,
# # During that read, job tasks are split up and sent to the worker queues.
# # # HeadProcessor should assign worker tasks to do ALL of the following, (Get ALL Chirps Data), (Get ALL Seasonal Forecast Data)

# (MULTIPLE THREADS - Parallel)
# Second, All the "components" get executed one by one.
# # This means, getting all chirps data, getting all ensemble data, etc

# (SINGLE THREAD)
# Third, All of those threads complete, and a final process gets called which ?sets the completed job status to 100%?
# # This means collating all data into a single return object.

# In short, we can intercept ALL of these processes and have the code execute from here (including responsibility of updating status)
# # Perhaps, some of those processes can still execute in the head and worker threads.. and just different parts get called here..

# Called from the Head Processor during receive function
def execute_part_1_of_3_analysis_tools_adapter():
    pass

# Called from the Worker Threads, (This may not even be necessary??  or maybe just expose a bunch of useful functions that support what the workers are doing?.)
def execute_part_2_of_3_analysis_tools_adapter():
    pass

# Called from the Head Processor, after any worker thread jobs are done.  This may be a good place to collate all the data together.
def execute_part_3_of_3_analysis_tools_adapter():
    pass

def test_function():
    '''
    Just to make sure the module is properly imported.
    '''
    return 'test_function reached the end'

def test_function2():
    '''
    Just to make sure the module is properly imported.
    '''
    return 'test_function2 reached the end'




# Console Test Notes
#  Open Console
#  sys.path.append('/Users/ks/ALL/CrisSquared/SoftwareProjects/SERVIR/ClimateSERV/Chirps/serviringest')
#  import CHIRPS.utils.processtools.AnalysisTools as at
#  at.test_function2() # Or whatever function we need to test..
