import csv
import os
import sys
import time
import calendar
import zmq
import json
import shutil
import numpy as np
import subprocess
from zipfile import ZipFile
from os.path import basename
from copy import deepcopy
from operator import itemgetter
import rasterio as rio

module_path = os.path.abspath(os.getcwd())
if module_path not in sys.path:
    sys.path.append(module_path)
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

try:
    import climateserv2.geoutils as geoutils
    import climateserv2.file.dateutils as dateutils

    import climateserv2.processtools.dateprocessor as dproc
    import climateserv2.file.TDSExtraction as GetTDSData
    import climateserv2.parameters as params
    import climateserv2.locallog.locallogging as llog
    import climateserv2.processtools.uutools as uu
    import climateserv2.geo.shapefile.readShapesfromFiles as sf
    import climateserv2.requestLog as reqLog
    import climateserv2.processtools.AnalysisTools as analysisTools
    import climateserv2.geoutils as geoutils
    import climateserv2.parameters as params
    from climateserv2.file import fileutils
except:
    import parameters as params
    import geoutils as geoutils
    import file.dateutils as dateutils
    import processtools.dateprocessor as dproc
    import file.TDSExtraction as GetTDSData
    import parameters as params
    import locallog.locallogging as llog
    import processtools.uutools as uu
    import geo.shapefile.readShapesfromFiles as sf
    import requestLog as reqLog
    import processtools.AnalysisTools as analysisTools
    import file.fileutils

class ZMQCHIRPSHeadProcessor():
    logger = llog.getNamedLogger("request_processor")
    workToBeDone = {}
    workDone = []
    name = None
    current_work_dict = None
    finished_items = []
    total_tasks_count= 0
    finished_task_count = 0
    request = None
    inputreceiver = None
    outputreceiver = None
    listeningreceiver = None
    inputconn = None
    outputconn = None
    listeningconn = None
    progress = 0
    process_start_time =0
    mathop = None
    zipFilePath=None
    isDownloadJob = False
    dj_OperationName = "download"

    # To initialize connections and start watching for inputs
    def __init__(self, name, inputconn, outputconn, listenconn):
        self.name = name
        self.inputconn = inputconn
        self.outputconn = outputconn
        self.listeningconn = listenconn
        self.logger.info("Creating Processor named: "+self.name+" listening on port: "+self.inputconn+" outputting to port: "+self.outputconn+"  listening for output on: "+self.listeningconn)
        try:
            self.logger.info("Making directories that do not exist..")
            fileutils.makeParamsFilePaths()
        except:
            self.logger.info("Could not make directories.. Please check the parameters_ops.py file for paths")
        self.__beginWatching__()

    # ZMQ Connection to watch for inputs
    def __beginWatching__(self):
        context = zmq.Context()
        self.inputreceiver = context.socket(zmq.PULL)
        self.inputreceiver.connect(self.inputconn)
        self.outputreceiver = context.socket(zmq.PUSH)
        self.outputreceiver.connect(self.outputconn)
        self.listenreceiver = context.socket(zmq.PULL)
        self.listenreceiver.connect(self.listeningconn)
        self.logger.info("Processor ("+self.name+")  Connected and Ready")
        self.__watchAgain__()

    # To load the data received from ZMQ connection, start processing and updating progress
    def __watchAgain__(self):
        while(True):
            self.logger.info("HeadProcessor ("+self.name+"): Waiting for input")
            request = json.loads(self.inputreceiver.recv())
            self.process_start_time = time.time()
            self.logger.info("Processing request "+request['uniqueid'])
            self.processWork(request)
            self.__processProgress__(self.progress)
            time_total = time.time()-self.process_start_time
            self.logger.info("Total time: "+str(time_total))
    
    # Tp log the path of Zip file having tifs
    def postProcessWork_ForDownloadTypes(self, request):
        if (self.isDownloadJob == True ):
            if (self.zipFilePath != None):
                self.logger.info("("+self.name+"):postProcessWork_ForDownloadTypes: Tif files have been zipped to: " + str(self.zipFilePath))
            else:
                self.logger.info("("+self.name+"):postProcessWork_ForDownloadTypes: ERROR ZIPPING TIF FILES.")

    # To process the request and watch for results
    def processWork(self,request):
        self.request = request
        # Get the worklist by doing some prerocessing
        error, workarray = self.__preProcessIncomingRequest__(request)
        if (error == None):
            # Workarray will be empty if there are no dates/values
            if len(workarray) == 0:
                self.progress=100.0
            else:
                self.worklist_length = len(workarray)
                self.total_task_count = len(workarray)
                # Dataset processing progress based on the worklist
                if self.isDownloadJob == False:
                    self.progress = 20 +(float(self.finished_task_count) / float(self.total_task_count)) * 80.
                # Monthly analysis processing progress based on the worklist
                if (self.derived_product == True):
                    self.progress = 20 + (float(self.finished_task_count) / float(self.total_task_count)) * 80.
            workingArray_guid_index_list = []
            for item in workarray:
                self.workToBeDone[item['workid']]= item
                workingArray_guid_index_list.append(item['workid'])
            workingArray = deepcopy(self.workToBeDone)
            self.logger.info("(" + self.name + "):processWork: About to call __watchForResults_and_keepSending__ ")
            self.__watchForResults_and_keepSending__(workingArray, workingArray_guid_index_list)
        else:
            self.logger.warning("Got an error processing request: "+str(error))
            try:
                theJobID = request['uniqueid']
            except:
                theJobID = ""
            self.__write_JobError_To_DB__(theJobID, str(error), str(request))
            # If there is any error during processing, progress is -1 indicating a processing error
            self.progress = -1
            self.__cleanup__()
            self.__processProgress__(self.progress)
            # Continue watching for more inputs
            self.__watchAgain__()
        self.logger.info("("+self.name+"):processWork: Process Work has reached the end!")

    # Divide into chunks based on size of worklist and process it
    def __watchForResults_and_keepSending__(self, workingArray, workingArray_guid_index_list):
        # Break the workingArray into chunks of this size
        message_chunkSize = 5000

        # If the working array is already less than the chunksize
        if(len(workingArray) < message_chunkSize):
            self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: About to do 'for item in workingArray' (len(workingArray)): " + str(len(workingArray)))
            item_counter = 0
            for item in workingArray:
                self.outputreceiver.send_string(json.dumps(str(workingArray[item])))
                self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: outputreceiver.send for item " + str(item_counter) + " of " + str(len(workingArray)))
                item_counter = item_counter + 1
            self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: About to call __watchForResults__ ")
            self.__watchForResults__()
            return

        # How many progress thresholds should we be checking
        number_of_progress_thresholds = (len(workingArray) / message_chunkSize) + 1
        current_workingArray_index = 0
        finished_sending_workingArray_data = False

        self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: message_chunkSize : " + str(message_chunkSize))
        self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: len(workingArray) : " + str(len(workingArray)))
        self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: number_of_progress_thresholds : " + str(number_of_progress_thresholds))

        # Send in the first chunk.
        for i in range(0, message_chunkSize):
            if (current_workingArray_index >= len(workingArray)):
                finished_sending_workingArray_data = True
            else:
                current_workid_index = workingArray_guid_index_list[current_workingArray_index]
                self.outputreceiver.send_string(json.dumps(str(workingArray[current_workid_index])) )# (workingArray[current_workingArray_index]))
                self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: outputreceiver.send for item " + str(current_workingArray_index) + " of " + str(len(workingArray)))
                current_workingArray_index = current_workingArray_index + 1

        last_chunk_sent = 1
        next_progress_threshold_to_check = ((80.0 / number_of_progress_thresholds) * last_chunk_sent)

        self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: ENTERING THE WHILE LOOP OF CHECKING PROGRESS.")
        self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: last_chunk_sent: " + str(last_chunk_sent))
        self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: next_progress_threshold_to_check: " + str(next_progress_threshold_to_check))

        # Start listening, and sending in future chunks
        while (self.progress < 100):
            # Normal receiving operation
            results = json.loads(self.listenreceiver.recv())
            self.processFinishedData(results)
            self.logger.info("("+self.name+"):__watchForResults_and_keepSending__: self.progress: " + str(self.progress))

            # Send more stuff down the queue..
            if(finished_sending_workingArray_data == True):
                self.progress=100.0
                self.__processProgress__(self.progress)
                pass
            else:
                if (self.progress > next_progress_threshold_to_check):
                    last_chunk_sent = last_chunk_sent + 1
                    next_progress_threshold_to_check = ((80.0 / number_of_progress_thresholds) * last_chunk_sent)

                    self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: PROGRESS THRESHOLD HIT: Changing the compare for the next time" )
                    self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: last_chunk_sent: " + str(last_chunk_sent))
                    self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: next_progress_threshold_to_check: " + str(next_progress_threshold_to_check))
                    self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: About to send in another chunk..")

                    # workingArray items to be processed
                    for i in range(0, message_chunkSize):
                        if (current_workingArray_index >= len(workingArray)):
                            finished_sending_workingArray_data = True
                        else:
                            current_workid_index = workingArray_guid_index_list[current_workingArray_index]
                            self.outputreceiver.send_string(eval(json.dumps(str(workingArray[current_workid_index]))))
                            self.logger.info(
                                "(" + self.name + "):__watchForResults_and_keepSending__: outputreceiver.send for item " + str(
                                    current_workingArray_index) + " of " + str(len(workingArray)))
                            current_workingArray_index = current_workingArray_index + 1
        self.__finishJob__()

    # To listen for workers to be done with their processing
    def __watchForResults__(self):
        if self.progress ==100.0:
            self.finished_items=[]
        while (self.progress < 100.0):
            try:
                results = json.loads(self.listenreceiver.recv())
            except:
                self.logger.info("Error in __WatchForResults__")
            self.processFinishedData(results)
            self.logger.info("("+self.name+"):__watchForResults__: self.progress: " + str(self.progress))
        self.__finishJob__()

    # To add the results to self.finished_items and update progress to database
    def processFinishedData(self, results):
        self.finished_task_count = self.finished_task_count +1
        self.workToBeDone.pop(results['workid'], None)
        if results['value'] is not None:
            if (self.isDownloadJob == True):
                    self.finished_items.append(results)
            else:
                    self.finished_items.append(results)
        self.__updateProgress__()

    # To sort results based on epoch time
    def __sortData__(self,array):
        try:
            newlist = sorted(array, key=itemgetter('epochTime'))
        except:
            return array
        return newlist

    # To update the progress to database
    def __updateProgress__(self,output_full=False):
        if self.total_task_count ==0:
            self.progress = 100
        else:
            self.progress = 20+(float(self.finished_task_count)/float(self.total_task_count))*80.
        if (self.progress < 100 or output_full == True):
            self.__processProgress__(self.progress)

    # To indicate that the processing of worklist is finished
    def __finishJob__(self):
        self.postProcessWork_ForDownloadTypes(self.request)
        try:
            theJobID = str(self.request['uniqueid'])
        except:
            theJobID = ""

        # Update the logging database indicating job is complete
        self.__write_JobCompleted_To_DB__(theJobID, str(self.request))

        # Need not sort the results if it is a download job as it has only path to downloaded file
        if (self.isDownloadJob == False):
            if len(self.finished_items)>0:
                self.finished_items = self.__sortData__(self.finished_items)

        if (self.derived_product == True):
            self.__outputDataForMonthlyAnalysis__()
        else:
            self.__outputData__()

        # If there are finished items, output is considered full and progress is updated to 100%
        if len(self.finished_items) > 0:
            self.__updateProgress__(output_full=True)
        self.__cleanup__()

    # Clear the objects/data arrays
    def __cleanup__(self):
        self.total_task_count = 0
        self.worklist_length = 0
        self.finished_task_count = 0
        self.current_work_dict = None
        self.finished_items = []
        # For derived product types.
        self.derived_product = False
        self.sub_types_finished = True
        self.derived_opname = "Unset"

    # To write results of a request with unique id to a file
    def __writeResults__(self,uniqueid,results):
        filename = params.getResultsFilename(uniqueid)
        f = open(filename, 'w+')
        json.dump(results,f)
        f.close()

    # To insert the progress corresponding to unique id
    def __insertProgressDb__(self,uniqueid):
        log = reqLog.Request_Progress(request_id=uniqueid, progress=0)
        log.save()
        
    # To log if the requested job has started
    def __write_JobStarted_To_DB__(self,uniqueid, objectInfo):
        try:
            theID = uniqueid
            theStatusNote = "JobStarted"
            theAdditionalNotes = "Server Job: " + str(theID) + " has started :: Object Info: " + str(objectInfo)
            rLog = reqLog.requestLog()
            rLog.logger = self.logger
            rLog.add_New_ServerSide_Request(theID, theStatusNote, theAdditionalNotes)
        except:
            pass
        
    # To log if the requested job has errors
    def __write_JobError_To_DB__(self,uniqueid,errorMessage, objectInfo):
        try:
            theID = uniqueid
            theStatusNote = "JobError"
            theAdditionalNotes = "Server Job: " + str(theID) + " had an Error.  Error Message: " + str(errorMessage) + " :: Object Info: " + str(objectInfo)
            rLog = reqLog.requestLog()
            rLog.logger = self.logger
            rLog.add_New_ServerSide_Request(theID, theStatusNote, theAdditionalNotes)
        except:
            pass
        
    # To log after the requested job is completed
    def __write_JobCompleted_To_DB__(self,uniqueid, objectInfo):
        try:
            theID = uniqueid
            theStatusNote = "JobCompleted"
            theAdditionalNotes = "Server Job: " + str(theID) + " has been completed :: Object Info: " + str(objectInfo)
            rLog = reqLog.requestLog()
            rLog.logger = self.logger
            rLog.add_New_ServerSide_Request(theID, theStatusNote, theAdditionalNotes)
        except:
            pass

    # To return True for a monthly analysis GEFS request
    def __is_custom_job_type__MonthlyGEFSRainfallAnalysis__(self, request):
        try:
            if 'custom_job_type' in request:
                custom_job_type = request['custom_job_type']
                if (custom_job_type == "MonthlyGEFSRainfallAnalysis"):
                    return True
                else:
                    return False
            else:
                return False
        except Exception as e:
            uniqueid = request['uniqueid']
            self.logger.warning("(" + self.name + "):Couldn't find custom_job_type in '__is_custom_job_type__MonthlyRainfallAnalysis__' in HeadProcessor: uniqueid: " + str(
                uniqueid) + " Exception Error Message: " + str(e))
        return False

	# To return True for a monthly analysis request
    def __is_custom_job_type__MonthlyRainfallAnalysis__(self, request):
        try:
            self.logger.info(request)
            if 'custom_job_type' in request:
                custom_job_type = request['custom_job_type']
                if (custom_job_type == "MonthlyRainfallAnalysis"):
                    return True
                else:
                    return False
            else:
                return False
        except Exception as e:
            uniqueid = request['uniqueid']
            self.logger.warning("(" + self.name + "):Couldn't find custom_job_type in '__is_custom_job_type__MonthlyRainfallAnalysis__' in HeadProcessor: uniqueid: " + str(
                uniqueid) + " Exception Error Message: " + str(e))
        return False

    def writeToTiff(self, uniqueid,dataObj):
        os.makedirs(params.zipFile_ScratchWorkspace_Path + uniqueid + '/', exist_ok=True)
        os.chmod(params.zipFile_ScratchWorkspace_Path + uniqueid + '/', 0o777)
        os.chdir(params.zipFile_ScratchWorkspace_Path + uniqueid + '/')
        fileName = dataObj.time.dt.strftime('%Y%m%dT%H%M%S').values[0] + '.tif'
        width = dataObj.longitude.size  # HOW DOES THIS CHANGE IF WE HAVE 2D LAT/LON ARRAYS
        height = dataObj.latitude.size  # HOW DOES THIS CHANGE IF WE HAVE 2D LAT/LON ARRAYS
        dataType = str(dataObj.dtype)
        missingValue = np.nan  # This could change if we are not using float arrays.
        crs = 'EPSG:4326'
        xres = np.abs(
            dataObj.longitude.values[1] - dataObj.longitude.values[0])  # again, could change if using 2D coord arrays.
        yres = np.abs(
            dataObj.latitude.values[1] - dataObj.latitude.values[0])  # again, could change if using 2D coord arrays.
        xmin = dataObj.longitude.values.min() - xres / 2.0  # shift to corner by 1/2 grid cell res
        ymax = dataObj.latitude.values.max() + yres / 2.0  # shift to corner by 1/2 grid cell res
        affTransform = rio.transform.from_origin(xmin, ymax, xres, yres)
        # Open the file.
        dst = rio.open(fileName, 'w', driver='GTiff', dtype=dataType, nodata=missingValue, \
                       width=width, height=height, count=1, \
                       crs=crs, transform=affTransform, \
                       compress='lzw')
        # Write the data.
        dst.write(np.flip(dataObj.values,
                          axis=1))  # Note, we  flip the data along the latitude dimension so that is is monotonically decreasing (i.e. N to S)
        # Close the file.
        dst.close()


    # Add tifs to zip update self.zipFilePath and cleanup of files
    def addToZip(self,uniqueid):
        with ZipFile(params.zipFile_ScratchWorkspace_Path + uniqueid + '.zip', 'w') as zipObj:
            # Iterate over all the files in zipFile_ScratchWorkspace_Path
            for folderName, subfolders, filenames in os.walk(
                    params.zipFile_ScratchWorkspace_Path + uniqueid + '/'):
                for filename in filenames:
                    # create complete filepath of file in zipFile_ScratchWorkspace_Path
                    filePath = os.path.join(folderName, filename)
                    # Add each file to zip
                    zipObj.write(filePath, basename(filePath))
        zipObj.close()
        shutil.rmtree(params.zipFile_ScratchWorkspace_Path + str(uniqueid), ignore_errors=True)
        self.zipFilePath = params.zipFile_ScratchWorkspace_Path + uniqueid + '.zip'

    # To process the request based on the type and return worklist in order to process and retrieve resultant data
    def __preProcessIncomingRequest__(self, request):
        self.derived_product = False
        is_job_type__MonthlyRainfallAnalysis = self.__is_custom_job_type__MonthlyRainfallAnalysis__(request)
        is_job_type__MonthlyGEFSRainfallAnalysis = self.__is_custom_job_type__MonthlyGEFSRainfallAnalysis__(request) 
        if(is_job_type__MonthlyRainfallAnalysis == True):
            self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: This IS a 'MonthlyRainfallAnalysis' type.  ")
            try:
                uniqueid = request['uniqueid']
                self.derived_product = True
                self.__insertProgressDb__(uniqueid)
                self.__write_JobStarted_To_DB__(uniqueid, str(request))
                self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: (MonthlyRainfallAnalysis_Type): uniqueid: " + str(uniqueid))
                self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: (MonthlyRainfallAnalysis_Type): uniqueid: " + str(request))
                self.logger.info(
                    "(" + self.name + "):__preProcessIncomingRequest__: (MonthlyRainfallAnalysis_Type): Don't forget about this: self.mathop, it is used again in the finish job code.   ")
                self.isDownloadJob = False
                self.dj_OperationName = "NotDLoad"
                self.derived_opname = "MonthlyRainfallAnalysis"
                self.__processProgress__(20.0)
                # Method retrieves NMME and CHIRPS data
                worklist = analysisTools.get_workList_for_headProcessor_for_MonthlyRainfallAnalysis_types(uniqueid, request)
                self.logger.info("(" + self.name + "):__preProcessIncomingRequest__ : (MonthlyRainfallAnalysis_Type): worklist length array value: " + str(len(worklist)))
                return None, worklist
            except Exception as e:
                self.logger.warning("(" + self.name + "): MonthlyRainfallAnalysis_Type: Error processing Request in HeadProcessor: uniqueid: " + str(
                    uniqueid) + " Exception Error Message: " + str(e))
                return e, None
        elif (is_job_type__MonthlyGEFSRainfallAnalysis == True):
            self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: This IS a 'CHIRPS-GEFS MonthlyRainfallAnalysis' type.  ")
            try:
                uniqueid = request['uniqueid']
                self.derived_product = True
                self.__insertProgressDb__(uniqueid)
                self.__write_JobStarted_To_DB__(uniqueid, str(request))
                self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: (MonthlyGEFSRainfallAnalysis_Type): uniqueid: " + str(uniqueid))
                self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: (MonthlyGEFSRainfallAnalysis_Type): uniqueid: " + str(request))

                self.logger.info(
                    "(" + self.name + "):__preProcessIncomingRequest__: (MonthlyGEFSRainfallAnalysis_Type): Don't forget about this: self.mathop, it is used again in the finish job code.   ")
                self.isDownloadJob = False
                self.dj_OperationName = "NotDLoad"
                self.derived_opname = "MonthlyGEFSRainfallAnalysis"
                worklist = analysisTools.get_workList_for_headProcessor_for_MonthlyGEFSRainfallAnalysis_types(uniqueid, request)
                self.logger.info("(" + self.name + "):__preProcessIncomingRequest__ : (MonthlyRainfallAnalysis_Type): worklist length array value: " + str(len(worklist)))
                return None, worklist
            except Exception as e:
                self.logger.warning("(" + self.name + "): MonthlyRainfallAnalysis_Type: Error processing Request in HeadProcessor: uniqueid: " + str(
                    uniqueid) + " Exception Error Message: " + str(e))
                return e, None
        else:
            self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: This is NOT a 'MonthlyRainfallAnalysis' type.  ")
        try:
            # Get values from request
            uniqueid = request['uniqueid']
            datatype = request['datatype']
            begintime = request['begintime']
            endtime = request['endtime']
            intervaltype = request['intervaltype']
            operationtype = request['operationtype']

            # Some database updation and logging of progress
            self.__insertProgressDb__(uniqueid)
            self.__write_JobStarted_To_DB__(uniqueid, str(request))
            self.logger.info("("+self.name+"):__preProcessIncomingRequest__: uniqueid: "+str(uniqueid))

            if(params.parameters[operationtype][1] == 'download'):
                self.isDownloadJob = True 
                self.dj_OperationName = "download"
            else:
                self.isDownloadJob = False
                self.dj_OperationName = "NotDLoad"

            polygon_Str_ToPass = None

            # Get some values from parameters_ops
            opn=params.parameters[operationtype][1]
            dataTypeCategory = params.dataTypes[datatype]['data_category']
            dataset_name = params.dataTypes[int(datatype)]['dataset_name'] + ".nc4"
            variable_name = params.dataTypes[int(datatype)]['variable']

            dates=[]
            values=[]

            # processing based on the geometry/shapefile
            if ('geometry' in request):
                polygon_Str_ToPass=request['geometry']
                jsonn = json.loads(str(polygon_Str_ToPass))

                # processsing based on operation
                if self.isDownloadJob == True:
                   # processing based on geometry and update progress to 20%
                    if  jsonn['features'][0]['geometry']['type'] != "Point":
                        try:
                            self.__processProgress__(20)
                            # Retrieve dataset after mapping the geometry
                            clipped_dataset = GetTDSData.get_aggregated_values(request['begintime'], request['endtime'], dataset_name, variable_name, request['geometry'], opn,datatype)
                            [self.writeToTiff(uniqueid,clipped_dataset.sel(time=[x])) for x in clipped_dataset.time.values]
                            self.addToZip(uniqueid)
                        except:
                            self.zipFilePath=None
                            self.logger.info("__preProcessIncomingRequest__: TDS URL Exception")
                    else:
                        dates, values= GetTDSData.get_aggregated_values(request['begintime'], request['endtime'], dataset_name, variable_name, request['geometry'], opn, datatype)
                        keylist = ["Date", "Value"]
                        dct={}
                        for ind in range(len(dates)):
                             dct[dates[ind]] = values[ind]
                        with open(params.zipFile_ScratchWorkspace_Path + request['uniqueid'] + '.csv', "w") as file:
                            outfile = csv.DictWriter(file, fieldnames=keylist)
                            outfile.writeheader()

                            for k, v in dct.items():
                                outfile.writerow({"Date": k, "Value": v})
                        self.zipFilePath =params.zipFile_ScratchWorkspace_Path + request['uniqueid'] + '.csv'
                        if os.path.exists(self.zipFilePath):
                            self.__processProgress__(20)
                else:
                    self.__processProgress__( 20)
                    dates, values= GetTDSData.get_aggregated_values(request['begintime'], request['endtime'], dataset_name, variable_name, request['geometry'], opn, datatype)

            # User Selected a Feature
            elif ('layerid' in request):
                layerid = request['layerid']
                featureids = request['featureids']
                geometries  = sf.getPolygons(layerid, featureids)

                if self.isDownloadJob == True:
                    try:
                        self.__processProgress__(20)
                        # get dataset after mapping geometry
                        clipped_dataset = GetTDSData.get_aggregated_values(request['begintime'],
                                                                                    request['endtime'], dataset_name,
                                                                                    variable_name, geometries,opn,datatype)
                        [self.writeToTiff(uniqueid, clipped_dataset.sel(time=[x])) for x in clipped_dataset.time.values]
                        self.addToZip(uniqueid)
                    except:
                        self.zipFilePath=None
                        self.logger.info("__preProcessIncomingRequest__: TDS URL Exception")
                else:
                    self.__processProgress__(20)
                    dates, values = GetTDSData.get_aggregated_values(request['begintime'], request['endtime'], dataset_name, variable_name, geometries, opn, datatype)

            current_mask_and_storage_uuid = uniqueid
            worklist = []

            # Finalize the worklist to be returned based on the operation
            if (self.isDownloadJob != True):
                for dateIndex in range(len(dates)):
                    workid = uu.getUUID()
                    gmt_midnight = calendar.timegm(time.strptime(dates[dateIndex] + " 00:00:00 UTC", "%Y-%m-%d %H:%M:%S UTC"))
                    workdict = {"uid":uniqueid, "current_mask_and_storage_uuid":current_mask_and_storage_uuid, "workid":workid,"datatype":datatype,"operationtype":operationtype, "intervaltype":intervaltype, "polygon_Str_ToPass":polygon_Str_ToPass, "derived_product": False}
                    workdict["year"] = int(dates[dateIndex][0:4])
                    workdict["month"] = int(dates[dateIndex][5:7])
                    workdict["day"] = int(dates[dateIndex][8:10])
                    workdict["epochTime"] = gmt_midnight
                    workdict["value"] = {opn: values[dateIndex]}
                    if (intervaltype == 0):
                        dateObject = dateutils.createDateFromYearMonthDay(workdict["year"], workdict["month"], workdict["day"] )
                    elif (intervaltype == 1):
                        dateObject = dateutils.createDateFromYearMonth(workdict["year"], workdict["month"] )
                    elif (intervaltype == 2):
                        dateObject = dateutils.createDateFromYear(workdict["year"])
                    workdict["isodate"] = dateObject.strftime(params.intervals[0]["pattern"])
                    worklist.extend([workdict])
            else:
                workid = uu.getUUID()
                if self.zipFilePath is not None:
                    workdict = {'uid': uniqueid, 'workid': workid, 'current_mask_and_storage_uuid': uniqueid,
                                'intervaltype': intervaltype, 'datatype': datatype, 'operationtype': operationtype,
                                'polygon_Str_ToPass': polygon_Str_ToPass, 'derived_product': False,
                                'value': {opn: self.zipFilePath}}
                    worklist.extend([workdict])
                else:
                    return "download_error", []
            self.logger.info("(" + self.name + "):__preProcessIncomingRequest__ : request['begintime']: " + str(begintime))
            self.logger.info("(" + self.name + "):__preProcessIncomingRequest__ : request['endtime']: " + str(endtime))
            self.logger.info("(" + self.name + "):__preProcessIncomingRequest__ : request['intervaltype']: " + str(intervaltype))
            return None, worklist
        except Exception as e:
            self.logger.warning("("+self.name+"):Error processing Request in HeadProcessor: uniqueid: "+str(uniqueid)+" Exception Error Message: "+str(e))
            return e,None

    # To update progress corresponding to uniqueid to the database
    def __processProgress__(self, progress):
        log = reqLog.Request_Progress.objects.get(request_id=self.request['uniqueid'])
        log.progress = progress
        log.save()

    # To write results of the dataset request to a file with uniqueid
    def __outputData__(self):
        self.logger.info("outputting data for "+self.request['uniqueid'])
        output = {'data':self.finished_items}
        self.__writeResults__(self.request['uniqueid'], output)

    # To write results of the monthly analysis request to a file with uniqueid
    def __outputDataForMonthlyAnalysis__(self):
        derived_product_output = analysisTools.get_output_for_MonthlyRainfallAnalysis_from(self.finished_items)
        self.logger.info("outputting data for "+self.request['uniqueid'])
        output = {'data':self.finished_items, 'MonthlyAnalysisOutput': derived_product_output}
        self.__writeResults__(self.request['uniqueid'], output)

    # To log any errors
    def __processErrors__(self, errors):
        self.logger.info("Errors  ",errors)
        
if __name__ == "__main__":
    name = sys.argv[1]
    inputconn = sys.argv[2]
    outputconn = sys.argv[3]
    listenconn = sys.argv[4]
    ZMQCHIRPSHeadProcessor(name, inputconn, outputconn, listenconn)