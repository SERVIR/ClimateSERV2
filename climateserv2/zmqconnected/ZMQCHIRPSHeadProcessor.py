'''
Created on Jan 30, 2015

@author: jeburks
@author: Kris Stanton
'''
import os
import sys
import time
import calendar
import zmq
import json
import shutil
from copy import deepcopy
from operator import itemgetter
module_path = os.path.abspath(os.getcwd())
if module_path not in sys.path:
    sys.path.append(module_path)
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))
try:
    import climateserv2.geoutils as geoutils
    import climateserv2.processtools.dateprocessor as dproc
    from climateserv2.file.TDSExtraction import get_aggregated_values as GetTDSData

    import climateserv2.parameters as params
    import climateserv2.file.npmemmapstorage as rp
    import climateserv2.geo.clippedmaskgenerator as mg
    import climateserv2.file.dateutils as dateutils
    import climateserv2.db.DBMDbprocessing as dbmDb
    import climateserv2.locallog.locallogging as llog
    import climateserv2.processtools.uutools as uu
    import climateserv2.file.MaskTempStorage  as mst
    import climateserv2.geo.shapefile.readShapesfromFiles as sf
    import climateserv2.processtools.pMathOperations as pMath
    import climateserv2.requestLog as reqLog
    import climateserv2.file.ExtractTifFromH5 as extractTif
    import climateserv2.processtools.AnalysisTools as analysisTools
    import climateserv2.geoutils as geoutils
except:
    import geoutils as geoutils
    import processtools.dateprocessor as dproc
    import file.TDSExtraction as GetTDSData
    import parameters as params
    import file.npmemmapstorage as rp
    import geo.clippedmaskgenerator as mg
    import file.dateutils as dateutils
    import db.DBMDbprocessing as dbmDb
    import locallog.locallogging as llog
    import processtools.uutools as uu
    import file.MaskTempStorage  as mst
    import geo.shapefile.readShapesfromFiles as sf
    import processtools.pMathOperations as pMath
    import requestLog as reqLog
    import file.ExtractTifFromH5 as extractTif
    import processtools.AnalysisTools as analysisTools

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
    
    # KS Refactor 2015  # Some items related to download data jobs
    isDownloadJob = False
    dj_OperationName = "download"  # Needed by   # "if results['value'][opname] != missingValue:"
    
    def __init__(self, name, inputconn, outputconn, listenconn):
        self.name = name
        self.inputconn = inputconn
        self.outputconn = outputconn
        self.listeningconn = listenconn
        self.logger.info("Creating Processor named: "+self.name+" listening on port: "+self.inputconn+" outputting to port: "+self.outputconn+"  listening for output on: "+self.listeningconn)
        
        ##Connect to the source
        self.__beginWatching__()
   
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
        
    # For download dataset types..
    def preProcessWork_ForDownloadTypes(self, request):
        if (self.isDownloadJob == True ):
            if (self.dj_OperationName == "download"):
                theJobID = None
                self.logger.info("("+self.name+"):preProcessWork_ForDownloadTypes: Pre_Processing a Download Data Job. " + str(request['uniqueid']))
                theJobID = request['uniqueid']


            elif (self.dj_OperationName == "download_all_climate_datasets"):
                # Placeholder for download_all_climate_datasets operations.... not even sure if going to use this here..
                pass
        else:
            # This is a statistical  do nothing
            return
    
    # After all the tif extracting is done, need to zip them all up in a single operation
    def postProcessWork_ForDownloadTypes(self, request):
        if (self.isDownloadJob == True ):
            if (self.dj_OperationName == "download"):

                theJobID = None
                try:
                    self.logger.info("("+self.name+"):postProcessWork_ForDownloadTypes: Post_Processing a Download Data Job. " + str(request['uniqueid']))
                    theJobID = request['uniqueid']
                    # Zip the files
                    # zipFilePath, errorMessage = extractTif.zip_Extracted_Tif_Files_Controller(theJobID)
                    if (self.zipFilePath != None):
                        self.logger.info("("+self.name+"):postProcessWork_ForDownloadTypes: Tif files have been zipped to: " + str(self.zipFilePath))
                    else:
                        self.logger.info("("+self.name+"):postProcessWork_ForDownloadTypes: ERROR ZIPPING TIF FILES.")
                        
                except:
                    pass
            elif (self.dj_OperationName == "download_all_climate_datasets"):
                # Placeholder for download_all_climate_datasets operations.... not even sure if going to use this here..
                pass
        else:
            # This is a statistical  do nothing
            return
        
        # Placeholder
        
        pass
    
        
    def processWork(self,request):
        self.request = request
        
        # ks notes // Generate a list of work to be done (each item represents a time interval)
        error, workarray = self.__preProcessIncomingRequest__(request)
        
        # KS Refactor 2015 // Additional pre-setup items specific to download request types
        self.preProcessWork_ForDownloadTypes(request)
        
        # ks notes // Dispatch that list of work through the output receiver (to be picked up by workers)
        if (error == None):

            self.worklist_length = len(workarray)
            self.total_task_count = len(workarray)
            self.__updateProgress__()
            workingArray_guid_index_list = []
            for item in workarray:
                self.workToBeDone[item['workid']]= item
                workingArray_guid_index_list.append(item['workid'])
            workingArray = deepcopy(self.workToBeDone)
            self.logger.info("(" + self.name + "):processWork: About to call __watchForResults_and_keepSending__ ")
            self.__watchForResults_and_keepSending__(workingArray, workingArray_guid_index_list)
        else:
            self.logger.warning("Got an error processing request: "+str(error))
            
            # ks refactor 2015 - Write Error to log, (also try and get the job id from the request)
            theJobID = ""
            try:
                theJobID = request['uniqueid']
            except:
                theJobID = ""
            self.__write_JobError_To_DB__(theJobID, str(error), str(request))
            
            self.progress = -1
            self.__cleanup__()
            self.__processProgress__(self.progress)
            self.__watchAgain__()
            
        self.logger.info("("+self.name+"):processWork: Process Work has reached the end!")


    # Use this when the size of the worklist is too large and we need to keep sending as some come in.
    def __watchForResults_and_keepSending__(self, workingArray, workingArray_guid_index_list):
        # Send the first 1000 items.
        # As new items come in for processing, send another item out.

        # Break the workingArray into chunks of this size. (so only this many get sent at a time.
        message_chunkSize = 5000

        # If the working array is already less than the max chunksize, use the original existing method
        if(len(workingArray) < message_chunkSize):
            # Send all the messages, and then call the original mehtod, then return so none of the below stuff happens.
            self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: About to do 'for item in workingArray' (len(workingArray)): " + str(len(workingArray)))
            item_counter = 0
            for item in workingArray:
                self.outputreceiver.send_string(json.dumps(str(workingArray[item])))
                self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: outputreceiver.send for item " + str(item_counter) + " of " + str(len(workingArray)))
                item_counter = item_counter + 1

            self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: About to call __watchForResults__ ")
            self.__watchForResults__()
            return

        # How many thresholds should we be checking.
        number_of_progress_thresholds = (len(workingArray) / message_chunkSize) + 1
        current_workingArray_index = 0
        finished_sending_workingArray_data = False

        # Debug reporting
        self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: message_chunkSize : " + str(message_chunkSize))
        self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: len(workingArray) : " + str(len(workingArray)))
        self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: number_of_progress_thresholds : " + str(number_of_progress_thresholds))


        # Send in the first chunk.
        for i in range(0, message_chunkSize):
            if (current_workingArray_index >= len(workingArray)):
                # Don't try and send this index as it does not exist!!!, just set the 'done' flag to true.
                finished_sending_workingArray_data = True
            else:
                current_workid_index = workingArray_guid_index_list[current_workingArray_index]
                self.outputreceiver.send_string(json.dumps(workingArray[current_workid_index])) # (workingArray[current_workingArray_index]))
                self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: outputreceiver.send for item " + str(current_workingArray_index) + " of " + str(len(workingArray)))
                current_workingArray_index = current_workingArray_index + 1

        #last_current_progress
        last_chunk_sent = 1
        next_progress_threshold_to_check = ((100.0 / number_of_progress_thresholds) * last_chunk_sent)

        self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: ENTERING THE WHILE LOOP OF CHECKING PROGRESS.")
        self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: last_chunk_sent: " + str(last_chunk_sent))
        self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: next_progress_threshold_to_check: " + str(next_progress_threshold_to_check))

        # Start listening, and sending in future chunks.
        while (self.progress < 100):
            # Normal receiving operation.
            results = json.loads(self.listenreceiver.recv())
            self.processFinishedData(results)
            self.logger.info("("+self.name+"):__watchForResults_and_keepSending__: self.progress: " + str(self.progress))

            # Send more stuff down the queue..
            if(finished_sending_workingArray_data == True):
                # Done sending worklist items, do nothing
                pass
            else:
                # We are not done sending items... check to see if the last_current_progress changed..
                #if(last_current_progress != self.progress):

                if (self.progress > next_progress_threshold_to_check):
                    # progress has changed...
                    # Set the next progress to check
                    #last_current_progress = self.progress
                    last_chunk_sent = last_chunk_sent + 1
                    next_progress_threshold_to_check = ((100.0 / number_of_progress_thresholds) * last_chunk_sent)

                    self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: PROGRESS THRESHOLD HIT: Changing the compare for the next time" )
                    self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: last_chunk_sent: " + str(last_chunk_sent))
                    self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: next_progress_threshold_to_check: " + str(next_progress_threshold_to_check))
                    self.logger.info("(" + self.name + "):__watchForResults_and_keepSending__: About to send in another chunk..")

                    # Send more workingArray items to be processed.
                    for i in range(0, message_chunkSize):
                        if (current_workingArray_index >= len(workingArray)):
                            # Don't try and send this index as it does not exist!!!, just set the 'done' flag to true.
                            finished_sending_workingArray_data = True
                        else:
                            current_workid_index = workingArray_guid_index_list[current_workingArray_index]
                            self.outputreceiver.send_string(json.dumps(workingArray[current_workid_index])) #(workingArray[current_workingArray_index]))
                            self.logger.info(
                                "(" + self.name + "):__watchForResults_and_keepSending__: outputreceiver.send for item " + str(
                                    current_workingArray_index) + " of " + str(len(workingArray)))
                            current_workingArray_index = current_workingArray_index + 1

        self.__finishJob__()
    # Original 'watchForResults' function
    # This is the part of the code that listens for workers to be done with their processing.
    # Once finished, it fires off the __finishJob__ method which completes the job.
    def __watchForResults__(self):
        # Normal existing code pipeline.
        while (self.progress < 100.0):

            try:
                results = json.loads(self.listenreceiver.recv())
            except:
                self.logger.info("**************************")
            self.processFinishedData(results)
            self.logger.info("("+self.name+"):__watchForResults__: self.progress: " + str(self.progress))
        self.__finishJob__()
        # if (self.derived_product == True):
        #     self.sub_types_finished = False
        #     while (self.sub_types_finished == False):
        #         results = json.loads(self.listenreceiver.recv())
        #         self.processFinishedData(results)
        #         if(self.finished_task_count == )
        #     self.__finishJob__()
        # else:
        #     # Normal existing code pipeline.
        #     while (self.progress < 100):
        #         results = json.loads(self.listenreceiver.recv())
        #         self.processFinishedData(results)
        #     self.__finishJob__()
            
                
        
    def processFinishedData(self, results):
        self.logger.info("("+self.name+"):processFinishedData: Process Finished Work "+str(self.request))
        self.logger.info("(" + self.name + "):processFinishedData: results:  " + str(results))

        
        #self.logger.info("Process Finished Work "+str(self.request))
        #Need to process the data
        self.finished_task_count = self.finished_task_count +1
        #self.workToBeDone.pop(results['workid'],None)
        missingValue = None
        if (self.derived_product == True):
            current_data_type = results['datatype']
            missingValue = params.dataTypes[current_data_type]['fillValue']  # Now it's dynamic (passed in from the worker)
        else:
            missingValue = params.dataTypes[self.request['datatype']]['fillValue']
        if (self.isDownloadJob == True):
            # For Download Jobs.
            # Need to figure out why we use 'self.finished_items' and what happens if I just skip it..
            if results['value'] is not None:
                self.finished_items.append(results)
        else:
            self.finished_items.append(results)
        self.__updateProgress__()
            
    def __sortData__(self,array):
        newlist = sorted(array, key=itemgetter('epochTime'))
        return newlist    
    
        
    def __updateProgress__(self,output_full=False):
        self.progress = (float(self.finished_task_count)/float(self.total_task_count))*100.
        if (self.progress < 100 or output_full == True):
            self.__processProgress__(self.progress)

    # When this function is called, we KNOW that all the worklist items have been completed
    # We can find the values for every job inside the variable called,
    # # self.finished_items = []
    def __finishJob__(self):

        # KS Refactor 2015 // Pipe the request into the postprocess for download pipeline
        self.postProcessWork_ForDownloadTypes(self.request)
        #self.logger.info("Finished Job:"+str(self.request))
        self.logger.info("("+self.name+"):__finishJob__:Finished Job:"+str(self.request))
        
        # KS Refactor 2015 - Logging Job Finished
        theJobID = ""
        try:
            theJobID = str(self.request['uniqueid'])
        except:
            theJobID = ""
        self.__write_JobCompleted_To_DB__(theJobID, str(self.request))

        if (self.isDownloadJob == False):
            self.logger.info("***")
            self.logger.info(self.finished_items)
            self.finished_items = self.__sortData__(self.finished_items)
#         ##Output Data
        if (self.derived_product == True):
            # Special output formatting for Monthly Analysis (we don't necessarily want all the raw data (maybe we do!?)
            self.__outputDataForMonthlyAnalysis__()
        else:
            # Normal output formatting
            self.__outputData__()
#         ##Update Progress
        self.__updateProgress__(output_full=True)
        self.__cleanup__()
#         ###Back to looking for work.

    def __cleanup__(self):
        # self.logger.info("Cleanup")
        self.total_task_count = 0
        self.worklist_length = 0
        self.finished_task_count = 0
        self.current_work_dict = None
        self.finished_items = []
        # Extra stuff for derived product types.
        self.derived_product = False
        self.sub_types_finished = True  # When this is False, the function that watches for finished worker progress keeps running
        self.derived_opname = "Unset"
        os.chmod(params.zipFile_ScratchWorkspace_Path+str(self.request['uniqueid'])+'.zip', 0o777)
        shutil.rmtree(params.zipFile_ScratchWorkspace_Path+str(self.request['uniqueid']), ignore_errors=True)


    def __writeResults__(self,uniqueid,results):
        filename = params.getResultsFilename(uniqueid)
        f = open(filename, 'w+')
        json.dump(results,f)
        f.close()
        f = None
        
    def __insertProgressDb__(self,uniqueid):
        conn = dbmDb.DBMConnector()
        conn.setProgress(uniqueid, 0)
        conn.close()
        
    def __updateProgressDb__(self,uniqueid, progress):
        conn = dbmDb.DBMConnector()
        conn.setProgress(uniqueid, progress)
        conn.close()
        
    # KS Refactor 2015 - Adding ServerSide Job Log to request logs area - Log when Jobs are started.
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
        
    
    # KS Refactor 2015 - Adding ServerSide Job Log to request logs area - Log when Jobs are completed
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
        
        
    # KS Refactor 2015 - Adding ServerSide Job Log to request logs area - Log when Jobs are completed
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
        
    def __writeMask__(self,uid,array,bounds):
        mst.writeHMaskToTempStorage(uid,array,bounds)
    def __is_custom_job_type__MonthlyGEFSRainfallAnalysis__(self, request):
        # # Inputs: From ZMQ (from the API Layer):      ( A location, ( (layerid + featureids) OR ( geometry ) ), custom_job_type ( Hard coded String "MonthlyRainfallAnalysis" ), uniqueid  )
        try:
            # uniqueid = request['uniqueid']
            # custom_job_type = request['custom_job_type']
            # # if(custom_job_type == 'MonthlyRainfallAnalysis'):
            # self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: uniqueid: " + str(
            #     uniqueid) + ", custom_job_type: " + custom_job_type)
            #
            # self.logger.info(
            #     "TODO, FINISH THE custom_job_type PART OF THIS PREPROCESS_INCOMING_REQUEST PIPELINE....remove the return statement before finishing.")
            if 'custom_job_type' in request:
                custom_job_type = request['custom_job_type']
                if (custom_job_type == "MonthlyGEFSRainfallAnalysis"):
                    return True
                else:
                    return False #None
            else:
                return False

        except Exception as e:
            uniqueid = request['uniqueid']
            self.logger.warning("(" + self.name + "):Couldn't find custom_job_type in '__is_custom_job_type__MonthlyRainfallAnalysis__' in HeadProcessor: uniqueid: " + str(
                uniqueid) + " Exception Error Message: " + str(e))
            return e, False  # REMOVE THIS RETURN PATH, POSSIBLE EXISTING BEHAVIOR SHOULD HAPPEN HERE.

        return False
		
    def __is_custom_job_type__MonthlyRainfallAnalysis__(self, request):
        # # Inputs: From ZMQ (from the API Layer):      ( A location, ( (layerid + featureids) OR ( geometry ) ), custom_job_type ( Hard coded String "MonthlyRainfallAnalysis" ), uniqueid  )
        try:
            # uniqueid = request['uniqueid']
            # custom_job_type = request['custom_job_type']
            # # if(custom_job_type == 'MonthlyRainfallAnalysis'):
            # self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: uniqueid: " + str(
            #     uniqueid) + ", custom_job_type: " + custom_job_type)
            #
            # self.logger.info(
            #     "TODO, FINISH THE custom_job_type PART OF THIS PREPROCESS_INCOMING_REQUEST PIPELINE....remove the return statement before finishing.")
            self.logger.info(request)
            if 'custom_job_type' in request:
                custom_job_type = request['custom_job_type']
                if (custom_job_type == "MonthlyRainfallAnalysis"):
                    return True
                else:
                    return False
            else:
                return False #None

        except Exception as e:
            uniqueid = request['uniqueid']
            self.logger.warning("(" + self.name + "):Couldn't find custom_job_type in '__is_custom_job_type__MonthlyRainfallAnalysis__' in HeadProcessor: uniqueid: " + str(
                uniqueid) + " Exception Error Message: " + str(e))
            return e, False  # REMOVE THIS RETURN PATH, POSSIBLE EXISTING BEHAVIOR SHOULD HAPPEN HERE.

        return False

    def __preProcessIncomingRequest__(self, request):

        # Check for Custom Job Type Here.
        self.derived_product = False  # Default
        is_job_type__MonthlyRainfallAnalysis = self.__is_custom_job_type__MonthlyRainfallAnalysis__(request) #False
        is_job_type__MonthlyGEFSRainfallAnalysis = self.__is_custom_job_type__MonthlyGEFSRainfallAnalysis__(request) 
        if(is_job_type__MonthlyRainfallAnalysis == True):
            self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: This IS a 'MonthlyRainfallAnalysis' type.  ")

            # Set up the Monthly Rainfall Analysis type here. (Note, there are return statements on both of these paths.. this should probably be moved to a separate pipeline.
            try:
                # Monthly Rainfall Analysis Setup.
                # So far, all we get as inputs from the client are the uniqueid and a geometry.

                # Following along the normal_ish code
                uniqueid = request['uniqueid']
                self.derived_product = True     # Signals the progress counter in a various way.
                self.__insertProgressDb__(uniqueid)
                self.__write_JobStarted_To_DB__(uniqueid, str(request))
                self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: (MonthlyRainfallAnalysis_Type): uniqueid: " + str(uniqueid))
                self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: (MonthlyRainfallAnalysis_Type): uniqueid: " + str(request))

                #self.mathop = pMath.mathOperations(operationtype, 1, params.dataTypes[datatype]['fillValue'], None)
                self.logger.info(
                    "(" + self.name + "):__preProcessIncomingRequest__: (MonthlyRainfallAnalysis_Type): Don't forget about this: self.mathop, it is used again in the finish job code.   ")
                self.isDownloadJob = False
                self.dj_OperationName = "NotDLoad"
                self.derived_opname = "MonthlyRainfallAnalysis"

                worklist = analysisTools.get_workList_for_headProcessor_for_MonthlyRainfallAnalysis_types(uniqueid, request)

                self.logger.info("(" + self.name + "):__preProcessIncomingRequest__ : (MonthlyRainfallAnalysis_Type): worklist length array value: " + str(len(worklist)))
                # With these two lines here, everything just goes into the ether.

                return None, worklist

            except Exception as e:
                self.logger.warning("(" + self.name + "): MonthlyRainfallAnalysis_Type: Error processing Request in HeadProcessor: uniqueid: " + str(
                    uniqueid) + " Exception Error Message: " + str(e))
                return e, None
        elif (is_job_type__MonthlyGEFSRainfallAnalysis == True):
            self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: This IS a 'CHIRPS-GEFS MonthlyRainfallAnalysis' type.  ")
            # Set up the Monthly Rainfall Analysis type here. (Note, there are return statements on both of these paths.. this should probably be moved to a separate pipeline.
            try:
                # Monthly Rainfall Analysis Setup.
                # So far, all we get as inputs from the client are the uniqueid and a geometry.

                # Following along the normal_ish code
                uniqueid = request['uniqueid']
                self.derived_product = True     # Signals the progress counter in a various way.
                self.__insertProgressDb__(uniqueid)
                self.__write_JobStarted_To_DB__(uniqueid, str(request))
                self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: (MonthlyGEFSRainfallAnalysis_Type): uniqueid: " + str(uniqueid))
                self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: (MonthlyGEFSRainfallAnalysis_Type): uniqueid: " + str(request))

                #self.mathop = pMath.mathOperations(operationtype, 1, params.dataTypes[datatype]['fillValue'], None)
                self.logger.info(
                    "(" + self.name + "):__preProcessIncomingRequest__: (MonthlyGEFSRainfallAnalysis_Type): Don't forget about this: self.mathop, it is used again in the finish job code.   ")
                self.isDownloadJob = False
                self.dj_OperationName = "NotDLoad"
                self.derived_opname = "MonthlyGEFSRainfallAnalysis"

                # HERE IS WHAT WE ACTUALLY NEED TO RETURN...
                # Some processing of all the input params (logging things along the way)
                # # Geometry one is a little complex but the example below does work.
                # Then A bunch of stuff to setup a worklist
                # return None, worklist

                #worklist = []
                worklist = analysisTools.get_workList_for_headProcessor_for_MonthlyGEFSRainfallAnalysis_types(uniqueid, request)
                # if (params.DEBUG_LIVE == True):
                #     self.logger.debug(
                #         "(" + self.name + "):__preProcessIncomingRequest__ : (MonthlyRainfallAnalysis_Type): worklist array value: " + str(worklist))
                self.logger.info("(" + self.name + "):__preProcessIncomingRequest__ : (MonthlyRainfallAnalysis_Type): worklist length array value: " + str(len(worklist)))
                # With these two lines here, everything just goes into the ether.

                #not_finished_yet = "TODO! NOT FINISHED YET!"
                #return not_finished_yet, None

                # Sets off the job task runners.
                return None, worklist

            except Exception as e:
                self.logger.warning("(" + self.name + "): MonthlyRainfallAnalysis_Type: Error processing Request in HeadProcessor: uniqueid: " + str(
                    uniqueid) + " Exception Error Message: " + str(e))
                return e, None
        else:
            self.logger.info("(" + self.name + "):__preProcessIncomingRequest__: This is NOT a 'MonthlyRainfallAnalysis' type.  ")


        # try:
        #     return None, worklist
        # except Exception as e:
        #     self.logger.warn("(" + self.name + "):Error processing Request in HeadProcessor: uniqueid: " + str(uniqueid) + " Exception Error Message: " + str(e))
        #     return e, None
        # # # Something is wrong with this new code???

        # (for MonthlyRainfallAnalysis Types)
        # # Notes: What needs to happen here is:
        # # # Jobs need to be split up however possible (worst case is NO splitting and one thread does ALL the analysis...)
        # # # Each thread needs to update the status when it does a chunk of the work. (simillar to existing code)
        # # # When all is done, a master process collates all the data into a single return object. (I think the entry point for that is found here in this file)
        # # # The data is returned.


        try:
            if(params.DEBUG_LIVE == True):
                self.logger.info("("+self.name+"):__preProcessIncomingRequest__: params.DEBUG_LIVE is set to True.  There will be a lot of textual output for this run.")
                
            uniqueid = request['uniqueid']
            self.__insertProgressDb__(uniqueid)
            self.__write_JobStarted_To_DB__(uniqueid, str(request))  # Log when Job has started. 
            
            self.logger.info("("+self.name+"):__preProcessIncomingRequest__: uniqueid: "+str(uniqueid))
            
            datatype = request['datatype']
            begintime = request['begintime']
            endtime = request['endtime']
            intervaltype = request['intervaltype']
            operationtype = request['operationtype']

            if(params.parameters[operationtype][1] == 'download'):


                # If this is a download dataset request, set the self.mathop prop to 0 (or 'max' operator.. this is just so I don't have to refactor a ton of code just to get this feature working at this time... note:  Refactor of this IS needed!)
                self.mathop = pMath.mathOperations(0,1,params.dataTypes[datatype]['fillValue'],None)

                # Additional customized code for download jobs
                self.isDownloadJob = True 
                self.dj_OperationName = "download"
            else:
                # This is pass through for all normal requests..
                self.mathop = pMath.mathOperations(operationtype,1,params.dataTypes[datatype]['fillValue'],None) 
                self.isDownloadJob = False
                self.dj_OperationName = "NotDLoad"

            #self.logger.info("("+self.name+"):__preProcessIncomingRequest__: DEBUG: MADE IT PASSED THE DIRTY OVERRIDE! requestID: "+uniqueid)

            size = params.getGridDimension(int(datatype))
            downloaddates = dproc.getListOfTimes(begintime, endtime,intervaltype)

            # KS Developer Note: The issue here is that I need to only cut simple rectangle shaped images out of the data.
            # All I really need is the largest bounding box that encompases all points (regardless of how complex the original polygon was)
            # Seems simple right? :)
            # The other part of this issue is that this only needs to happen on download data requests.  If I change the code for all requests, it becomes less efficient for stats type jobs.
            #self.logger.info("("+self.name+"):__preProcessIncomingRequest__: DEBUG ALERT: Right now, only user drawn polygons are supported for download requests.  Need to write a function that gets geometry values from features as well.. VERY IMPORTANT TODO BEFORE RELEASE!!")
            #geometry_ToPass = None
            polygon_Str_ToPass = None
            dataTypeCategory = params.dataTypes[datatype]['data_category'] #  == 'ClimateModel'

            # User Drawn Polygon
            if ('geometry' in request):

                # Process input polygon string
                if(params.DEBUG_LIVE == True):
                    self.logger.info("("+self.name+"):__preProcessIncomingRequest__: DEBUG: GEOMETRY FOUND (POLYGON DRAWN BY USER)")
                polygonstring = request['geometry']
                geometry = geoutils.decodeGeoJSON(polygonstring)


                dataset_name=params.dataTypes[int(datatype)]['dataset_name']+".nc4"

                variable_name=params.dataTypes[int(datatype)]['variable']

                coordinates=json.loads(polygonstring)["features"][0]["geometry"]["coordinates"]
                # geotransform, wkt = rp.getSpatialReference(int(datatype))

                if(params.DEBUG_LIVE == True):
                    self.logger.debug("("+self.name+"):__preProcessIncomingRequest__ : polygonstring (request['geometry']) value: " + str(polygonstring))
                if(params.parameters[operationtype][1] == 'download'):
                    dates=downloaddates
                    self.zipFilePath,operation = GetTDSData.get_aggregated_values(request['begintime'], request['endtime'], dataset_name, variable_name, polygonstring, request['uniqueid'], params.parameters[request['operationtype']][1],params.zipFile_ScratchWorkspace_Path + str(uniqueid) )
                else:
                    dates, operation, values = GetTDSData.get_aggregated_values(request['begintime'], request['endtime'], dataset_name, variable_name, polygonstring, request['uniqueid'], params.parameters[request['operationtype']][1])
                
                if(params.DEBUG_LIVE == True):
                    self.logger.debug("("+self.name+"):__preProcessIncomingRequest__ : polygonstring (request['geometry']) value: " + str(polygonstring))
            # User Selected a Feature
            elif ('layerid' in request):
                geotransform, wkt = rp.getSpatialReference(int(datatype))

                if(params.DEBUG_LIVE == True):
                    self.logger.info("("+self.name+"):__preProcessIncomingRequest__: DEBUG: LAYERID FOUND (FEATURE SELECTED BY USER)")
                
                layerid = request['layerid']
                featureids = request['featureids']
                geometries  = sf.getPolygons(layerid, featureids)

                if(params.DEBUG_LIVE == True):
                    self.logger.debug("("+self.name+"):__preProcessIncomingRequest__ : (FeatureSelection) geometries value: " + str(geometries))

                # For Download data types, convert all of the geometries into a bounding box that covers the whole map.
                # RIGHT HERE!!
                #if(self.dj_OperationName == "download"):
                if((self.dj_OperationName == "download") | (dataTypeCategory == 'ClimateModel')):
                    # Convert all the geometries to the rounded polygon string, and then pass that through the system
                    polygonstring = extractTif.get_ClimateDataFiltered_PolygonString_FromMultipleGeometries(geometries)
                    polygon_Str_ToPass = polygonstring
                    geometry = geoutils.decodeGeoJSON(polygonstring)
                    bounds, mask = mg.rasterizePolygon(geotransform, size[0], size[1], geometry)

                else:
                    bounds,mask = mg.rasterizePolygons(geotransform, size[0], size[1], geometries)

                # Break up date
                # Check for cached polygon
                # if no cached polygon exists rasterize polygon
                clippedmask = mask[bounds[2]:bounds[3], bounds[0]:bounds[1]]
                # self.logger.debug("("+self.name+"):__preProcessIncomingRequest__ : debug : Value of 'mask': " + str(mask))
                # self.logger.debug("("+self.name+"):__preProcessIncomingRequest__ : debug : Value of 'clippedmask': " + str(clippedmask))

                current_mask_and_storage_uuid = uniqueid
                # self.__writeMask__(uniqueid,clippedmask,bounds)
                self.__writeMask__(current_mask_and_storage_uuid, clippedmask, bounds)



                del mask
                del clippedmask
            worklist =[]
            if (self.dj_OperationName != "download"):
                for dateIndex in range(len(dates)):
                    workid = uu.getUUID()
                    gmt_midnight = calendar.timegm(time.strptime(dates[dateIndex] + " 00:00:00 UTC", "%Y-%m-%d %H:%M:%S UTC"))
                    workdict = {'uid':uniqueid, 'current_mask_and_storage_uuid':uniqueid, 'workid':workid,'datatype':datatype,'operationtype':operationtype, 'intervaltype':intervaltype, 'polygon_Str_ToPass':polygon_Str_ToPass, 'derived_product': False} #'geometryToClip':geometry_ToPass}
                    workdict['year'] = int(dates[dateIndex][0:4])
                    workdict['month'] = int(dates[dateIndex][5:7])
                    workdict['day'] = int(dates[dateIndex][8:10])
                    workdict['epochTime'] = gmt_midnight
                    workdict['value'] = {operation: values[dateIndex]}
                    if (intervaltype == 0):
                        dateObject = dateutils.createDateFromYearMonthDay(workdict['year'], workdict['month'], workdict['day'] )
                    elif (intervaltype == 1):
                        dateObject = dateutils.createDateFromYearMonth(workdict['year'], workdict['month'] )
                    elif (intervaltype == 2):
                        dateObject = dateutils.createDateFromYear(workdict['year'])
                    workdict['isodate'] = dateObject.strftime(params.intervals[0]['pattern'])
                    worklist.extend([workdict])
            else:
                workid = uu.getUUID()
                workdict = {'uid': uniqueid, 'workid':workid,'current_mask_and_storage_uuid': uniqueid, 'intervaltype':intervaltype,
                            'datatype': datatype, 'operationtype': operationtype,
                            'polygon_Str_ToPass': polygon_Str_ToPass,
                            'derived_product': False}  # 'geometryToClip':geometry_ToPass}
                workdict['value'] = {operation: self.zipFilePath}
                worklist.extend([workdict])
            self.logger.info("(" + self.name + "):__preProcessIncomingRequest__ : request['begintime']: " + str(begintime))
            self.logger.info("(" + self.name + "):__preProcessIncomingRequest__ : request['endtime']: " + str(endtime))
            self.logger.info("(" + self.name + "):__preProcessIncomingRequest__ : request['intervaltype']: " + str(intervaltype))
            return None, worklist
        except Exception as e:
            self.logger.warning("("+self.name+"):Error processing Request in HeadProcessor: uniqueid: "+str(uniqueid)+" Exception Error Message: "+str(e))
            return e,None
        
    def __processProgress__(self, progress):
        self.__updateProgressDb__(self.request['uniqueid'],progress)
        
    def __outputData__(self):
        self.logger.info("outputting data for "+self.request['uniqueid'])
        output = {'data':self.finished_items}
        self.__writeResults__(self.request['uniqueid'], output)

    def __outputDataForMonthlyAnalysis__(self):
        # This is the place where we KNOW the worklist is completed, and now we can do the derived product info on it.
        # Then we can output a specifically formatted array of objects that the client is ready to graph.
        # TODO! Right here, formout final output for derived product!
        #derived_product_output_list = [{"testObjectKey":"testObjectValue_TODO_FINISH_THIS_CODE"}]
        derived_product_output = analysisTools.get_output_for_MonthlyRainfallAnalysis_from(self.finished_items)
        # So in short, we want to do something like this.
        # derived_product_output_list = AnalysisTools.get_output_for_MonthlyAnalysis(self.finished_items)
        #
        # Debug, testing to see what one item from 'self.finished_items' looks like
        self.logger.info("Example of: self.finished_items[0]: " + str(self.finished_items[0]) )

        self.logger.info("outputting data for "+self.request['uniqueid'])

        # FOR TESTING (Outputs Raw Data AND MonthlyAnalysisChart data)
        output = {'data':self.finished_items, 'MonthlyAnalysisOutput': derived_product_output}

        # FOR PRODUCTION (Only outputs the stuff we need for the MonthlyAnalysisChart)
        #output = {'MonthlyAnalysisOutput': derived_product_output}

        self.__writeResults__(self.request['uniqueid'], output)
        
    def __processErrors__(self, errors):
        self.logger.info("Errors  ",errors)
        
if __name__ == "__main__":
    name = sys.argv[1]
    inputconn = sys.argv[2]
    outputconn = sys.argv[3]
    listenconn = sys.argv[4]
    ZMQCHIRPSHeadProcessor(name, inputconn, outputconn, listenconn)