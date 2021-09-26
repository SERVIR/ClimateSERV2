import datetime
import sys
import os
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "climateserv2.settings")
django.setup()
from django.apps import apps
Request_Log = apps.get_model('api', 'Request_Log')
Request_Progress = apps.get_model('api', 'Request_Progress')


class requestLog:
    logger = None
    dateTimeFormat = "%Y-%m-%dT%H:%M:%SZ" # ISO 8601 datetime    #"%Y-%m-%dT%H:%M:%SZ_%f"
    earliest_Possible_DateTime=None
    db_BaseFileName_DateFormat = "%Y_%m_%d"  # Year, Month, Day

    def logOutput(self, msg):
        if (self.logger == None):
            print(msg)
        else:
            try:
                self.logger.info(msg)
            except:
                print(msg)
                
    def add_New_Request(self, dataToLog):
        
        currentDateTime = datetime.datetime.utcnow()
        currentDateTime_Str = currentDateTime.strftime(self.dateTimeFormat)
        
        logObj = {
                  'iso_DateTime':currentDateTime_Str,
                  'request_Data':dataToLog
                  }
        try:
            log = Request_Log(unique_id=currentDateTime_Str, log=logObj)
            log.save()
        except:
            e = sys.exc_info()[0]
            errMsg = "requestLog: Request Log Error Line: System Error Message: " + str(e) + " logObj Details: " + str(logObj)
            print(errMsg)

        
    def get_RequestData_ByRange(self, earliest_DateTime, latest_DateTime):
        
        retLogs = [] # Scoping
        try:
             retLogs = self.get_RequestLogs_ForDatetimeRange(earliest_DateTime, latest_DateTime)
        except:
            pass
        
        return retLogs

    def decode_Request_For_Logging(self, theRequest, theIP):

        reqParams = None
        httpUserAgent = None
        apiURLPath = None
        

        # reqParams
        try:
            reqParams = str(theRequest.REQUEST)  # Should contain list of params passed in.
        except:
            pass
        
        # httpUserAgent
        try:
            httpUserAgent = str(theRequest.META.get('HTTP_USER_AGENT'))
        except:
            pass
            
        # apiURLPath
        try:
            apiURLPath = str(theRequest.META.get('PATH_INFO'))
        except:
            pass
    
        #client_IP = get_client_ip(theRequest)

        retObj = {
                  "RequestParams":reqParams,
                  "httpUserAgent":httpUserAgent,
                  "client_ip":str(theIP),
                  "API_URL_Path":apiURLPath
                  }
        
        return retObj
    
    # Mapping serverside request fields to existing client side fields
    def decode_ServerSideRequest_For_Logging(self, theServerSide_Request):

        try:
            retObj = {
                      "RequestParams":theServerSide_Request['Additional_Notes'],
                      "httpUserAgent":theServerSide_Request['jobStatus_Note'],
                      "client_ip":theServerSide_Request['IPMapping'],
                      "API_URL_Path":theServerSide_Request['jobID_Info']
                      }

        except:
            retObj = "SERVER SIDE ERROR in decode_ServerSideRequest_For_Logging()"
        return retObj
    
    # A way to keep objects standardized between functions and to easily add params
    def get_ServerSide_RequestParams_FromInputs(self, theJobID, server_OneWord_JobStatusNote, server_AdditionalNotes_String):
        theIP = "0.0.0.0"
        jobID_Info = "Server:JobID: " + str(theJobID)
        jobStatus_Note = "Status: " + str(server_OneWord_JobStatusNote)
        additional_Note = "AdditionalNotes: " + str(server_AdditionalNotes_String)
        
        retObj = {
                  "jobID_Info":jobID_Info,  # Hold the JobID info
                  "jobStatus_Note":jobStatus_Note,  # SimpleNote (Has the job started or not)
                  "Additional_Notes":additional_Note,
                  "IPMapping":theIP
                  }
        
        return retObj
    
    # Interface for external objects to call this function, one call, and the new serverside request is saved.
    def add_New_ServerSide_Request(self, theJobID, server_OneWord_JobStatusNote, server_AdditionalNotes_String):
        currentDateTime = datetime.datetime.utcnow()
        currentDateTime_Str = currentDateTime.strftime( "%Y_%m_%d_%H_%M_%S_%f"  )
        # Create a new serverside request object
        serverRequestObject = self.get_ServerSide_RequestParams_FromInputs(theJobID, server_OneWord_JobStatusNote, server_AdditionalNotes_String)
        readyToLogObj = self.decode_ServerSideRequest_For_Logging(serverRequestObject)

        log = Request_Log(unique_id=currentDateTime_Str, log=readyToLogObj)
        log.save()

    def get_All_Data_From_DB_ForFile(self, theFileName):
        theDB = Request_Log.objects.filter(log_date=theFileName)
        return theDB

    def _does_DB_Exist(self, theFileName):
        return Request_Log.objects.filter(log_date=theFileName)

    def _get_DB_FileName(self, theDateTimeObj):
        baseFileName = theDateTimeObj.strftime(self.db_BaseFileName_DateFormat)
        return baseFileName

    def get_RequestLogs_ForDatetimeRange(self, earliest_UTC_DateTime, latest_UTC_DateTime):
        # First, validate that the search range
        rightNow = datetime.datetime.utcnow()
        if (earliest_UTC_DateTime < self.earliest_Possible_DateTime):
            # Set the beginning of the range to the first possible log date
            earliest_UTC_DateTime = self.earliest_Possible_DateTime

        if (latest_UTC_DateTime > rightNow):
            # set the end of the range to the last possible log date (Today's date)
            latest_UTC_DateTime = rightNow

        # Interval setup
        intervalString = "1 days"
        intervalValue = int(intervalString.split(" ")[0])
        intervalType = intervalString.split(" ")[1]
        time_DeltaArgs = {intervalType: intervalValue}

        dailyFormatString = self.db_BaseFileName_DateFormat
        tempDateTime_str = earliest_UTC_DateTime.strftime(dailyFormatString)
        reusable_Interval_DateTime = datetime.datetime.strptime(tempDateTime_str, dailyFormatString)

        # A place to hold all the possible log file names.
        db_FileNames_List = []

        # Create the list of strings by incrementing reusable_Interval_DateTime
        while (reusable_Interval_DateTime <= latest_UTC_DateTime):

            # Add the string to the list
            current_FileName = self._get_DB_FileName(
                reusable_Interval_DateTime)  # reusable_Interval_DateTime.strftime(dailyFormatString)

            # optimization, only add this string if a log file can be found for it. (Saves a lot of processing on the next step)
            isExist = self._does_DB_Exist(current_FileName)
            if (isExist == True):
                db_FileNames_List.append(current_FileName)

            # Increment the reusable datetime by 1 day
            reusable_Interval_DateTime = reusable_Interval_DateTime + datetime.timedelta(**time_DeltaArgs)

        logEntriesToReturn = []

        # Only proceed if the list of files is not empty
        if (len(db_FileNames_List) > 0):

            # Simple iteration,
            firstFile_InList = db_FileNames_List[0]
            lastFile_InList = db_FileNames_List[-1]
            isOneFileOnly = False
            if (firstFile_InList == lastFile_InList):
                isOneFileOnly = True

            for current_DB_FileName in db_FileNames_List:

                try:
                    currentData = self.get_All_Data_From_DB_ForFile(current_DB_FileName)
                    for currentData_Item in currentData:
                        logEntriesToReturn.append(
                            currentData_Item)  # 'key' and 'value'
                except:
                    pass

        return logEntriesToReturn
    