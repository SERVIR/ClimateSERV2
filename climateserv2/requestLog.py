import climateserv2.db.bddbprocessing as bdp
import datetime
import sys



# Manage interfacing with the RequestLog BDDB. (opening and closing of DB after requests)
# Handle additional formatting requirements on the output of log data (if we need it in CSV, JSON, etc)
# Adding DateTime info to request data.
class requestLog:
    logger = None
    dateTimeFormat = "%Y-%m-%dT%H:%M:%SZ" # ISO 8601 datetime    #"%Y-%m-%dT%H:%M:%SZ_%f"
    
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
        
        requestLog_Connection = None
        isConnected = False
        # Make the DB Connection
        try:
            requestLog_Connection = bdp.BDDbConnector_RequestLog()
            isConnected = True
        except:
            e = sys.exc_info()[0]
            errMsg = "requestLog: Error Connecting to Request Log DB Error Line: requestLog_Connection = bdp.BDDbConnector_RequestLog() System Error Message: " + str(e)
            self.logOutput(errMsg)
            
        
        # Submit the request and close the connection
        if(isConnected == True):
            try:
                requestLog_Connection.add_Request(logObj, False)
                #requestLog_Connection.add_Request(logObj)
                requestLog_Connection.close()
            except:
                e = sys.exc_info()[0]
                errMsg = "requestLog: Error Connecting to Request Log DB Error Line: requestLog_Connection = bdp.BDDbConnector_RequestLog() System Error Message: " + str(e) + " logObj Details: " + str(logObj)
                print(errMsg)
                requestLog_Connection.close()
            
        
    def get_RequestData_ByRange(self, earliest_DateTime, latest_DateTime):
        
        retLogs = [] # Scoping
        try:
            requestLog_Connection = bdp.BDDbConnector_RequestLog()
            requestLog_Connection.close()  # Fix for request logs crashing worker threads.. the function on the next line looks into older connections not newer ones.
            retLogs = requestLog_Connection.get_RequestLogs_ForDatetimeRange(earliest_DateTime, latest_DateTime)
        except:
            pass
        
        return retLogs
        
    
    
    # This is where we decide what we are storing
    def decode_Request_For_Logging(self, theRequest, theIP):
        #request.REQUEST
        
        reqParams = None
        httpUserAgent = None
        apiURLPath = None
        
        # Note on all the try/except.  Not sure if these fail or simply return None.. so just trying to keep the code from breaking here..
        
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
    
        # Just pass this one in (better function for it found in the views.py file
        #client_IP = get_client_ip(theRequest)
        
        
        retObj = {
                  "RequestParams":reqParams,
                  "httpUserAgent":httpUserAgent,
                  "client_ip":str(theIP),
                  "API_URL_Path":apiURLPath
                  }
        
        return retObj
    
    # ks Update - We also need to log certain serverside events
    # Mapping serverside request fields to existing client side fields
    def decode_ServerSideRequest_For_Logging(self, theServerSide_Request):
        # Extra Params that go into a normal clientside request
        
        
        retObj = None
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
        # Create a new serverside request object
        serverRequestObject = self.get_ServerSide_RequestParams_FromInputs(theJobID, server_OneWord_JobStatusNote, server_AdditionalNotes_String)
        readyToLogObj = self.decode_ServerSideRequest_For_Logging(serverRequestObject)
        self.add_New_Request(readyToLogObj)
        
    