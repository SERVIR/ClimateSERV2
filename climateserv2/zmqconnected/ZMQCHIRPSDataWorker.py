'''
Created on Jan 30, 2015
@author: jeburks

Modified starting from Sept 2015
@author: Kris Stanton
'''
from climateserv2.file import DataCalculator as dc
import json
import climateserv2.zmqconnected as zmq
from climateserv2.locallog import locallogging as llog
from climateserv2.file import MaskTempStorage  as mst
import sys
import climateserv2.processtools.pMathOperations as pMath
import os


class ZMQCHIRPSDataWorker():
    logger = llog.getNamedLogger("request_processor")
    inputreceiver=None
    outputreceiver = None
    operatingData = None
    inputconn = None
    outputConn = None
    name = None
    
    # KS Refactor 2015 // Turns out the worker duplication issue was really just a double logging issue combined with an old ghost instance.
    # logging and outputing the PID to be sure this is not a duplication issue.
    pid = None
    
    def __getUniqueWorkerNameString__(self):
        return self.name + ":("+self.pid+")"
    
    def __doWork__(self):

        # Right here, insert a bit of code that goes into the AnalysisTools and attempts to figure out if this is a custom request.
        # return true/false
        # Then have an if statement, if customJob == False, Just do the normal stuff that was already here before,
        # If customJob == True, call another AnalysisTools function which bypasses this whole process here below.

        # Actually, the above does not even need to be this complicated, the issue is mainly about which mask to read and where to write the data.

        # Starting Defaults
        # bounds = None
        # clippedmask = None

        # Nope
        # current_worker_sub_type_name = (self.operatingData['sub_type_name'])
        # if (current_worker_sub_type_name == "CHIRPS_REQUEST"):
        #     uuid_to_use_for_mask = self.operatingData['datatype_uuid_for_CHIRPS']
        # if (current_worker_sub_type_name == "SEASONAL_FORECAST"):
        #     uuid_to_use_for_mask = self.operatingData['datatype_uuid_for_CHIRPS']
        # bounds, clippedmask = self.__readMask__(self.operatingData['uid'])

        # WRONG.. this code needs to remain HIGHLY EFFICIENT... so instead of all this checking and try/catching..
        # # I added the 'current_mask_and_storage_uuid' param to the normal worklist (and just duplicated the unique id there)
        # # That should leave this thing that gets called thousands of times per request almost completely unchanged..
        #
        # # The whole point of this new section, Figure out which UUID to use for getting the mask.
        # uuid_to_use_for_mask = None
        #
        # # is this a derived product?
        # is_derived_product = False
        # try:
        #     is_derived_product = self.operatingData['derived_product']  # Expecting this to be True if it exists..
        # except:
        #     is_derived_product = False
        #
        # # If this is a derived product, Then we need to drill into which type so that we know which mask to load
        # # If not, then just the uid of the job it self should be enough.
        # if (is_derived_product == True):
        #     try:
        #         uuid_to_use_for_mask = self.operatingData['current_mask_and_storage_uuid']
        #     except Exception, e:
        #         self.logger.warn("Error: " + str(e))
        # else:
        #     # Set the bounds and clippedmask based on the UID (This is the way a 'normal' request for data flows through.
        #     try:
        #         uuid_to_use_for_mask = self.operatingData['uid']
        #         #bounds, clippedmask = self.__readMask__(self.operatingData['uid'])
        #     except Exception, e:
        #         self.logger.warn("Error: " + str(e))

        # Existing
        try:

            #bounds, clippedmask = self.__readMask__(self.operatingData['uid'])   
            #bounds, clippedmask = self.__readMask__(uuid_to_use_for_mask)
            bounds, clippedmask = self.__readMask__(self.operatingData['current_mask_and_storage_uuid'])
            #self.logger.debug("Working on datatype: "+self.operatingData['datatype'])
            #self.logger.debug("Worker: " +str(self.name)+ " : Value of bounds: " + str(bounds))  # ks refactor
            self.operatingData['clippedmask'] = clippedmask
            if (self.operatingData['intervaltype'] == 0):
                value = dc.getDayValueByDictionary(self.operatingData)
                dateOfOperation = str(self.operatingData['month'])+"/"+str(self.operatingData['day'])+"/"+str(self.operatingData['year'])
                self.logger.debug("DateOfOperation: "+str(dateOfOperation))
                return {"date":dateOfOperation,"epochTime":self.operatingData['epochTime'],'value':value}
            elif (self.operatingData['intervaltype'] == 1):
                value = dc.getMonthValueByDictionary(self.operatingData)
                dateOfOperation = str(self.operatingData['month'])+"/"+str(self.operatingData['year'])
                self.logger.debug("DateOfOperation: "+str(dateOfOperation))
                return {"date":dateOfOperation,"epochTime":self.operatingData['epochTime'],'value':value}
            elif (self.operatingData['intervaltype'] == 2):
                value = dc.getYearValueByDictionary(self.operatingData)
                dateOfOperation = str(self.operatingData['year'])
                self.logger.debug("DateOfOperation: "+str(dateOfOperation))
                return {"date":dateOfOperation,"epochTime":self.operatingData['epochTime'],'value':value}
        except Exception as e:
            self.logger.warn("Error: "+str(e))
        finally:
            del clippedmask
        mathop = pMath.mathOperations(dict['operationtype'],1,self.operatingData['datatype']['fillValue'],None) 
        if (self.operatingData['intervaltype'] == 0):
            
            dateOfOperation = str(self.operatingData['month'])+"/"+str(self.operatingData['day'])+"/"+str(self.operatingData['year'])
            return {"date":dateOfOperation,"epochTime":self.operatingData['epochTime'],'value':mathop.getFillValue()}
        elif (self.operatingData['intervaltype'] == 1): 
            dateOfOperation = str(self.operatingData['month'])+"/"+str(self.operatingData['year'])
            return {"date":dateOfOperation,"epochTime":self.operatingData['epochTime'],'value':mathop.getFillValue()}
        elif (self.operatingData['intervaltype'] == 2):
            dateOfOperation = str(self.operatingData['year'])
            return {"date":dateOfOperation,"epochTime":self.operatingData['epochTime'],'value':mathop.getFillValue()}
    
    def __listen__(self):
        while (True) :
            # KS Refactor 2015 // possible issue where multiple workers are processing the same work items.
            #time.sleep(1)  # Issue may actually be located in the .sh scripts where the workers are created for each head.
            request = json.loads(self.inputreceiver.recv())
            self.operatingData = request
            self.doWork()
        
    def __readMask__(self, uid):
        return mst.readHMaskFromTempStorage(uid)
        
    
    def __cleanup__(self):
       
        self.operatingData = None
    
     
    def __init__(self, name, inputconn, outputconn):
        
        # Get the PID to ensure the thread isn't duplicated.
        self.pid = os.getpid()
        
        self.name = name
        self.inputconn = inputconn
        self.outputconn = outputconn
        self.logger.info("Init CHIRPSDataWorker (PID: "+str(self.pid)+" ) : ("+self.name+") listening on "+self.inputconn+" outputting to "+self.outputconn)
        context = zmq.Context()
        self.inputreceiver = context.socket(zmq.PULL)
        self.inputreceiver.connect(self.inputconn)
        self.outputreceiver = context.socket(zmq.PUSH)
        self.outputreceiver.connect(self.outputconn)
        self.__listen__()
        
        
    def doWork(self):  
        #self.logger.info("("+self.name+"):doWork: About to work on request: " +str(self.operatingData))
        #self.logger.info("(" + self.name + "):doWork: About to work on request: " + str(self.operatingData))

        results = self.__doWork__()

        # Add a few more params,

        # Need to extend this 'results' object to include some more differentiating data types (if they exist)
        try:
            # Attempt to extend the results object
            if (self.operatingData['derived_product'] == True):
                results['derived_product'] = self.operatingData['derived_product']
                results['current_mask_and_storage_uuid'] = self.operatingData['current_mask_and_storage_uuid']
                results['sub_type_name'] = self.operatingData['sub_type_name']
                results['datatype'] = self.operatingData['datatype']
        except:
            pass

        # When 'results' comes back.  It looks something like this.
        # results = {"date":dateOfOperation,"epochTime":self.operatingData['epochTime'],'value':mathop.getFillValue()}
        #

        # TODO!
        # # The data is actually returned right here... but what we need to do is save the data to separate UUIDs than what would normally happen on a normal request.
        # # So for Monthly Analysis, need to save to the UUIDs that match the storage
        # # IF Monthly Storage type, then do this  # 'sub_type_name': sub_type_name, 'derived_product': True, 'special_type': 'MonthlyRainfallAnalysis' }
        # results['special_type'] = self.operatingData['special_type']  # Maybe this value is 'MonthlyRainfallAnalysis'
        # results['sub_type_name'] = self.operatingData['sub_type_name']   # Tells us if this is CHIRPS or SEASONAL_FORECAST
        # results['current_mask_and_storage_uuid'] = self.operatingData['current_mask_and_storage_uuid']
        # #results['datatype_uuid_for_CHIRPS'] = self.operatingData['datatype_uuid_for_CHIRPS']
        # #results['datatype_uuid_for_SeasonalForecast'] = self.operatingData['datatype_uuid_for_SeasonalForecast']


        # ks refactor // Understanding how the 'workers' do their work!
        #self.logger.debug("Worker: " +str(self.name)+ " : doWork : Value of results: " + str(results))  
        self.logger.debug("("+self.name+"):doWork: Value of json.dumps(results): " + str(json.dumps(results)))
        
        results['workid'] = self.operatingData['workid']
        self.logger.debug("Worker ("+self.name+"): "+"Working on "+results['workid'])
        self.outputreceiver.send_string(json.dumps(results))
        #self.logger.debug("Worker (" + self.name + "): " + " About to call __cleanup__")
        self.__cleanup__()
        
if __name__ == "__main__":
    name = sys.argv[1]
    inputconn = sys.argv[2]
    outputconn = sys.argv[3]
    #print("alert 1")
    #logger2 = llog.getNamedLogger("request_processor")
    #logger2.debug("Alert 1")
    ZMQCHIRPSDataWorker(name, inputconn, outputconn)       
        