'''
Created on Jan 30, 2015
@author: jeburks

Modified starting from Sept 2015
@author: Kris Stanton
'''
import os
import sys
import json
import zmq

module_path = os.path.abspath(os.getcwd())
if module_path not in sys.path:
    sys.path.append(module_path)

try:
    from climateserv2.locallog import locallogging as llog
    import climateserv2.processtools.pMathOperations as pMath
except:
    import locallog.locallogging as llog
    import processtools.pMathOperations as pMath
    import parameters as params


class ZMQCHIRPSDataWorker():
    logger = llog.getNamedLogger("request_processor")
    inputreceiver = None
    outputreceiver = None
    operatingData = None
    inputconn = None
    outputConn = None
    name = None

    # KS Refactor 2015 // Turns out the worker duplication issue was really just a double logging issue combined with an old ghost instance.
    # logging and outputing the PID to be sure this is not a duplication issue.
    pid = None

    def __getUniqueWorkerNameString__(self):
        return self.name + ":(" + self.pid + ")"

    def __doWork__(self):

        self.operatingData=eval(self.operatingData)

        if (self.operatingData["intervaltype"] == 0):
            if(self.operatingData["operationtype"] ==6):
                return { "value": self.operatingData["value"]}
            else:
                dateOfOperation = str(self.operatingData["month"]) + "/" + str(self.operatingData["day"]) + "/" + str(
                    self.operatingData["year"])
                return {"date": dateOfOperation, "epochTime": self.operatingData["epochTime"],
                    "value": self.operatingData["value"]}
        elif (self.operatingData["intervaltype"] == 1):
            dateOfOperation = str(self.operatingData["month"]) + "/" + str(self.operatingData["year"])
            return {"date": dateOfOperation, "epochTime": self.operatingData["epochTime"],
                    "value": self.operatingData["value"]}
        elif (self.operatingData["intervaltype"] == 2):
            dateOfOperation = str(self.operatingData["year"])
            return {"date": dateOfOperation, "epochTime": self.operatingData["epochTime"],
                    "value": self.operatingData["value"]}

    def __listen__(self):
        while (True):
            # KS Refactor 2015 // possible issue where multiple workers are processing the same work items.
            # time.sleep(1)  # Issue may actually be located in the .sh scripts where the workers are created for each head.
            request = json.loads(self.inputreceiver.recv())
            self.operatingData = request
            self.doWork()


    def __cleanup__(self):

        self.operatingData = None

    def __init__(self, name, inputconn, outputconn):

        # Get the PID to ensure the thread isn't duplicated.
        self.pid = os.getpid()

        self.name = name
        self.inputconn = inputconn
        self.outputconn = outputconn
        self.logger.info("Init CHIRPSDataWorker (PID: " + str(
            self.pid) + " ) : (" + self.name + ") listening on " + self.inputconn + " outputting to " + self.outputconn)
        context = zmq.Context()
        self.inputreceiver = context.socket(zmq.PULL)
        self.inputreceiver.connect(self.inputconn)
        self.outputreceiver = context.socket(zmq.PUSH)
        self.outputreceiver.connect(self.outputconn)
        self.__listen__()

    def doWork(self):

        results = self.__doWork__()
        try:
            # Attempt to extend the results object
            if (self.operatingData['derived_product'] == True):
                results['derived_product'] = self.operatingData['derived_product']
                results['current_mask_and_storage_uuid'] = self.operatingData['current_mask_and_storage_uuid']
                results['sub_type_name'] = self.operatingData['sub_type_name']
                results['datatype'] = self.operatingData['datatype']
        except:
            pass

        self.logger.debug("(" + self.name + "):doWork: Value of json.dumps(results): " + str(json.dumps(results)))

        results['workid'] = self.operatingData['workid']
        self.logger.debug("Worker (" + self.name + "): " + "Working on " + results['workid'])
        self.outputreceiver.send_string(json.dumps(results))
        self.__cleanup__()


if __name__ == "__main__":
    name = sys.argv[1]
    inputconn = sys.argv[2]
    outputconn = sys.argv[3]
    ZMQCHIRPSDataWorker(name, inputconn, outputconn)
