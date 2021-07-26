import zmq
import CHIRPS.utils.locallog.locallogging as llog
import sys

class ARGProxyQueue:
    location1 = None
    location2 = None
    logger = llog.getNamedLogger("request_processor")
    
    def __init__(self,location1, location2):
        self.location1 = location1
        self.location2 = location2
        
        self.__setup__()
        
    def __setup__(self):
        
        self.logger.info('Starting Arg Proxy Queue listening on: '+self.location1+" returning to: "+self.location2)
        context = zmq.Context()
        inputsocket = context.socket(zmq.PULL)
        inputsocket.bind(self.location1)
        outputsocket = context.socket(zmq.PUSH)
        outputsocket.bind(self.location2)
         
        zmq.device(zmq.QUEUE, inputsocket, outputsocket)
    
if __name__ == "__main__":
    location1 = sys.argv[1]
    location2 = sys.argv[2]
    proxy= ARGProxyQueue(location1,location2)