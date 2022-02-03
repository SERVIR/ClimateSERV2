import logging

try:
    import climateserv2.parameters as params
except:
    import parameters as params
import time
import os

# To write to a log file based on log level
class StreamToLogger(object):
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

# Set up log file with permissions and logging level
def getNamedLogger(nameofLogger):
    logfilepath = params.logfilepath + nameofLogger + time.strftime("%Y%m%d") + ".log"
    # os.chmod(params.logfilepath, 0o777)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename=logfilepath,
                        filemode='a')
    logger = logging.getLogger(nameofLogger)
    if params.logToConsole:
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)
    return logger
