import logging
import time

from api.models import Parameters


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
def getNamedLogger(name_of_logger):
    try:
        params = Parameters.objects.first()
        log_path = params.logfilepath
        if params.logToConsole:
            log_to_console = True
        else:
            log_to_console = False
    except:
        log_path = ""
        log_to_console = False
    log_file_path = log_path + name_of_logger + time.strftime("%Y%m%d") + ".log"
    # os.chmod(params.logfilepath, 0o777)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename=log_file_path,
                        filemode='a')
    logger = logging.getLogger(name_of_logger)
    if log_to_console:
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)
    return logger
