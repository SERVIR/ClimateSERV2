import time
import CHIRPS.utils.configuration.parameters as params
import re
import datetime
import CHIRPS.utils.processtools.dateIndexTools as dit

def breakApartEsiName(filename):
    ''' 
    
    :param filename:
    '''
    parts = filename.split(".")
    #ending = parts[-1]
    if "12WK" in filename:
		year = int(parts[0][11:15])
		newDate=datetime.datetime(int(year),1,1)+ datetime.timedelta(days=(int(parts[0][-3:])-1))
		#calculate day month year
		day =newDate.day
		month = newDate.month
    if "4WK" in filename:		
		year = int(parts[0][10:14])
		newDate=datetime.datetime(int(year),1,1)+ datetime.timedelta(days=(int(parts[0][-3:])-1))
		#calculate day month year
		day =newDate.day
		month = newDate.month		
    
    return {'year':year,'month':month,'day':day}

def breakApartChripsName(filename):
    '''
    
    :param filename:
    '''
    parts = filename.split(".")
    ending = parts[-1]
    day = int(parts[-2])
    month = int(parts[-3])
    year = int(parts[-4])
    chirps = parts[:-5]
    
    return {'year':year,'month':month,'day':day,'ending':ending, 'chirps':chirps}
def breakApartGEFSNewName(filename):
    '''
	data.1985.0101.created-from.1985.0101.tif
    :param filename:
    '''
    parts = filename.split(".")
    ending = parts[-1]
    day = int(parts[2][-2:])
    month = int(parts[2][:2])
    year = int(parts[1])

    return {'year':year,'month':month,'day':day,'ending':ending}
	
def breakApartGEFSName(filename):
    '''
    data.1985.041.tif
    :param filename:
    '''
    parts = filename.split(".")
    ending = parts[-1]
    decad = int(parts[-2][-1])
    month = int(parts[-2][:2])
    year = int(parts[-3])
    chirps = parts[:-4]

    return {'year':year,'month':month,'decad':decad,'ending':ending, 'chirps':chirps}

def breakApartemodisName(filename):
    '''
    
    :param filename:
    '''
    parts = filename.split(".")
    ending = parts[-1]
    index = int(parts[0][2:-2])-1
    area = parts[0][0:2]
    year = int("20"+parts[0][-2:])
    dateAtFile = dit.DecadalIndex().getDateBasedOnIndex(index, year)
	#dateAtFile = dit.EveryTenDaysIndex().indexAndYearToDate(year, index)
    #calculate day month year
    day =dateAtFile.day
    month = dateAtFile.month
    
    
    return {'year':year,'month':month,'day':day,'ending':ending,'area':area}

def breakApartemodisNameAdjust(filename, adjust):
    '''
    
    :param filename:
    '''
    parts = filename.split(".")
    ending = parts[-1]
    index = int(parts[0][adjust:-2])-1
    area = parts[0][0:adjust]
    year = int("20"+parts[0][-2:])
    dateAtFile = dit.DecadalIndex().getDateBasedOnIndex(index, year)
    #dateAtFile = dit.EveryTenDaysIndex().indexAndYearToDate(year, index)
    #calculate day month year
    day =dateAtFile.day
    month = dateAtFile.month
    
    
    return {'year':year,'month':month,'day':day,'ending':ending,'area':area}

def convertTimeToEpoch(timein,interval):
    '''
    
    :param timein:
    :param interval:
    '''
     #ISO-8601
    pattern = params.intervals[interval]['pattern']
    return int(time.mktime(time.strptime(timein, pattern)))
       
       
def extractYear(time):
    '''
    
    :param time:
    '''
    pattern = re.compile("(\d{4})")
    
def extractMonth(time):
    '''
    
    :param time:
    '''
    pattern = re.compile("(\d{4})")
     
def getListOfTimes(begintime,endtime,interval):
    '''
    
    :param begintime:
    :param endtime:
    :param interval:
    '''
     
    starttimeepoch = convertTimeToEpoch(begintime,interval)
    endtimeepoch = convertTimeToEpoch(endtime,interval)
      
      
def createDateFromYear(year):
    '''
    
    :param year:
    '''
    return datetime.date(year,1,1)

def createDateFromYearMonth(year,month):
    '''
    
    :param year:
    :param month:
    '''
    return datetime.date(year,month,1)
      
def createDateFromYearMonthDay(year,month,day):
    '''
    
    :param year:
    :param month:
    :param day:
    '''
    return datetime.date(year,month,day)

def formatDateBasedOnInterval(date,interval):
    return date.strftime(params.intervals['pattern'])






