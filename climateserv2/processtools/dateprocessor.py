'''
Created on Jun 24, 2014

@author: jeburks
'''

from datetime import date
import CHIRPS.utils.configuration.parameters as params
import time
import logging
import math

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def checkDayValid(day,month,year):
    '''
    
    :param day:
    :param month:
    :param year:
    '''
    try:
        value= date(year,month,day)
        return True
    except ValueError:
        return False
    
def createListMonthPlusYear(beginmonth,endmonth,year):
    '''
    
    :param beginmonth:
    :param endmonth:
    :param year:
    '''
    list=[]
    for x in range(beginmonth,endmonth+1):
        list.extend([[x,year]])
    return list

def createListDayPlusMonthYear(beginday,endday,month, year):
    '''
    
    :param beginday:
    :param endday:
    :param month:
    :param year:
    '''
    list=[]
    for x in range(beginday,endday+1):
        if (checkDayValid(x,month,year)):
            list.extend([[x,month,year]])
    return list

def getListOfYears(beginyear,endyear):
    '''
    
    :param beginyear:
    :param endyear:
    '''
    return range(beginyear,endyear+1)

def getListOfMonths(beginmonthyear,endmonthyear):
    '''
    
    :param beginmonthyear:
    :param endmonthyear:
    '''
    years = getListOfYears(beginmonthyear[1],endmonthyear[1])
    firstyear = years.pop(0)
    
    output = []
    if (len(years)==0):
        item = createListMonthPlusYear(beginmonthyear[0],endmonthyear[0],firstyear)
        output.extend(item)
        return output
    else:
        lastyear = years.pop()
        item = createListMonthPlusYear(beginmonthyear[0],12,firstyear)
        output.extend(item)
        for x in years:
            item2 = createListMonthPlusYear(1,12,x)
            output.extend(item2)
        item3 = createListMonthPlusYear(1,endmonthyear[0],lastyear) 
        output.extend(item3)
        return output
    
def getListOfDays(begindaymonthyear,enddaymonthyear):
    '''
    
    :param begindaymonthyear:
    :param enddaymonthyear:
    '''
    days=[]
    listofmonths = getListOfMonths([begindaymonthyear[1],begindaymonthyear[2]],[enddaymonthyear[1],enddaymonthyear[2]])
    
    firstmonth = listofmonths.pop(0)
    if (len(listofmonths) ==0) :
        lastmonth = firstmonth                
    else:                        
        lastmonth = listofmonths.pop()
    output = []
    if (lastmonth == firstmonth):
        item = createListDayPlusMonthYear(begindaymonthyear[0],enddaymonthyear[0],enddaymonthyear[1],enddaymonthyear[2])
        output.extend(item)
        return output
    else:
        item = createListDayPlusMonthYear(begindaymonthyear[0],31,begindaymonthyear[1],begindaymonthyear[2])
        output.extend(item)
        for x in listofmonths:
            item2 = createListDayPlusMonthYear(1,31,x[0],x[1])
            output.extend(item2)
        item3 = createListDayPlusMonthYear(1,enddaymonthyear[0],lastmonth[0],lastmonth[1]) 
        output.extend(item3)
        return output

def breakApartDate(datestring,intervaltype):
    '''
    
    :param datestring:
    :param intervaltype:
    '''
    interval = params.intervals[0]
    timedecoded = time.strptime(datestring, interval['pattern'])
    #logger.debug(timedecoded)
    if (intervaltype == 0):
        return [timedecoded[2],timedecoded[1],timedecoded[0]]
    elif (intervaltype ==1):
        return [timedecoded[1],timedecoded[0]]
    elif (intervaltype ==2):
        return [timedecoded[0]]

def getListOfTimes(beginstring,endstring,intervaltype):
    '''
    
    :param beginstring:
    :param endstring:
    :param intervaltype:
    '''
    startDate = breakApartDate(beginstring,intervaltype)
    endDate = breakApartDate(endstring,intervaltype)
    if (intervaltype == 0):
        return getListOfDays(startDate,endDate)
    elif (intervaltype ==1):
        return getListOfMonths(startDate,endDate)
    elif (intervaltype ==2):
        return getListOfYears(startDate[0],endDate[0])




    
    
