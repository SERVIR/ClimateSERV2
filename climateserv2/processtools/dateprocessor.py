from datetime import date
import time
import logging

try:
    import climateserv2.parameters as params
except:
    import parameters as params


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# To check if day, month, year form a valid date
def checkDayValid(day,month,year):
    try:
        value= date(year,month,day)
        return True
    except ValueError:
        return False

# To create a list based on a range of months in an year
def createListMonthPlusYear(beginmonth,endmonth,year):
    list=[]
    for x in range(beginmonth,endmonth+1):
        list.extend([[x,year]])
    return list

# To create a list based on a range of days in a month of an year
def createListDayPlusMonthYear(beginday,endday,month, year):
    list=[]
    for x in range(beginday,endday+1):
        if (checkDayValid(x,month,year)):
            list.extend([[x,month,year]])
    return list

# To get list of years based on a range
def getListOfYears(beginyear,endyear):
    return range(beginyear,endyear+1)

# To get list of months based on start month/year and end month/year
def getListOfMonths(beginmonthyear,endmonthyear):
    years = getListOfYears(beginmonthyear[1],endmonthyear[1])
    firstyear = list(years).pop(0)
    output = []
    if (len(years)==0):
        item = createListMonthPlusYear(beginmonthyear[0],endmonthyear[0],firstyear)
        output.extend(item)
        return output
    else:
        lastyear = list(years).pop()
        item = createListMonthPlusYear(beginmonthyear[0],12,firstyear)
        output.extend(item)
        for x in years:
            item2 = createListMonthPlusYear(1,12,x)
            output.extend(item2)
        item3 = createListMonthPlusYear(1,endmonthyear[0],lastyear)
        output.extend(item3)
        return output

# To get list of days based on start day/month/year and end day/month/year
def getListOfDays(begindaymonthyear,enddaymonthyear):
    try:
        listofmonths = getListOfMonths([begindaymonthyear[1],begindaymonthyear[2]],[enddaymonthyear[1],enddaymonthyear[2]])
        firstmonth = list(listofmonths).pop(0)
    except:
        print(listofmonths)
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

# To get date based on a date string and interval type
def breakApartDate(datestring,intervaltype):
    interval = params.intervals[0]
    timedecoded = time.strptime(datestring, interval['pattern'])
    if (intervaltype == 0):
        return [timedecoded[2],timedecoded[1],timedecoded[0]]
    elif (intervaltype ==1):
        return [timedecoded[1],timedecoded[0]]
    elif (intervaltype ==2):
        return [timedecoded[0]]

# to get list of times based on range of dates and interval type
def getListOfTimes(beginstring,endstring,intervaltype):
    startDate = breakApartDate(beginstring,intervaltype)
    endDate = breakApartDate(endstring,intervaltype)
    if (intervaltype == 0):
        return getListOfDays(startDate,endDate)
    elif (intervaltype ==1):
        return getListOfMonths(startDate,endDate)
    elif (intervaltype ==2):
        return getListOfYears(startDate[0],endDate[0])





    
    
