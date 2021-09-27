import math
import time
import datetime 

# To convert epoch time to Julian day number
def convertEpochToJulianDay(epochTime):
    return int(time.strftime("%j",time.gmtime(epochTime)))

# To convert given day, month, year to epoch value
def convertDayMonthYearToEpoch(day,month,year):
    return float(datetime.date(year, month, day).strftime("%s"))

# To convert given month, year to epoch value
def convertMonthYearToEpoch(month,year):
    return float(datetime.date(year, month, 1).strftime("%s"))

# To retrieve last day of month in an year
def getLastDayOfMonth(month,year):
    monthToProcess = month+1
    yearToProcess = year
    if (month == 12):
        monthToProcess = 1
        yearToProcess = year+1
    epochTime = float(datetime.date(yearToProcess, monthToProcess, 1).strftime("%s"))-86400
    return int(time.strftime("%d",time.gmtime(epochTime)))

# Indexing on daily basis
class DailyIndex:
    
    def getIndexesBasedOnEpoch(self,startEpochTime, endEpochTime):
        jStart = convertEpochToJulianDay(startEpochTime)-1
        jEnd = convertEpochToJulianDay(endEpochTime)
        if (jStart == jEnd):
            return [jStart]
        return range(jStart,jEnd)
    def getIndexesBasedOnDate(self,daystart,monthstart,yearstart,dayend,monthend,yearend):
        return self.getIndexesBasedOnEpoch(convertDayMonthYearToEpoch(daystart,monthstart,yearstart),convertDayMonthYearToEpoch(dayend,monthend,yearend))
    
    def getIndexBasedOnDate(self,day,month,year):
        return self.getIndexBasedOnEpoch(convertDayMonthYearToEpoch(day,month,year))
    
    def getIndexBasedOnEpoch(self,epochTime):
        return convertEpochToJulianDay(epochTime)-1
    
    def getDateBasedOnIndex(self,index,year):
        return datetime.datetime(year, 1, 1) + datetime.timedelta(index*5)
    
    def cullDateList(self,dates):
        return dates

# Indexing every five days
class EveryFiveDaysIndex:
    def getIndexesBasedOnEpoch(self,startEpochTime, endEpochTime):
        jStart = convertEpochToJulianDay(startEpochTime)
        jEnd = convertEpochToJulianDay(endEpochTime)
        start = int(jStart/5.)
        end = int(math.ceil((jEnd)/5.))
        if (start == end):
            return [start]
        return range(start, end)
    
    def getIndexBasedOnDate(self,day,month,year):
        return self.getIndexBasedOnEpoch(convertDayMonthYearToEpoch(day,month,year))
    
    def getIndexesBasedOnDate(self,daystart,monthstart,yearstart,dayend,monthend,yearend):
        return self.getIndexesBasedOnEpoch(convertDayMonthYearToEpoch(daystart,monthstart,yearstart),convertDayMonthYearToEpoch(dayend,monthend,yearend))
    
    def getIndexBasedOnEpoch(self,startEpochTime):
        return int(convertEpochToJulianDay(startEpochTime)/5.)
    
    def getDateBasedOnIndex(self,index,year):
        return datetime.datetime(year, 1, 1) + datetime.timedelta(days=(index*5.))
    
    def indexAndYearToDate(self,year,index):
        return datetime.date(year, 1, 1)+datetime.timedelta(days=(index*5.))
    
    def cullDateList(self,dates):
        indexList = []
        for date in dates:
            index = self.getIndexBasedOnDate(date[0],date[1],date[2])
            if (index ==73):
                year = date[2]+1
                index = 0
            else :
                year = date[2]
                
            try:
                indexList.index(str(index)+"_"+str(year))
            except:
                indexList.append(str(index)+"_"+str(year))
        dates = []
        count = 0
        for item in indexList:
            parts = item.split("_")
            date = self.getDateBasedOnIndex(int(parts[0]),int(parts[1]))
            dates.append([date.day,date.month,date.year])
            ++count
        dates.sort()
        return dates

# Indexing every ten days
class EveryTenDaysIndex:
    def getIndexesBasedOnEpoch(self,startEpochTime, endEpochTime):
        jStart = convertEpochToJulianDay(startEpochTime)
        jEnd = convertEpochToJulianDay(endEpochTime)
        start = int(jStart/10.)
        end = int(math.ceil((jEnd)/10.))
        if (start == end):
            return [start]
        return range(start, end)
    
    def getIndexBasedOnEpoch(self,startEpochTime):
        return int(convertEpochToJulianDay(startEpochTime)/10.)
    
    def getIndexBasedOnDate(self,day,month,year):
        return self.getIndexBasedOnEpoch(convertDayMonthYearToEpoch(day,month,year))
    
    def getDateBasedOnIndex(self,index,year):
        return datetime.datetime(year, 1, 1) + datetime.timedelta(index*10.)
    
    def indexAndYearToDate(self,year,index):
        return datetime.date(year, 1, 1)+datetime.timedelta(days=index*10.)
    
    def getIndexesBasedOnDate(self,daystart,monthstart,yearstart,dayend,monthend,yearend):
        return self.getIndexesBasedOnEpoch(convertDayMonthYearToEpoch(daystart,monthstart,yearstart),convertDayMonthYearToEpoch(dayend,monthend,yearend))   
    
    def cullDateList(self,dates):
        indexList = []
        years = []
        for date in dates:
            index = self.getIndexBasedOnDate(date[0],date[1],date[2])
            try:
                indexList.index(index)
            except:
                indexList.append(index)
                years.append(date[2])
        dates = []
        count = 0
        for item in indexList:
            date = self.getDateBasedOnIndex(item, years[count])
            dates.append([date.day,date.month,date.year])
            ++count
        dates.sort()
        return dates

# Indexing every decad
class DecadalIndex:

    def getIndexesBasedOnEpoch(self, startEpochTime, endEpochTime):
        jStart = convertEpochToJulianDay(startEpochTime)
        jEnd = convertEpochToJulianDay(endEpochTime)
        start = int(jStart / 10.)
        end = int(math.ceil((jEnd) / 10.))
        if (start == end):
            return [start]
        return range(start, end)

    def getIndexBasedOnEpoch(self, startEpochTime):
        return int(convertEpochToJulianDay(startEpochTime) / 10.)

    def getIndexBasedOnDate(self, day, month, year):
        decad = None
        if int(day) <= int(10):
            decad = int(1)
        elif int(day) <= int(20) and int(day) > int(10) and int(month) != int(2):
            decad = int(2)
        elif int(day) <= int(31) and int(day) > int(20) and int(month) != int(2):
            decad = int(3)
        elif int(day) <= int(20) and int(day) > int(10) and int(month) == int(2):
            decad = int(2)
        elif int(day) <= int(29) and int(day) > int(20) and int(month) == int(2):
            decad = int(3)

        return self.getIndexBasedOnDecad(decad, month, year)

    def getIndexBasedOnDecad(self, decad, month, year):
        tIn = [x for x in range(0, 36)]
        decadChunks = [tIn[i:i + 3] for i in range(0, len(tIn), 3)]

        return int(decadChunks[int(month) - 1][int(decad) - 1])

    def getDateBasedOnIndex(self, index, year):
        tIn = [x for x in range(0, 36)]
        decadChunks = [tIn[i:i + 3] for i in range(0, len(tIn), 3)]
        decadIndex = [[i, j] for i, lst in enumerate(decadChunks) for j, pos in enumerate(lst) if pos == index]
        month = int((decadIndex)[0][0]) + 1
        decad = int((decadIndex)[0][1]) + 1
        if int(decad) != int(3):
            return datetime.datetime(year, month, 10) + datetime.timedelta(decadIndex[0][1] * 10.)
        else:
            any_day = datetime.datetime(year, month, 10)
            next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
            return next_month - datetime.timedelta(days=next_month.day)

    def indexAndYearToDate(self, year, index):
        return datetime.date(year, 1, 1) + datetime.timedelta(days=index * 10.)

    def getIndexesBasedOnDate(self, daystart, monthstart, yearstart, dayend, monthend, yearend):
        return self.getIndexesBasedOnEpoch(convertDayMonthYearToEpoch(daystart, monthstart, yearstart),
                                           convertDayMonthYearToEpoch(dayend, monthend, yearend))

    def cullDateList(self, dates):
        indexList = []
        years = []
        for date in dates:
            index = self.getIndexBasedOnDate(date[0], date[1], date[2])
            try:
                indexList.index(str(index) + "_" + str(date[2]))
            except:
                indexList.append(str(index) + "_" + str(date[2]))
                years.append(date[2])
        dates = []
        count = 0
        for item in indexList:
            parts = item.split("_")
            date = self.getDateBasedOnIndex(int(parts[0]), int(parts[1]))
            dates.append([date.day, date.month, date.year])
            ++count
        dates.sort()
        return dates
    
# Support for GenericIndex and TimeDeltaProcessing
# Just adding a 'months' member to the datetime.timedelta class as a subclass.
class TimeDeltaWithMonths(datetime.timedelta):
    months = 0
class TimeDeltaProcessing: #ISOTimeCodeProcessing
    
    def _chew_ISO8601_String_And_GetValue(self, currentISOString, testSplitValueString):
        if (testSplitValueString in currentISOString) and (len(currentISOString) > 0):
            # Found a value, grab the value and remove this part of the string
            ret_Value = int(currentISOString.split(testSplitValueString)[0])
            ret_String = currentISOString.split(testSplitValueString)[1]
        else:
            ret_Value = 0
            ret_String = currentISOString
        return ret_Value, ret_String
    # Expects iso8601_Code to be a string that starts with P and in this format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
    # Returns a TimeDelta Object and NumberOfMonths interval (representation of the ISO8601 Interval PnTn Code format)
    def get_TimeDeltaWithMonths_From_ISO8601_Code(self, iso8601_Code):
        # years, months, weeks, days, hours, minutes, seconds
        default_Value = 1
        final_Years = 0
        final_Months = 0
        final_Weeks = 0
        final_Hours = 0
        final_Minutes = 0
        final_Seconds = 0
        
        try:
            iso8601_Code = iso8601_Code.upper()
            
            # Validate the Code (Make sure the first letter is a P)
            if(iso8601_Code[0] == "P"):
                # The reason to do it this way, is because we can't know the number of digits for any given number in the parsing.
                parsing_Step1 = iso8601_Code.split("P")[1] # expected result: [n]Y[n]M[n]DT[n]H[n]M[n]S
                timeString = None
                
                # Break the date and time parts into 2 (because they both could share a common letter 'M')
                if "T" in parsing_Step1:
                    dateString = parsing_Step1.split("T")[0]  # expected result: [n]Y[n]M[n]D
                    timeString = parsing_Step1.split("T")[1]  # expected result: [n]H[n]M[n]S
                else:
                    dateString = parsing_Step1  # expected result: [n]Y[n]M[n]D
                    
                if dateString == None:
                    final_Years = 0
                    final_Months = 0
                    final_Weeks = 0
                    final_Days = 0
                else:
                    # Parse the Date String
                    currentDateString = dateString
                    final_Years, currentDateString = self._chew_ISO8601_String_And_GetValue(currentDateString, "Y")
                    final_Months, currentDateString = self._chew_ISO8601_String_And_GetValue(currentDateString, "M") 
                    final_Weeks, currentDateString = self._chew_ISO8601_String_And_GetValue(currentDateString, "W")
                    final_Days, currentDateString = self._chew_ISO8601_String_And_GetValue(currentDateString, "D")

                # Check for Time
                if timeString == None:
                    final_Hours = 0
                    final_Minutes = 0
                    final_Seconds = 0
                else:
                    # Parse the Time String
                    currentTimeString = timeString
                    final_Hours, currentTimeString = self._chew_ISO8601_String_And_GetValue(currentTimeString, "H")
                    final_Minutes, currentTimeString = self._chew_ISO8601_String_And_GetValue(currentTimeString, "M")
                    final_Seconds, currentTimeString = self._chew_ISO8601_String_And_GetValue(currentTimeString, "S") 
            else:
                final_Days = default_Value
            
        except:
            final_Days = default_Value
        
        # If all values are 0, set the days to 1
        if ((final_Years == 0) and (final_Months == 0) and (final_Weeks == 0) and (final_Days == 0) and (final_Hours == 0) and (final_Minutes == 0) and (final_Seconds == 0)):
            final_Days = default_Value
        
        time_DeltaArgs = {
                          "weeks":final_Weeks,
                          "days":final_Days,
                          "hours":final_Hours,
                          "minutes":final_Minutes,
                          "seconds":final_Seconds,
                          }
        ret_TimeDeltaWithMonths = TimeDeltaWithMonths(**time_DeltaArgs)
        ret_TimeDeltaWithMonths.months = final_Months
        return ret_TimeDeltaWithMonths

# If there is need for something out of the ordinary (like decadal or based on a monthly dataset)
class DynamicIndex:
    _iso8601_IntervalCode = "P1D"
    _timeDelta_WithMonths = None
    _yearlyOffsetSeconds = 0.0 # Number of seconds that a dataset is offset by in any given year
    def __init__(self, iso8601_IntervalCode = "P1D"):
        self._iso8601_IntervalCode = iso8601_IntervalCode
        tdProcessingObject = TimeDeltaProcessing()
        self._timeDelta_WithMonths = tdProcessingObject.get_TimeDeltaWithMonths_From_ISO8601_Code(self._iso8601_IntervalCode)
        self._yearlyOffsetSeconds = 0.0
        
    def _convertEpochToJulianDay(self, epochTime):
        return int(time.strftime("%j",time.gmtime(epochTime)))
    def _convertDayMonthYearToEpoch(self, day,month,year):
        return float(datetime.date(year, month, day).strftime("%s"))
    def _convertDayMonthYearHourMinSecondToEpoch(self, day,month,year,hour,minute,second):
        return float(datetime.datetime(year, month, day, hour, minute, second).strftime("%s"))

    def _getYearFromEpochTime(self, epochTime):
        return time.gmtime(epochTime).tm_year
    
    def _isEpochTimeRangeInSameYear(self, startEpochTime, endEpochTime):
        year_Start = self._getYearFromEpochTime(startEpochTime)
        year_End = self._getYearFromEpochTime(endEpochTime)
        if(year_Start == year_End):
            return True
        else:
            return False
        
    def _getEpochTime_Of_Index_0_ForYear(self,currentYear): #,epochTime):
        firstEpoch_ForYear = float(datetime.date(currentYear, 1, 1).strftime("%s"))
        epoch_Index_0_ForYear = firstEpoch_ForYear + self._yearlyOffsetSeconds
        return epoch_Index_0_ForYear
        
    def _getTotalIntervalSeconds(self):
        return float (self._timeDelta_WithMonths.total_seconds())  # Currently does NOT support the Months Member
        
    def _getEstNumberOfIndexes_In_A_SingleYear(self):
        totalSeconds = self._getTotalIntervalSeconds()
        if(totalSeconds == 0):
            totalSeconds = 1
        est_Seconds_Per_Year = 32000000
        return int(est_Seconds_Per_Year/totalSeconds)
        
    # Exposed Methods for getting index values    
    def getIndexesBasedOnEpoch(self,startEpochTime, endEpochTime):

        # Validate that the two EpochTime's are in the same year.
        if(self._isEpochTimeRangeInSameYear(startEpochTime, endEpochTime) == False):
            return []
        
        # Validate that endEpochTime is greater than startEpochTime
        if(endEpochTime < startEpochTime):
            return []
        
        # Get the Year from one of the EpochTimes
        theYear = self._getYearFromEpochTime(startEpochTime)
        
        # Find the EpochTime for Index 0 of the current year
        epoch_index_0_Position_ForCurrentYear = self._getEpochTime_Of_Index_0_ForYear(theYear)
        
        # Get the number of seconds from the timeDeltaObject
        interval_TotalSeconds = self._getTotalIntervalSeconds()
        
        # Get the max expected indexes for a year
        est_Max_Indexes = self._getEstNumberOfIndexes_In_A_SingleYear()
        
        # List to hold indexes to return
        ret_List = []
        
        # Iterate through the possible indexes for the year starting with the zero point and build the list to return
        yearIndexCounter = 0
        is_StartEpoch_Found = False
        is_EndEpoch_Found = False
        while(is_EndEpoch_Found == False):

            # Validate for possible infinite loop bugs
            if(yearIndexCounter > est_Max_Indexes):
                return []
            
            # Checking the NEXT index
            currentEpochToCheck =  (interval_TotalSeconds * (yearIndexCounter + 1)) + epoch_index_0_Position_ForCurrentYear
            
            # Check if Start epoch is found.
            if (is_StartEpoch_Found == True):
                ret_List.append(yearIndexCounter)
                if(endEpochTime < currentEpochToCheck):
                    is_EndEpoch_Found = True
                    
            else:
                if(startEpochTime < currentEpochToCheck):
                    # Just found the start epoch, add it and change the flag
                    ret_List.append(yearIndexCounter)
                    is_StartEpoch_Found = True
            yearIndexCounter += 1

        return ret_List

    # Find the index from the given epochTime
    def getIndexBasedOnEpoch(self,epochTime):
        
        # Get the Year from one of the EpochTimes
        theYear = self._getYearFromEpochTime(epochTime)
        
        # Find the EpochTime for Index 0 of the current year
        epoch_index_0_Position_ForCurrentYear = self._getEpochTime_Of_Index_0_ForYear(theYear)
        interval_TotalSeconds = self._getTotalIntervalSeconds()
        est_Max_Indexes = self._getEstNumberOfIndexes_In_A_SingleYear()
        
        for i in range(0, est_Max_Indexes):
            currentEpochToCheck =  (interval_TotalSeconds * (i + 1)) + epoch_index_0_Position_ForCurrentYear
            if(epochTime < currentEpochToCheck):
                return i
        return 0

    # Using default values on new params so we can maintain backwards compatibility with existing datasets
    def getIndexBasedOnDate(self,day,month,year, hours=0, minutes=0, seconds=0):
        return self.getIndexBasedOnEpoch(self._convertDayMonthYearHourMinSecondToEpoch(day,month,year, hours, minutes, seconds))

    # Using default values on new params so we can maintain backwards compatibility with existing datasets
    def getIndexesBasedOnDate(self,daystart,monthstart,yearstart,dayend,monthend,yearend, hourStart=0, minuteStart=0, secondStart=0, hourEnd=0, minuteEnd=0, secondEnd=0):
        startEpoch = self._convertDayMonthYearHourMinSecondToEpoch(daystart, monthstart, yearstart, hourStart, minuteStart, secondStart)
        endEpoch   = self._convertDayMonthYearHourMinSecondToEpoch(dayend,   monthend,   yearend,   hourEnd,   minuteEnd,   secondEnd)
        return self.getIndexesBasedOnEpoch(startEpoch,endEpoch)

    def getDateBasedOnIndex(self,index,year):

        interval_TotalSeconds = self._getTotalIntervalSeconds()
        seconds_FromYearZeroPoint = (interval_TotalSeconds * index) + self._yearlyOffsetSeconds

        # Make a datetime that represents the absolute zero point for the whole year and add the seconds to it.
        ret_DateTime_Obj = datetime.datetime(year, 1, 1) + datetime.timedelta(seconds = seconds_FromYearZeroPoint)
        return ret_DateTime_Obj

    def getDateTimeBasedOnIndex(self,index,year):
        return self.getDateBasedOnIndex(index,year)
    
    def cullDateList(self,dates):
        indexList = []
        for date in dates:
            # Gather the inputs
            theDay = date[0]
            theMonth = date[1]
            theYear = date[2]
            theHour = 0
            theMinute = 0
            theSecond = 0
            # Get Index based on Date
            theIndex = self.getIndexBasedOnDate(theDay, theMonth, theYear, theHour, theMinute, theSecond)
             
            # Build the index list with _Year appended to each index string
            index_String_To_Add = str(theIndex) + "_" + str(theYear)
            try:
                indexList.index(index_String_To_Add)
            except:
                indexList.append(index_String_To_Add)

        # Now use the index and years list (unique list) to 
        ret_Dates = []
        for item in indexList:
            parts = item.split("_")
            validated_Date = self.getDateBasedOnIndex( int(parts[0]), int(parts[1]) )  
            ret_Dates.append( [validated_Date.day, validated_Date.month, validated_Date.year ])
        return ret_Dates