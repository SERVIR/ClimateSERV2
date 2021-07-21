'''
Created on ??
@author: jeburks  

Modified starting from Sept 2015
@author: Kris Stanton 
'''

import math
import time
import datetime 

def convertEpochToJulianDay(epochTime):
    return int(time.strftime("%j",time.gmtime(epochTime)))

def convertDayMonthYearToEpoch(day,month,year):
    return float(datetime.date(year, month, day).strftime("%s"))

def convertMonthYearToEpoch(month,year):
    return float(datetime.date(year, month, 1).strftime("%s"))

def getLastDayOfMonth(month,year):
    monthToProcess = month+1
    yearToProcess = year
    if (month == 12):
        monthToProcess = 1
        yearToProcess = year+1
    epochTime = float(datetime.date(yearToProcess, monthToProcess, 1).strftime("%s"))-86400
    return int(time.strftime("%d",time.gmtime(epochTime)))
    

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
        print "************************django*************************"	
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
        decadChunks = [tIn[i:i + 3] for i in xrange(0, len(tIn), 3)]

        return int(decadChunks[int(month) - 1][int(decad) - 1])

    def getDateBasedOnIndex(self, index, year):
        tIn = [x for x in range(0, 36)]
        decadChunks = [tIn[i:i + 3] for i in xrange(0, len(tIn), 3)]
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
# Support for GenericIndex
# Get a Time Delta from an ISO Time Code
# https://en.wikipedia.org/wiki/ISO_8601
class TimeDeltaProcessing: #ISOTimeCodeProcessing:
    
    def _chew_ISO8601_String_And_GetValue(self, currentISOString, testSplitValueString):
        
        ret_Value = 0
        ret_String = currentISOString
        if (testSplitValueString in currentISOString) and (len(currentISOString) > 0):
            # Found a value, grab the value and remove this part of the string
            ret_Value = int(currentISOString.split(testSplitValueString)[0])
            ret_String = currentISOString.split(testSplitValueString)[1]
        else:
            ret_Value = 0
            ret_String = currentISOString
        return ret_Value, ret_String
    
    #    if ("Y" in currentDateString) and (len(currentDateString) > 0):
    #        # Found a year, Grab it and remove it.
    #        final_Years = int(currentDateString.split("Y")[0])
    #        currentDateString = currentDateString.split("Y")[1]
    #    else:
    #        final_Years = 0
        
    # Expects iso8601_Code to be a string that starts with P and in this format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
    # Returns a TimeDelta Object and NumberOfMonths interval (representation of the ISO8601 Interval PnTn Code format)
    def get_TimeDeltaWithMonths_From_ISO8601_Code(self, iso8601_Code):
        # Convert Any ISO8601 Interval Code to a TimeDeltaWithMonths object and return it
        
        # TimeDeltaWithMonths uses the following
        # weeks, days, hours, minutes, seconds    And now extended to hold months also
        
        # Default value
        default_Value = 1
        
        # Values we need at the end
        final_Years = 0
        final_Months = 0
        final_Weeks = 0
        final_Days = 0
        final_Hours = 0
        final_Minutes = 0
        final_Seconds = 0
        
        # Simplifies the parsing a bit
        try:
            # Convert to uppercase
            iso8601_Code = iso8601_Code.upper()
            
            # Validate the Code (Make sure the first letter is a P)
            if(iso8601_Code[0] == "P"):
                # Continue parsing
                
                # Parse the code, do necessary conversions, ignore values smaller than 1 second and larger than 'n' months
                
                # Parsing works likes this, split the string to remove the 'P' component, then split it to remove the 'T' component, then for each of those splits, attempt to split and read each additional component from left to right (greatest value to smallest value)
                # The reason to do it this way, is because we can't know the number of digits for any given number in the parsing.
                parsing_Step1 = iso8601_Code.split("P")[1] # expected result: [n]Y[n]M[n]DT[n]H[n]M[n]S
                
                dateString = None
                timeString = None
                
                # Break the date and time parts into 2 (because they both could share a common letter 'M')
                if "T" in parsing_Step1:
                    dateString = parsing_Step1.split("T")[0]  # expected result: [n]Y[n]M[n]D
                    timeString = parsing_Step1.split("T")[1]  # expected result: [n]H[n]M[n]S
                else:
                    dateString = parsing_Step1  # expected result: [n]Y[n]M[n]D
                    
                # Check for Date
                if dateString == None:
                    final_Years = 0
                    final_Months = 0
                    final_Weeks = 0
                    final_Days = 0
                else:
                    # Parse the Date String
                    currentDateString = dateString  # As we progress forward, this will get smaller and smaller
                    
                    
                    # Years
                    # Original First Implementation of this (This is now moved to 'self._chew_ISO8601_String_And_GetValue()'
                    #if ("Y" in currentDateString) and (len(currentDateString) > 0):
                    #    # Found a year, Grab it and remove it.
                    #    final_Years = int(currentDateString.split("Y")[0])
                    #    currentDateString = currentDateString.split("Y")[1]
                    #else:
                    #    final_Years = 0
                        
                    # Process the Parts
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
                    
                    # Process the Parts
                    final_Hours, currentTimeString = self._chew_ISO8601_String_And_GetValue(currentTimeString, "H")
                    final_Minutes, currentTimeString = self._chew_ISO8601_String_And_GetValue(currentTimeString, "M")
                    final_Seconds, currentTimeString = self._chew_ISO8601_String_And_GetValue(currentTimeString, "S") 
            else:
                # Failed validation.. set default
                final_Days = default_Value  # Default 1
            
        except:
            final_Days = default_Value  # Default 1
        
        # Last Validation, if all values are 0, set the days to 1  # I did find one test that actually made it through all if statements and validation checks with all 0's and no errors.. so thats why this is here.
        if ((final_Years == 0) and (final_Months == 0) and (final_Weeks == 0) and (final_Days == 0) and (final_Hours == 0) and (final_Minutes == 0) and (final_Seconds == 0)):
            final_Days = default_Value  # Default 1
        
        # Load the args
        time_DeltaArgs = {
                          "weeks":final_Weeks,
                          "days":final_Days,
                          "hours":final_Hours,
                          "minutes":final_Minutes,
                          "seconds":final_Seconds,
                          }
        #ret_TimeDeltaWithMonths = TimeDeltaWithMonths(weeks=1, days=1, hours=1, minutes=1, seconds=1)
        ret_TimeDeltaWithMonths = TimeDeltaWithMonths(**time_DeltaArgs)
        ret_TimeDeltaWithMonths.months = final_Months
        #print("DEBUG: time_DeltaArgs " + str(time_DeltaArgs))
        return ret_TimeDeltaWithMonths
    
# Test Codes
# test_iso8601Code_1Day = "P1D"                                             
# test_iso8601Code_30Min = "PT30M"
# test_iso8601Code_Complex = "P3Y6M4DT12H30M5S"    # thanks wikipedia!
# test_iso8601Code_ComplexAll = "P3Y6M12W4DT12H30M5S"    
# test_iso8601Code_NoTime = "P22M11D"  # 22 Months and 11 Days
# test_iso8601Code_BadInput = "15days"

# Test Results (All tests passed so far!)
# DEBUG: time_DeltaArgs {'hours': 0, 'seconds': 0, 'weeks': 0, 'minutes': 0, 'days': 1} -- (test_iso8601Code_1Day = "P1D")
# DEBUG: time_DeltaArgs {'hours': 0, 'seconds': 0, 'weeks': 0, 'minutes': 30, 'days': 0} -- (test_iso8601Code_30Min = "PT30M")
# DEBUG: time_DeltaArgs {'hours': 12, 'seconds': 5, 'weeks': 0, 'minutes': 30, 'days': 4} -- (test_iso8601Code_Complex = "P3Y6M4DT12H30M5S")  (Also, z.months output: 6)
# DEBUG: time_DeltaArgs {'hours': 12, 'seconds': 5, 'weeks': 12, 'minutes': 30, 'days': 4} -- (test_iso8601Code_ComplexAll = "P3Y6M12W4DT12H30M5S")  (Also, z.months output: 6)
# DEBUG: time_DeltaArgs {'hours': 0, 'seconds': 0, 'weeks': 0, 'minutes': 0, 'days': 11} -- (test_iso8601Code_NoTime = "P22M11D") (Also, z.months output: 22)
# DEBUG: time_DeltaArgs {'hours': 0, 'seconds': 0, 'weeks': 0, 'minutes': 0, 'days': 1} -- (test_iso8601Code_BadInput = "15days") # THIS IS GOOD, IT OUTPUT THE DEFAULT OF 1 DAY!


# Update, this is best used for ranges that are based on a regular pattern of days or less (supported down to seconds)
# if there is need for something out of the ordinary (like decadal or based on a monthly dataset) than we will need updates to this class or even a new class to handle those types which acts as a hybrid. 
class DynamicIndex:
    # Notes, So when converting to indexes that are smaller than a single day, we need new code.
    # If the index does not contain any increments below the 1 day mark, we can just use legacy code and it should be handled perfectly,
    # If the index does contain any 'seconds' increments (below the 1 day mark) we need to go down to epoch time
    # The question, is it better just to write ALL the code to be 'seconds' based
    # Also, an assumption is made that the first index for any year is always in the same position 
    _iso8601_IntervalCode = "P1D"
    _timeDelta_WithMonths = None
    _yearlyOffsetSeconds = 0.0 # Number of seconds that a dataset is offset by in any given year
    def __init__(self, iso8601_IntervalCode = "P1D"):
        # Store the Code and timeDelta_WithMonths object. (timeDelta object used by all the new code that does index processing on intervals between 1 second and 1 day
        self._iso8601_IntervalCode = iso8601_IntervalCode
        tdProcessingObject = TimeDeltaProcessing()
        self._timeDelta_WithMonths = tdProcessingObject.get_TimeDeltaWithMonths_From_ISO8601_Code(self._iso8601_IntervalCode)
        self._yearlyOffsetSeconds = 0.0
        
    # Moving external functions into this class as private members
    def _convertEpochToJulianDay(self, epochTime):
        return int(time.strftime("%j",time.gmtime(epochTime)))
    def _convertDayMonthYearToEpoch(self, day,month,year):
        return float(datetime.date(year, month, day).strftime("%s"))
    def _convertDayMonthYearHourMinSecondToEpoch(self, day,month,year,hour,minute,second):
        #return float(datetime.date(year, month, day, hour, minute, second).strftime("%s"))
        return float(datetime.datetime(year, month, day, hour, minute, second).strftime("%s"))



    # Get the Year from EpochTime
    def _getYearFromEpochTime(self, epochTime):
        return time.gmtime(epochTime).tm_year
    
    # Are the Epoch Ranges passed in within the same year?
    def _isEpochTimeRangeInSameYear(self, startEpochTime, endEpochTime):
        year_Start = self._getYearFromEpochTime(startEpochTime)
        year_End = self._getYearFromEpochTime(endEpochTime)
        if(year_Start == year_End):
            return True
        else:
            return False
        
    # Get the epochTime for Index 0 for given year.
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
        # Dynamic Method
        
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
        # This process actually rounds the numbers down (when a start or end epoch is passed, the LAST index (the one Before) is the one that gets added to the list)
        yearIndexCounter = 0
        is_StartEpoch_Found = False
        is_EndEpoch_Found = False
        while(is_EndEpoch_Found == False):

            # Validate for possible infinite loop bugs
            # just in case of some kind of crazy bug that causes this loop to run forever, bail out after the max number of possible indexes for a single year is passed. (something like 31 million seconds in a year)
            # To keep this simpler, I am dynamically limiting the loop.
            if(yearIndexCounter > est_Max_Indexes):
                return []
            
            # Checking the NEXT index
            currentEpochToCheck =  (interval_TotalSeconds * (yearIndexCounter + 1)) + epoch_index_0_Position_ForCurrentYear
            
            # Check if Start epoch is found.
            if (is_StartEpoch_Found == True):
                # Start Epoch already found, 
                
                # Add this index to the list
                ret_List.append(yearIndexCounter)
                
                # Now check to see if this is the end epoch.
                if(endEpochTime < currentEpochToCheck):
                    # We already added it to the list, so no need to add again, just set the flag
                    is_EndEpoch_Found = True
                    
            else:
                # Start Epoch not yet found, check to see if this index is it.
                if(startEpochTime < currentEpochToCheck):
                    # Just found the start epoch, add it and change the flag
                    ret_List.append(yearIndexCounter)
                    is_StartEpoch_Found = True
            
            # move on to the next index
            yearIndexCounter += 1
            # END WHILE LOOP
        
        # return the list
        return ret_List
        
        ## Original 'Daily' method, 
        ## strip the year off by converting to JulianDay, 
        ## Use -1 so that the first day of the year is actually index 0
        ## return a single index as a list, or return the range between them
        #jStart = convertEpochToJulianDay(startEpochTime)-1
        #jEnd = convertEpochToJulianDay(endEpochTime)
        #if (jStart == jEnd):
        #    return [jStart]
        #return range(jStart,jEnd)
    
    # Find the index from the given epochTime
    def getIndexBasedOnEpoch(self,epochTime):
        
        # Get the Year from one of the EpochTimes
        theYear = self._getYearFromEpochTime(epochTime)
        
        # Find the EpochTime for Index 0 of the current year
        epoch_index_0_Position_ForCurrentYear = self._getEpochTime_Of_Index_0_ForYear(theYear)
        
        # Get the number of seconds from the timeDeltaObject
        interval_TotalSeconds = self._getTotalIntervalSeconds()
        
        # Get the max expected indexes for a year
        est_Max_Indexes = self._getEstNumberOfIndexes_In_A_SingleYear()
        
        # Iterate through the possible indexes for the year starting with the zero point and return the index once it is found
        for i in range(0, est_Max_Indexes):
            
            # Checking the NEXT index
            currentEpochToCheck =  (interval_TotalSeconds * (i + 1)) + epoch_index_0_Position_ForCurrentYear
            
            if(epochTime < currentEpochToCheck):
                return i
            
        # if the index is not found in the above loop, return 0 (not sure what else to do in this situation honestly)
        return 0
        
        # Original 'Daily' Method
        # This only supports down to the day
        #return convertEpochToJulianDay(epochTime)-1
    
    # Using default values on new params so we can maintain backwards compatibility with existing datasets
    def getIndexBasedOnDate(self,day,month,year, hours=0, minutes=0, seconds=0):
        return self.getIndexBasedOnEpoch(self._convertDayMonthYearHourMinSecondToEpoch(day,month,year, hours, minutes, seconds))
        
        # Original 'Daily' Method
        # This only supports down to the day
        #return self.getIndexBasedOnEpoch(convertDayMonthYearToEpoch(day,month,year))

    # Using default values on new params so we can maintain backwards compatibility with existing datasets
    def getIndexesBasedOnDate(self,daystart,monthstart,yearstart,dayend,monthend,yearend, hourStart=0, minuteStart=0, secondStart=0, hourEnd=0, minuteEnd=0, secondEnd=0):
        startEpoch = self._convertDayMonthYearHourMinSecondToEpoch(daystart, monthstart, yearstart, hourStart, minuteStart, secondStart)
        endEpoch   = self._convertDayMonthYearHourMinSecondToEpoch(dayend,   monthend,   yearend,   hourEnd,   minuteEnd,   secondEnd)
        return self.getIndexesBasedOnEpoch(startEpoch,endEpoch)
        
        # Original 'Daily' Method
        # This only supports down to the day
        #return self.getIndexesBasedOnEpoch(convertDayMonthYearToEpoch(daystart,monthstart,yearstart),convertDayMonthYearToEpoch(dayend,monthend,yearend))
    
    
    def getDateBasedOnIndex(self,index,year):
        
        # Using Epoch, get the 0 position
        #epoch_index_0_Position_ForCurrentYear = self._getEpochTime_Of_Index_0_ForYear(year)
        
        # Get the number of seconds from the timeDeltaObject
        interval_TotalSeconds = self._getTotalIntervalSeconds()
        
        # Calculate the Index
        #epochAtIndex =  (interval_TotalSeconds * index) + epoch_index_0_Position_ForCurrentYear
        
        # Calculate the number of seconds from the zero point of the year to this index.
        seconds_FromYearZeroPoint = (interval_TotalSeconds * index) + self._yearlyOffsetSeconds
        
        # Make a datetime that represents the absolute zero point for the whole year and add the seconds to it.
        ret_DateTime_Obj = datetime.datetime(year, 1, 1) + datetime.timedelta(seconds = seconds_FromYearZeroPoint)
    
        # Finally, return the 'Date' (the datetime object)    
        return ret_DateTime_Obj
    
        # Original 'Daily' Method
        # This only supports down to the day
        #return datetime.datetime(year, 1, 1) + datetime.timedelta(index*5)
    
    # Just making this to sooth my naming convention a bit
    def getDateTimeBasedOnIndex(self,index,year):
        return self.getDateBasedOnIndex(index,year)
    
    # This one will take a little bit of extra work work.
    # currently only supports single daily datasets
    # Notes, the way this works is that the head processor is the only object that uses it and it only uses it to create a list of dates to seperate the work between the various worker threads.
    # this is critical functionality for the parallel processing bit.
    # What we need to do:  Keep this function working at the day and above level, and then add support for items down to the second. (Perhaps in a different function?)
    # OR, keep following the call stack and add support into all those functions to make them compatible with items at less than a day resolution...
    # I like the first idea best.. it makes things a bit more complex but it also should keep legacy support working just fine.
    #
    # Also note, the list of 'dates' comes in as a specific format (not necessarily datetime objects) and the code has to remain very fast (as this happens at run time as a user is expecting fast results)
    # So a bottle neck here slows the performance of the whole system down.
    #
    # TODO!! ADD SUPPORT FOR INPUTING HOURS, MINUTES, SECONDS (TRACE WHERE THE CODE WAS BEFORE THIS), AND ADD SUPPORT FOR RETURNING THOSE AS WELL (TRACE WHAT THE CODE DOES WITH THE RESULTS FROM THIS FUNCTION CALL)
    def cullDateList(self,dates):
        indexList = []
        #yearsList = [] # We need a synchronized list of year values  # Wait, no we don't because then we may have false positives of duplicates for different years (jan 1 from 2 different years will both have the same index.. see!)
        # Get index and store Year info in the index list
        for date in dates:
            
            # ACTUALLY, SIMPLIFY THIS, ONLY DO THE GET INDEX BASED ON DATE?  WHY IS THAT EXTRA CODE IN HERE FOR THE YEAR?? (IN THE OLD 'EVERYFIVEDAY' VERSION?
            
            # Gather the inputs
            theDay = date[0]
            theMonth = date[1]
            theYear = date[2]
            theHour = 0 # future will be date[3] # TODO! Requires datepocessing and zmqheadproc edits
            theMinute = 0 # future will be date[4] # TODO! Requires datepocessing and zmqheadproc edits
            theSecond = 0 # future will be date[5] # TODO! Requires datepocessing and zmqheadproc edits
            
            # Second attempt,
            # Get Index based on Date
            theIndex = self.getIndexBasedOnDate(theDay, theMonth, theYear, theHour, theMinute, theSecond)
             
            # Build the index list with _Year appended to each index string
            index_String_To_Add = str(theIndex) + "_" + str(theYear)
            # This may be a little inefficient, but it does serve to keep from having duplicate index values
            try:
                # If this line fails, the index is not already part of the list.  If it succeeds, we skip adding 
                indexList.index(index_String_To_Add)
            except:
                # So then we add it here
                indexList.append(index_String_To_Add)
                #yearsList.append(theYear)
                
            
            # First attempt (we actually have another function which performs these same two steps exactly..
            ## Get the Epoch
            #epoch_DateTime = self._convertDayMonthYearHourMinSecondToEpoch(theDay, theMonth, theYear, theHour, theMinute, theSecond)
            ## Get index from epoch
            #theIndex = self.getIndexBasedOnEpoch(epoch_DateTime)
            
        # Now use the index and years list (unique list) to 
        ret_Dates = []
        #for i in range(0, len(indexList)):
            #dateToAdd = self.getDateBasedOnIndex(theIndex[i], theYear[i])
        for item in indexList:
            parts = item.split("_")
            validated_Date = self.getDateBasedOnIndex( int(parts[0]), int(parts[1]) )  
            ret_Dates.append( [validated_Date.day, validated_Date.month, validated_Date.year ])
            
        return ret_Dates
        
        ## Only supports daily datasets.
        #return dates
    
    # Actually, I can refactor the 'cullDateList' to handle date times as well
    #def cullDateTimeList(self,datetimes):
    #    pass
    
# Time to test all these functions!!
# Test against Existing Dataset Indexer (a daily dataset), a 5 day dataset)  (Just compare final out puts of each function to eachother for some numbers that are kind of in the middle some where and near the end)
# Test against the existing Imerg dataset (not yet in the system), these are 30 min increments. Check mathematically to ensure last dataset and last index match up, also check against a leap year.

# for cullDateList
