import time
import datetime
import climateserv2.processtools.dateIndexTools as dit

# To split the ESI filename
from api.models import Parameters


def breakApartEsiName(filename):
    parts = filename.split(".")
    if "12WK" in filename:
        year = int(parts[0][11:15])
        newDate = datetime.datetime(int(year), 1, 1) + datetime.timedelta(days=(int(parts[0][-3:]) - 1))
        day = newDate.day
        month = newDate.month
    if "4WK" in filename:
        year = int(parts[0][10:14])
        newDate = datetime.datetime(int(year), 1, 1) + datetime.timedelta(days=(int(parts[0][-3:]) - 1))
        day = newDate.day
        month = newDate.month
    return {'year': year, 'month': month, 'day': day}

# To split the CHIRPS filename
def breakApartChripsName(filename):
    parts = filename.split(".")
    ending = parts[-1]
    day = int(parts[-2])
    month = int(parts[-3])
    year = int(parts[-4])
    chirps = parts[:-5]
    return {'year': year, 'month': month, 'day': day, 'ending': ending, 'chirps': chirps}

# To split the GEFS new filename
def breakApartGEFSNewName(filename):
    parts = filename.split(".")
    ending = parts[-1]
    day = int(parts[2][-2:])
    month = int(parts[2][:2])
    year = int(parts[1])
    return {'year': year, 'month': month, 'day': day, 'ending': ending}

# To split the GEFS filename
def breakApartGEFSName(filename):
    parts = filename.split(".")
    ending = parts[-1]
    decad = int(parts[-2][-1])
    month = int(parts[-2][:2])
    year = int(parts[-3])
    chirps = parts[:-4]
    return {'year': year, 'month': month, 'decad': decad, 'ending': ending, 'chirps': chirps}

# To split the EMODIS new filename
def breakApartemodisName(filename):
    parts = filename.split(".")
    ending = parts[-1]
    index = int(parts[0][2:-2]) - 1
    area = parts[0][0:2]
    year = int("20" + parts[0][-2:])
    dateAtFile = dit.DecadalIndex().getDateBasedOnIndex(index, year)
    day = dateAtFile.day
    month = dateAtFile.month
    return {'year': year, 'month': month, 'day': day, 'ending': ending, 'area': area}

# To split the EMODIS new filename
def breakApartemodisNameAdjust(filename, adjust):
    parts = filename.split(".")
    ending = parts[-1]
    index = int(parts[0][adjust:-2]) - 1
    area = parts[0][0:adjust]
    year = int("20" + parts[0][-2:])
    dateAtFile = dit.DecadalIndex().getDateBasedOnIndex(index, year)
    day = dateAtFile.day
    month = dateAtFile.month
    return {'year': year, 'month': month, 'day': day, 'ending': ending, 'area': area}

# To get epoch value of time
def convertTimeToEpoch(timein, interval):
    params = Parameters.objects.first()
    pattern = params.intervals[interval]['pattern']
    return int(time.mktime(time.strptime(timein, pattern)))

# To create datetime object from an year
def createDateFromYear(year):
    return datetime.date(year, 1, 1)

# To create datetime object from an year, month
def createDateFromYearMonth(year, month):
    return datetime.date(year, month, 1)

# To create datetime object from an year, month, day
def createDateFromYearMonthDay(year, month, day):
    return datetime.date(year, month, day)

# To create date string from datetime object
def formatDateBasedOnInterval(date, interval):
    params = Parameters.objects.first()
    return date.strftime(params.intervals['pattern'])