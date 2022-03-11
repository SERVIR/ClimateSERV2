import smtplib

import requests

from ..models import Profile
from django.contrib.auth.models import User
from bs4 import BeautifulSoup
from django.conf import settings
#from django.utils import timezone

from random import randint
import re  # Regular Expressions

import sys  # For returning error infos.
from datetime import datetime, timedelta    # For some of the datetime utils..
import pytz  # For Timezone forcing # ret_DateTimeObj.replace(tzinfo=pytz.utc)

# Gets a Random Unambiguous Char String For the purposes of generating a unique ID string (20 chars)    // 9.2e34 possible combos  (56^20)
def get_Random_String_UniqueID_20Chars():
    return get_Random_String(length_Of_Result_String=20)

# Gets a Random Char String (The function that actually gets the string from the choices).
def get_Random_String(length_Of_Result_String = 187):
    chars_To_Choose = settings.UNAMBIGUOUS_ALPHA_NUMERIC_CHARS
    resultString = ""
    for i in range(int(length_Of_Result_String)):
        nextChar = randint(0, len(chars_To_Choose) - 1)
        resultString += str(chars_To_Choose[nextChar])
    return resultString

# Removes all occurrences of numbers from 0 to 9
def str_util__RemoveNumbers(inStr):
    inStr = str(inStr)
    for x in range(0, 10):
        strToRemove = str(x)
        inStr = inStr.replace(strToRemove, "")
    return inStr

# Removes all chars that are not Alphabet OR Number chars (and replaces them with the replacement char)
def str_util__ReplaceAll_NonAlphaNumeric_Chars_with(inStr, replaceValue='-'):
    inStr = str(inStr)
    replaceValue = str(replaceValue)  # replaceValue = "-"
    inStr = re.sub('[^0-9a-zA-Z]+', replaceValue, inStr)
    # To replace with a '-' do this:  # re.sub('[^a-zA-Z]+', '-', inStr)
    return inStr

# Does Key Exist?
def does_Key_Exist(obj, key_to_check):
    if (type(obj.get(str(key_to_check))) == type(None)):
        return False
    else:
        return True

# Is this Key a string type?
def is_Key_a_Str_Type(obj, key_to_check):
    if (does_Key_Exist(obj, key_to_check) == True):
        if (type(obj.get(str(key_to_check))) == type(str())):
            return True
        else:
            return False
    else:
        return False

# Is this Key a Dict type?
def is_Key_a_Dict_Type(obj, key_to_check):
    if (does_Key_Exist(obj, key_to_check) == True):
        if (type(obj.get(str(key_to_check))) == type(dict())):
            return True
        else:
            return False
    else:
        return False

# Is this Key a Dict type?
def is_Key_a_List_Type(obj, key_to_check):
    if (does_Key_Exist(obj, key_to_check) == True):
        if (type(obj.get(str(key_to_check))) == type(list())):
            return True
        else:
            return False
    else:
        return False

# Utility - # Get Client IP address
# https://stackoverflow.com/questions/4581789/how-do-i-get-user-ip-address-in-django
def get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


def get_True_or_False_from_boolish_string(bool_ish_str_value, defaultBoolValue):
    lowercase_boolish_str = str(bool_ish_str_value).lower()

    # Obviously True Cases
    if (lowercase_boolish_str == "yes"):
        return True
    if (lowercase_boolish_str == "y"):
        return True
    if (lowercase_boolish_str == "true"):
        return True
    if (lowercase_boolish_str == "t"):
        return True
    if (lowercase_boolish_str == "1"):
        return True

    # Obviously False Cases
    if (lowercase_boolish_str == "no"):
        return False
    if (lowercase_boolish_str == "n"):
        return False
    if (lowercase_boolish_str == "false"):
        return False
    if (lowercase_boolish_str == "f"):
        return False
    if (lowercase_boolish_str == "0"):
        return False

    # Couldn't exactly figure out what the input was meant to be... so just return default
    #print("get_True_or_False_from_boolish_string: WARNING: Could not parse the obvious cases for a Bool Compare, RETURNING DEFAULT BOOL VALUE")
    return defaultBoolValue

def get_Int_Value_Forced__FromString(int_ish_str_value, defaultIntValue):
    retValue = defaultIntValue
    try:
        retValue = int(int_ish_str_value)
    except:
        retValue = defaultIntValue
    return retValue

# USAGE: (Model object that has a DateTimeField database field)
# dateTimeNow_UTC_With_TimeZoneSupport = utils.get_UTC_NOW__ReadyFor_Postgresql_Input()
# model_Object.updated_at = dateTimeNow_UTC_With_TimeZoneSupport
def get_UTC_NOW__ReadyFor_Postgresql_Input():
    ret_DateTimeObj = datetime.utcnow()
    try:
        utc_pytz = pytz.timezone('UTC')
        ret_DateTimeObj = utc_pytz.localize(ret_DateTimeObj)
    except:
        pass
    return ret_DateTimeObj

ext = 'tif'

def listFD(url, ext=''):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    return [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]

def sendNotification(uuid, dataset_name, dates_arr):
    user_arr = []
    etl_user_arr = []
    admin = []
    SUBJECT = "ClimateSERV2.0 ETL run failed for the dataset "+dataset_name+"!!"
    try:
        TEXT = "This email informs you that the ETL run for the dataset " +dataset_name+ " with ETL_PipelineRun__UUID " + uuid + " has failed for following dates(YYYYMMDD).\n\n " +(', ').join(dates_arr)
        message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
        for profile in Profile.objects.all():
            if profile.storage_alerts:
                user_arr.append(profile.user)
        for user in User.objects.all():
            for us in user_arr:
                if str(us) == str(user.username):
                    etl_user_arr.append(user.email)
        for user in User.objects.all():
            if str(user.username) == "email_admin":
                for p in Profile.objects.all():
                    if str(p.user) == "email_admin":
                        admin.append(user.email)
                        admin.append(p.gmail_password)
                        break
                break
        for storage_user in etl_user_arr:
            mail = smtplib.SMTP('smtp.gmail.com', 587)
            mail.ehlo()
            mail.starttls()
            mail.login(admin[0], admin[1])
            mail.sendmail(admin[0], storage_user, message)
            mail.close()
    except Exception as e:
        print(e)