import datetime, gzip, os, shutil, sys
from urllib import request as urllib_request
from shutil import copyfile, rmtree
import xarray as xr
import pandas as pd
import numpy as np
import re
from collections import OrderedDict

from .common import common
from .etl_dataset_subtype_interface import ETL_Dataset_Subtype_Interface
from ..models import Config_Setting

class esi(ETL_Dataset_Subtype_Interface):

    class_name = "esi"
    etl_parent_pipeline_instance = None # def log_etl_error(self, activity_event_type="default_error", activity_description="an error occurred", etl_granule_uuid="", is_alert=True, additional_json={}):

    # esi Has more than 1 mode which refer to sub dataset products ("12week" and "4week")
    esi_mode = "12week"  # Choices at this time are "12week" and "4week" // Controlled by setter functions. // Default is "12week"

    # Input Settings
    YYYY__Year__Start                       = 2021   # 2019
    YYYY__Year__End                         = 2021
    MM__Month__Start                        = 6      # 12    # 2 #1
    MM__Month__End                          = 6      # 4 #6
    N_offset_for_weekly_julian_start_date   = 0   # 0 means, use Jan 1st as the starting point for the julian dates (which are counted weekly, in 7's)

    relative_dir_path__WorkingDir = 'working_dir'

    # DRAFTING - Suggestions
    _expected_remote_full_file_paths    = []    # Place to store a list of remote file paths (URLs) that the script will need to download.
    _expected_granules                  = []    # Place to store granules
    #
    # TODO: Other Props used by the script


    # init (Passing a reference from the calling class, so we can callback the error handler)
    def __init__(self, etl_parent_pipeline_instance):
        self.etl_parent_pipeline_instance = etl_parent_pipeline_instance

    def set_esi_mode__To__4week(self):
        self.esi_mode = "4week"

    def set_esi_mode__To__12week(self):
        self.esi_mode = "12week"

    # Validate type or use existing default for each
    def set_esi_params(self, YYYY__Year__Start, YYYY__Year__End, MM__Month__Start, MM__Month__End, N_offset_for_weekly_julian_start_date):
        try:
            self.YYYY__Year__Start = int(YYYY__Year__Start)
        except:
            pass
        try:
            self.YYYY__Year__End = int(YYYY__Year__End)
        except:
            pass
        try:
            self.MM__Month__Start = int(MM__Month__Start)
        except:
            pass
        try:
            self.MM__Month__End = int(MM__Month__End)
        except:
            pass

        try:
            self.offset_for_weekly_julian_start_date = int(N_offset_for_weekly_julian_start_date)
        except:
            pass

    def get_expected_file_name_wk_number_string(self):
        ret_Str = "1WK"
        if(self.esi_mode == "4week"):
            ret_Str = "4WK"
        if(self.esi_mode == "12week"):
            ret_Str = "12WK"
        return ret_Str

        # Get the local filesystem place to store data

    @staticmethod
    def get_root_local_temp_working_dir(subtype_filter):
        # Type Specific Settings
        esi__4week__rootoutputworkingdir    = Config_Setting.get_value(setting_name="PATH__TEMP_WORKING_DIR__ESI__4WEEK", default_or_error_return_value="")  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/temp_etl_data/esi/4week/'
        esi__12week__rootoutputworkingdir   = Config_Setting.get_value(setting_name="PATH__TEMP_WORKING_DIR__ESI__12WEEK", default_or_error_return_value="")  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/temp_etl_data/esi/12week/'

        esi__4week__rootoutputworkingdir = '/Users/rfontanarosa/git/ClimateSERV2/data/esi/4week'

        # ret_rootlocal_working_dir = settings.PATH__TEMP_WORKING_DIR__DEFAULT # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/data/image/input/UNKNOWN/'
        ret_rootlocal_working_dir = Config_Setting.get_value(setting_name="PATH__TEMP_WORKING_DIR__DEFAULT", default_or_error_return_value="")  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/data/image/input/UNKNOWN/'
        subtype_filter = str(subtype_filter).strip()
        if (subtype_filter == '4week'):
            ret_rootlocal_working_dir = esi__4week__rootoutputworkingdir
        if (subtype_filter == '12week'):
            ret_rootlocal_working_dir = esi__12week__rootoutputworkingdir

        # # Add the Year as a string.
        # year = str(year).strip()    # Expecting 'year' to be something like 2019 or "2019"
        # year_dir_name_to_append = year + "/"
        # ret_rootlocal_working_dir = ret_rootlocal_working_dir + year_dir_name_to_append

        return ret_rootlocal_working_dir

    # Get the local filesystem place to store the final NC4 files (The THREDDS monitored Directory location)
    @staticmethod
    def get_final_load_dir(subtype_filter):
        esi__4week__finalloaddir = Config_Setting.get_value(setting_name="PATH__THREDDS_MONITORING_DIR__ESI__4WEEK", default_or_error_return_value="")      # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/THREDDS/thredds/catalog/climateserv/sport-esi/global/0.05deg/4wk/'
        esi__12week__finalloaddir = Config_Setting.get_value(setting_name="PATH__THREDDS_MONITORING_DIR__ESI__12WEEK", default_or_error_return_value="")    # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/THREDDS/thredds/catalog/climateserv/sport-esi/global/0.05deg/12wk/'
        esi__4week__finalloaddir = '/Users/rfontanarosa/git/ClimateSERV2/data/THREDDS/esi/4week'
        ret_dir = Config_Setting.get_value(setting_name="PATH__THREDDS_MONITORING_DIR__DEFAULT", default_or_error_return_value="")  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/THREDDS/UNKNOWN/'
        subtype_filter = str(subtype_filter).strip()
        if (subtype_filter == '4week'):
            ret_dir = esi__4week__finalloaddir
        if (subtype_filter == '12week'):
            ret_dir = esi__12week__finalloaddir

        # # Add the Year as a string.
        # year = str(year).strip()  # Expecting 'year' to be something like 2019 or "2019"
        # year_dir_name_to_append = year + "/"
        # ret_dir = ret_dir + year_dir_name_to_append

        return ret_dir

    # Get the Remote Locations for each of the subtypes
    @staticmethod
    def get_roothttp_for_subtype(subtype_filter):

        esi__4wk__roothttp      = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__ESI_4WK", default_or_error_return_value="")        # 'https://geo.nsstc.nasa.gov/SPoRT/outgoing/crh/4servir/'
        esi__12wk__roothttp     = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__ESI_12WK", default_or_error_return_value="")       # 'https://geo.nsstc.nasa.gov/SPoRT/outgoing/crh/4servir/'

        # ret_roothttp = settings.REMOTE_PATH__ROOT_HTTP__DEFAULT #'localhost://UNKNOWN_URL'
        ret_roothttp = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__DEFAULT", default_or_error_return_value="")  # ret_roothttp = settings.REMOTE_PATH__ROOT_HTTP__DEFAULT #'localhost://UNKNOWN_URL'
        subtype_filter = str(subtype_filter).strip()
        if (subtype_filter == '4week'):
            ret_roothttp = esi__4wk__roothttp
        if (subtype_filter == '12week'):
            ret_roothttp = esi__12wk__roothttp

        return ret_roothttp

    def execute__Step__Pre_ETL_Custom(self):
        ret__function_name = "execute__Step__Pre_ETL_Custom"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}
        #
        # TODO: Subtype Specific Logic Here
        #

        # Get the root http path based on the region.
        current_root_http_path          = self.get_roothttp_for_subtype(subtype_filter=self.esi_mode)
        root_file_download_path         = os.path.join(esi.get_root_local_temp_working_dir(subtype_filter=self.esi_mode), self.relative_dir_path__WorkingDir)
        final_load_dir_path             = esi.get_final_load_dir(subtype_filter=self.esi_mode)

        self.temp_working_dir = str(root_file_download_path).strip()
        self._expected_granules = []

        print('EEEEE')
        print(current_root_http_path)
        print(root_file_download_path)
        print(final_load_dir_path)

        # (1) Generate Expected remote file paths
        try:
            # Create the list of Days (From start time to end time)
            start_Date = datetime.datetime(year=self.YYYY__Year__Start, month=self.MM__Month__Start, day=1)     #self.DD__Day__Start)
            end_Date = datetime.datetime(year=self.YYYY__Year__End, month=self.MM__Month__End, day=1)           #self.DD__Day__End)

            print(start_Date)
            print(end_Date)

            #first_day__Start_30Min_Increment = self.NN__30MinIncrement__Start  # = 0  # 0
            #last_day__End_30Min_Increment = self.NN__30MinIncrement__End  # = 48  # 0

            delta = end_Date - start_Date
            print("DELTA: " + str(delta))

            # Now let's iterate through years and months properly, inside of a month range, we need to find each of the Julian Dates that fall in that range.
            for YYYY__Year in range(self.YYYY__Year__Start, (self.YYYY__Year__End + 1)):
                print('SSSS')
                start_month__Current_Year = 1
                end_month__Current_Year = 12
                if (YYYY__Year == self.YYYY__Year__Start):
                    start_month__Current_Year = self.MM__Month__Start
                if (YYYY__Year == self.YYYY__Year__End):
                    end_month__Current_Year = self.MM__Month__End
                print('ZZZZ')
                # Calculate ALL of the expected Julian Dates (by offset)
                # Subtract items from that list that are not within the month range
                # Use that final list to create all the file names for the granules.
                #

                # Calculate the Latest Julian Month for current year and month

                print(self.N_offset_for_weekly_julian_start_date)
                print('TTTT')

                first_year_Julian_Day_number = 1 + self.N_offset_for_weekly_julian_start_date

                date_times_for_the_year = []
                week_loop_counter = 1

                is_at_end_of_year = False

                # Date time Components as strings
                year_num_to_use__As_Str             = str(YYYY__Year) # Year Component of the datetime string
                start_month__Current_Year__As_Str   = str(start_month__Current_Year)
                end_month__Current_Year__As_Str     = str(end_month__Current_Year)
                earliest_date__As_YM_Str            = str(year_num_to_use__As_Str) + str(start_month__Current_Year__As_Str)
                latest_date__As_YM_Str              = str(year_num_to_use__As_Str) + str(end_month__Current_Year__As_Str)

                #
                earliest_user_set_datetime__Current_Year    = datetime.datetime.strptime(earliest_date__As_YM_Str, "%Y%m")
                latest_user_set_datetime__Current_Year      = datetime.datetime.strptime(latest_date__As_YM_Str, "%Y%m")

                print(earliest_user_set_datetime__Current_Year)
                print(latest_user_set_datetime__Current_Year)

                # Collect a list of ALL julian dates for the current year
                while(is_at_end_of_year == False):
                    try:
                        julian_num_to_use__As_Str   = str(first_year_Julian_Day_number + (week_loop_counter * 7) )   # Each time this loop happens the Julian Date number increases by 7
                        #year_num_to_use__As_Str = str(YYYY__Year)
                        date_time_str = '' + str(year_num_to_use__As_Str) + str(julian_num_to_use__As_Str)  # // YYYYJJJ  	// a 001 Julian date Should be Jan 1st, 2020
                        date_time_obj   = datetime.datetime.strptime(date_time_str, '%Y%j')

                        #print("julian_num_to_use__As_Str: " + str(julian_num_to_use__As_Str))
                        #print("date_time_str: " + str(date_time_str))
                        #print("date_time_obj: " + str(date_time_obj))

                        date_times_for_the_year.append(date_time_obj)

                        week_loop_counter = week_loop_counter + 1
                    except:
                        # once a julian number that exceeds 365 (or 366) happens, this should break here
                        is_at_end_of_year = True
                        pass

                date_times_for_the_year = [datetime.datetime(2021, 6, 4, 0, 0)]

                # Create the Expected Granules for the current year
                for current_datetime_ToCheck in date_times_for_the_year:
                    # earliest_user_set_datetime__Current_Year.month        # We are only looking at monthly increments for this compare
                    # end_month__Current_Year__As_Str.month                 # We are only looking at monthly increments for this compare

                    # Validate that the current date time we are looking at is in the correct range of dates the user specified
                    is_date_to_check_in_user_range = True
                    if(current_datetime_ToCheck.month < earliest_user_set_datetime__Current_Year.month):
                        # The Current date time to check's month is less than the user specified earliest month
                        is_date_to_check_in_user_range = False
                    if (current_datetime_ToCheck.month > latest_user_set_datetime__Current_Year.month):
                        # The Current date time to check's month is greater than the user specified latest month
                        is_date_to_check_in_user_range = False

                    if(is_date_to_check_in_user_range == True):
                        # Only if the date (year and month) is within the current range, do we continue with setting up a granule for this object.

                        # Make the expected file name (examples)
                        # DFPPM_4WK_2020106.tif.gz
                        # DFPPM_12WK_2020106.tif.gz


                        # Gather date Parts (For final NC4 file name)
                        current_year__YYYY_str  = "{:0>4d}".format(current_datetime_ToCheck.year)
                        current_month__MM_str   = "{:02d}".format(current_datetime_ToCheck.month)
                        current_day__DD_str     = "{:02d}".format(current_datetime_ToCheck.day)


                        product_week_number_string          = self.get_expected_file_name_wk_number_string()        # Get the "4WK" or "12WK" part
                        expected_filename_datepart_string   = current_datetime_ToCheck.strftime("%Y%j")             # "2020183"

                        # Build the expected filename
                        expected_remote_base_filename               = "DFPPM_" + str(product_week_number_string) + "_" + str(expected_filename_datepart_string)  # DFPPM_4WK_2020106
                        expected_remote_gz_filename                 = expected_remote_base_filename + ".tif.gz"         # DFPPM_4WK_2020106.tif.gz
                        expected_remote_tif_filename_after_extract  = expected_remote_base_filename + ".tif"            # DFPPM_4WK_2020106.tif
                        #expected_remote_gz_file_name = "DFPPM_" + str(product_week_number_string) + "_" + str(expected_filename_datepart_string) + ".tif.gz"  # DFPPM_4WK_2020106.tif.gz
                        #print("Found Acceptable In Range Date: " + str(current_datetime_ToCheck) + " : expected_remote_gz_filename: " + expected_remote_gz_filename)

                        # Example TDS Filename
                        # SPORT-ESI.250m.12-week.20200318T00:00:00Z.GLOBAL.nc4

                        #print("IMPORTANT - CHANGE ME - LOOK FOR 'final_nc4_filename' and then look up the THREDDS standard doc to see what the name should be and adjust the code for that.")
                        #final_nc4_filename = ''
                        #final_nc4_filename += 'esi_NAME_'
                        #final_nc4_filename += '__CHANGEME__'
                        #final_nc4_filename += 'DFPPM_' + str(product_week_number_string) + '_' + str(expected_filename_datepart_string) + '.nc4'
                        # sport-esi
                        #
                        final_nc4_filename = ''
                        final_nc4_filename += 'SPORT-ESI.250.'
                        if (self.esi_mode == "4week"):
                            final_nc4_filename += '4-week'     # SPORT-ESI.250.4-week
                        if (self.esi_mode == "12week"):
                            final_nc4_filename += '12-week'      # SPORT-ESI.250.12-week
                        final_nc4_filename += '.'               # SPORT-ESI.250.4-week.
                        final_nc4_filename += current_year__YYYY_str    # SPORT-ESI.250.4-week.2020
                        final_nc4_filename += current_month__MM_str     # SPORT-ESI.250.4-week.202001
                        final_nc4_filename += current_day__DD_str       # SPORT-ESI.250.4-week.20200130
                        final_nc4_filename += 'T'                       # SPORT-ESI.250.4-week.20200130T
                        #final_nc4_filename += '00:00:00Z'               # SPORT-ESI.250.4-week.20200130T00:00:00Z
                        #  Can't put ':' in file names...
                        final_nc4_filename += '000000Z'                 # SPORT-ESI.250.4-week.20200130T000000Z
                        final_nc4_filename += '.GLOBAL.nc4'              # SPORT-ESI.250.4-week.20200130T000000Z.GLOBAL.nc4

                        print("final_nc4_filename: " + str(final_nc4_filename))

                        # Now get the remote File Paths (Directory) based on the date infos.
                        # # https://geo.nsstc.nasa.gov/SPoRT/outgoing/crh/4servir/
                        remote_directory_path = current_root_http_path
                        print(9999)
                        # remote_directory_path = "UNSET/"
                        # if (self.esi_mode == "4week"):
                        #     remote_directory_path = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__ESI_4WK", default_or_error_return_value="ERROR_GETTING_DIR_FOR_ESI_4WEEK/")
                        # if (self.esi_mode == "12week"):
                        #     remote_directory_path = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__ESI_12WK", default_or_error_return_value="ERROR_GETTING_DIR_FOR_ESI_12WEEK/")

                        # PATH__TEMP_WORKING_DIR__ESI__12WEEK
                        # PATH__TEMP_WORKING_DIR__ESI__4WEEK

                        # PATH__THREDDS_MONITORING_DIR__ESI__12WEEK
                        # PATH__THREDDS_MONITORING_DIR__ESI__4WEEK
                        # # /Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/THREDDS/thredds/catalog/climateserv/sport-esi/global/0.05deg/12wk/
                        # # /Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/THREDDS/thredds/catalog/climateserv/sport-esi/global/0.05deg/4wk/


                        # Get the full paths and set ot the granule object
                        #remote_full_filepath_tif = str(os.path.join(remote_directory_path, tif_filename)).strip()
                        #local_full_filepath_tif = os.path.join(self.temp_working_dir, tif_filename)
                        tif_gz_filename                     = expected_remote_gz_filename
                        extracted_tif_filename              = expected_remote_tif_filename_after_extract
                        remote_full_filepath_gz_tif         = str(os.path.join(remote_directory_path, tif_gz_filename)).strip()
                        local_full_filepath_final_nc4_file  = os.path.join(final_load_dir_path, final_nc4_filename)

                        #print("DONE - Create a granule with all the above info")

                        current_obj = {}


                        # Filename and Granule Name info
                        local_extract_path = self.temp_working_dir  # We are using the same directory for the download and extract path
                        local_final_load_path = final_load_dir_path
                        #local_full_filepath     = os.path.join(root_file_download_path, filename)
                        local_full_filepath_download = os.path.join(local_extract_path, tif_gz_filename)

                        #current_obj['local_download_path'] = local_extract_path     # Download path and extract path
                        current_obj['local_extract_path'] = local_extract_path      # Download path and extract path
                        current_obj['local_final_load_path'] = local_final_load_path  # The path where the final output granule file goes.
                        current_obj['remote_directory_path'] = remote_directory_path
                        #
                        current_obj['tif_gz_filename'] = tif_gz_filename
                        current_obj['extracted_tif_filename']   = extracted_tif_filename
                        current_obj['final_nc4_filename']       = final_nc4_filename
                        current_obj['granule_name']             = final_nc4_filename
                        #
                        current_obj['remote_full_filepath_gz_tif']          = remote_full_filepath_gz_tif           # remote_full_filepath_tif
                        current_obj['local_full_filepath_download']         = local_full_filepath_download # local_full_filepath
                        current_obj['local_full_filepath_final_nc4_file']   = local_full_filepath_final_nc4_file

                        print(current_obj)

                        #print("Current Granule Object: " + str(current_obj))

                        #
                        # Create a new Granule Entry - The first function 'log_etl_granule' is the one that actually creates a new ETL Granule Attempt (There is one granule per dataset per pipeline attempt run in the ETL Granule Table)
                        # # Granule Helpers
                        # # # def log_etl_granule(self, granule_name="unknown_etl_granule_file_or_object_name", granule_contextual_information="", granule_pipeline_state=settings.GRANULE_PIPELINE_STATE__ATTEMPTING, additional_json={}):
                        # # # def etl_granule__Update__granule_pipeline_state(self, granule_uuid, new__granule_pipeline_state, is_error):
                        # # # def etl_granule__Update__is_missing_bool_val(self, granule_uuid, new__is_missing__Bool_Val):
                        # # # def etl_granule__Append_JSON_To_Additional_JSON(self, granule_uuid, new_json_key_to_append, sub_jsonable_object):
                        granule_name = final_nc4_filename
                        granule_contextual_information = ""
                        # granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__ATTEMPTING  # At the start of creating a new Granule, it starts off as 'attempting' - So we can see Active Granules in the database while the system is running.
                        granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__ATTEMPTING", default_or_error_return_value="Attempting")  # settings.GRANULE_PIPELINE_STATE__ATTEMPTING
                        additional_json = current_obj  # {}
                        new_Granule_UUID = self.etl_parent_pipeline_instance.log_etl_granule(granule_name=granule_name, granule_contextual_information=granule_contextual_information, granule_pipeline_state=granule_pipeline_state, additional_json=additional_json)
                        #
                        # Save the Granule's UUID for reference in later steps
                        current_obj['Granule_UUID'] = str(new_Granule_UUID).strip()

                        # Add to the granules list
                        self._expected_granules.append(current_obj)


                        pass




                    pass
                    # Check to make sure this date time is in the selected user range

                # Filter down the list of datetime objects so that only the dates in the range are kept


                # print("DEBUG: len(date_times_for_the_year) " + str(len(date_times_for_the_year)))
                # print("DEBUG: len(self._expected_granules) " + str(len(self._expected_granules)))
                # # print("")
                # print("DEBUG: Next up - Download, Extract, Transform and Load Steps")
                #
                # print("")
                #print("DEBUG: PAUSE NOTE: ")
                #print("DEBUG: PAUSE NOTE: I STOPPED RIGHT HERE AFTER ADDING ALL OF THE DATETIMES TO THE ARRAY.  NEXT, I NEED TO GO THROUGH AND MAKE A NEW LIST WHICH ONLY CONTAINS THE ITEMS THAT FALL WITHIN THE MONTH RANGE (SO IF THE END MONTH IS MAY, THEN ALL OF THE MAY DATES NEED TO BE INSIDE THAT ARRAY.  ")
                #print("DEBUG: PAUSE NOTE: AFTER THE ABOVE, THEN GO THROUGH AND FINISH CREATING ALL THE EXPECTED GRANULES.")
                #print("")
                #print("DEBUG: PAUSE NOTE: If lost... See the Emodis AND Imerg scripts for example code here.")
                #print("")
                #print("")
                #print("")


                #


            pass
        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an error when generating the expected remote filepaths.  See the additional data for details on which expected file caused the error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "esi"
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Call Error handler right here (If this is commented out, then the info should be bubbling up to the calling function))
            # activity_event_type         = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR
            # self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=error_JSON)
            #
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj


        # Make sure the directories exist
        #
        print('TTTTTTTTTTT')
        print(self.temp_working_dir)
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(self.temp_working_dir)
        print(111111)
        if (is_error_creating_directory == True):
            error_JSON = {}
            error_JSON['error'] = "Error: There was an error when the pipeline tried to create a new directory on the filesystem.  The path that the pipeline tried to create was: " + str(self.temp_working_dir) + ".  There should be another error logged just before this one that contains system error info.  That info should give clues to why the directory was not able to be created."
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "esi"
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        print(final_load_dir_path)
        # final_load_dir_path
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(final_load_dir_path)
        if (is_error_creating_directory == True):
            error_JSON = {}
            error_JSON['error'] = "Error: There was an error when the pipeline tried to create a new directory on the filesystem.  The path that the pipeline tried to create was: " + str(final_load_dir_path) + ".  There should be another error logged just before this one that contains system error info.  That info should give clues to why the directory was not able to be created."
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "esi"
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj


        # Ended, now for reporting
        ret__detail_state_info['class_name'] = "esi"
        # ret__detail_state_info['number_of_expected_remote_full_file_paths'] = str(len(self._expected_remote_full_file_paths)).strip()
        ret__detail_state_info['number_of_expected_granules'] = str(len(self._expected_granules)).strip()
        ret__event_description = "Success.  Completed Step execute__Step__Pre_ETL_Custom by generating " + str(len(self._expected_remote_full_file_paths)).strip() + " expected full file paths to download and " + str(len(self._expected_granules)).strip() + " expected granules to process."

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Download(self):
        ret__function_name = "execute__Step__Download"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}
        #
        # TODO: Subtype Specific Logic Here
        #



        download_counter = 0
        loop_counter = 0
        error_counter = 0
        detail_errors = []

        # # expected_granule Has these properties (and possibly more)
        #                 current_obj['local_extract_path'] = local_extract_path      # Download path and extract path
        #                 current_obj['local_final_load_path'] = local_final_load_path  # The path where the final output granule file goes.
        #                 current_obj['remote_directory_path'] = remote_directory_path
        #                 #
        #                 current_obj['tif_gz_filename'] = tif_gz_filename
        #                 current_obj['extracted_tif_filename']   = extracted_tif_filename
        #                 current_obj['final_nc4_filename']       = final_nc4_filename
        #                 current_obj['granule_name']             = final_nc4_filename
        #                 #
        #                 current_obj['remote_full_filepath_gz_tif']          = remote_full_filepath_gz_tif           # remote_full_filepath_tif
        #                 current_obj['local_full_filepath_download']         = local_full_filepath_download # local_full_filepath
        #                 current_obj['local_full_filepath_final_nc4_file']   = local_full_filepath_final_nc4_file
        #                 #
        #                 current_obj['Granule_UUID'] = str(new_Granule_UUID).strip()
        #
        #
        # # Example (Local Data)
        #   {
        #       'local_extract_path':                   '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/temp_etl_data/esi/4week/working_dir',
        #       'local_final_load_path':                '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/THREDDS/thredds/catalog/climateserv/sport-esi/global/0.05deg/4wk/',
        #       'remote_directory_path':                '/SPoRT/outgoing/crh/4servir/',
        #       'tif_gz_filename':                      'DFPPM_4WK_2020274.tif.gz',
        #       'extracted_tif_filename':               'DFPPM_4WK_2020274.tif',
        #       'final_nc4_filename':                   'SPORT-ESI.250.12-week.20200930T00:00:00Z.GLOBAL.nc4',
        #       'granule_name':                         'SPORT-ESI.250.12-week.20200930T00:00:00Z.GLOBAL.nc4',
        #       'remote_full_filepath_gz_tif':          '/SPoRT/outgoing/crh/4servir/DFPPM_4WK_2020274.tif.gz',
        #       'local_full_filepath_final_nc4_file':   '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/THREDDS/thredds/catalog/climateserv/sport-esi/global/0.05deg/4wk/SPORT-ESI.250.12-week.20200930T00:00:00Z.GLOBAL.nc4'
        #   }

        expected_granules = self._expected_granules
        num_of_objects_to_process = len(expected_granules)
        num_of_download_activity_events = 4
        modulus_size = int(num_of_objects_to_process / num_of_download_activity_events)
        if (modulus_size < 1):
            modulus_size = 1

        # Connect FTP
        # # - No FTP in ESI types (just do direct downloads)

        # Process each expected granule
        for expected_granule in expected_granules:
            try:
                if (((loop_counter + 1) % modulus_size) == 0):
                    event_message = "About to download file: " + str(loop_counter + 1) + " out of " + str(num_of_objects_to_process)
                    print(event_message)
                    # activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS
                    activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS", default_or_error_return_value="ETL Download Progress")  # settings.ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS
                    activity_description = event_message
                    additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                    self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

                # Current Granule to download
                #remote_full_filepath_gz_tif = expected_granule['remote_full_filepath_gz_tif']
                current_url_to_download                             = expected_granule['remote_full_filepath_gz_tif']
                current_download_destination_local_full_file_path   = expected_granule['local_full_filepath_download'] # current_download_destination_local_full_file_path = expected_granule['local_full_filepath']
                #FIXTHIS__THERE_IS_NO_LOCAL_FULL_FILEPATH___CHECK_NDVI    -   current_download_destination_local_full_file_path   = expected_granule['local_full_filepath']

                # remote_directory_path = expected_granule['remote_directory_path']
                # # remote_full_filepath_tif    = expected_granule['remote_full_filepath_tif']
                # tif_filename                = expected_granule['tif_filename']
                # local_full_filepath_tif     = expected_granule['local_full_filepath_tif']
                #
                # Granule info
                Granule_UUID = expected_granule['Granule_UUID']
                granule_name = expected_granule['granule_name']

                # Download the file.
                # Actually do the download now
                try:
                    urllib_request.urlretrieve(current_url_to_download, current_download_destination_local_full_file_path)  # urllib_request.urlretrieve(url, endfilename)
                    # print(" - (GRANULE LOGGING) Log Each Download into the Granule Storage Area: (current_download_destination_local_full_file_path): " + str(current_download_destination_local_full_file_path))

                    download_counter = download_counter + 1
                except:
                    error_counter = error_counter + 1
                    sysErrorData = str(sys.exc_info())
                    # print("DEBUG Warn: (WARN LEVEL) (File can not be downloaded).  System Error Message: " + str(sysErrorData))
                    warn_JSON = {}
                    warn_JSON['warning'] = "Warning: There was an uncaught error when attempting to download file at URL: " + str(current_url_to_download) + ".  If the System Error message says something like 'nodename nor servname provided, or not known', then one common cause of that error is an unstable or disconnected internet connection.  Double check that the internet connection is working and try again.  System Error Message: " + str(sysErrorData)
                    warn_JSON['is_error'] = True
                    warn_JSON['class_name'] = "esi"
                    warn_JSON['function_name'] = "execute__Step__Download"
                    warn_JSON['current_object_info'] = expected_granule
                    # Call Error handler right here to send a warning message to ETL log. - Note this warning will not make it back up to the overall pipeline, it is being sent here so admin can still be aware of it and handle it.
                    # activity_event_type         = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING
                    activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING", default_or_error_return_value="ETL Warning")
                    activity_description = warn_JSON['warning']
                    self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=warn_JSON)

            except:
                error_counter = error_counter + 1
                sysErrorData = str(sys.exc_info())
                error_message = "esi.execute__Step__Download: Generic Uncaught Error.  At least 1 download failed.  System Error Message: " + str(sysErrorData)
                detail_errors.append(error_message)
                #print("esi.execute__Step__Download: Generic Uncaught Error: " + str(sysErrorData))
                print(error_message)

            loop_counter = loop_counter + 1

        # Ended, now for reporting
        #
        ret__detail_state_info['class_name'] = "esi"
        ret__detail_state_info['download_counter'] = download_counter
        ret__detail_state_info['error_counter'] = error_counter
        ret__detail_state_info['loop_counter'] = loop_counter
        ret__detail_state_info['detail_errors'] = detail_errors
        # ret__detail_state_info['number_of_expected_remote_full_file_paths'] = str(len(self._expected_remote_full_file_paths)).strip()
        # ret__detail_state_info['number_of_expected_granules'] = str(len(self._expected_granules)).strip()
        ret__event_description = "Success.  Completed Step execute__Step__Download by downloading " + str(download_counter).strip() + " files."
        #

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Extract(self):

        ret__function_name = "execute__Step__Extract"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}
        #
        # TODO: Subtype Specific Logic Here
        #

        detail_errors = []
        error_counter = 0

        try:
            expected_granules = self._expected_granules
            for expected_granules_object in expected_granules:

                #import gzip
                #import shutil

                #local_full_filepath = '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/temp_etl_data/esi/4week/working_dir/DFPPM_4WK_2020218.tif.gz'
                #local_extract_full_filepath = '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/temp_etl_data/esi/4week/working_dir/DFPPM_4WK_2020218.tif'

                print(',,,,,,,,,,')
                print(expected_granules_object)


                local_full_filepath_download    = expected_granules_object['local_full_filepath_download']
                local_extract_path              = expected_granules_object['local_extract_path']
                extracted_tif_filename          = expected_granules_object['extracted_tif_filename']
                local_extract_full_filepath     = os.path.join(local_extract_path, extracted_tif_filename)

                print(local_full_filepath_download)
                print(local_extract_full_filepath)

                if not os.path.isfile(local_full_filepath_download):
                    continue

                try:
                    with gzip.open(local_full_filepath_download, 'rb') as f_in:
                        with open(local_extract_full_filepath, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)

                except Exception as e:
                    print(e)
                    # This granule errored on the Extract step.
                    sysErrorData = str(sys.exc_info())

                    Granule_UUID = expected_granules_object['Granule_UUID']

                    error_message = "esi.execute__Step__Extract: An Error occurred during the Extract step with ETL_Granule UUID: " + str(Granule_UUID) + ".  System Error Message: " + str(sysErrorData)

                    # print("DEBUG: PRINT ERROR HERE: (error_message) " + str(error_message))

                    # Individual Transform Granule Error
                    error_counter = error_counter + 1
                    detail_errors.append(error_message)

                    error_JSON = {}
                    error_JSON['error_message'] = error_message

                    # Update this Granule for Failure (store the error info in the granule also)
                    # Granule_UUID = expected_granules_object['Granule_UUID']
                    # new__granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__FAILED  # When a granule has a NC4 file in the correct location, this counts as a Success.
                    new__granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")  #
                    is_error = True
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)
                    new_json_key_to_append = "execute__Step__Extract"
                    is_update_succeed_2 = self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(granule_uuid=Granule_UUID, new_json_key_to_append=new_json_key_to_append, sub_jsonable_object=error_JSON)


                print('++++++++')

                # print("")
                # print("extract: (local_full_filepath_download): " + str(local_full_filepath_download))
                # print("extract: (local_extract_path): " + str(local_extract_path))
                # print("extract: (extracted_tif_filename): " + str(extracted_tif_filename))
                # print("extract: (local_extract_full_filepath): " + str(local_extract_full_filepath))
                # print("extract: (expected_granules_object): " + str(expected_granules_object))
                # print("")

        except:
            sysErrorData = str(sys.exc_info())
            ret__is_error = True
            ret__error_description = "esi.execute__Step__Extract: There was a generic, uncaught error when attempting to Extract the Granules.  System Error Message: " + str(sysErrorData)


        ret__detail_state_info['class_name'] = "esi"
        ret__detail_state_info['error_counter'] = error_counter
        ret__detail_state_info['detail_errors'] = detail_errors

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Transform(self):
        ret__function_name = "execute__Step__Transform"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}
        #
        # TODO: Subtype Specific Logic Here
        #

        # error_counter, detail_errors
        error_counter = 0
        detail_errors = []

        try:
            expected_granules = self._expected_granules
            for expected_granules_object in expected_granules:
                try:

                    #print("A")
                    # Getting info ready for the current granule.
                    local_extract_path                                  = expected_granules_object['local_extract_path']
                    tif_filename                                        = expected_granules_object['extracted_tif_filename']   #  tif_filename to extracted_tif_filename
                    final_nc4_filename                                  = expected_granules_object['final_nc4_filename']
                    expected_full_path_to_local_extracted_tif_file      = os.path.join(local_extract_path, tif_filename)

                    geotiffFile_FullPath = expected_full_path_to_local_extracted_tif_file

                    #print("B")

                    # Note there are only 3 lines of differences between the 4wk and 12 wk scripts
                    # TODO: Place those difference variables here.
                    mode_var__pd_timedelta = ""
                    mode_var__attr_composite_interval = ""
                    mode_var__attr_comment = ""
                    mode_var__TemporalResolution = ""
                    if (self.esi_mode == "4week"):
                        mode_var__pd_timedelta += '28d'
                        mode_var__attr_composite_interval += '4 week'
                        mode_var__attr_comment += '4-week mean composite estimate of evaporative stress index'
                        mode_var__TemporalResolution += '4-week'
                    if (self.esi_mode == "12week"):
                        mode_var__pd_timedelta += '84d'
                        mode_var__attr_composite_interval += '12 week'
                        mode_var__attr_comment += '12-week mean composite estimate of evaporative stress index'
                        mode_var__TemporalResolution += '12-week'

                    #print("C")

                    # Matching to the other script

                    ############################################################
                    # Start extracting data and creating output netcdf file.
                    ############################################################

                    # !/usr/bin/env python
                    # Program: Convert SPORT-ESI 4-week (and 12-week) composite geoTiff files into netCDF-4 for storage on the ClimateSERV 2.0 thredds data server.
                    # Calling: esi4week2netcdf.py geotiffFile               (and esi12week2netcdf)
                    # geotiffFile: The inputfile to be processed.
                    #
                    # General Flow:
                    # Determine the date associated with the geoTiff file.
                    # 1) Use  xarray+rasterio to read the geotiff data from a file into a data array.
                    # 2) Convert to a dataset and add an appropriate time dimension
                    # 3) Clean up the dataset: Rename and add dimensions, attributes, and scaling factors as appropriate.
                    # 4) Dump the dataset to a netCDF-4 file with a filename conforming to the ClimateSERV 2.0 TDS conventions.

                    # import xarray as xr
                    # import pandas as pd
                    # import numpy as np
                    # import sys
                    # import re
                    # from collections import OrderedDict

                    #geotiffFile = sys.argv[1]

                    # Set region ID
                    regionID = 'Global'  # technically only semi-global as it spans 60S to 90N in latitude

                    # Based on the geotiffFile name, determine the time string elements.
                    # Split elements by period
                    TimeStrSplit = tif_filename.split('.')  #TimeStrSplit = geotiffFile.split('_')              # 'DFPPM_4WK_2020232' , 'tif'
                    #print("TimeStrSplit: " + str(TimeStrSplit))
                    #yyyyddd = TimeStrSplit[2].split('.')[0]  # added extra split to trim off .tif
                    yyyyddd = TimeStrSplit[0].split('_')[2] # Should just get the '2020232' part        # TimeStrSplit[2].split('.')[0]  # added extra split to trim off .tif
                    #print("yyyyddd: " + str(yyyyddd))

                    # Determine starting and ending times.
                    # # pandas can't seem to use datetime to parse timestrings...
                    # # System Error Message: (<class 'NotImplementedError'>, NotImplementedError('Timestamp.strptime() is not implemented.Use to_datetime() to parse date strings.')
                    #endTime = pd.Timestamp.strptime(yyyyddd, '%Y%j')
                    endTime = datetime.datetime.strptime(yyyyddd, '%Y%j')
                    #startTime = endTime - pd.Timedelta('28d')  # 4 weeks (i.e. 28 days), # 12 weeks (i.e. 84 days)
                    #startTime = endTime - pd.Timedelta(mode_var__pd_timedelta)  # 4 weeks (i.e. 28 days), # 12 weeks (i.e. 84 days)
                    startTime = endTime - pd.Timedelta(mode_var__pd_timedelta)  # 4 weeks (i.e. 28 days), # 12 weeks (i.e. 84 days)

                    #print("D")

                    ############################################################
                    # Beging extracting data and creating output netcdf file.
                    ############################################################
                    # 1) Read the geotiff data into an xarray data array
                    tiffData = xr.open_rasterio(geotiffFile_FullPath)  #tiffData = xr.open_rasterio(geotiffFile)
                    # 2) Convert to a dataset.  (need to assign a name to the data array)
                    esi = tiffData.rename('esi').to_dataset()
                    # Handle selecting/adding the dimesions
                    esi = esi.isel(band=0).reset_coords('band', drop=True)  # select the singleton band dimension and drop out the associated coordinate.
                    # Add the time dimension as a new coordinate.
                    esi = esi.assign_coords(time=endTime).expand_dims(dim='time', axis=0)
                    # Add an additional variable "time_bnds" for the time boundaries.
                    esi['time_bnds'] = xr.DataArray(np.array([startTime, endTime]).reshape((1, 2)), dims=['time', 'nbnds'])
                    # 3) Rename and add attributes to this dataset.
                    #esi.rename({'y': 'latitude', 'x': 'longitude'}, inplace=True)  # rename lat/lon
                    esi = esi.rename({'y': 'latitude', 'x': 'longitude'})  # rename lat/lon
                    # Lat/Lon/Time dictionaries.
                    # Use Ordered dict
                    latAttr = OrderedDict([('long_name', 'latitude'), ('units', 'degrees_north'), ('axis', 'Y')])
                    lonAttr = OrderedDict([('long_name', 'longitude'), ('units', 'degrees_east'), ('axis', 'X')])
                    timeAttr = OrderedDict([('long_name', 'time'), ('axis', 'T'), ('bounds', 'time_bnds')])
                    timeBoundsAttr = OrderedDict([('long_name', 'time_bounds')])
                    esiAttr = OrderedDict([('long_name', 'evaporative_stress_index'), ('units', 'unitless'), ('composite_interval', str(mode_var__attr_composite_interval)), ('comment', str(mode_var__attr_comment))])
                    fileAttr = OrderedDict([('Description', 'The Evaporative Stress Index (ESI) at ~5-kilometer resolution for the entire globe reveals regions of drought where vegetation is stressed due to lack of water, enabling agriculture ministries to provide farmers with actionable advice about irrigation.  The 4 week composite responds to fast-changing conditions, while the 12-week composite integrates data over a longer period and responds to slower evolving conditions.'), \
                                            ('DateCreated', pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%SZ')), \
                                            ('Contact', 'Lance Gilliland, lance.gilliland@nasa.gov'), \
                                            ('Source', 'NASA/MSFC SPORT; C. Hain, christopher.hain@nasa.gov; https://geo.nsstc.nasa.gov/SPoRT/outgoing/crh/4servir/'), \
                                            ('Version', '1.0'), \
                                            ('Reference', 'Hain, C. R. and M.C. Anderson, 2017: Estimating morning change in land surface temperature from MODIS day/night observations: Applications for surface energy balance modeling., Geophys. Res. Letts., 44, 9723-9733, doi:10.1002/2017GL074952.'), \
                                            ('RangeStartTime', startTime.strftime('%Y-%m-%dT%H:%M:%SZ')), \
                                            ('RangeEndTime', endTime.strftime('%Y-%m-%dT%H:%M:%SZ')), \
                                            ('SouthernmostLatitude', np.min(esi.latitude.values)), \
                                            ('NorthernmostLatitude', np.max(esi.latitude.values)), \
                                            ('WesternmostLongitude', np.min(esi.longitude.values)), \
                                            ('EasternmostLongitude', np.max(esi.longitude.values)), \
                                            ('TemporalResolution', str(mode_var__TemporalResolution)), \
                                            ('SpatialResolution', '0.05deg')])

                    #print("E")

                    # missing_data/_FillValue , relative time units etc. are handled as part of the encoding dictionary used in to_netcdf() call.
                    esiEncoding = {'_FillValue': np.float32(-9999.0), 'missing_value': np.float32(-9999.0), 'dtype': np.dtype('float32')}
                    timeEncoding = {'units': 'seconds since 1970-01-01T00:00:00Z'}
                    timeBoundsEncoding = {'units': 'seconds since 1970-01-01T00:00:00Z'}
                    # Set the Attributes
                    esi.latitude.attrs = latAttr
                    esi.longitude.attrs = lonAttr
                    esi.time.attrs = timeAttr
                    esi.time_bnds.attrs = timeBoundsAttr
                    esi.esi.attrs = esiAttr
                    esi.attrs = fileAttr
                    # Set the Endcodings
                    esi.esi.encoding = esiEncoding
                    esi.time.encoding = timeEncoding
                    esi.time_bnds.encoding = timeBoundsEncoding

                    #print("F")

                    # 5) Output File
                    #outputFile = 'SPORT-ESI.' + endTime.strftime('%Y%m%dT%H%M%SZ') + '.' + regionID + '.nc4'
                    #esi.to_netcdf(outputFile, unlimited_dims='time')
                    outputFile_FullPath = os.path.join(local_extract_path, final_nc4_filename)
                    esi.to_netcdf(outputFile_FullPath, unlimited_dims='time')

                    #print("outputFile_FullPath: " + str(outputFile_FullPath))
                    # Can't put ':' in file names...

                    #print("G")

                except:

                    sysErrorData = str(sys.exc_info())

                    Granule_UUID = expected_granules_object['Granule_UUID']

                    error_message = "esi.execute__Step__Transform: An Error occurred during the Transform step with ETL_Granule UUID: " + str(Granule_UUID) + ".  System Error Message: " + str(sysErrorData)

                    print("DEBUG: PRINT ERROR HERE: (error_message) " + str(error_message))

                    # Individual Transform Granule Error
                    error_counter = error_counter + 1
                    detail_errors.append(error_message)

                    error_JSON = {}
                    error_JSON['error_message'] = error_message

                    # Update this Granule for Failure (store the error info in the granule also)
                    # Granule_UUID = expected_granules_object['Granule_UUID']
                    # new__granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__FAILED  # When a granule has a NC4 file in the correct location, this counts as a Success.
                    new__granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")  #
                    is_error = True
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)
                    new_json_key_to_append = "execute__Step__Transform"
                    is_update_succeed_2 = self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(granule_uuid=Granule_UUID, new_json_key_to_append=new_json_key_to_append, sub_jsonable_object=error_JSON)

                pass

        except:

            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an uncaught error when processing the Transform step on all of the expected Granules.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "esi"
            error_JSON['function_name'] = "execute__Step__Transform"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        ret__detail_state_info['class_name'] = "esi"
        ret__detail_state_info['error_counter'] = error_counter
        ret__detail_state_info['detail_errors'] = detail_errors

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Load(self):
        ret__function_name = "execute__Step__Load"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}
        #
        # TODO: Subtype Specific Logic Here
        #

        try:
            expected_granules = self._expected_granules
            for expected_granules_object in expected_granules:

                expected_full_path_to_local_working_nc4_file = "UNSET"
                expected_full_path_to_local_final_nc4_file = "UNSET"

                try:
                    local_extract_path = expected_granules_object['local_extract_path']
                    local_final_load_path = expected_granules_object['local_final_load_path']
                    final_nc4_filename = expected_granules_object['final_nc4_filename']
                    expected_full_path_to_local_working_nc4_file = os.path.join(local_extract_path, final_nc4_filename)  # Where the NC4 file was generated during the Transform Step
                    expected_full_path_to_local_final_nc4_file = os.path.join(local_final_load_path, final_nc4_filename)  # Where the final NC4 file should be placed for THREDDS Server monitoring

                    # Copy the file from the working directory over to the final location for it.  (Where THREDDS Monitors for it)
                    copyfile(expected_full_path_to_local_working_nc4_file, expected_full_path_to_local_final_nc4_file)  # (src, dst)

                    print(44444444)

                    # Create a new Granule Entry - The first function 'log_etl_granule' is the one that actually creates a new ETL Granule Attempt (There is one granule per dataset per pipeline attempt run in the ETL Granule Table)
                    # # Granule Helpers
                    # # # def log_etl_granule(self, granule_name="unknown_etl_granule_file_or_object_name", granule_contextual_information="", granule_pipeline_state=settings.GRANULE_PIPELINE_STATE__ATTEMPTING, additional_json={}):
                    # # # def etl_granule__Update__granule_pipeline_state(self, granule_uuid, new__granule_pipeline_state, is_error):
                    # # # def etl_granule__Update__is_missing_bool_val(self, granule_uuid, new__is_missing__Bool_Val):
                    # # # def etl_granule__Append_JSON_To_Additional_JSON(self, granule_uuid, new_json_key_to_append, sub_jsonable_object):
                    Granule_UUID = expected_granules_object['Granule_UUID']

                    print(Granule_UUID)

                    # new__granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__SUCCESS # When a granule has a NC4 file in the correct location, this counts as a Success.
                    new__granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__SUCCESS", default_or_error_return_value="SUCCESS")  #
                    is_error = False
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)
                    #



                    # Now that the granule is in it's destination location, we can do a create_or_update 'Available Granule' so that the database knows this granule exists in the system (so the client side will know it is available)
                    #
                    # # TODO - Possible Parameter updates needed here.  (As we learn more about what the specific client side needs are)
                    # # def create_or_update_Available_Granule(self, granule_name, granule_contextual_information, etl_pipeline_run_uuid, etl_dataset_uuid, created_by, additional_json):
                    granule_name = final_nc4_filename
                    granule_contextual_information = ""
                    additional_json = {}
                    additional_json['MostRecent__ETL_Granule_UUID'] = str(Granule_UUID).strip()
                    # self.etl_parent_pipeline_instance.create_or_update_Available_Granule(granule_name=granule_name, granule_contextual_information=granule_contextual_information, additional_json=additional_json)

                except:
                    sysErrorData = str(sys.exc_info())
                    error_JSON = {}
                    error_JSON['error'] = "Error: There was an error when attempting to copy the current nc4 file to it's final directory location.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
                    error_JSON['is_error'] = True
                    error_JSON['class_name'] = "esi"
                    error_JSON['function_name'] = "execute__Step__Load"
                    #
                    # Additional infos
                    error_JSON['expected_full_path_to_local_working_nc4_file'] = str(expected_full_path_to_local_working_nc4_file).strip()
                    error_JSON['expected_full_path_to_local_final_nc4_file'] = str(expected_full_path_to_local_final_nc4_file).strip()
                    #

                    # Update this Granule for Failure (store the error info in the granule also)
                    Granule_UUID = expected_granules_object['Granule_UUID']
                    # new__granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__FAILED  # When a granule has a NC4 file in the correct location, this counts as a Success.
                    new__granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")  #
                    is_error = True
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)
                    new_json_key_to_append = "execute__Step__Load"
                    is_update_succeed_2 = self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(granule_uuid=Granule_UUID, new_json_key_to_append=new_json_key_to_append, sub_jsonable_object=error_JSON)

                    # # Exit Here With Error info loaded up
                    # # UPDATE - NO - Exiting here would fail the entire pipeline run when only a single granule fails..
                    # ret__is_error = True
                    # ret__error_description = error_JSON['error']
                    # ret__detail_state_info = error_JSON
                    # retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
                    # return retObj

            pass
        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an uncaught error when processing the Load step on all of the expected Granules.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "esi"
            error_JSON['function_name'] = "execute__Step__Load"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Post_ETL_Custom(self):
        ret__function_name = "execute__Step__Post_ETL_Custom"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}
        #
        # TODO: Subtype Specific Logic Here
        #
        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Clean_Up(self):
        ret__function_name = "execute__Step__Clean_Up"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}
        #
        # TODO: Subtype Specific Logic Here
        #
        try:
            temp_working_dir = str(self.temp_working_dir).strip()
            if(temp_working_dir == ""):

                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                #activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_BLANK
                activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_BLANK", default_or_error_return_value="Temp Working Dir Blank")  #
                activity_description = "Could not remove the temporary working directory.  The value for self.temp_working_dir was blank. "
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = "esi"
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

            else:
                #shutil.rmtree
                rmtree(temp_working_dir)

                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                #activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_REMOVED
                activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_REMOVED", default_or_error_return_value="Temp Working Dir Removed")  #
                activity_description = "Temp Working Directory, " + str(self.temp_working_dir).strip() + ", was removed."
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = "esi"
                additional_json['temp_working_dir'] = str(temp_working_dir).strip()
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)


            #print("execute__Step__Clean_Up: Cleanup is finished.")

        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an uncaught error when processing the Clean Up step.  This function is supposed to simply remove the working directory.  This means the working directory was not removed.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "esi"
            error_JSON['function_name'] = "execute__Step__Clean_Up"
            #
            # Additional info
            error_JSON['self__temp_working_dir'] = str(self.temp_working_dir).strip()
            #
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj


        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj
