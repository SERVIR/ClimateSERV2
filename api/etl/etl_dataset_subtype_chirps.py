import datetime, gzip, os, shutil, sys
from urllib import request as urllib_request
from shutil import copyfile, rmtree
import xarray as xr
import pandas as pd
import numpy as np
from collections import OrderedDict

from .common import common
from .etl_dataset_subtype_interface import ETL_Dataset_Subtype_Interface

from api.services import Config_SettingService
from ..models import Config_Setting

class chirps(ETL_Dataset_Subtype_Interface):

    class_name = 'chirps'
    etl_parent_pipeline_instance = None

    chirps_mode = 'chirp'

    # Input Settings
    YYYY__Year__Start = 2020  # 2019
    YYYY__Year__End = 2020
    MM__Month__Start = 1  # 12    # 2 #1
    MM__Month__End = 1  # 4 #6
    DD__Day__Start = 1  # 30    # 23
    DD__Day__End = 1  # 2     # 9

    relative_dir_path__WorkingDir = 'working_dir'

    # DRAFTING - Suggestions
    _expected_remote_full_file_paths    = []    # Place to store a list of remote file paths (URLs) that the script will need to download.
    _expected_granules                  = []    # Place to store granules

    # init (Passing a reference from the calling class, so we can callback the error handler)
    def __init__(self, etl_parent_pipeline_instance, subtype):
        self.etl_parent_pipeline_instance = etl_parent_pipeline_instance
        if subtype == 'chirp':
            self.chirps_mode = 'chirp'
        elif subtype == 'chirps':
            self.chirps_mode = 'chirps'
        elif subtype == 'chirps_gefs':
            self.chirps_mode = 'chirps_gefs'

    def set_optional_parameters(self, YYYY__Year__Start, YYYY__Year__End, MM__Month__Start, MM__Month__End, DD__Day__Start, DD__Day__End):
        self.YYYY__Year__Start = YYYY__Year__Start if YYYY__Year__Start != 0 else self.YYYY__Year__Start
        self.YYYY__Year__End = YYYY__Year__End if YYYY__Year__End != 0 else self.YYYY__Year__End
        self.MM__Month__Start = MM__Month__Start if MM__Month__Start != 0 else self.MM__Month__Start
        self.MM__Month__End = MM__Month__End if MM__Month__End != 0 else self.MM__Month__End
        self.DD__Day__Start = DD__Day__Start if DD__Day__Start != 0 else self.DD__Day__Start
        self.DD__Day__End = DD__Day__End if DD__Day__End != 0 else self.DD__Day__End

    # Get the local filesystem place to store data
    @staticmethod
    def get_root_local_temp_working_dir(subtype_filter):
        # Type Specific Settings
        chirps__chirp__rootoutputworkingdir  = Config_Setting.get_value(setting_name="PATH__TEMP_WORKING_DIR__CHIRP", default_or_error_return_value="/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/temp_etl_data/chirps/chirp/")
        chirps__chirps__rootoutputworkingdir = Config_Setting.get_value(setting_name="PATH__TEMP_WORKING_DIR__CHIRPS", default_or_error_return_value="/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/temp_etl_data/chirps/chirps/")
        chirps__chirps_gefs__rootoutputworkingdir = Config_Setting.get_value(setting_name="PATH__TEMP_WORKING_DIR__CHIRPS_GEFS", default_or_error_return_value="/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/temp_etl_data/chirps/chirps_gefs/")

        ret_rootlocal_working_dir = Config_Setting.get_value(setting_name="PATH__TEMP_WORKING_DIR__DEFAULT", default_or_error_return_value="")  # settings.PATH__TEMP_WORKING_DIR__DEFAULT  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/data/image/input/UNKNOWN/'
        subtype_filter = str(subtype_filter).strip()
        if (subtype_filter == 'chirp'):
            ret_rootlocal_working_dir = chirps__chirp__rootoutputworkingdir
        if (subtype_filter == 'chirps'):
            ret_rootlocal_working_dir = chirps__chirps__rootoutputworkingdir
        if (subtype_filter == 'chirps_gefs'):
            ret_rootlocal_working_dir = chirps__chirps_gefs__rootoutputworkingdir

        return ret_rootlocal_working_dir


    # Get the local filesystem place to store the final NC4 files (The THREDDS monitored Directory location)
    @staticmethod
    def get_final_load_dir(subtype_filter):
        chirps__chirp__finalloaddir = Config_Setting.get_value(setting_name="PATH__THREDDS_MONITORING_DIR__CHIRP", default_or_error_return_value="")
        chirps__chirps__finalloaddir = Config_Setting.get_value(setting_name="PATH__THREDDS_MONITORING_DIR__CHIRPS", default_or_error_return_value="")
        chirps__chirps_gefs__finalloaddir = Config_Setting.get_value(setting_name="PATH__THREDDS_MONITORING_DIR__CHIRPS_GEFS", default_or_error_return_value="")
        ret_dir = Config_Setting.get_value(setting_name="PATH__THREDDS_MONITORING_DIR__DEFAULT", default_or_error_return_value="")
        subtype_filter = str(subtype_filter).strip()
        if (subtype_filter == 'chirp'):
            ret_dir = chirps__chirp__finalloaddir
        if (subtype_filter == 'chirps'):
            ret_dir = chirps__chirps__finalloaddir
        if (subtype_filter == 'chirps_gefs'):
            ret_dir = chirps__chirps_gefs__finalloaddir

        return ret_dir


    # Get the Remote Locations for each of the subtypes
    @staticmethod
    def get_roothttp_for_subtype(subtype_filter):
        chirps__chirp__roothttp = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__CHIRP", default_or_error_return_value="")
        chirps__chirps__roothttp = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__CHIRPS", default_or_error_return_value="")
        chirps__chirps_gefs__roothttp = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__CHIRPS_GEFS", default_or_error_return_value="")
        # ret_roothttp = settings.REMOTE_PATH__ROOT_HTTP__DEFAULT #'localhost://UNKNOWN_URL'
        ret_roothttp = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__DEFAULT", default_or_error_return_value="")  # ret_roothttp = settings.REMOTE_PATH__ROOT_HTTP__DEFAULT #'localhost://UNKNOWN_URL'
        subtype_filter = str(subtype_filter).strip()
        if (subtype_filter == 'chirp'):
            ret_roothttp = chirps__chirp__roothttp
        if (subtype_filter == 'chirps'):
            ret_roothttp = chirps__chirps__roothttp
        if (subtype_filter == 'chirps_gefs'):
            ret_roothttp = chirps__chirps_gefs__roothttp
        return ret_roothttp

    @staticmethod
    def append_YEAR_to_dir_path(dirPath, year_int):
        # # Add the Year as a string.
        # year_YYYY = str(year_YYYY).strip()    # Expecting 'year' to be something like 2019 or "2019"
        year_YYYY = "{:0>4d}".format(year_int)
        year_dir_name_to_append = year_YYYY + "/"
        dirPath = dirPath + year_dir_name_to_append
        return dirPath





    # Specialized Functions (For each Mode)
    @staticmethod
    def get__base_filename__for_chirps_mode__chirp(datetime_obj):
        current_year__YYYY_str  = "{:0>4d}".format(datetime_obj.year)
        current_month__MM_str   = "{:02d}".format(datetime_obj.month)
        current_day__DD_str     = "{:02d}".format(datetime_obj.day)
        base_filename = ''
        base_filename += 'chirp.'                   # chirp.
        base_filename += current_year__YYYY_str     # chirp.2020
        base_filename += '.'                        # chirp.2020.
        base_filename += current_month__MM_str      # chirp.2020.04
        base_filename += '.'                        # chirp.2020.04.
        base_filename += current_day__DD_str        # chirp.2020.04.02
        #base_filename += '.tif'                     # chirp.2020.04.02.tif
        return base_filename

    @staticmethod
    def get__base_filename__for_chirps_mode__chirps(datetime_obj):
        current_year__YYYY_str = "{:0>4d}".format(datetime_obj.year)
        current_month__MM_str = "{:02d}".format(datetime_obj.month)
        current_day__DD_str = "{:02d}".format(datetime_obj.day)
        base_filename = ''
        base_filename += 'chirps-v2.0.'             # chirps-v2.0.
        base_filename += current_year__YYYY_str     # chirps-v2.0.2020
        base_filename += '.'                        # chirps-v2.0.2020.
        base_filename += current_month__MM_str      # chirps-v2.0.2020.04
        base_filename += '.'                        # chirps-v2.0.2020.04.
        base_filename += current_day__DD_str        # chirps-v2.0.2020.04.02
        #base_filename += '.tif'                     # chirps-v2.0.2020.04.02.tif
        return base_filename

    @staticmethod
    def get__base_filename__for_chirps_mode__chirps_gefs(datetime_obj):
        current_year__YYYY_str = "{:0>4d}".format(datetime_obj.year)
        current_month__MM_str = "{:02d}".format(datetime_obj.month)
        current_day__DD_str = "{:02d}".format(datetime_obj.day)
        dekad_day_str = "01"
        if(datetime_obj.day > 10):
            dekad_day_str = "11"
        if (datetime_obj.day > 20):
            dekad_day_str = "21"
        base_filename = ''
        base_filename += 'data.'                    # data.
        base_filename += current_year__YYYY_str     # data.2020
        base_filename += '.'                        # data.2020.
        base_filename += current_month__MM_str      # data.2020.04
        base_filename += current_day__DD_str        # data.2020.0402
        base_filename += '.created-from.'           # data.2020.0402.created-from.
        base_filename += current_year__YYYY_str     # data.2020.0402.created-from.2020
        base_filename += '.'                        # data.2020.0402.created-from.2020.
        base_filename += current_month__MM_str      # data.2020.0402.created-from.2020.04
        base_filename += dekad_day_str              # data.2020.0402.created-from.2020.0401
        #base_filename += '.tif'                     # data.2020.0402.created-from.2020.0401.tif
        return base_filename

    # Function to decide which basefile name function to call based on the current mode.  (The file structure differs between each mode).
    @staticmethod
    def get__base_filename(subtype_filter, datetime_obj):
        base_filename = 'default__chirps_mode_not_recognized'
        subtype_filter = str(subtype_filter).strip()
        if (subtype_filter == 'chirp'):
            base_filename = chirps.get__base_filename__for_chirps_mode__chirp(datetime_obj=datetime_obj)
        if (subtype_filter == 'chirps'):
            base_filename = chirps.get__base_filename__for_chirps_mode__chirps(datetime_obj=datetime_obj)
        if (subtype_filter == 'chirps_gefs'):
            base_filename = chirps.get__base_filename__for_chirps_mode__chirps_gefs(datetime_obj=datetime_obj)
        return base_filename


    # ETL Pipeline Functions
    def execute__Step__Pre_ETL_Custom(self):
        ret__function_name = "execute__Step__Pre_ETL_Custom"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        # Get the root http path based on the region.
        current_root_http_path      = self.get_roothttp_for_subtype(subtype_filter=self.chirps_mode)
        root_file_download_path     = os.path.join(chirps.get_root_local_temp_working_dir(subtype_filter=self.chirps_mode), self.relative_dir_path__WorkingDir)
        final_load_dir_path         = chirps.get_final_load_dir(subtype_filter=self.chirps_mode)

        self.temp_working_dir       = str(root_file_download_path).strip()
        self._expected_granules     = []
        # expected_granules = []


        # (1) Generate Expected remote file paths
        try:
            # Create the list of Days (From start time to end time)
            start_Date = datetime.datetime(year=self.YYYY__Year__Start, month=self.MM__Month__Start, day=self.DD__Day__Start)
            end_Date = datetime.datetime(year=self.YYYY__Year__End, month=self.MM__Month__End, day=self.DD__Day__End)

            delta = end_Date - start_Date
            #print("DELTA: " + str(delta))
            for i in range(delta.days + 1):
                # print start_Date + datetime.timedelta(days=i)
                currentDate = start_Date + datetime.timedelta(days=i)
                current_year__YYYY_str = "{:0>4d}".format(currentDate.year)
                current_month__MM_str = "{:02d}".format(currentDate.month)
                current_day__DD_str = "{:02d}".format(currentDate.day)


                # Create the base filename
                # TODO - Call a function passing in the date object AND the Mode
                #base_filename = ''
                base_filename = chirps.get__base_filename(subtype_filter=self.chirps_mode, datetime_obj=currentDate) # Returns everything except the '.extension'
                tif_filename = base_filename + '.tif'

                # Create the final nc4 filename
                final_nc4_filename = ''
                final_nc4_filename += 'ucsb-'               # ucsb-
                if (self.chirps_mode == "chirp"):
                    final_nc4_filename += 'chirp'           # ucsb-chirp
                if (self.chirps_mode == "chirps"):
                    final_nc4_filename += 'chirps'          # ucsb-chirps
                if (self.chirps_mode == "chirps_gefs"):
                    final_nc4_filename += 'chirps-gefs'     # ucsb-chirps-gefs
                final_nc4_filename += '.'                       # ucsb-chirps.
                final_nc4_filename += current_year__YYYY_str    # ucsb-chirps.2020
                final_nc4_filename += current_month__MM_str     # ucsb-chirps.202001
                final_nc4_filename += current_day__DD_str       # ucsb-chirps.20200130
                final_nc4_filename += 'T'                       # ucsb-chirps.20200130T
                final_nc4_filename += '000000'                  # ucsb-chirps.20200130T000000
                final_nc4_filename += 'Z.global.nc4'            # ucsb-chirps.20200130T000000Z.global.nc4
                # final_nc4_filename += both_hh_str  # nasa-imerg-late.20200130T23
                # final_nc4_filename += start_mm_str  # nasa-imerg-late.20200130T2330
                # final_nc4_filename += start_ss_str  # nasa-imerg-late.20200130T233000


                # Now get the remote File Paths (Directory) based on the date infos.
                remote_directory_path = "UNSET/"
                if (self.chirps_mode == "chirp"):
                    remote_directory_path = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__CHIRP", default_or_error_return_value="ERROR_GETTING_DIR_FOR_CHIRP/")
                if (self.chirps_mode == "chirps"):
                    remote_directory_path = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__CHIRPS", default_or_error_return_value="ERROR_GETTING_DIR_FOR_CHIRPS/")
                if (self.chirps_mode == "chirps_gefs"):
                    remote_directory_path = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__CHIRPS_GEFS", default_or_error_return_value="ERROR_GETTING_DIR_FOR_CHIRPS_GEFS/")

                # All 3 of these chirps_mode products use a year appended to the end of their path.
                # Add the Year to the directory path.
                remote_directory_path += current_year__YYYY_str
                remote_directory_path += '/'

                # Getting full paths
                remote_full_filepath_tif            = str(os.path.join(remote_directory_path, tif_filename)).strip()
                local_full_filepath_tif             = os.path.join(self.temp_working_dir, tif_filename)
                local_full_filepath_final_nc4_file  = os.path.join(final_load_dir_path, final_nc4_filename)

                # Make the current Granule Object
                current_obj = {}

                # Date Info (Which Day Is it?)
                current_obj['date_YYYY']    = current_year__YYYY_str
                current_obj['date_MM']      = current_month__MM_str
                current_obj['date_DD']      = current_day__DD_str

                # Filename and Granule Name info
                local_extract_path      = self.temp_working_dir  # There is no extract step, so just using the working directory as the local extract path.
                local_final_load_path   = final_load_dir_path
                current_obj['local_extract_path']       = local_extract_path  # Download path
                current_obj['local_final_load_path']    = local_final_load_path  # The path where the final output granule file goes.
                current_obj['remote_directory_path']    = remote_directory_path
                current_obj['base_filename']            = base_filename
                current_obj['tif_filename']             = tif_filename
                current_obj['final_nc4_filename']       = final_nc4_filename
                current_obj['granule_name']             = final_nc4_filename

                # Full Paths
                current_obj['remote_full_filepath_tif']             = remote_full_filepath_tif
                current_obj['local_full_filepath_tif']              = local_full_filepath_tif
                current_obj['local_full_filepath_final_nc4_file']   = local_full_filepath_final_nc4_file

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

                # print("DEBUG: JUST DO ONE GRANULE - BREAKING THIS FOR LOOP AFTER 1 ITERATION... BREAKING NOW.")
                # break


            # DEBUG
            # print("len(expected_granules): " + str(len(self._expected_granules)))
            # print("First Granule: str(expected_granules[0]): " + str(self._expected_granules[0]))

        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an error when generating the expected remote filepaths.  See the additional data for details on which expected file caused the error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "chirps"
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
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(self.temp_working_dir)
        if (is_error_creating_directory == True):
            error_JSON = {}
            error_JSON['error'] = "Error: There was an error when the pipeline tried to create a new directory on the filesystem.  The path that the pipeline tried to create was: " + str(self.temp_working_dir) + ".  There should be another error logged just before this one that contains system error info.  That info should give clues to why the directory was not able to be created."
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "chirps"
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        # final_load_dir_path
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(final_load_dir_path)
        if (is_error_creating_directory == True):
            error_JSON = {}
            error_JSON['error'] = "Error: There was an error when the pipeline tried to create a new directory on the filesystem.  The path that the pipeline tried to create was: " + str(final_load_dir_path) + ".  There should be another error logged just before this one that contains system error info.  That info should give clues to why the directory was not able to be created."
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "chirps"
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        # Ended, now for reporting
        ret__detail_state_info['class_name'] = "chirps"
        #ret__detail_state_info['number_of_expected_remote_full_file_paths'] = str(len(self._expected_remote_full_file_paths)).strip()
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

        # Note: In chirps, each granule has 1 file associated with it.
        # # That file is a tif file that we can download directly.  The file is not zipped so there is no extract step needed.

        download_counter    = 0
        loop_counter        = 0
        error_counter       = 0
        detail_errors       = []

        # Setting up for the periodic reporting on the terminal
        expected_granules = self._expected_granules
        num_of_objects_to_process = len(expected_granules)
        num_of_download_activity_events = 4
        modulus_size = int(num_of_objects_to_process / num_of_download_activity_events)
        if (modulus_size < 1):
            modulus_size = 1

        # No FTP here, just http

        # Loop through each expected granule
        for expected_granule in expected_granules:
            try:
                if (((loop_counter + 1) % modulus_size) == 0):
                    # print("Output a log, (and send pipeline activity log) saying, --- about to download file: " + str(loop_counter + 1) + " out of " + str(num_of_objects_to_process))
                    # print(" - Output a log, (and send pipeline activity log) saying, --- about to download file: " + str(loop_counter + 1) + " out of " + str(num_of_objects_to_process))
                    event_message = "About to download file: " + str(loop_counter + 1) + " out of " + str(num_of_objects_to_process)
                    print(event_message)
                    # activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS
                    activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS", default_or_error_return_value="ETL Download Progress")  # settings.ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS
                    activity_description = event_message
                    additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                    self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

                # Current Granule to download
                remote_directory_path       = expected_granule['remote_directory_path']
                tif_filename                = expected_granule['tif_filename']
                current_url_to_download     = remote_directory_path + tif_filename + '.gz'
                local_full_filepath_tif     = expected_granule['local_full_filepath_tif'] + '.gz'
                #
                # Granule info
                Granule_UUID    = expected_granule['Granule_UUID']
                granule_name    = expected_granule['granule_name']

                # Debug / Testing the download step
                # print("")
                # print("DEBUG: (remote_directory_path): " + str(remote_directory_path))
                # print("DEBUG: (tif_filename): " + str(tif_filename))
                # print("DEBUG: (current_url_to_download): " + str(current_url_to_download))
                # print("DEBUG: (local_full_filepath_tif): " + str(local_full_filepath_tif))
                # print("")

                # Actually do the download now
                try:
                    urllib_request.urlretrieve(current_url_to_download, local_full_filepath_tif)  # urllib_request.urlretrieve(url, endfilename)
                    # print(" - (GRANULE LOGGING) Log Each Download into the Granule Storage Area: (current_download_destination_local_full_file_path): " + str(current_download_destination_local_full_file_path))

                    download_counter = download_counter + 1
                except:
                    error_counter = error_counter + 1
                    sysErrorData = str(sys.exc_info())
                    #print("DEBUG Warn: (WARN LEVEL) (File can not be downloaded).  System Error Message: " + str(sysErrorData))
                    warn_JSON = {}
                    warn_JSON['warning']                = "Warning: There was an uncaught error when attempting to download file at URL: " +str(current_url_to_download)+ ".  If the System Error message says something like 'nodename nor servname provided, or not known', then one common cause of that error is an unstable or disconnected internet connection.  Double check that the internet connection is working and try again.  System Error Message: " + str(sysErrorData)
                    warn_JSON['is_error']               = True
                    warn_JSON['class_name']             = "chirps"
                    warn_JSON['function_name']          = "execute__Step__Download"
                    warn_JSON['current_object_info']    = expected_granule
                    # Call Error handler right here to send a warning message to ETL log. - Note this warning will not make it back up to the overall pipeline, it is being sent here so admin can still be aware of it and handle it.
                    #activity_event_type         = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING
                    activity_event_type         = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING", default_or_error_return_value="ETL Warning")
                    activity_description        = warn_JSON['warning']
                    self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=warn_JSON)




            except:
                error_counter = error_counter + 1
                sysErrorData = str(sys.exc_info())
                error_message = "chirps.execute__Step__Download: Generic Uncaught Error.  At least 1 download failed.  System Error Message: " + str(sysErrorData)
                detail_errors.append(error_message)
                print(error_message)

            # Increment the loop counter
            loop_counter = loop_counter + 1



        # Ended, now for reporting
        #
        ret__detail_state_info['class_name']        = "chirps"
        ret__detail_state_info['download_counter']  = download_counter
        ret__detail_state_info['error_counter']     = error_counter
        ret__detail_state_info['loop_counter']      = loop_counter
        ret__detail_state_info['detail_errors']     = detail_errors
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

        detail_errors = []
        error_counter = 0

        try:
            expected_granules = self._expected_granules
            for expected_granules_object in expected_granules:

                print(expected_granules_object)

                local_full_filepath_download    = expected_granules_object['local_full_filepath_tif'] + '.gz'
                local_extract_path              = expected_granules_object['local_extract_path']
                extracted_tif_filename          = expected_granules_object['tif_filename']
                local_extract_full_filepath     = os.path.join(local_extract_path, extracted_tif_filename)

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
                    local_extract_path              = expected_granules_object['local_extract_path']
                    tif_filename                    = expected_granules_object['tif_filename']
                    final_nc4_filename              = expected_granules_object['final_nc4_filename']
                    expected_full_path_to_local_extracted_tif_file = os.path.join(local_extract_path, tif_filename)

                    geotiffFile_FullPath    = expected_full_path_to_local_extracted_tif_file

                    #print("B")

                    mode_var__precipAttr_comment    = ''
                    mode_var__fileAttr_Description  = ''
                    mode_var__fileAttr_Version      = ''
                    if (self.chirps_mode == 'chirp'):
                        mode_var__precipAttr_comment    = 'Climate Hazards group InfraRed Precipitation'
                        mode_var__fileAttr_Description  = 'Climate Hazards group InfraRed Precipitation at 0.05x0.05 degree resolution'
                        mode_var__fileAttr_Version      = '1.0'
                    if (self.chirps_mode == 'chirps'):
                        mode_var__precipAttr_comment    = 'Climate Hazards group InfraRed Precipitation with Stations'
                        mode_var__fileAttr_Description  = 'Climate Hazards group InfraRed Precipitation with Stations at 0.05x0.05 degree resolution'
                        mode_var__fileAttr_Version      = '2.0'
                    if (self.chirps_mode == 'chirps_gefs'):
                        mode_var__precipAttr_comment    = 'chirps_gefs'
                        mode_var__fileAttr_Description  = 'chirps_gefs'
                        mode_var__fileAttr_Version      = 'chirps_gefs'


                    #print("C")



                    ############################################################
                    # Start extracting data and creating output netcdf file.
                    ############################################################

                    # !/usr/bin/env python
                    # Program: Convert UCSB CHIRP daily rainfall geoTiff files into netCDF-4 for storage on the ClimateSERV 2.0 thredds data server.
                    # Program: Convert UCSB CHIRPS (with station) daily rainfall geoTiff files into netCDF-4 for storage on the ClimateSERV 2.0 thredds data server.
                    # Program: Convert UCSB CHIRPS-GEFS 10-day rainfall geoTiff files (mean and anomaly) into netCDF-4 for storage on the ClimateSERV 2.0 thredds data server.
                    # Calling: chirp2netcdf.py geotiffFile
                    # Calling: chirps2netcdf.py geotiffFile
                    # Calling: chirpsgefs10day2netcdf.py meanGeotiffFile anomalyGeotiffFile
                    # geotiffFile: The inputfile to be processed.
                    #
                    # General Flow:
                    # Determine the date associated with the geoTiff file.
                    # 1) Use  xarray+rasterio to read the geotiff data from a file into a data array.
                    # 2) Convert to a dataset and add an appropriate time dimension
                    # 3) Clean up the dataset: Rename and add dimensions, attributes, and scaling factors as appropriate.
                    # 4) Dump the precipitation dataset to a netCDF-4 file with a filename conforming to the ClimateSERV 2.0 TDS conventions.

                    # Set region ID
                    regionID = 'Global'

                    # TimeStrSplit_TEST = geotiffFile_FullPath.split('.')
                    # print("C: TimeStrSplit_TEST: " + str(TimeStrSplit_TEST))

                    # Based on the geotiffFile name, determine the time string elements.
                    # Split elements by period
                    TimeStrSplit = ""
                    yearStr = ""
                    monthStr = ""
                    dayStr = ""
                    if(self.chirps_mode == 'chirp'):
                        TimeStrSplit    = geotiffFile_FullPath.split('.') #geotiffFile.split('.')
                        yearStr         = TimeStrSplit[1]
                        monthStr        = TimeStrSplit[2]
                        dayStr          = TimeStrSplit[3]
                    if (self.chirps_mode == 'chirps'):
                        TimeStrSplit    = geotiffFile_FullPath.split('.')  # geotiffFile.split('.')
                        yearStr         = TimeStrSplit[2]
                        monthStr        = TimeStrSplit[3]
                        dayStr          = TimeStrSplit[4]
                    if (self.chirps_mode == 'chirps_gefs'):
                        pass

                    #TimeStrSplit = geotiffFile_FullPath.split('.')
                    #print("C: TimeStrSplit: " + str(TimeStrSplit))

                    # Determine the timestamp for the data.
                    startTime   = pd.Timestamp(yearStr + '-' + monthStr + '-' + dayStr + 'T00:00:00')  # begin of day
                    endTime     = pd.Timestamp(yearStr + '-' + monthStr + '-' + dayStr + 'T23:59:59')  # end of day
                    #startTime   = datetime.datetime.strptime(yearStr + '-' + monthStr + '-' + dayStr + 'T00:00:00')  # begin of day
                    #endTime     = datetime.datetime.strptime(yearStr + '-' + monthStr + '-' + dayStr + 'T23:59:59')  # end of day

                    #print("D")
                    #print("D: (startTime): " + str(startTime))
                    #print("D: (endTime): " + str(endTime))

                    ############################################################
                    # Beging extracting data and creating output netcdf file.
                    ############################################################

                    # 1) Read the geotiff data into an xarray data array
                    tiffData = xr.open_rasterio(geotiffFile_FullPath) # tiffData = xr.open_rasterio(geotiffFile)

                    #print("D1")

                    # 2) Convert to a dataset.  (need to assign a name to the data array)
                    chirps_data = tiffData.rename('precipitation_amount').to_dataset()

                    #print("D2")

                    # Handle selecting/adding the dimesions
                    chirps_data = chirps_data.isel(band=0).reset_coords('band', drop=True)  # select the singleton band dimension and drop out the associated coordinate.

                    #print("D3")
                    # Add the time dimension as a new coordinate.
                    chirps_data = chirps_data.assign_coords(time=startTime).expand_dims(dim='time', axis=0)

                    #print("D4")

                    # Add an additional variable "time_bnds" for the time boundaries.
                    chirps_data['time_bnds'] = xr.DataArray(np.array([startTime, endTime]).reshape((1, 2)), dims=['time', 'nbnds'])

                    #print("D5")

                    # 3) Rename and add attributes to this dataset.
                    #chirps.rename({'y': 'latitude', 'x': 'longitude'}, inplace=True)  # rename lat/lon
                    chirps_data = chirps_data.rename({'y': 'latitude', 'x': 'longitude'}) # rename lat/lon

                    #print("D6")

                    # Lat/Lon/Time dictionaries.
                    # Use Ordered dict
                    latAttr = OrderedDict([('long_name', 'latitude'), ('units', 'degrees_north'), ('axis', 'Y')])
                    lonAttr = OrderedDict([('long_name', 'longitude'), ('units', 'degrees_east'), ('axis', 'X')])
                    timeAttr = OrderedDict([('long_name', 'time'), ('axis', 'T'), ('bounds', 'time_bnds')])
                    timeBoundsAttr = OrderedDict([('long_name', 'time_bounds')])
                    precipAttr = OrderedDict([('long_name', 'precipitation_amount'), ('units', 'mm'), ('accumulation_interval', '1 day'), ('comment', str(mode_var__precipAttr_comment) )])   # 'Climate Hazards group InfraRed Precipitation with Stations'

                    # 'Climate Hazards group InfraRed Precipitation at 0.05x0.05 degree resolution'
                    # '1.0'  '2.0'
                    fileAttr = OrderedDict([('Description', str(mode_var__fileAttr_Description) ), \
                                            ('DateCreated', pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%SZ')), \
                                            ('Contact', 'Lance Gilliland, lance.gilliland@nasa.gov'), \
                                            ('Source', 'University of California at Santa Barbara; Climate Hazards Group; Pete Peterson, pete@geog.ucsb.edu; ftp://chg-ftpout.geog.ucsb.edu/pub/org/chg/products/CHIRP/daily/'), \
                                            ('Version', str(mode_var__fileAttr_Version)), \
                                            ('Reference', 'Funk, C.C., Peterson, P.J., Landsfeld, M.F., Pedreros, D.H., Verdin, J.P., Rowland, J.D., Romero, B.E., Husak, G.J., Michaelsen, J.C., and Verdin, A.P., 2014, A quasi-global precipitation time series for drought monitoring: U.S. Geological Survey Data Series 832, 4 p., http://dx.doi.org/110.3133/ds832.'), \
                                            ('RangeStartTime', startTime.strftime('%Y-%m-%dT%H:%M:%SZ')), \
                                            ('RangeEndTime', endTime.strftime('%Y-%m-%dT%H:%M:%SZ')), \
                                            ('SouthernmostLatitude', np.min(chirps_data.latitude.values)), \
                                            ('NorthernmostLatitude', np.max(chirps_data.latitude.values)), \
                                            ('WesternmostLongitude', np.min(chirps_data.longitude.values)), \
                                            ('EasternmostLongitude', np.max(chirps_data.longitude.values)), \
                                            ('TemporalResolution', 'daily'), \
                                            ('SpatialResolution', '0.05deg')])

                    #print("E")

                    # missing_data/_FillValue , relative time units etc. are handled as part of the encoding dictionary used in to_netcdf() call.
                    precipEncoding = {'_FillValue': np.float32(-9999.0), 'missing_value': np.float32(-9999.0), 'dtype': np.dtype('float32')}
                    timeEncoding = {'units': 'seconds since 1970-01-01T00:00:00Z'}
                    timeBoundsEncoding = {'units': 'seconds since 1970-01-01T00:00:00Z'}
                    # Set the Attributes
                    chirps_data.latitude.attrs              = latAttr
                    chirps_data.longitude.attrs             = lonAttr
                    chirps_data.time.attrs                  = timeAttr
                    chirps_data.time_bnds.attrs             = timeBoundsAttr
                    chirps_data.precipitation_amount.attrs  = precipAttr
                    chirps_data.attrs                       = fileAttr
                    # Set the Endcodings
                    chirps_data.precipitation_amount.encoding   = precipEncoding
                    chirps_data.time.encoding                   = timeEncoding
                    chirps_data.time_bnds.encoding              = timeBoundsEncoding

                    #print("F")

                    # 5) Output File
                    #outputFile = 'UCSB-CHIRP.' + startTime.strftime('%Y%m%dT%H%M%SZ') + '.' + regionID + '.nc4'
                    #chirps_data.to_netcdf(outputFile, unlimited_dims='time')
                    outputFile_FullPath = os.path.join(local_extract_path, final_nc4_filename)
                    chirps_data.to_netcdf(outputFile_FullPath, unlimited_dims='time')

                    #print("G")

                    #print("chirps: Transform: DONE: COMPLETE THE TRANSFORM STEP - (Most of this is to Port over the 3 external tif to netcdf conversion scripts (the ones that set the props) )")

                    #pass

                except:

                    sysErrorData = str(sys.exc_info())

                    Granule_UUID = expected_granules_object['Granule_UUID']

                    error_message = "chirps.execute__Step__Transform: An Error occurred during the Transform step with ETL_Granule UUID: " + str(Granule_UUID) + ".  System Error Message: " + str(sysErrorData)

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
                    new_json_key_to_append = "execute__Step__Transform"
                    is_update_succeed_2 = self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(granule_uuid=Granule_UUID, new_json_key_to_append=new_json_key_to_append, sub_jsonable_object=error_JSON)

                pass

        except:

            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an uncaught error when processing the Transform step on all of the expected Granules.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "chirps"
            error_JSON['function_name'] = "execute__Step__Transform"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        ret__detail_state_info['class_name'] = "chirps"
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

                    # Create a new Granule Entry - The first function 'log_etl_granule' is the one that actually creates a new ETL Granule Attempt (There is one granule per dataset per pipeline attempt run in the ETL Granule Table)
                    # # Granule Helpers
                    # # # def log_etl_granule(self, granule_name="unknown_etl_granule_file_or_object_name", granule_contextual_information="", granule_pipeline_state=settings.GRANULE_PIPELINE_STATE__ATTEMPTING, additional_json={}):
                    # # # def etl_granule__Update__granule_pipeline_state(self, granule_uuid, new__granule_pipeline_state, is_error):
                    # # # def etl_granule__Update__is_missing_bool_val(self, granule_uuid, new__is_missing__Bool_Val):
                    # # # def etl_granule__Append_JSON_To_Additional_JSON(self, granule_uuid, new_json_key_to_append, sub_jsonable_object):
                    Granule_UUID = expected_granules_object['Granule_UUID']
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
                    error_JSON['class_name'] = "chirps"
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
            error_JSON['class_name'] = "chirps"
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
                additional_json['subclass'] = "chirps"
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

            else:
                #shutil.rmtree
                rmtree(temp_working_dir)

                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                #activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_REMOVED
                activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_REMOVED", default_or_error_return_value="Temp Working Dir Removed")  #
                activity_description = "Temp Working Directory, " + str(self.temp_working_dir).strip() + ", was removed."
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = "chirps"
                additional_json['temp_working_dir'] = str(temp_working_dir).strip()
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)


            #print("execute__Step__Clean_Up: Cleanup is finished.")

        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an uncaught error when processing the Clean Up step.  This function is supposed to simply remove the working directory.  This means the working directory was not removed.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "chirps"
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


    def test_class_instance(self):
        print("chirps.test_class_instance: Reached the end.")
