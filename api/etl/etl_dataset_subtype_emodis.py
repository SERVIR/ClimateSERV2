import datetime, os, re, shutil, sys, zipfile
from urllib import request as urllib_request
import xarray as xr
import pandas as pd
import numpy as np
from collections import OrderedDict

from .common import common
from .etl_dataset_subtype_interface import ETL_Dataset_Subtype_Interface

from api.services import Config_SettingService

class ETL_Dataset_Subtype_EMODIS(ETL_Dataset_Subtype_Interface):

    # init (Passing a reference from the calling class, so we can callback the error handler)
    def __init__(self, etl_parent_pipeline_instance):
        self.etl_parent_pipeline_instance = etl_parent_pipeline_instance
        self.class_name = self.__class__.__name__
        self._expected_remote_full_file_paths = []
        self._expected_granules = []

    # Set default parameters or using default
    def set_optional_parameters(self, params):
        today = datetime.date.today()
        self.YYYY__Year__Start = params.get('YYYY__Year__Start') or today.year
        self.YYYY__Year__End = params.get('YYYY__Year__End') or today.year
        self.MM__Month__Start = params.get('MM__Month__Start') or 1
        self.MM__Month__End = params.get('MM__Month__End') or today.month
        self.XX__Region_Code = params.get('XX__Region_Code') or 'ea'

    # Get the local filesystem place to store data
    def get_root_local_temp_working_dir(self, region_code):
        ret_dir = self.etl_parent_pipeline_instance.dataset.temp_working_dir
        if region_code == 'ea':
            ret_dir = os.path.join(ret_dir, 'eastafrica')
        elif region_code == 'wa':
            ret_dir = os.path.join(ret_dir, 'westafrica')
        elif region_code == 'sa':
            ret_dir = os.path.join(ret_dir, 'southernafrica')
        elif region_code == 'cta':
            ret_dir = os.path.join(ret_dir, 'centralasia')
        return ret_dir

    # Get the local filesystem place to store the final NC4 files (The THREDDS monitored Directory location)
    def get_final_load_dir(self, region_code):
        ret_dir = self.etl_parent_pipeline_instance.dataset.final_load_dir
        if region_code == 'ea':
            ret_dir = os.path.join(ret_dir, 'eastafrica')
        elif region_code == 'wa':
            ret_dir = os.path.join(ret_dir, 'westafrica')
        elif region_code == 'sa':
            ret_dir = os.path.join(ret_dir, 'southernafrica')
        elif region_code == 'cta':
            ret_dir = os.path.join(ret_dir, 'centralasia')
        ret_dir = os.path.join(ret_dir, '250m', '10dy')
        return ret_dir

    # Get the Remote Locations for each of the regions
    def get_roothttp_for_regioncode(self, region_code):
        ret_roothttp = self.etl_parent_pipeline_instance.dataset.source_url
        if region_code == 'ea':
            ret_roothttp = os.path.join(ret_roothttp, 'africa/east')
        elif region_code == 'wa':
            ret_roothttp = os.path.join(ret_roothttp, 'africa/west')
        elif region_code == 'sa':
            ret_roothttp = os.path.join(ret_roothttp, 'africa/southern')
        elif region_code == 'cta':
            ret_roothttp = os.path.join(ret_roothttp, 'asia/centralasia')
        ret_roothttp = os.path.join(ret_roothttp, 'dekadal/emodis/ndvi_c6/temporallysmoothedndvi/downloads/dekadal/')
        return ret_roothttp

    # Whatever Month we are in, multiple by 3  and then subtract 2, (Jan would be 1 (3 - 2), Dec would be 34 (36 - 2) )
    @staticmethod
    def get_Earliest_Dekadal_Number_From_Month_Number(month_Number=1):
        return (month_Number * 3) - 2

    # Whatever Month we are in, multiple by 3 (Jan would be 3, Dec would be 36)
    @staticmethod
    def get_Latest_Dekadal_Number_From_Month_Number(month_Number=1):
        return month_Number * 3

    # Months between two dates
    @staticmethod
    def diff_month(latest_datetime, earliest_datetime):
        return (latest_datetime.year - earliest_datetime.year) * 12 + latest_datetime.month - earliest_datetime.month

    # Calculate the Month Number from a Dekadal input
    @staticmethod
    def get_Month_Number_From_Dekadal(dekadal_string='01'):
        monthNumber = 0
        try:
            dekadal_number = int(dekadal_string)
            monthNumber = int(((dekadal_number - 1) / 3) + 1)  # int rounds down
        except:
            pass
        return monthNumber

    @staticmethod
    def get_Begin_Day_Number_From_Dekadal(dekadal_string='01'):
        begin_DayNumber = 1
        try:
            dekadal_number = int(dekadal_string)
            dekadal_number_CyclePart = dekadal_number % 3
            if dekadal_number_CyclePart == 1:
                begin_DayNumber = 1
            if dekadal_number_CyclePart == 2:
                begin_DayNumber = 11
            if dekadal_number_CyclePart == 0:
                begin_DayNumber = 21
        except:
            pass
        return begin_DayNumber

    @staticmethod
    def get_Final_NC4_FileName_From_Inputs(region_code, year_YYYY, dekadal_N):
        monthNumber = ETL_Dataset_Subtype_EMODIS.get_Month_Number_From_Dekadal(dekadal_string=str(dekadal_N))
        begin_DayNumber = ETL_Dataset_Subtype_EMODIS.get_Begin_Day_Number_From_Dekadal(dekadal_string=str(dekadal_N))
        nc4_region_name_part = 'regionnamepart'
        region_code = str(region_code).strip()
        if region_code == 'ea':
            nc4_region_name_part = "eastafrica"
        if region_code == 'wa':
            nc4_region_name_part = "westafrica"
        if region_code == 'sa':
            nc4_region_name_part = "southafrica"
        if region_code == 'cta':
            nc4_region_name_part = "centralasia"
        # emodis-ndvi.20020701T000000Z.eastafrica.250m.10dy.nc4
        final_nc4_filename = 'emodis-ndvi.{}{}{}T000000Z.{}.250m.10dy.nc4'.format(
            '{:0>4d}'.format(year_YYYY),
            '{:02d}'.format(monthNumber),
            '{:02d}'.format(begin_DayNumber),
            nc4_region_name_part
        )
        return final_nc4_filename

    # Get file paths infos (Remote file paths, local file paths, final location for load files, etc.
    @staticmethod
    def get_expected_filepath_infos(root_path, region_code, year_YYYY, dekadal_N, root_file_download_path, final_load_dir_path):
        retObj = {}
        retObj['is_error'] = False
        try:
            filenum = "{:0>2d}{:0>2d}".format(year_YYYY - 2000, dekadal_N)
            filename = '{}{}.zip'.format(region_code, filenum)
            remote_full_filepath    = os.path.join(root_path, filename)
            local_full_filepath     = os.path.join(root_file_download_path, filename)
            local_extract_path      = root_file_download_path
            local_final_load_path   = final_load_dir_path

            retObj['filename']              = str(filename).strip()
            retObj['remote_full_filepath']  = str(remote_full_filepath).strip()
            retObj['local_full_filepath']   = str(local_full_filepath).strip()
            retObj['local_extract_path']    = str(local_extract_path).strip()
            retObj['local_final_load_path'] = str(local_final_load_path).strip()

            retObj['region_code']           = region_code
            retObj['current_year']          = year_YYYY
            retObj['current_dekadal']       = dekadal_N
        except:
            sysErrorData = str(sys.exc_info())
            retObj['error']     = "Error getting an expected remote filepath.  System Error Message: " + str(sysErrorData)
            retObj['is_error']  = True
            retObj['class_name']    = "emodis"
            retObj['function_name'] = "get_expected_filepath_infos"
            #
            params = {}
            params['root_path']     = str(root_path).strip()
            params['region_code']   = str(region_code).strip()
            params['year_YYYY']     = str(year_YYYY).strip()
            params['dekadal_N']     = str(dekadal_N).strip()
            retObj['params'] = params

        return retObj

    # Get Expected File Downloads and Expected Granules
    def execute__Step__Pre_ETL_Custom(self):
        ret__function_name = "execute__Step__Pre_ETL_Custom"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        # Get the root http path based on the region.

        root_file_download_path = self.get_root_local_temp_working_dir(self.XX__Region_Code)
        self.temp_working_dir = root_file_download_path
        final_load_dir_path = self.get_final_load_dir(self.XX__Region_Code)
        current_root_http_path = self.get_roothttp_for_regioncode(self.XX__Region_Code)

        # (1) Generate Expected remote file paths
        try:

            # # Calculate Month Deltas
            # earliest_date = datetime.datetime(year=2020, month=0, day=1)
            # latest_date = datetime.datetime(year=2020, month=0, day=1)
            #
            # # Months between two dates
            # num_of_months = (self.diff_month(latest_datetime=latest_date, earliest_datetime=earliest_date) + 1)  # Always add 1 to this (to include the ending month)
            #

            # Iterate on the Year Ranges
            # The second parameter of 'range' is non-inclusive, so if the actual last year we want to process is 2020, then the range value needs to be 2021, and the last iteration through the loop will be 2020
            for YYYY__Year in range(self.YYYY__Year__Start, (self.YYYY__Year__End + 1)):
                start_month__Current_Year = 1
                end_month__Current_Year = 12
                if YYYY__Year == self.YYYY__Year__Start:
                    start_month__Current_Year = self.MM__Month__Start
                if YYYY__Year == self.YYYY__Year__End:
                    end_month__Current_Year = self.MM__Month__End

                # Calculate the start and end dekadals for the current year.
                start_dekadal = self.get_Earliest_Dekadal_Number_From_Month_Number(month_Number=start_month__Current_Year)
                end_dekadal = self.get_Latest_Dekadal_Number_From_Month_Number(month_Number=end_month__Current_Year)

                # Validate Dekadals
                if start_dekadal < 1:
                    start_dekadal = 1
                if end_dekadal > 36:
                    end_dekadal = 36

                # Iterate on each month (then do all 3 of the dekadal numbers
                # Iterate on Dekadal Ranges
                for NN__Dekadal in range(start_dekadal, end_dekadal):
                    # Get the expected remote file to download
                    result__ExpectedRemoteFilePath_Object = self.get_expected_filepath_infos(root_path=current_root_http_path, region_code=self.XX__Region_Code, year_YYYY=YYYY__Year, dekadal_N=NN__Dekadal, root_file_download_path=root_file_download_path, final_load_dir_path=final_load_dir_path)
                    is_error = result__ExpectedRemoteFilePath_Object['is_error']
                    if is_error == True:
                        activity_description    = "Error: There was an error when generating a specific expected remote file path.  See the additional data for details on which expected file caused the error."
                        error_JSON              = result__ExpectedRemoteFilePath_Object
                        # Call Error handler right here (If this is commented out, then the info should be bubbling up to the calling function))
                        #activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR
                        #self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=error_JSON)
                        #
                        # Exit Here With Error info loaded up
                        ret__is_error           = True
                        ret__error_description  = activity_description
                        ret__detail_state_info  = error_JSON
                        retObj                  = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
                        return retObj
                    else:
                        self._expected_remote_full_file_paths.append(result__ExpectedRemoteFilePath_Object)
        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error']         = "Error: There was an error when generating the expected remote filepaths.  See the additional data for details on which expected file caused the error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error']      = True
            error_JSON['class_name']    = self.__class__.__name__
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Call Error handler right here (If this is commented out, then the info should be bubbling up to the calling function))
            #activity_event_type         = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR
            #self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=error_JSON)
            #
            # Exit Here With Error info loaded up
            ret__is_error           = True
            ret__error_description  = error_JSON['error']
            ret__detail_state_info  = error_JSON
            retObj                  = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        # (2) Generate Expected Granules
        # Granules from emodis are fairly straight forward.  The zip files contain 2 files with the same name.
        # # A wa 2020 for Dekad 12 would be these files
        # # # Zip file: wa2012.zip
        # # # # The zip file would contain
        # # # # # Tif file:   wa2012.tif
        # # # # # World file: wa2012.tfw
        try:

            for expected_file_path_object in self._expected_remote_full_file_paths:
                try:
                    # # expected_file_path_object Has these properties, 'filename', 'remote_full_filepath', 'region_code', 'current_year', 'current_dekadal'
                    base_file_name      = expected_file_path_object['filename'].split(".")[0]  # Getting wa2012 from wa2012.zip
                    region_code         = expected_file_path_object['region_code']
                    current_year        = expected_file_path_object['current_year']
                    current_dekadal     = expected_file_path_object['current_dekadal']

                    final_nc4_filename = ETL_Dataset_Subtype_EMODIS.get_Final_NC4_FileName_From_Inputs(region_code=region_code, year_YYYY=current_year, dekadal_N=current_dekadal)

                    expected_file_path_object['tif_filename']       = base_file_name + ".tif"
                    expected_file_path_object['tfw_filename']       = base_file_name + ".tfw"  # NOT .twf - these kinds of bugs are TONS of fun!!
                    expected_file_path_object['final_nc4_filename'] = final_nc4_filename  # NETCDF4 // Expected THREDDS Output File

                    # Create a new Granule Entry - The first function 'log_etl_granule' is the one that actually creates a new ETL Granule Attempt (There is one granule per dataset per pipeline attempt run in the ETL Granule Table)
                    granule_name = final_nc4_filename
                    granule_contextual_information = ""
                    granule_pipeline_state = Config_SettingService.get_value(setting_name="GRANULE_PIPELINE_STATE__ATTEMPTING", default_or_error_return_value="Attempting") #settings.GRANULE_PIPELINE_STATE__ATTEMPTING
                    additional_json = expected_file_path_object
                    new_Granule_UUID = self.etl_parent_pipeline_instance.log_etl_granule(granule_name=granule_name, granule_contextual_information=granule_contextual_information, granule_pipeline_state=granule_pipeline_state, additional_json=additional_json)

                    # Save the Granule's UUID for reference in later steps
                    expected_file_path_object['Granule_UUID'] = str(new_Granule_UUID).strip()

                    # Add to the granules list
                    self._expected_granules.append(expected_file_path_object)

                except Exception as e:
                    print(e)
                    sysErrorData = str(sys.exc_info())
                    error_JSON = {}
                    error_JSON['error'] = "Error: There was an error when generating a specific expected Granule.  See the additional data [expected_file_path_object] for details on which expected granule caused the error.  System Error Message: " + str(sysErrorData)
                    error_JSON['is_error'] = True
                    error_JSON['class_name'] = self.__class__.__name__
                    error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
                    error_JSON['expected_file_path_object'] = expected_file_path_object
                    # Exit Here With Error info loaded up
                    ret__is_error = True
                    ret__error_description = error_JSON['error']
                    ret__detail_state_info = error_JSON
                    retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
                    return retObj
        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an uncaught error when generating the expected Granules.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.__class__.__name__
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        # Make sure the directories exist
        rootWorking_Dir = self.get_root_local_temp_working_dir(region_code=self.XX__Region_Code)
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(rootWorking_Dir)
        if is_error_creating_directory == True:
            error_JSON = {}
            error_JSON['error'] = "Error: There was an error when the pipeline tried to create a new directory on the filesystem.  The path that the pipeline tried to create was: " + str(rootWorking_Dir) + ".  There should be another error logged just before this one that contains system error info.  That info should give clues to why the directory was not able to be created."
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.__class__.__name__
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        # final_load_dir_path
        final_load_dir_path  = self.get_root_local_temp_working_dir(region_code=self.XX__Region_Code)
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(final_load_dir_path)
        if is_error_creating_directory == True:
            error_JSON = {}
            error_JSON['error'] = "Error: There was an error when the pipeline tried to create a new directory on the filesystem.  The path that the pipeline tried to create was: " + str(final_load_dir_path) + ".  There should be another error logged just before this one that contains system error info.  That info should give clues to why the directory was not able to be created."
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.__class__.__name__
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        # Ended, now for reporting
        ret__detail_state_info['class_name'] = self.__class__.__name__
        ret__detail_state_info['number_of_expected_remote_full_file_paths'] = str(len(self._expected_remote_full_file_paths)).strip()
        ret__detail_state_info['number_of_expected_granules'] = str(len(self._expected_granules)).strip()
        ret__event_description = "Success.  Completed Step execute__Step__Pre_ETL_Custom by generating "+str(len(self._expected_remote_full_file_paths)).strip() + " expected full file paths to download and " + str(len(self._expected_granules)).strip() + " expected granules to process."

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Download(self):
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        download_counter = 0
        loop_counter = 0
        error_counter = 0
        detail_errors = []

        # Setting up for the periodic reporting on the terminal
        num_of_objects_to_process = len(self._expected_remote_full_file_paths)
        num_of_download_activity_events = 4
        modulus_size = int(num_of_objects_to_process / num_of_download_activity_events)
        if modulus_size < 1:
            modulus_size = 1

        # Process each expected granule
        for expected_remote_file_path_object in self._expected_remote_full_file_paths:
            try:
                if (loop_counter + 1) % modulus_size == 0:
                    event_message = "About to download file: " + str(loop_counter + 1) + " out of " + str(num_of_objects_to_process)
                    print(event_message)
                    activity_event_type = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS", default_or_error_return_value="ETL Download Progress") #settings.ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS
                    activity_description = event_message
                    additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                    self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

                # Current Granule to download
                current_url_to_download                             = expected_remote_file_path_object['remote_full_filepath']
                current_download_destination_local_full_file_path   = expected_remote_file_path_object['local_full_filepath']

                # Actually do the download now
                try:
                    urllib_request.urlretrieve(current_url_to_download, current_download_destination_local_full_file_path) # urllib_request.urlretrieve(url, endfilename)
                    #print(" - (GRANULE LOGGING) Log Each Download into the Granule Storage Area: (current_download_destination_local_full_file_path): " + str(current_download_destination_local_full_file_path))

                    download_counter = download_counter + 1
                except:
                    error_counter = error_counter + 1
                    sysErrorData = str(sys.exc_info())
                    #print("DEBUG Warn: (WARN LEVEL) (File can not be downloaded).  System Error Message: " + str(sysErrorData))
                    warn_JSON = {}
                    warn_JSON['warning']                = "Warning: There was an uncaught error when attempting to download file at URL: " +str(current_url_to_download)+ ".  If the System Error message says something like 'nodename nor servname provided, or not known', then one common cause of that error is an unstable or disconnected internet connection.  Double check that the internet connection is working and try again.  System Error Message: " + str(sysErrorData)
                    warn_JSON['is_error']               = True
                    warn_JSON['class_name']             = "emodis"
                    warn_JSON['function_name']          = "execute__Step__Download"
                    warn_JSON['current_object_info']    = expected_remote_file_path_object
                    # Call Error handler right here to send a warning message to ETL log. - Note this warning will not make it back up to the overall pipeline, it is being sent here so admin can still be aware of it and handle it.
                    #activity_event_type         = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING
                    activity_event_type         = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING", default_or_error_return_value="ETL Warning")
                    activity_description        = warn_JSON['warning']
                    self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=warn_JSON)

            except:
                error_counter = error_counter + 1
                sysErrorData = str(sys.exc_info())
                error_message = "emodis.execute__Step__Download: Generic Uncaught Error.  At least 1 download failed.  System Error Message: " + str(sysErrorData)
                detail_errors.append(error_message)
                #print("emodis.execute__Step__Download: Generic Uncaught Error: " + str(sysErrorData))
                print(error_message)

                # Maybe in here is an error with sending the warning in an earlier step?
            loop_counter = loop_counter + 1

        # Ended, now for reporting
        #
        ret__detail_state_info['class_name'] = self.__class__.__name__
        ret__detail_state_info['download_counter'] = download_counter
        ret__detail_state_info['error_counter'] = error_counter
        ret__detail_state_info['loop_counter'] = loop_counter
        ret__detail_state_info['detail_errors'] = detail_errors
        #ret__detail_state_info['number_of_expected_remote_full_file_paths'] = str(len(self._expected_remote_full_file_paths)).strip()
        #ret__detail_state_info['number_of_expected_granules'] = str(len(self._expected_granules)).strip()
        ret__event_description = "Success.  Completed Step execute__Step__Download by downloading " + str(download_counter).strip() + " files."
        #
        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Extract(self):
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        error_counter = 0
        detail_errors = []

        try:

            for expected_granules_object in self._expected_granules:

                local_full_filepath = expected_granules_object['local_full_filepath']
                local_extract_path  = expected_granules_object['local_extract_path']
                tfw_filename        = expected_granules_object['tfw_filename']
                expected_full_path_to_local_extracted_tfw_file = os.path.join(local_extract_path, tfw_filename)

                print(local_full_filepath)

                # Unzip the current zip file # Example: path_to_working_dir/ea2001.zip
                try:
                    list_of_filenames_inside_zipfile = []
                    with zipfile.ZipFile(local_full_filepath, "r") as z:
                        list_of_filenames_inside_zipfile = z.namelist()
                        z.extractall(local_extract_path)

                except Exception as e:
                    print(e)
                    # This granule errored on the Extract step.
                    sysErrorData = str(sys.exc_info())

                    Granule_UUID = expected_granules_object['Granule_UUID']

                    error_message = "emodis.execute__Step__Extract: An Error occurred during the Extract step with ETL_Granule UUID: " + str(Granule_UUID) + ".  System Error Message: " + str(sysErrorData)

                    # print("DEBUG: PRINT ERROR HERE: (error_message) " + str(error_message))

                    # Individual Transform Granule Error
                    error_counter = error_counter + 1
                    detail_errors.append(error_message)

                    error_JSON = {}
                    error_JSON['error_message'] = error_message

                    # Update this Granule for Failure (store the error info in the granule also)
                    new__granule_pipeline_state = Config_SettingService.get_value(setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")  #
                    is_error = True
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)
                    new_json_key_to_append = "execute__Step__Extract"
                    is_update_succeed_2 = self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(granule_uuid=Granule_UUID, new_json_key_to_append=new_json_key_to_append, sub_jsonable_object=error_JSON)

                # Remove the tfw file # Last version of ClimateSERV had a bit here where the TFW file is removed after extracting - So porting that over as well.  If I remember correctly, having a TFW file present affects the reading of the TIF file in a later step.
                # local_extract_path
                try:
                    #print("execute__Step__Extract: About to try and remove file at: " + str(expected_full_path_to_local_extracted_tfw_file))
                    os.remove(expected_full_path_to_local_extracted_tfw_file)
                    #print("execute__Step__Extract: PLACEHOLDER: REMOVED FILE: " + str(expected_full_path_to_local_extracted_tfw_file))
                except:
                    sysErrorData = str(sys.exc_info())
                    errorMessage = "emodis.execute__Step__Extract: Could not remove TWF file before the Transform step.  File location was: " + str(expected_full_path_to_local_extracted_tfw_file) + "  System Error message: " + str(sysErrorData)
                    detail_errors.append(errorMessage)
                    error_counter = error_counter + 1
                    #ret__is_error = True
                    ret__error_description = "Could not remove TWF file after performing extract step at least " + str(error_counter) + " times.  Check for key 'detail_errors' for a list of each occurrence.  Here is the Error message from the most recent error.  " + str(errorMessage)
                    ret__event_description = "WARNING: The TWF file could not be removed.  See the error description for more details."

        except:
            sysErrorData = str(sys.exc_info())
            ret__is_error = True
            ret__error_description = "emodis.execute__Step__Extract: There was a generic, uncaught error when attempting to Extract the Granules.  System Error Message: " + str(sysErrorData)

        ret__detail_state_info['class_name'] = self.__class__.__name__
        ret__detail_state_info['error_counter'] = error_counter
        ret__detail_state_info['detail_errors'] = detail_errors

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Transform(self):
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        error_counter = 0
        detail_errors = []

        try:

            for expected_granules_object in self._expected_granules:
                try:

                    # print("A")

                    # Getting info ready for the current granule.
                    local_extract_path = expected_granules_object['local_extract_path']
                    tif_filename = expected_granules_object['tif_filename']
                    tif_filename = tif_filename.split('.')[0] + 'm.' + tif_filename.split('.')[1]
                    final_nc4_filename = expected_granules_object['final_nc4_filename']
                    expected_full_path_to_local_extracted_tif_file = os.path.join(local_extract_path, tif_filename)

                    geotiffFile_FullPath = expected_full_path_to_local_extracted_tif_file

                    print(final_nc4_filename)

                    # print("B")

                    # Based on the geotiffFile name, determine the region and time string elements.
                    # The file naming conventions are as:  ea2001.tif , wa2001.tif , sa2001.tif , and cta.tif
                    # Split elements by period , then split the name by letters/numbers
                    #regionTimeStr = geotiffFile.split('.')[0]
                    regionTimeStr       = tif_filename.split('.')[0]
                    regionTimeStrSplit  = re.split('(\d+)', regionTimeStr)
                    timeStr             = regionTimeStrSplit[1]

                    # Based on the year, create an index of the dates associated with the dekads 1-36
                    yearStr = '20' + timeStr[0:2]
                    dekadNum = int(timeStr[2:])
                    dekadTimes = [['NaT', 'NaT']]  # init nested list.
                    for imonth in np.arange(1, 13):
                        monthStr = '{:02d}'.format(imonth)
                        startDays = ['01', '11', '21']
                        endDays = ['10', '20', '{:02d}'.format(pd.Period(yearStr + '-' + monthStr + '-01').daysinmonth)]
                        for idekad in np.arange(0, 3):
                            startTime = pd.Timestamp(yearStr + '-' + monthStr + '-' + startDays[idekad] + 'T00:00:00')
                            endTime = pd.Timestamp(yearStr + '-' + monthStr + '-' + endDays[idekad] + 'T23:59:59')
                            dekadTimes.append([startTime, endTime])

                    # Given the dekadNum, set the startTime and endTime
                    startTime = dekadTimes[dekadNum][0]
                    endTime = dekadTimes[dekadNum][1]

                    # print("C")
                    # print("D")

                    ############################################################
                    # Start extracting data and creating output netcdf file.
                    ############################################################

                    # 1) Read the geotiff data into an xarray data array
                    da = xr.open_rasterio(geotiffFile_FullPath)
                    # 2) Convert to NDVI. (data array)
                    da = (da - 100.0) / 100.0
                    # 3) Convert to a dataset.  (need to assign a name to the ndvi data array)
                    ds = da.rename('ndvi').to_dataset()
                    # Handle selecting/adding the dimesions
                    ds = ds.isel(band=0).reset_coords('band', drop=True)  # select the singleton band dimension and drop out the associated coordinate.
                    # Add the time dimension as a new coordinate.
                    ds = ds.assign_coords(time=startTime).expand_dims(dim='time', axis=0)
                    # Add an additional variable "time_bnds" for the time boundaries.
                    ds['time_bnds'] = xr.DataArray(np.array([startTime, endTime]).reshape((1, 2)), dims=['time', 'nbnds'])
                    # 4) Rename and add attributes to this dataset.
                    ds = ds.rename({'y': 'latitude', 'x': 'longitude'})
                    # 4) Reorder latitude dimension into ascending order
                    if ds.latitude.values[1] - ds.latitude.values[0] < 0:
                        ds = ds.reindex(latitude=ds.latitude[::-1])

                    # print("E")

                    # Set the Attributes
                    ds.latitude.attrs = OrderedDict([('long_name', 'latitude'), ('units', 'degrees_north'), ('axis', 'Y')])
                    ds.longitude.attrs = OrderedDict([('long_name', 'longitude'), ('units', 'degrees_east'), ('axis', 'X')])
                    ds.time.attrs = OrderedDict([('long_name', 'time'), ('axis', 'T'), ('bounds', 'time_bnds')])
                    ds.time_bnds.attrs = OrderedDict([('long_name', 'time_bounds')])
                    ds.ndvi.attrs = OrderedDict([
                        ('long_name', 'ndvi'),
                        ('units', 'unitless'),
                        ('comment', 'Maximum value composite over dekad defined by time_bnds')
                    ])
                    ds.attrs = OrderedDict([
                        ('Description', 'EMODIS NDVI C6 at 250m resolution'),
                        ('DateCreated', pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%SZ')),
                        ('Contact', 'Lance Gilliland, lance.gilliland@nasa.gov'),
                        ('Source', 'EMODIS NDVI C6, https://earlywarning.usgs.gov/fews/datadownloads/East%20Africa/eMODIS%20NDVI%20C6'),
                        ('Version', 'C6'), ('RangeStartTime', startTime.strftime('%Y-%m-%dT%H:%M:%SZ')),
                        ('RangeEndTime', endTime.strftime('%Y-%m-%dT%H:%M:%SZ')),
                        ('SouthernmostLatitude', np.min(ds.latitude.values)),
                        ('NorthernmostLatitude', np.max(ds.latitude.values)),
                        ('WesternmostLongitude', np.min(ds.longitude.values)),
                        ('EasternmostLongitude', np.max(ds.longitude.values)),
                        ('TemporalResolution', 'dekad'),
                        ('SpatialResolution', '250m')
                    ])
                    # Set the Endcodings
                    ds.ndvi.encoding = {
                        '_FillValue': np.int8(127),
                        'missing_value': np.int8(127),
                        'dtype': np.dtype('int8'),
                        'scale_factor': 0.01,
                        'add_offset': 0.0,
                        'chunksizes': (1, 256, 256)
                    }
                    ds.time.encoding = {'units': 'seconds since 1970-01-01T00:00:00Z', 'dtype': np.dtype('int32')}
                    ds.time_bnds.encoding = {'units': 'seconds since 1970-01-01T00:00:00Z', 'dtype': np.dtype('int32')}

                    outputFile_FullPath     = os.path.join(local_extract_path, final_nc4_filename)
                    ds.to_netcdf(outputFile_FullPath, unlimited_dims='time')

                    print(outputFile_FullPath)

                except Exception as e:
                    print(e)
                    sysErrorData = str(sys.exc_info())

                    Granule_UUID = expected_granules_object['Granule_UUID']

                    error_message = "emodis.execute__Step__Transform: An Error occurred during the Transform step with ETL_Granule UUID: " + str(Granule_UUID) + ".  System Error Message: " + str(sysErrorData)

                    # Individual Transform Granule Error
                    error_counter = error_counter + 1
                    detail_errors.append(error_message)

                    error_JSON = {}
                    error_JSON['error_message'] = error_message

                    # Update this Granule for Failure (store the error info in the granule also)
                    new__granule_pipeline_state = Config_SettingService.get_value(setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED") #
                    is_error = True
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)
                    new_json_key_to_append = "execute__Step__Transform"
                    is_update_succeed_2 = self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(granule_uuid=Granule_UUID, new_json_key_to_append=new_json_key_to_append, sub_jsonable_object=error_JSON)

        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an uncaught error when processing the Transform step on all of the expected Granules.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.__class__.__name__
            error_JSON['function_name'] = "execute__Step__Transform"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        ret__detail_state_info['class_name'] = self.__class__.__name__
        ret__detail_state_info['error_counter'] = error_counter
        ret__detail_state_info['detail_errors'] = detail_errors

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Load(self):
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        try:

            for expected_granules_object in self._expected_granules:

                expected_full_path_to_local_working_nc4_file = "UNSET"
                expected_full_path_to_local_final_nc4_file = "UNSET"

                try:
                    local_extract_path = expected_granules_object['local_extract_path']
                    local_final_load_path = expected_granules_object['local_final_load_path']
                    final_nc4_filename = expected_granules_object['final_nc4_filename']
                    expected_full_path_to_local_working_nc4_file = os.path.join(local_extract_path, final_nc4_filename)  # Where the NC4 file was generated during the Transform Step
                    expected_full_path_to_local_final_nc4_file = os.path.join(local_final_load_path, final_nc4_filename)  # Where the final NC4 file should be placed for THREDDS Server monitoring

                    # Copy the file from the working directory over to the final location for it.  (Where THREDDS Monitors for it)
                    shutil.copyfile(expected_full_path_to_local_working_nc4_file, expected_full_path_to_local_final_nc4_file)

                    # Create a new Granule Entry - The first function 'log_etl_granule' is the one that actually creates a new ETL Granule Attempt (There is one granule per dataset per pipeline attempt run in the ETL Granule Table)
                    Granule_UUID                = expected_granules_object['Granule_UUID']

                    new__granule_pipeline_state = Config_SettingService.get_value(setting_name="GRANULE_PIPELINE_STATE__SUCCESS", default_or_error_return_value="SUCCESS")
                    is_error                    = False
                    is_update_succeed           = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)

                    # # TODO - Possible Parameter updates needed here.  (As we learn more about what the specific client side needs are)
                    additional_json                 = {}
                    additional_json['MostRecent__ETL_Granule_UUID'] = str(Granule_UUID).strip()

                except:
                    sysErrorData = str(sys.exc_info())
                    error_JSON = {}
                    error_JSON['error'] = "Error: There was an error when attempting to copy the current nc4 file to it's final directory location.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
                    error_JSON['is_error'] = True
                    error_JSON['class_name'] = self.__class__.__name__
                    error_JSON['function_name'] = "execute__Step__Load"

                    # Additional infos
                    error_JSON['expected_full_path_to_local_working_nc4_file']  = str(expected_full_path_to_local_working_nc4_file).strip()
                    error_JSON['expected_full_path_to_local_final_nc4_file']    = str(expected_full_path_to_local_final_nc4_file).strip()

                    # Update this Granule for Failure (store the error info in the granule also)
                    Granule_UUID = expected_granules_object['Granule_UUID']
                    new__granule_pipeline_state = Config_SettingService.get_value(setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")
                    is_error = True
                    is_update_succeed   = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)
                    new_json_key_to_append = "execute__Step__Load"
                    is_update_succeed_2 = self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(granule_uuid=Granule_UUID, new_json_key_to_append=new_json_key_to_append, sub_jsonable_object=error_JSON)

        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an uncaught error when processing the Load step on all of the expected Granules.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.__class__.__name__
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

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Clean_Up(self):
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        try:
            temp_working_dir = str(self.temp_working_dir).strip()
            if temp_working_dir == '':
                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                activity_event_type = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_BLANK", default_or_error_return_value="Temp Working Dir Blank")
                activity_description = "Could not remove the temporary working directory.  The value for self.temp_working_dir was blank. "
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = self.__class__.__name__
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
            else:
                shutil.rmtree(temp_working_dir)
                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                activity_event_type = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_REMOVED", default_or_error_return_value="Temp Working Dir Removed")
                activity_description = "Temp Working Directory, " + str(self.temp_working_dir).strip() + ", was removed."
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = self.__class__.__name__
                additional_json['temp_working_dir'] = str(temp_working_dir).strip()
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an uncaught error when processing the Clean Up step.  This function is supposed to simply remove the working directory.  This means the working directory was not removed.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.__class__.__name__
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
