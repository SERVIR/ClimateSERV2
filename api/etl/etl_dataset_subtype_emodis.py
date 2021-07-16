import os, sys
import zipfile
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

class emodis(ETL_Dataset_Subtype_Interface):
    class_name = "emodis"
    etl_parent_pipeline_instance = None

    # Input Settings
    YYYY__Year__Start = 2019
    YYYY__Year__End = 2020
    MM__Month__Start = 11  # 2 #1
    MM__Month__End = 2  # 4 #6

    # Removing Dekadals (Because Datetime object can't easily convert between these and normal datetimes for custom ranges)
    # NN__Dekadal__Start      = 2 #2 #1
    # NN__Dekadal__End        = 3 #4 #6
    XX__Region_Code = "ea"
    relative_dir_path__WorkingDir = 'working_dir'
    temp_working_dir = ""

    # init (Passing a reference from the calling class, so we can callback the error handler)
    def __init__(self, etl_parent_pipeline_instance):
        self.etl_parent_pipeline_instance = etl_parent_pipeline_instance

    # Validate type or use existing default for each
    def set_emodis_params(self, YYYY__Year__Start, YYYY__Year__End, MM__Month__Start, MM__Month__End, XX__Region_Code):
        try:
            self.YYYY__Year__Start = int(YYYY__Year__Start) if YYYY__Year__Start != 0 else self.YYYY__Year__Start
        except:
            pass
        try:
            self.YYYY__Year__End = int(YYYY__Year__End) if YYYY__Year__End != 0 else self.YYYY__Year__End
        except:
            pass
        try:
            self.MM__Month__Start = int(MM__Month__Start) if MM__Month__Start != 0 else self.MM__Month__Start
        except:
            pass
        try:
            self.MM__Month__End = int(MM__Month__End) if MM__Month__End != 0 else self.MM__Month__End
        except:
            pass
        try:
            self.XX__Region_Code = str(XX__Region_Code).strip() if XX__Region_Code != "" else "ea"
        except:
            pass

# Get the local filesystem place to store data
    @staticmethod
    def get_root_local_temp_working_dir(region_code):
        # Type Specific Settings
        emodis__ea__rootoutputworkingdir = Config_Setting.get_value(setting_name="PATH__TEMP_WORKING_DIR__EMODIS__EA", default_or_error_return_value="") # settings.PATH__TEMP_WORKING_DIR__EMODIS__EA  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/temp_etl_data/emodis/eastafrica/'
        emodis__wa__rootoutputworkingdir = Config_Setting.get_value(setting_name="PATH__TEMP_WORKING_DIR__EMODIS__WA", default_or_error_return_value="") # settings.PATH__TEMP_WORKING_DIR__EMODIS__WA  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/temp_etl_data/emodis/westafrica/'
        emodis__sa__rootoutputworkingdir = Config_Setting.get_value(setting_name="PATH__TEMP_WORKING_DIR__EMODIS__SA", default_or_error_return_value="") # settings.PATH__TEMP_WORKING_DIR__EMODIS__SA  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/temp_etl_data/emodis/southernafrica/'
        emodis__cta__rootoutputworkingdir = Config_Setting.get_value(setting_name="PATH__TEMP_WORKING_DIR__EMODIS__CTA", default_or_error_return_value="") # settings.PATH__TEMP_WORKING_DIR__EMODIS__CTA  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/temp_etl_data/emodis/centralasia/'

        #ret_rootlocal_working_dir = settings.PATH__TEMP_WORKING_DIR__DEFAULT # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/data/image/input/UNKNOWN/'
        ret_rootlocal_working_dir = Config_Setting.get_value(setting_name="PATH__TEMP_WORKING_DIR__DEFAULT", default_or_error_return_value="") # settings.PATH__TEMP_WORKING_DIR__DEFAULT  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/data/image/input/UNKNOWN/'
        region_code = str(region_code).strip()
        if (region_code == 'ea'):
            ret_rootlocal_working_dir = emodis__ea__rootoutputworkingdir
        if (region_code == 'wa'):
            ret_rootlocal_working_dir = emodis__wa__rootoutputworkingdir
        if (region_code == 'sa'):
            ret_rootlocal_working_dir = emodis__sa__rootoutputworkingdir
        if (region_code == 'cta'):
            ret_rootlocal_working_dir = emodis__cta__rootoutputworkingdir
        return ret_rootlocal_working_dir

    # Get the local filesystem place to store the final NC4 files (The THREDDS monitored Directory location)
    @staticmethod
    def get_final_load_dir(region_code):
        # Type Specific Settings
        emodis__ea__finalloaddir = Config_Setting.get_value(setting_name="PATH__THREDDS_MONITORING_DIR__EMODIS__EA", default_or_error_return_value="") # settings.PATH__THREDDS_MONITORING_DIR__EMODIS__EA  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/THREDDS/thredds/catalog/climateserv/emodis-ndvi/eastafrica/250m/10dy/'
        emodis__wa__finalloaddir = Config_Setting.get_value(setting_name="PATH__THREDDS_MONITORING_DIR__EMODIS__WA", default_or_error_return_value="") # settings.PATH__THREDDS_MONITORING_DIR__EMODIS__WA  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/THREDDS/thredds/catalog/climateserv/emodis-ndvi/westafrica/250m/10dy/'
        emodis__sa__finalloaddir = Config_Setting.get_value(setting_name="PATH__THREDDS_MONITORING_DIR__EMODIS__SA", default_or_error_return_value="") # settings.PATH__THREDDS_MONITORING_DIR__EMODIS__SA  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/THREDDS/thredds/catalog/climateserv/emodis-ndvi/southernafrica/250m/10dy/'
        emodis__cta__finalloaddir = Config_Setting.get_value(setting_name="PATH__THREDDS_MONITORING_DIR__EMODIS__CTA", default_or_error_return_value="") # settings.PATH__THREDDS_MONITORING_DIR__EMODIS__CTA  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/THREDDS/thredds/catalog/climateserv/emodis-ndvi/centralasia/250m/10dy/'

        #ret_dir = settings.PATH__THREDDS_MONITORING_DIR__DEFAULT # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/THREDDS/UNKNOWN/'
        ret_dir = Config_Setting.get_value(setting_name="PATH__THREDDS_MONITORING_DIR__DEFAULT", default_or_error_return_value="")  # '/Volumes/TestData/Data/SERVIR/ClimateSERV_2_0/data/THREDDS/UNKNOWN/'
        region_code = str(region_code).strip()
        if (region_code == 'ea'):
            ret_dir = emodis__ea__finalloaddir
        if (region_code == 'wa'):
            ret_dir = emodis__wa__finalloaddir
        if (region_code == 'sa'):
            ret_dir = emodis__sa__finalloaddir
        if (region_code == 'cta'):
            ret_dir = emodis__cta__finalloaddir
        return ret_dir

    # Get the Remote Locations for each of the regions
    @staticmethod
    def get_roothttp_for_regioncode(region_code):
        # Type Specific Settings
        emodis__ea__roothttp = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__EMODIS__EA", default_or_error_return_value="") # settings.REMOTE_PATH__ROOT_HTTP__EMODIS__EA  # 'https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/africa/east/dekadal/emodis/ndvi_c6/temporallysmoothedndvi/downloads/dekadal/'      # East Africa
        emodis__wa__roothttp = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__EMODIS__WA", default_or_error_return_value="") # settings.REMOTE_PATH__ROOT_HTTP__EMODIS__WA  # 'https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/africa/west/dekadal/emodis/ndvi_c6/temporallysmoothedndvi/downloads/dekadal/'      # West Africa
        emodis__sa__roothttp = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__EMODIS__SA", default_or_error_return_value="") # settings.REMOTE_PATH__ROOT_HTTP__EMODIS__SA  # 'https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/africa/southern/dekadal/emodis/ndvi_c6/temporallysmoothedndvi/downloads/dekadal/'  # Southern Africa
        emodis__cta__roothttp = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__EMODIS__CTA", default_or_error_return_value="") # settings.REMOTE_PATH__ROOT_HTTP__EMODIS__CTA  # 'https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/asia/centralasia/dekadal/emodis/ndvi_c6/temporallysmoothedndvi/downloads/dekadal/' # Central Asia

        # ret_roothttp = settings.REMOTE_PATH__ROOT_HTTP__DEFAULT #'localhost://UNKNOWN_URL'
        ret_roothttp = Config_Setting.get_value(setting_name="REMOTE_PATH__ROOT_HTTP__DEFAULT", default_or_error_return_value="")  # ret_roothttp = settings.REMOTE_PATH__ROOT_HTTP__DEFAULT #'localhost://UNKNOWN_URL'
        region_code = str(region_code).strip()
        if(region_code == 'ea'):
            ret_roothttp = emodis__ea__roothttp
        if(region_code == 'wa'):
            ret_roothttp = emodis__wa__roothttp
        if (region_code == 'sa'):
            ret_roothttp = emodis__sa__roothttp
        if (region_code == 'cta'):
            ret_roothttp = emodis__cta__roothttp
        return ret_roothttp


    # DRAFTING - Suggestions
    # # List of objects with these properties, 'filename', 'remote_full_filepath', 'region_code', 'current_year', 'current_dekadal'
    _expected_remote_full_file_paths    = []    # Place to store a list of remote file paths (URLs) that the script will need to download.
    # # List of objects with these properties, 'Granule_UUID', 'filename', 'remote_full_filepath', 'region_code', 'current_year', 'current_dekadal', 'tfw_filename', 'tif_filename', 'final_nc4_filename'
    _expected_granules                  = []    # Place to store granules
    #
    # TODO: Other Props used by the script

    # init (Passing a reference from the calling class, so we can callback the error handler)
    def __init__(self, etl_parent_pipeline_instance):
        self.etl_parent_pipeline_instance = etl_parent_pipeline_instance

    # Supporting Functions



    # Whatever Month we are in, multiple by 3  and then subtract 2, (Jan would be 1 (3 - 2), Dec would be 34 (36 - 2) )
    @staticmethod
    def get_Earliest_Dekadal_Number_From_Month_Number(month_Number=1):
        dekadal_number = month_Number * 3
        dekadal_number = dekadal_number - 2
        return dekadal_number

    # Whatever Month we are in, multiple by 3 (Jan would be 3, Dec would be 36)
    @staticmethod
    def get_Latest_Dekadal_Number_From_Month_Number(month_Number=1):
        dekadal_number = month_Number * 3
        return dekadal_number

    # Months between two dates
    def diff_month(latest_datetime, earliest_datetime):
        return (latest_datetime.year - earliest_datetime.year) * 12 + latest_datetime.month - earliest_datetime.month

    # Calculate the Month Number from a Dekadal input
    @staticmethod
    def get_Month_Number_From_Dekadal(dekadal_string="01"):
        monthNumber = 0
        try:
            dekadal_number = int(dekadal_string)
            monthNumber = int(((dekadal_number - 1) / 3) + 1)  # int rounds down
        except:
            pass

        # DEBUG
        # for dekadal_number in range(1, 37):  # 1 to 36, inclusive
        #     month_number = int(((dekadal_number - 1) / 3) + 1)  # int rounds down
        #     print("dekadal, month_number: " + str(dekadal_number) + ", " + str(month_number))

        return monthNumber

    @staticmethod
    def get_Begin_Day_Number_From_Dekadal(dekadal_string="01"):
        begin_DayNumber = 1
        try:
            dekadal_number = int(dekadal_string)
            dekadal_number_CyclePart = dekadal_number % 3
            if(dekadal_number_CyclePart == 1 ):
                begin_DayNumber = 1
            if (dekadal_number_CyclePart == 2):
                begin_DayNumber = 11
            if (dekadal_number_CyclePart == 0):
                begin_DayNumber = 21
        except:
            pass
        return begin_DayNumber

    # Example output: emodis-ndvi.20151021T000000Z.eastafrica.nc4
    @staticmethod
    def get_Final_NC4_FileName_From_Inputs(region_code, year_YYYY, dekadal_N):
        #
        monthNumber         = emodis.get_Month_Number_From_Dekadal(dekadal_string=str(dekadal_N))
        begin_DayNumber     = emodis.get_Begin_Day_Number_From_Dekadal(dekadal_string=str(dekadal_N))
        #
        nc4_region_name_part = 'regionnamepart'
        region_code = str(region_code).strip()
        if (region_code == 'ea'):
            nc4_region_name_part = "eastafrica"
        if (region_code == 'wa'):
            nc4_region_name_part = "westafrica"
        if (region_code == 'sa'):
            nc4_region_name_part = "southafrica"
        if (region_code == 'cta'):
            nc4_region_name_part = "centralasia"
        #
        ret_FileName = ""
        ret_FileName += "emodis-ndvi."                      # "emodis-ndvi."
        ret_FileName += "{:0>4d}".format(year_YYYY)         # "2015"            # "emodis-ndvi.2015"
        ret_FileName += "{:02d}".format(monthNumber)        # "10"              # "emodis-ndvi.201510"
        ret_FileName += "{:02d}".format(begin_DayNumber)    # "21"              # "emodis-ndvi.20151021"
        ret_FileName += "T000000Z."                         # "T000000Z."       # "emodis-ndvi.20151021T000000Z."
        ret_FileName += nc4_region_name_part                # "eastafrica"      # "emodis-ndvi.20151021T000000Z.eastafrica"
        ret_FileName += ".nc4"                              # ".nc4"            # "emodis-ndvi.20151021T000000Z.eastafrica.nc4"

        return ret_FileName

    # Get file paths infos (Remote file paths, local file paths, final location for load files, etc.
    #@staticmethod
    #def get_expected_remote_filepath(root_path, region_code, year_YYYY, dekadal_N, root_file_download_path, final_load_dir_path):
    @staticmethod
    def get_expected_filepath_infos(root_path, region_code, year_YYYY, dekadal_N, root_file_download_path, final_load_dir_path):
        retObj = {}
        retObj['is_error'] = False
        try:
            filenum = "{:0>2d}{:0>2d}".format(year_YYYY - 2000, dekadal_N)
            filename = region_code + filenum + ".zip"
            remote_full_filepath    = str(os.path.join(root_path, filename)).strip()
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
        current_root_http_path      = self.get_roothttp_for_regioncode(region_code=self.XX__Region_Code)
        root_file_download_path     = os.path.join(emodis.get_root_local_temp_working_dir(region_code=self.XX__Region_Code), self.relative_dir_path__WorkingDir)
        final_load_dir_path         = emodis.get_final_load_dir(region_code=self.XX__Region_Code)

        self.temp_working_dir       = str(root_file_download_path).strip()

        # (1) Generate Expected remote file paths
        try:

            # # Calculate Month Deltas
            # earliest_date = datetime.datetime(year=2020, month=0, day=1)
            # latest_date = datetime.datetime(year=2020, month=0, day=1)
            #
            # # Months between two dates
            # num_of_months = (self.diff_month(latest_datetime=latest_date, earliest_datetime=earliest_date) + 1)  # Always add 1 to this (to include the ending month)
            #

            # get_Earliest_Dekadal_Number_From_Month_Number(month_Number=1)  get_Latest_Dekadal_Number_From_Month_Number(month_Number=1)

            # Iterate on the Year Ranges
            # The second parameter of 'range' is non-inclusive, so if the actual last year we want to process is 2020, then the range value needs to be 2021, and the last iteration through the loop will be 2020
            for YYYY__Year in range(self.YYYY__Year__Start, (self.YYYY__Year__End + 1)):
                start_month__Current_Year = 1
                end_month__Current_Year = 12
                if(YYYY__Year == self.YYYY__Year__Start):
                    start_month__Current_Year = self.MM__Month__Start
                if(YYYY__Year == self.YYYY__Year__End):
                    end_month__Current_Year = self.MM__Month__End

                # Calculate the start and end dekadals for the current year.
                start_dekadal = self.get_Earliest_Dekadal_Number_From_Month_Number(month_Number=start_month__Current_Year)
                end_dekadal = self.get_Latest_Dekadal_Number_From_Month_Number(month_Number=end_month__Current_Year)
                # Validate Dekadals
                if(start_dekadal < 1):
                    start_dekadal = 1
                if (end_dekadal > 36):
                    end_dekadal = 36
                # Iterate on each month (then do all 3 of the dekadal numbers
                # Iterate on Dekadal Ranges
                # for NN__Dekadal in range(1, 4):  # 1, 2, 3 (Dekadals, just do all 3 for any given month)
                #for NN__Dekadal in range(self.NN__Dekadal__Start, (self.NN__Dekadal__End + 1)):
                for NN__Dekadal in range(start_dekadal, (end_dekadal + 1)):
                    # Get the expected remote file to download
                    #result__ExpectedRemoteFilePath_Object = self.get_expected_remote_filepath(root_path=current_root_http_path, region_code=self.XX__Region_Code, year_YYYY=YYYY__Year, dekadal_N=NN__Dekadal, root_file_download_path=root_file_download_path, final_load_dir_path=final_load_dir_path)
                    result__ExpectedRemoteFilePath_Object = self.get_expected_filepath_infos(root_path=current_root_http_path, region_code=self.XX__Region_Code, year_YYYY=YYYY__Year, dekadal_N=NN__Dekadal, root_file_download_path=root_file_download_path, final_load_dir_path=final_load_dir_path)
                    is_error = result__ExpectedRemoteFilePath_Object['is_error']
                    if(is_error == True):
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
            error_JSON['class_name']    = "emodis"
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
            expected_file_path_objects = self._expected_remote_full_file_paths
            print(expected_file_path_objects)
            for expected_file_path_object in expected_file_path_objects:
                try:
                    # # expected_file_path_object Has these properties, 'filename', 'remote_full_filepath', 'region_code', 'current_year', 'current_dekadal'
                    base_file_name      = expected_file_path_object['filename'].split(".")[0]  # Getting wa2012 from wa2012.zip
                    region_code         = expected_file_path_object['region_code']
                    current_year        = expected_file_path_object['current_year']
                    current_dekadal     = expected_file_path_object['current_dekadal']
                    #
                    final_nc4_filename = emodis.get_Final_NC4_FileName_From_Inputs(region_code=region_code, year_YYYY=current_year, dekadal_N=current_dekadal)
                    #
                    expected_file_path_object['tif_filename']       = base_file_name + ".tif"
                    expected_file_path_object['tfw_filename']       = base_file_name + ".tfw"  # NOT .twf - these kinds of bugs are TONS of fun!!
                    expected_file_path_object['final_nc4_filename'] = final_nc4_filename  # NETCDF4 // Expected THREDDS Output File
                    #
                    # Create a new Granule Entry - The first function 'log_etl_granule' is the one that actually creates a new ETL Granule Attempt (There is one granule per dataset per pipeline attempt run in the ETL Granule Table)
                    # # Granule Helpers
                    # # # def log_etl_granule(self, granule_name="unknown_etl_granule_file_or_object_name", granule_contextual_information="", granule_pipeline_state=settings.GRANULE_PIPELINE_STATE__ATTEMPTING, additional_json={}):
                    # # # def etl_granule__Update__granule_pipeline_state(self, granule_uuid, new__granule_pipeline_state, is_error):
                    # # # def etl_granule__Update__is_missing_bool_val(self, granule_uuid, new__is_missing__Bool_Val):
                    # # # def etl_granule__Append_JSON_To_Additional_JSON(self, granule_uuid, new_json_key_to_append, sub_jsonable_object):
                    granule_name = final_nc4_filename
                    granule_contextual_information = ""
                    #granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__ATTEMPTING  # At the start of creating a new Granule, it starts off as 'attempting' - So we can see Active Granules in the database while the system is running.
                    granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__ATTEMPTING", default_or_error_return_value="Attempting") #settings.GRANULE_PIPELINE_STATE__ATTEMPTING
                    additional_json = expected_file_path_object #{}
                    new_Granule_UUID = self.etl_parent_pipeline_instance.log_etl_granule(granule_name=granule_name, granule_contextual_information=granule_contextual_information, granule_pipeline_state=granule_pipeline_state, additional_json=additional_json)
                    #
                    # Save the Granule's UUID for reference in later steps
                    expected_file_path_object['Granule_UUID'] = str(new_Granule_UUID).strip()

                    #
                    self._expected_granules.append(expected_file_path_object)

                except:
                    sysErrorData = str(sys.exc_info())
                    error_JSON = {}
                    error_JSON['error'] = "Error: There was an error when generating a specific expected Granule.  See the additional data [expected_file_path_object] for details on which expected granule caused the error.  System Error Message: " + str(sysErrorData)
                    error_JSON['is_error'] = True
                    error_JSON['class_name'] = "emodis"
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
            error_JSON['class_name'] = "emodis"
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj
        # Make sure the directories exist
        #
        #rootWorking_Dir = self.get_root_local_temp_working_dir(region_code=self.XX__Region_Code)
        rootWorking_Dir = os.path.join(emodis.get_root_local_temp_working_dir(region_code=self.XX__Region_Code), self.relative_dir_path__WorkingDir) # os.path.join(emodis.get_root_local_temp_working_dir(region_code=self.XX__Region_Code), self.relative_dir_path__WorkingDir)
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(rootWorking_Dir)
        if(is_error_creating_directory == True):
            error_JSON = {}
            error_JSON['error'] = "Error: There was an error when the pipeline tried to create a new directory on the filesystem.  The path that the pipeline tried to create was: " + str(rootWorking_Dir) + ".  There should be another error logged just before this one that contains system error info.  That info should give clues to why the directory was not able to be created."
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "emodis"
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        # final_load_dir_path
        final_load_dir_path  = os.path.join(emodis.get_root_local_temp_working_dir(region_code=self.XX__Region_Code), self.relative_dir_path__WorkingDir)
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(final_load_dir_path)
        if (is_error_creating_directory == True):
            error_JSON = {}
            error_JSON['error'] = "Error: There was an error when the pipeline tried to create a new directory on the filesystem.  The path that the pipeline tried to create was: " + str(final_load_dir_path) + ".  There should be another error logged just before this one that contains system error info.  That info should give clues to why the directory was not able to be created."
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "emodis"
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        # DEBUG
        # print("")
        # print("DEBUG: emodis.execute__Step__Pre_ETL_Custom:")
        # print("DEBUG: self._expected_granules[0]: " + str(self._expected_granules[0]))
        # print("DEBUG: ")
        # print("DEBUG: self._expected_granules[len(self._expected_granules - 1)]: " + str(self._expected_granules[len(self._expected_granules) - 1]))
        # print("")

        # Ended, now for reporting
        ret__detail_state_info['class_name'] = "emodis"
        ret__detail_state_info['number_of_expected_remote_full_file_paths'] = str(len(self._expected_remote_full_file_paths)).strip()
        ret__detail_state_info['number_of_expected_granules']               = str(len(self._expected_granules)).strip()
        ret__event_description = "Success.  Completed Step execute__Step__Pre_ETL_Custom by generating "+str(len(self._expected_remote_full_file_paths)).strip()+" expected full file paths to download and "+str(len(self._expected_granules)).strip()+" expected granules to process."

        #
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
        # TODO - Iterate each expected file, attempt to download it to the correct expected location (the working directory).
        # # TODO - Log errors as warnings (not show stoppers) - Add indexes to allow skipping over items (so the ETL job can continue in an automated way). - Need to think this through very carefully, so that the clientside can know about these errors and skip over them as well (and allow the clientside to handle random missing granules - which happens sometimes).
        #

        download_counter    = 0
        loop_counter        = 0
        error_counter       = 0
        detail_errors       = []

        #root_file_download_path = os.path.join( emodis.get_root_local_temp_working_dir(region_code=self.XX__Region_Code), self.relative_dir_path__WorkingDir)

        # # expected_file_path_object Has these properties, 'filename', 'remote_full_filepath', 'region_code', 'current_year', 'current_dekadal'
        expected_remote_file_path_objects = self._expected_remote_full_file_paths
        num_of_objects_to_process = len(expected_remote_file_path_objects)
        num_of_download_activity_events = 4
        modulus_size = int(num_of_objects_to_process / num_of_download_activity_events)
        if(modulus_size < 1):
            modulus_size = 1

        #print("DEBUG: modulus_size: " + str(modulus_size))

        for expected_remote_file_path_object in expected_remote_file_path_objects:
            try:
                if( ((loop_counter + 1) % modulus_size) == 0):
                    #print("Output a log, (and send pipeline activity log) saying, --- about to download file: " + str(loop_counter + 1) + " out of " + str(num_of_objects_to_process))
                    #print(" - Output a log, (and send pipeline activity log) saying, --- about to download file: " + str(loop_counter + 1) + " out of " + str(num_of_objects_to_process))
                    event_message = "About to download file: " + str(loop_counter + 1) + " out of " + str(num_of_objects_to_process)
                    print(event_message)
                    #activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS
                    activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS", default_or_error_return_value="ETL Download Progress") #settings.ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS
                    activity_description = event_message
                    additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                    self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

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
                    activity_event_type         = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING", default_or_error_return_value="ETL Warning")
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

        # #DEBUG
        # print("")
        # print("DEBUG: ")
        # print("DEBUG: emodis.execute__Step__Download:")
        # print("DEBUG: download_counter: " + str(download_counter))
        # print("DEBUG: ")
        # print("")

        # Ended, now for reporting
        #
        ret__detail_state_info['class_name'] = "emodis"
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
        ret__function_name = "execute__Step__Extract"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}
        #
        # TODO: Subtype Specific Logic Here
        # TODO - Iterate each expected zip file, and then extract it's contents to the correct expected location (the working directory).
        #
        # TODO: FUNCTIONAL_PLACEHOLDER_NOTE_2020 - Convert this placeholder code which only extracts files into something that can catch errors and output logging correctly as per the requirements
        # # # # TODO: Part of removing the placeholder function is to pull this back out and now have an overall try/except around the whole loop.

        detail_errors = []
        error_counter = 0

        try:
            expected_granules = self._expected_granules
            for expected_granules_object in expected_granules:
                #   ['tif_filename'], ['tfw_filename'] ['final_nc4_filename'], ['local_full_filepath']
                local_full_filepath = expected_granules_object['local_full_filepath']
                local_extract_path  = expected_granules_object['local_extract_path']
                tfw_filename        = expected_granules_object['tfw_filename']
                expected_full_path_to_local_extracted_tfw_file = os.path.join(local_extract_path, tfw_filename)

                #print("execute__Step__Extract: PLACEHOLDER: (local_full_filepath): " + str(local_full_filepath))

                # Unzip the current zip file # Example: path_to_working_dir/ea2001.zip
                try:
                    list_of_filenames_inside_zipfile = []
                    with zipfile.ZipFile(local_full_filepath, "r") as z:
                        list_of_filenames_inside_zipfile = z.namelist()
                        z.extractall(local_extract_path)
                except:
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
                    # Granule_UUID = expected_granules_object['Granule_UUID']
                    # new__granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__FAILED  # When a granule has a NC4 file in the correct location, this counts as a Success.
                    new__granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")  #
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

                    # print("")
                    # print("execute__Step__Extract: PLACEHOLDER_CODE_ERROR: COULD NOT REMOVE TFW FILE. (sysErrorData): " + str(sysErrorData))
                    # print("")

                pass
            pass
        except:
            sysErrorData = str(sys.exc_info())
            ret__is_error = True
            ret__error_description = "emodis.execute__Step__Extract: There was a generic, uncaught error when attempting to Extract the Granules.  System Error Message: " + str(sysErrorData)
            # print("")
            # print("execute__Step__Extract: PLACEHOLDER_CODE_ERROR: (sysErrorData): " + str(sysErrorData))
            # print("")
        ret__detail_state_info['class_name'] = "emodis"
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
        # TODO - Iterate each expected tif file, and then Follow Brent's Scripts to modify them appropriately, and then save them as netcdf4 (.nc4) files in the working directory).
        #
        # TODO: FUNCTIONAL_PLACEHOLDER_NOTE_2020 - Convert this placeholder code which performs the existing NETCDF modification and creation operations on each file into something that can catch errors and output logging correctly as per the requirements
        # # # # TODO: Part of removing the placeholder function is to pull this back out and now have an overall try/except around the whole loop.

        # error_counter, detail_errors
        error_counter = 0
        detail_errors = []

        try:
            expected_granules = self._expected_granules
            for expected_granules_object in expected_granules:
                try:

                    local_extract_path  = expected_granules_object['local_extract_path']
                    tif_filename        = expected_granules_object['tif_filename']
                    final_nc4_filename  = expected_granules_object['final_nc4_filename']
                    expected_full_path_to_local_extracted_tif_file = os.path.join(local_extract_path, tif_filename)

                    geotiffFile_FullPath = expected_full_path_to_local_extracted_tif_file

                    #print("execute__Step__Transform: PLACEHOLDER: DEBUG: (geotiffFile_FullPath): " + str(geotiffFile_FullPath))

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

                    # CHANGE ME TO BE A GRANULE LOG UPDATE - # print("execute__Step__Transform: PLACEHOLDER: GRANULE Made it to CHECK POINT 1 of 2 - ABOUT TO USE xr ON GEO TIFF FILE: (startTime, endTime): " + str(startTime) + ", " + str(endTime))
                    #print("execute__Step__Transform: PLACEHOLDER: GRANULE Made it to CHECK POINT 1 of 2 - ABOUT TO USE xr ON GEO TIFF FILE: (startTime, endTime): " + str(startTime) + ", " + str(endTime))


                    ############################################################
                    # Start extracting data and creating output netcdf file.
                    ############################################################

                    # 1) Read the geotiff data into an xarray data array
                    #tiffData = xr.open_rasterio(geotiffFile)
                    tiffData = xr.open_rasterio(geotiffFile_FullPath)

                    # 2) Convert to NDVI. (data array)
                    #    See: https://earlywarning.usgs.gov/fews/product/448 Documentation tab on why the data has to be rescaled from uint8 as stored in geotiff
                    ndvi = (tiffData - 100.0) / 100.0

                    # 3) Convert to a dataset.  (need to assign a name to the ndvi data array)
                    ndvi = ndvi.rename('ndvi').to_dataset()
                    # Handle selecting/adding the dimesions
                    ndvi = ndvi.isel(band=0).reset_coords('band', drop=True)  # select the singleton band dimension and drop out the associated coordinate.
                    # Add the time dimension as a new coordinate.
                    ndvi = ndvi.assign_coords(time=startTime).expand_dims(dim='time', axis=0);
                    # Add an additional variable "time_bnds" for the time boundaries.
                    ndvi['time_bnds'] = xr.DataArray(np.array([startTime, endTime]).reshape((1, 2)), dims=['time', 'nbnds'])

                    # 4) Rename and add attributes to this dataset.
                    # # ERROR with this one - as it was
                    #ndvi.rename({'y': 'latitude', 'x': 'longitude'}, inplace=True)  # rename lat/lon
                    # # Error was: (<class 'TypeError'>, TypeError("The `inplace` argument has been removed from xarray. You can achieve an identical effect with python's standard assignment."),
                    #
                    # Solution: Just remove the 'inplace=True' param
                    # # Not sure how to validate that this actually worked though...
                    # # # (It looks like the next several lines reference this part.  That would mean that if this fix doesn't work, it should be obvious on next run..)
                    # ndvi.rename({'y': 'latitude', 'x': 'longitude'})  # rename lat/lon
                    # # Nope: Got this error now
                    # # # <class 'AttributeError'>, AttributeError("'Dataset' object has no attribute 'latitude'"), <traceback object at 0x1159f3ac8>
                    #
                    # Now making the assignment (look like the above ones)
                    ndvi = ndvi.rename({'y': 'latitude', 'x': 'longitude'})  # rename lat/lon

                    # Lat/Lon/Time dictionaries.
                    # Use Ordered dict
                    latAttr = OrderedDict([('long_name', 'latitude'), ('units', 'degrees_north'), ('axis', 'Y')])
                    lonAttr = OrderedDict([('long_name', 'longitude'), ('units', 'degrees_east'), ('axis', 'X')])
                    timeAttr = OrderedDict([('long_name', 'time'), ('axis', 'T'), ('bounds', 'time_bnds')])
                    timeBoundsAttr = OrderedDict([('long_name', 'time_bounds')])
                    ndviAttr = OrderedDict([('long_name', 'ndvi'), ('units', 'unitless'), ('comment', 'Maximum value composite over dekad defined by time_bnds')])
                    fileAttr = OrderedDict([('Description', 'EMODIS NDVI C6 at 250m resolution'), ('DateCreated', pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%SZ')), ('Contact', 'Lance Gilliland, lance.gilliland@nasa.gov'), ('Source', 'EMODIS NDVI C6, https://earlywarning.usgs.gov/fews/datadownloads/East%20Africa/eMODIS%20NDVI%20C6'), ('Version', 'C6'), ('RangeStartTime', startTime.strftime('%Y-%m-%dT%H:%M:%SZ')), ('RangeEndTime', endTime.strftime('%Y-%m-%dT%H:%M:%SZ')), ('SouthernmostLatitude', np.min(ndvi.latitude.values)), ('NorthernmostLatitude', np.max(ndvi.latitude.values)), ('WesternmostLongitude', np.min(ndvi.longitude.values)), ('EasternmostLongitude', np.max(ndvi.longitude.values)), ('TemporalResolution', 'dekad'), ('SpatialResolution', '250m')])

                    # missing_data/_FillValue , relative time units etc. are handled as part of the encoding dictionary used in to_netcdf() call.
                    # ndvi.ndvi.values=100.0*ndvi.ndvi.values+100.0
                    ndviEncoding = {'_FillValue': np.int8(127), 'missing_value': np.int8(127), 'dtype': np.dtype('int8'), 'scale_factor': 0.01, 'add_offset': 0.0}
                    timeEncoding = {'units': 'seconds since 1970-01-01T00:00:00Z'}
                    timeBoundsEncoding = {'units': 'seconds since 1970-01-01T00:00:00Z'}
                    # Set the Attributes
                    ndvi.latitude.attrs = latAttr
                    ndvi.longitude.attrs = lonAttr
                    ndvi.time.attrs = timeAttr
                    ndvi.time_bnds.attrs = timeBoundsAttr
                    ndvi.ndvi.attrs = ndviAttr
                    ndvi.attrs = fileAttr
                    # Set the Endcodings
                    ndvi.ndvi.encoding = ndviEncoding
                    ndvi.time.encoding = timeEncoding
                    ndvi.time_bnds.encoding = timeBoundsEncoding

                    outputFile_FullPath     = os.path.join(local_extract_path, final_nc4_filename)
                    ndvi.to_netcdf(outputFile_FullPath, unlimited_dims='time')

                    # : CHANGE ME TO BE A GRANULE LOG UPDATE - # print("execute__Step__Transform: PLACEHOLDER: GRANULE Made it to CHECK POINT 2 OF 2: (outputFile_FullPath): " + str(outputFile_FullPath))
                    #print("execute__Step__Transform: PLACEHOLDER: GRANULE Made it to CHECK POINT 2 OF 2: (outputFile_FullPath): " + str(outputFile_FullPath))

                except:
                    sysErrorData = str(sys.exc_info())

                    Granule_UUID = expected_granules_object['Granule_UUID']

                    error_message = "emodis.execute__Step__Transform: An Error occurred during the Transform step with ETL_Granule UUID: " + str(Granule_UUID) + ".  System Error Message: " + str(sysErrorData)

                    # Individual Transform Granule Error
                    error_counter = error_counter + 1
                    detail_errors.append(error_message)

                    error_JSON = {}
                    error_JSON['error_message'] = error_message

                    # Update this Granule for Failure (store the error info in the granule also)
                    #Granule_UUID = expected_granules_object['Granule_UUID']
                    #new__granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__FAILED  # When a granule has a NC4 file in the correct location, this counts as a Success.
                    new__granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED") #
                    is_error = True
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)
                    new_json_key_to_append = "execute__Step__Transform"
                    is_update_succeed_2 = self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(granule_uuid=Granule_UUID, new_json_key_to_append=new_json_key_to_append, sub_jsonable_object=error_JSON)
                    pass
            pass

        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an uncaught error when processing the Transform step on all of the expected Granules.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "emodis"
            error_JSON['function_name'] = "execute__Step__Transform"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        ret__detail_state_info['class_name'] = "emodis"
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
        # Subtype Specific Logic Here
        # - Iterate each expected nc4 file, and then simply copy them to their final location (With THREDDS, all this means is to copy the files to a directory that THREDDS is listening to)
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
                    copyfile(expected_full_path_to_local_working_nc4_file, expected_full_path_to_local_final_nc4_file) #(src, dst)

                    # Create a new Granule Entry - The first function 'log_etl_granule' is the one that actually creates a new ETL Granule Attempt (There is one granule per dataset per pipeline attempt run in the ETL Granule Table)
                    # # Granule Helpers
                    # # # def log_etl_granule(self, granule_name="unknown_etl_granule_file_or_object_name", granule_contextual_information="", granule_pipeline_state=settings.GRANULE_PIPELINE_STATE__ATTEMPTING, additional_json={}):
                    # # # def etl_granule__Update__granule_pipeline_state(self, granule_uuid, new__granule_pipeline_state, is_error):
                    # # # def etl_granule__Update__is_missing_bool_val(self, granule_uuid, new__is_missing__Bool_Val):
                    # # # def etl_granule__Append_JSON_To_Additional_JSON(self, granule_uuid, new_json_key_to_append, sub_jsonable_object):
                    Granule_UUID                = expected_granules_object['Granule_UUID']
                    #new__granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__SUCCESS # When a granule has a NC4 file in the correct location, this counts as a Success.
                    new__granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__SUCCESS", default_or_error_return_value="SUCCESS")  #
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
                    error_JSON['class_name'] = "emodis"
                    error_JSON['function_name'] = "execute__Step__Load"

                    # Additional infos
                    error_JSON['expected_full_path_to_local_working_nc4_file']  = str(expected_full_path_to_local_working_nc4_file).strip()
                    error_JSON['expected_full_path_to_local_final_nc4_file']    = str(expected_full_path_to_local_final_nc4_file).strip()

                    # Update this Granule for Failure (store the error info in the granule also)
                    Granule_UUID = expected_granules_object['Granule_UUID']
                    #new__granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__FAILED  # When a granule has a NC4 file in the correct location, this counts as a Success.
                    new__granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")  #
                    is_error = True
                    is_update_succeed   = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)
                    new_json_key_to_append = "execute__Step__Load"
                    is_update_succeed_2 = self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(granule_uuid=Granule_UUID, new_json_key_to_append=new_json_key_to_append, sub_jsonable_object=error_JSON)
        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an uncaught error when processing the Load step on all of the expected Granules.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "emodis"
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
        # TODO - Add any relevant info to the Datasets Database (So the clientside will have what it needs to know in order to display and process output)
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
        # Subtype Specific Logic Here
        # - The Cleanup step: all this means here is to remove the working directory and it's contents
        #
        try:
            temp_working_dir = str(self.temp_working_dir).strip()
            if(temp_working_dir == ""):

                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                #activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_BLANK
                activity_event_type = "temp_dir_blank"#Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_BLANK", default_or_error_return_value="Temp Working Dir Blank")  #
                activity_description = "Could not remove the temporary working directory.  The value for self.temp_working_dir was blank. "
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = "emodis"
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
            else:
                #shutil.rmtree
                rmtree(temp_working_dir)

                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                #activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_REMOVED
                activity_event_type ="removed" #Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_REMOVED", default_or_error_return_value="Temp Working Dir Removed")  #
                activity_description = "Temp Working Directory, " + str(self.temp_working_dir).strip() + ", was removed."
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = "emodis"
                additional_json['temp_working_dir'] = str(temp_working_dir).strip()
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an uncaught error when processing the Clean Up step.  This function is supposed to simply remove the working directory.  This means the working directory was not removed.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "emodis"
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
        print("emodis.test_class_instance: Reached the end.")