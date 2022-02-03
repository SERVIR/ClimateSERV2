import calendar, datetime, glob, os, shutil, sys
import urllib

import requests
import xarray as xr
import pandas as pd
import numpy as np
from collections import OrderedDict
import ftplib
import time
from ftplib import FTP
from urllib.parse import urlparse
import wget
from bs4 import BeautifulSoup

from .common import common
from .etl_dataset_subtype_interface import ETL_Dataset_Subtype_Interface

from api.services import Config_SettingService
from ..models import Config_Setting

class ETL_Dataset_Subtype_NMME(ETL_Dataset_Subtype_Interface):

    class_name = 'nmme'
    etl_parent_pipeline_instance = None
    etl_dataset_instance = None

    # DRAFTING - Suggestions
    _expected_remote_full_file_paths    = []    # Place to store a list of remote file paths (URLs) that the script will need to download.
    _expected_granules                  = []    # Place to store granules

    # init (Passing a reference from the calling class, so we can callback the error handler)
    def __init__(self, etl_parent_pipeline_instance):
        self.etl_parent_pipeline_instance = etl_parent_pipeline_instance

    # Set default parameters or using default
    def set_optional_parameters(self, params):
        self.YYYY__Year__Start = params.get('YYYY__Year__Start') or datetime.date.today().year
        self.YYYY__Year__End = params.get('YYYY__Year__End') or datetime.date.today().year
        self.MM__Month__Start = params.get('MM__Month__Start') or datetime.date.today().month
        self.MM__Month__End = params.get('MM__Month__End') or datetime.date.today().month

    def execute__Step__Pre_ETL_Custom(self):
        ret__function_name = "execute__Step__Pre_ETL_Custom"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}
        final_load_dir_path = self.etl_parent_pipeline_instance.dataset.final_load_dir
        current_root_http_path = self.etl_parent_pipeline_instance.dataset.source_url
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(
            os.path.join(final_load_dir_path))
        if is_error_creating_directory == True:
            error_JSON = {}
            error_JSON[
                'error'] = "Error: There was an error when the pipeline tried to create a new directory on the filesystem.  The path that the pipeline tried to create was: " + str(
                final_load_dir_path) + ".  There should be another error logged just before this one that contains system error info.  That info should give clues to why the directory was not able to be created."
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.__class__.__name__
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                         is_error=ret__is_error,
                                                         event_description=ret__event_description,
                                                         error_description=ret__error_description,
                                                         detail_state_info=ret__detail_state_info)
            return retObj
        ret__detail_state_info['class_name'] = self.__class__.__name__
        ret__detail_state_info['number_of_expected_granules'] = str(len(self._expected_granules)).strip()
        ret__event_description = "Success.  Completed Step execute__Step__Pre_ETL_Custom by generating " + str(len(self._expected_remote_full_file_paths)).strip() + " expected full file paths to download and " + str(len(self._expected_granules)).strip() + " expected granules to process."

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj


    def listFD(url, ext=''):
        page = requests.get(url).text
        soup = BeautifulSoup(page, 'html.parser')
        return [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]



    def execute__Step__Download(self):
        ret__detail_state_info = {}
        download_counter = 0
        loop_counter = 0
        error_counter = 0
        detail_errors = []
        ret__function_name = "execute__Step__Download"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        current_root_https_path = self.etl_parent_pipeline_instance.dataset.source_url
        final_load_dir_path = self.etl_parent_pipeline_instance.dataset.final_load_dir
        for path in ETL_Dataset_Subtype_NMME.listFD(current_root_https_path, ".nc4"):
            try:
                filename = os.path.basename(path)
                if 'ccsm4' in filename:
                    print(path)
                    url_to_download = os.path.join(current_root_https_path, filename)
                    wget.download(url_to_download , final_load_dir_path)
                    #urllib.urlretrieve(url_to_download, final_load_dir_path + filename)
                    download_counter = download_counter + 1
            except Exception as e:
                print(e)
                error_counter = error_counter + 1
                sysErrorData = str(sys.exc_info())
                warn_JSON = {}
                warn_JSON[
                    'warning'] = "Warning: There was an uncaught error when attempting to download file at URL: " + str(
                    path) + ".  If the System Error message says something like 'nodename nor servname provided, or not known', then one common cause of that error is an unstable or disconnected internet connection.  Double check that the internet connection is working and try again.  System Error Message: " + str(
                    sysErrorData)
                warn_JSON['is_error'] = True
                warn_JSON['class_name'] = self.__class__.__name__
                warn_JSON['function_name'] = "execute__Step__Download"
                activity_event_type = Config_Setting.get_value(
                    setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING",
                    default_or_error_return_value="ETL Warning")
                activity_description = warn_JSON['warning']
                self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type,
                                                                activity_description=activity_description,
                                                                etl_granule_uuid="", is_alert=True,
                                                            additional_json=warn_JSON)
        # Ended, now for reporting
        ret__detail_state_info['class_name'] = self.__class__.__name__
        ret__detail_state_info['download_counter'] = download_counter
        ret__detail_state_info['error_counter'] = error_counter
        ret__detail_state_info['loop_counter'] = loop_counter
        ret__detail_state_info['detail_errors'] = detail_errors
        # ret__detail_state_info['number_of_expected_remote_full_file_paths'] = str(len(self._expected_remote_full_file_paths)).strip()
        # ret__detail_state_info['number_of_expected_granules'] = str(len(self._expected_granules)).strip()
        ret__event_description = "Success.  Completed Step execute__Step__Download by downloading " + str(
            download_counter).strip() + " files."

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                     is_error=ret__is_error,
                                                     event_description=ret__event_description,
                                                     error_description=ret__error_description,
                                                     detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Extract(self):
        ret__function_name = "execute__Step__Extract"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        ret__detail_state_info['class_name'] = self.class_name
        ret__detail_state_info[
            'custom_message'] = "NMME types do not need to be extracted.  The source files are non-compressed Tif files."

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                     is_error=ret__is_error, event_description=ret__event_description,
                                                     error_description=ret__error_description,
                                                     detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Transform(self):
        ret__function_name = "execute__Step__Transform"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}
        error_counter=0
        detail_errors={}
        ret__detail_state_info['class_name'] = self.__class__.__name__
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
        ret__function_name = "execute__Step__Clean_Up"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}
        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj