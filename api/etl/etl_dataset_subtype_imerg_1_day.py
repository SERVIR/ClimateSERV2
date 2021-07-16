from collections import OrderedDict
import datetime
import ftplib
import numpy as np
import os
import pandas as pd
from shutil import copyfile, rmtree
import sys
import time
import xarray as xr

from .common import common
from .etl_dataset_subtype_interface import ETL_Dataset_Subtype_Interface

from api.services import Config_SettingService
from ..models import Config_Setting

class ImergOneDay(ETL_Dataset_Subtype_Interface):
    class_name = "ImergOneDay"
    etl_parent_pipeline_instance = None

    @staticmethod
    def get_run_time_list():
        return [
            'S023000-E025959.0150',
            'S053000-E055959.0330',
            'S083000-E085959.0510',
            'S113000-E115959.0690',
            'S143000-E145959.0870',
            'S173000-E175959.1050',
            'S203000-E205959.1230',
            'S233000-E235959.1410'
        ]

    # Imerg Has more than 1 mode which refer to sub datasets (Early and Late)
    imerg_mode = "LATE"  # Choices at this time are "LATE" and "EARLY" // Controlled by setter functions. // Default
    # is "LATE"

    # Input Settings
    YYYY__Year__Start = 2020  # 2019
    YYYY__Year__End = 2020
    MM__Month__Start = 1  # 12    # 2 #1
    MM__Month__End = 1  # 4 #6
    DD__Day__Start = 1  # 30    # 23
    DD__Day__End = 1  # 2     # 9

    relative_dir_path__WorkingDir = 'D:/imerg1day/'

    # DRAFTING - Suggestions
    _expected_remote_full_file_paths = []  # Place to store a list of remote file paths (URLs) that the script will
    # need to download.
    _expected_granules = []  # Place to store granules
    #
    # TODO: Other Props used by the script

    _remote_connection__Username = ""
    _remote_connection__Password = ""

    # init (Passing a reference from the calling class, so we can callback the error handler)
    def __init__(self, etl_parent_pipeline_instance):
        print('initt')
        self.etl_parent_pipeline_instance = etl_parent_pipeline_instance
        root_file_download_path = os.path.join(self.get_root_local_temp_working_dir(subtype_filter=self.imerg_mode),
                                               self.relative_dir_path__WorkingDir)
        self.temp_working_dir = str(root_file_download_path).strip()
        self._expected_granules = []

    def set_imerg_mode(self, which):
        self.imerg_mode = which

    # Validate type or use existing default for each
    def set_imerg_1_day_params(self, start_year, end_year, start_month, end_month, start_day, end_day):
        try:
            self.YYYY__Year__Start = int(start_year) if start_year != 0 else self.YYYY__Year__Start
        except ValueError:
            pass
        try:
            self.YYYY__Year__End = int(end_year) if end_year != 0 else self.YYYY__Year__End
        except ValueError:
            pass
        try:
            self.MM__Month__Start = int(start_month) if start_month != 0 else self.MM__Month__Start
        except ValueError:
            pass
        try:
            self.MM__Month__End = int(end_month) if end_month != 0 else self.MM__Month__End
        except ValueError:
            pass
        try:
            self.DD__Day__Start = int(start_day) if start_day != 0 else self.DD__Day__Start
        except ValueError:
            pass
        try:
            self.DD__Day__End = int(end_day) if end_day != 0 else self.DD__Day__End
        except ValueError:
            pass

    # Get the credentials from the settings for connecting to the IMERG Data Source, and then set them to the
    # Instance Vars.
    def get_credentials_and_set_to_class(self):
        self._remote_connection__Username = Config_Setting.get_value(setting_name="FTP_CREDENTIAL_IMERG__USER",
                                                                     default_or_error_return_value="")
        self._remote_connection__Password = Config_Setting.get_value(setting_name="FTP_CREDENTIAL_IMERG__PASS",
                                                                     default_or_error_return_value="")

    # I don't believe this is ever used, remove if it doesn't cause issues
    # # Months between two dates
    # def diff_month(self, latest_datetime, earliest_datetime):
    #     return (latest_datetime.year - earliest_datetime.year) * 12 + latest_datetime.month - earliest_datetime.month

    # Get the local filesystem place to store data
    @staticmethod
    def get_root_local_temp_working_dir(subtype_filter):
        subtype_filter = str(subtype_filter).strip()
        if str(subtype_filter).strip() == 'EARLY':
            return Config_Setting.get_value(
                setting_name="PATH__TEMP_WORKING_DIR__IMERG__EARLY_1DAY",
                default_or_error_return_value="")
        else:
            return Config_Setting.get_value(
                setting_name="PATH__TEMP_WORKING_DIR__IMERG__LATE_1DAY",
                default_or_error_return_value="")

    # Get the local filesystem place to store the final NC4 files (The THREDDS monitored Directory location)
    @staticmethod
    def get_final_load_dir(subtype_filter):
        if str(subtype_filter).strip() == 'EARLY':
            return Config_Setting.get_value(
                setting_name="PATH__THREDDS_MONITORING_DIR__IMERG__EARLY_1DAY",
                default_or_error_return_value="")
        else:
            return Config_Setting.get_value(
                setting_name="PATH__THREDDS_MONITORING_DIR__IMERG__LATE_1DAY",
                default_or_error_return_value="")

    # Get the Remote Locations for each of the subtypes
    @staticmethod
    def get_roothttp_for_subtype(subtype_filter):
        if str(subtype_filter).strip() == 'EARLY':
            return Config_Setting.get_value(
                setting_name="REMOTE_PATH__ROOT_HTTP__IMERG__EARLY",
                default_or_error_return_value="")
            # 'ftp://jsimpson.pps.eosdis.nasa.gov/data/imerg/gis/early/'
            # Early # Note: EARLY from here only requires /yyyy/mm/ appended to path
        else:
            return Config_Setting.get_value(
                setting_name="REMOTE_PATH__ROOT_HTTP__IMERG__LATE",
                default_or_error_return_value="")
            # 'ftp://jsimpson.pps.eosdis.nasa.gov/data/imerg/gis/'
            # Late # Note: LATE, from here only requires /yyyy/mm/ appended to path

    def execute__Step__Pre_ETL_Custom(self):
        ret__function_name = "execute__Step__Pre_ETL_Custom"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""

        final_load_dir_path = self.get_final_load_dir(subtype_filter=self.imerg_mode)

        # (1) Generate Expected remote file paths
        try:

            # Create the list of Days (From start time to end time)
            start_date = datetime.datetime(year=self.YYYY__Year__Start, month=self.MM__Month__Start,
                                           day=self.DD__Day__Start)
            end_date = datetime.datetime(year=self.YYYY__Year__End, month=self.MM__Month__End, day=self.DD__Day__End)

            delta = end_date - start_date

            for i in range(delta.days + 1):
                current_date = start_date + datetime.timedelta(days=i)
                current_year__yyyy_str = "{:0>4d}".format(current_date.year)
                current_month__mm_str = "{:02d}".format(current_date.month)
                current_day__dd_str = "{:02d}".format(current_date.day)

                for runTime in self.get_run_time_list():
                    base_filename = '3B-HHR-' + (
                        "L" if self.imerg_mode == "LATE" else "E") \
                                    + '.MS.MRG.3IMERG.' + current_year__yyyy_str \
                                    + current_month__mm_str + current_day__dd_str \
                                    + '-' + runTime + '.V06B.1day.'
                    tfw_filename = base_filename + 'tfw'
                    tif_filename = base_filename + 'tif'

                    final_nc4_filename = 'nasa-imerg-1day-' + (
                        "late" if self.imerg_mode == "LATE" else "early") + \
                        '.' + current_year__yyyy_str + current_month__mm_str \
                        + current_day__dd_str + 'T' + runTime[1:7] + 'Z.global.nc4'

                    # Now get the remote File Paths (Directory) based on the date infos.
                    remote_directory_path = "UNSET/"
                    if self.imerg_mode == "LATE":
                        remote_directory_path = Config_Setting.get_value(
                            setting_name="REMOTE_PATH__ROOT_HTTP__IMERG__LATE",
                            default_or_error_return_value="ERROR_GETTING_DIR_FOR_LATE/")
                    if self.imerg_mode == "EARLY":
                        remote_directory_path = Config_Setting.get_value(
                            setting_name="REMOTE_PATH__ROOT_HTTP__IMERG__EARLY",
                            default_or_error_return_value="ERROR_GETTING_DIR_FOR_EARLY/")

                    # Add the Year and Month to the directory path.
                    remote_directory_path += current_year__yyyy_str
                    remote_directory_path += '/'
                    remote_directory_path += current_month__mm_str
                    remote_directory_path += '/'

                    # Getting full paths
                    remote_full_filepath_tif = str(os.path.join(remote_directory_path, tif_filename)).strip()
                    remote_full_filepath_tfw = str(os.path.join(remote_directory_path, tfw_filename)).strip()

                    local_full_filepath_tif = os.path.join(self.temp_working_dir, tif_filename)
                    local_full_filepath_tfw = os.path.join(self.temp_working_dir, tfw_filename)

                    local_full_filepath_final_nc4_file = os.path.join(final_load_dir_path, final_nc4_filename)

                    # Filename and Granule Name info
                    local_extract_path = self.temp_working_dir
                    # There is no extract step, so just using the working directory as the local extract path.
                    local_final_load_path = final_load_dir_path

                    current_obj = {'runTime': runTime, 'date_YYYY': current_year__yyyy_str,
                                   'date_MM': current_month__mm_str, 'date_DD': current_day__dd_str,
                                   'local_extract_path': local_extract_path,
                                   'local_final_load_path': local_final_load_path,
                                   'remote_directory_path': remote_directory_path, 'base_filename': base_filename,
                                   'tfw_filename': tfw_filename, 'tif_filename': tif_filename,
                                   'final_nc4_filename': final_nc4_filename, 'granule_name': final_nc4_filename,
                                   'remote_full_filepath_tif': remote_full_filepath_tif,
                                   'remote_full_filepath_tfw': remote_full_filepath_tfw,
                                   'local_full_filepath_tif': local_full_filepath_tif,
                                   'local_full_filepath_tfw': local_full_filepath_tfw,
                                   'local_full_filepath_final_nc4_file': local_full_filepath_final_nc4_file}

                    granule_name = final_nc4_filename
                    granule_contextual_information = ""
                    granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__ATTEMPTING",
                                                                      default_or_error_return_value="Attempting")
                    # settings.GRANULE_PIPELINE_STATE__ATTEMPTING
                    additional_json = current_obj  # {}
                    new_granule_uuid = self.etl_parent_pipeline_instance.log_etl_granule(
                        granule_name=granule_name,
                        granule_contextual_information=granule_contextual_information,
                        granule_pipeline_state=granule_pipeline_state,
                        additional_json=additional_json)
                    #
                    # Save the Granule's UUID for reference in later steps
                    current_obj['Granule_UUID'] = str(new_granule_uuid).strip()

                    # Add to the granules list
                    self._expected_granules.append(current_obj)

        except Exception:
            error_message = "Error: There was an error when generating the expected remote file paths.  "
            "See the additional data for details on which expected file caused the "
            "error.  System Error Message: " + str(sys.exc_info())
            return common.get_function_response_object(class_name=self.__class__.__name__,
                                                       function_name=ret__function_name, is_error=True,
                                                       event_description=ret__event_description,
                                                       error_description=error_message,
                                                       detail_state_info={
                                                           "error": error_message,
                                                           "is_error": True,
                                                           "class_name": self.__class__.__name__,
                                                           "function_name": "execute__Step__Pre_ETL_Custom"
                                                       })

        # Make sure the directories exist
        #
        if self.etl_parent_pipeline_instance.create_dir_if_not_exist(self.temp_working_dir):
            error_message = "Error: There was an error when the pipeline tried to create a new directory on " \
                            "the filesystem.  The path that the pipeline tried to create was: " \
                            + str(self.temp_working_dir) + ".  There should be another error logged " \
                                                           "just before this one that contains " \
                                                           "system error info.  That info should give " \
                                                           "clues to why the directory was not able to be created. "
            return common.get_function_response_object(class_name=self.__class__.__name__,
                                                       function_name=ret__function_name,
                                                       is_error=True,
                                                       event_description=ret__event_description,
                                                       error_description=error_message,
                                                       detail_state_info={
                                                           "error": error_message,
                                                           "is_error": True,
                                                           "class_name": self.__class__.__name__,
                                                           "function_name": "execute__Step__Pre_ETL_Custom"
                                                       })

        # final_load_dir_path
        if self.etl_parent_pipeline_instance.create_dir_if_not_exist(final_load_dir_path):
            error_message = "Error: There was an error when the pipeline tried to create a new directory on the " \
                            "filesystem.  The path that the pipeline tried to create was: " \
                            + str(final_load_dir_path) + ".  There should be another error logged " \
                                                         "just before this one that contains " \
                                                         "system error info.  That info should give clues to " \
                                                         "why the directory was not able to be created. "
            return common.get_function_response_object(class_name=self.__class__.__name__,
                                                       function_name=ret__function_name,
                                                       is_error=True,
                                                       event_description=ret__event_description,
                                                       error_description=error_message,
                                                       detail_state_info={
                                                           "error": error_message,
                                                           "is_error": True,
                                                           "class_name": self.__class__.__name__,
                                                           "function_name": "execute__Step__Pre_ETL_Custom"
                                                       })

        # Ended, now for reporting

        ret__event_description = "Success.  Completed Step execute__Step__Pre_ETL_Custom by generating " + str(
            len(self._expected_remote_full_file_paths)).strip() + " expected full file paths to download and " + str(
            len(self._expected_granules)).strip() + " expected granules to process."

        return common.get_function_response_object(class_name=self.__class__.__name__,
                                                   function_name=ret__function_name, is_error=ret__is_error,
                                                   event_description=ret__event_description,
                                                   error_description=ret__error_description,
                                                   detail_state_info={
                                                       "class_name": self.__class__.__name__,
                                                       "number_of_expected_granules": str(
                                                           len(self._expected_granules)).strip()
                                                   })

    def execute__Step__Download(self):
        ret__function_name = "execute__Step__Download"
        ret__is_error = False
        ret__error_description = ""

        download_counter = 0
        loop_counter = 0
        error_counter = 0
        detail_errors = []
        expected_granules = self._expected_granules
        num_of_objects_to_process = len(expected_granules)
        num_of_download_activity_events = 4
        modulus_size = int(num_of_objects_to_process / num_of_download_activity_events)
        if modulus_size < 1:
            modulus_size = 1

        # Connect to the FTP Server and download all of the files in the list.
        try:
            ftp_connection = ftplib.FTP_TLS(
                host=Config_Setting.get_value(setting_name="FTP_CREDENTIAL_IMERG__HOST",
                                              default_or_error_return_value="error.getting.ftp-host.nasa.gov"),
                user=Config_Setting.get_value(setting_name="FTP_CREDENTIAL_IMERG__USER",
                                              default_or_error_return_value="error_getting_user_name"),
                passwd=Config_Setting.get_value(setting_name="FTP_CREDENTIAL_IMERG__PASS",
                                                default_or_error_return_value="error_getting_user_password"))
            ftp_connection.prot_p()

            time.sleep(1)

        except Exception:
            error_counter = error_counter + 1
            error_message = "imerg.execute__Step__Download: Error Connecting to FTP.  There was an error when " \
                            "attempting to connect to the Remote FTP Server.  System Error Message: " \
                            + str(sys.exc_info())
            detail_errors.append(error_message)
            print(error_message)

            # Ended, now for reporting
            ret__event_description = "Error During Step execute__Step__Download by downloading " + str(
                download_counter).strip() + " files."
            #
            return common.get_function_response_object(class_name=self.__class__.__name__,
                                                       function_name=ret__function_name, is_error=ret__is_error,
                                                       event_description=ret__event_description,
                                                       error_description=ret__error_description,
                                                       detail_state_info={
                                                           "class_name": self.__class__.__name__,
                                                           "download_counter": download_counter,
                                                           "error_counter": error_counter,
                                                           "loop_counter": loop_counter,
                                                           "detail_errors": detail_errors
                                                       })

        for expected_granule in expected_granules:

            try:
                if ((loop_counter + 1) % modulus_size) == 0:
                    event_message = "About to download file: " + str(loop_counter + 1) + " out of " + str(
                        num_of_objects_to_process)
                    print(event_message)
                    # activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS
                    activity_event_type = Config_Setting.get_value(
                        setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS",
                        default_or_error_return_value="ETL Download Progress")
                    # settings.ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS
                    activity_description = event_message
                    additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                    self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type,
                                                                    activity_description=activity_description,
                                                                    etl_granule_uuid="", is_alert=False,
                                                                    additional_json=additional_json)

                # Current Granule to download
                remote_directory_path = expected_granule['remote_directory_path']
                # remote_full_filepath_tif    = expected_granule['remote_full_filepath_tif']
                # remote_full_filepath_tfw    = expected_granule['remote_full_filepath_tfw']
                tfw_filename = expected_granule['tfw_filename']
                tif_filename = expected_granule['tif_filename']
                local_full_filepath_tif = expected_granule['local_full_filepath_tif']
                local_full_filepath_tfw = expected_granule['local_full_filepath_tfw']
                #
                # Granule info
                Granule_UUID = expected_granule['Granule_UUID']
                # unused variable
                # granule_name = expected_granule['granule_name']

                # FTP Processes
                # # 1 - Change Directory to the directory path
                ftp_connection.cwd(remote_directory_path)

                # TODO - Fix the problems with checking if a file exists        START
                # # 2 - Check to see if the files exists
                hasFiles = False
                file_list = []  # to store all files
                ftp_connection.retrlines('LIST', file_list.append)  # append to list
                file_found_count = 0
                # Looking for two specific file matches out of the whole list of files in the current remote directory
                for f in file_list:
                    if tfw_filename in f:
                        file_found_count = file_found_count + 1
                    if tif_filename in f:
                        file_found_count = file_found_count + 1

                if file_found_count == 2:
                    hasFiles = True

                # Validation
                if not hasFiles:
                    print("Could not find both TIF and TFW files in the directory.  - TODO - Granule Error Recording "
                          "here.")
                # TODO - Fix the problems with checking if a file exists        END

                # # Let's assume the files DO exist on the remote server - until we can get the rest of the stuff
                # working. hasFiles = True

                if hasFiles:
                    # Both files were found, so let's now download them.

                    # Backwards compatibility
                    # # Remote paths (where the files are coming from)
                    ftp_PathTo_TIF = tif_filename
                    ftp_PathTo_TWF = tfw_filename
                    # # Local Paths (Where the files are being saved)
                    local_FullFilePath_ToSave_Tif = local_full_filepath_tif
                    local_FullFilePath_ToSave_Twf = local_full_filepath_tfw

                    try:
                        # Download the Tif
                        fx = open(local_FullFilePath_ToSave_Tif, "wb")
                        fx.close()
                        os.chmod(local_FullFilePath_ToSave_Tif, 0o0777)  # 0777

                        try:
                            with open(local_FullFilePath_ToSave_Tif, "wb") as f:
                                ftp_connection.retrbinary("RETR " + ftp_PathTo_TIF,
                                                          f.write)  # "RETR %s" % ftp_PathTo_TIF
                        except Exception:
                            os.remove(local_FullFilePath_ToSave_Tif)
                            local_FullFilePath_ToSave_Tif = local_FullFilePath_ToSave_Tif.replace("03E", "04A")
                            ftp_PathTo_TIF = ftp_PathTo_TIF.replace("03E", "04A")
                            fx = open(local_FullFilePath_ToSave_Tif, "wb")
                            fx.close()
                            os.chmod(local_FullFilePath_ToSave_Tif, 0o0777)  # 0777
                            try:
                                with open(local_FullFilePath_ToSave_Tif, "wb") as f:
                                    ftp_connection.retrbinary("RETR " + ftp_PathTo_TIF,
                                                              f.write)  # "RETR %s" % ftp_PathTo_TIF
                            except Exception:
                                os.remove(local_FullFilePath_ToSave_Tif)
                                ftp_PathTo_TIF = ftp_PathTo_TIF.replace("04A", "04B")
                                local_FullFilePath_ToSave_Tif = local_FullFilePath_ToSave_Tif.replace("04A", "04B")
                                fx = open(local_FullFilePath_ToSave_Tif, "wb")
                                fx.close()
                                os.chmod(local_FullFilePath_ToSave_Tif, 0o0777)  # 0777
                                try:
                                    with open(local_FullFilePath_ToSave_Tif, "wb") as f:
                                        ftp_connection.retrbinary("RETR " + ftp_PathTo_TIF,
                                                                  f.write)  # "RETR %s" % ftp_PathTo_TIF
                                except Exception:
                                    error_counter = error_counter + 1
                                    activity_event_type = Config_Setting.get_value(
                                        setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING",
                                        default_or_error_return_value="ETL Warning")
                                    activity_description = "Warning: There was an error when downloading tif file: " \
                                                           + str(tif_filename) + " from FTP directory: " \
                                                           + str(remote_directory_path) + ".  If the System Error " \
                                                                                          "message says something " \
                                                                                          "like 'nodename nor " \
                                                                                          "servname provided, or " \
                                                                                          "not known', then one " \
                                                                                          "common cause of that " \
                                                                                          "error is an unstable " \
                                                                                          "or disconnected " \
                                                                                          "internet connection.  " \
                                                                                          "Double check that the " \
                                                                                          "internet connection is" \
                                                                                          " working and try again." \
                                                                                          "  System Error Message: " \
                                                           + str(str(sys.exc_info()))
                                    self.etl_parent_pipeline_instance.log_etl_error(
                                        activity_event_type=activity_event_type,
                                        activity_description=activity_description, etl_granule_uuid=Granule_UUID,
                                        is_alert=True, additional_json={
                                            "warning": activity_description,
                                            "is_error": True,
                                            "class_name": self.__class__.__name__,
                                            "function_name": "execute__Step__Download",
                                            "current_object_info": expected_granule
                                        })

                                    # Give the FTP Connection a short break (Server spam protection mitigation)
                        time.sleep(3)

                        # Download the Tfw
                        fx = open(local_FullFilePath_ToSave_Twf, "wb")
                        fx.close()
                        os.chmod(local_FullFilePath_ToSave_Twf, 0o0777)  # 0777
                        try:
                            with open(local_FullFilePath_ToSave_Twf, "wb") as f:
                                ftp_connection.retrbinary("RETR " + ftp_PathTo_TWF,
                                                          f.write)  # "RETR %s" % ftp_PathTo_TIF
                        except Exception:
                            os.remove(local_FullFilePath_ToSave_Twf)
                            local_FullFilePath_ToSave_Twf = local_FullFilePath_ToSave_Twf.replace("03E", "04A")
                            ftp_PathTo_TWF = ftp_PathTo_TWF.replace("03E", "04A")
                            fx = open(local_FullFilePath_ToSave_Twf, "wb")
                            fx.close()
                            os.chmod(local_FullFilePath_ToSave_Twf, 0o0777)  # 0777
                            try:
                                with open(local_FullFilePath_ToSave_Twf, "wb") as f:
                                    ftp_connection.retrbinary("RETR " + ftp_PathTo_TWF,
                                                              f.write)  # "RETR %s" % ftp_PathTo_TIF
                            except Exception:
                                os.remove(local_FullFilePath_ToSave_Twf)
                                ftp_PathTo_TWF = ftp_PathTo_TWF.replace("04A", "04B")
                                local_FullFilePath_ToSave_Twf = local_FullFilePath_ToSave_Twf.replace("04A", "04B")
                                fx = open(local_FullFilePath_ToSave_Twf, "wb")
                                fx.close()
                                os.chmod(local_FullFilePath_ToSave_Twf, 0o0777)  # 0777
                                try:
                                    with open(local_FullFilePath_ToSave_Twf, "wb") as f:
                                        ftp_connection.retrbinary("RETR " + ftp_PathTo_TWF,
                                                                  f.write)  # "RETR %s" % ftp_PathTo_TIF
                                except Exception:
                                    error_counter = error_counter + 1

                                    warning_message = "Warning: There was an error when downloading tfw file: " + str(
                                        tfw_filename) + " from FTP directory: " + str(
                                        remote_directory_path) + ".  If the System Error message says something " \
                                                                 "like 'nodename nor servname provided, or not " \
                                                                 "known', then one common cause of that error is " \
                                                                 "an unstable or disconnected internet connection.  " \
                                                                 "Double check that the internet connection is " \
                                                                 "working and try again.  System Error " \
                                                                 "Message: " + str(str(sys.exc_info()))
                                    # Call Error handler right here to send a warning message to ETL log. - Note this
                                    # warning will not make it back up to the overall pipeline, it is being sent here
                                    # so admin can still be aware of it and handle it. activity_event_type         =
                                    # settings.ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING

                                    self.etl_parent_pipeline_instance.log_etl_error(
                                        activity_event_type=Config_Setting.get_value(
                                            setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING",
                                            default_or_error_return_value="ETL Warning"),
                                        activity_description=warning_message, etl_granule_uuid=Granule_UUID,
                                        is_alert=True, additional_json={
                                            "warning": warning_message,
                                            "is_error": True,
                                            "class_name": self.__class__.__name__,
                                            "function_name": "execute__Step__Download",
                                            "current_object_info": expected_granule
                                        })

                        # Give the FTP Connection a short break (Server spam protection mitigation)
                        time.sleep(3)

                        # Counting Granule downloads, not individual files (in this case, 1 granule is made up from
                        # two files)
                        download_counter = download_counter + 1

                    except Exception:
                        error_counter = error_counter + 1
                        warning_message = "Warning: There was an uncaught error when attempting to download 1 of " \
                                          "these files (tif or tfw), " + str(tif_filename) + ", or" \
                                          " " + str(tfw_filename) + " from FTP " \
                                          "directory: " + str(remote_directory_path) + ".  If the System Error " \
                                          "message says something like 'nodename " \
                                          "nor servname provided, or not known', then one common cause of " \
                                          "that error is an unstable or disconnected internet connection." \
                                          "  Double check that the internet connection is working and try " \
                                          "again.  System Error Message: " + str(str(sys.exc_info()))
                        # Call Error handler right here to send a warning message to ETL log. - Note this warning
                        # will not make it back up to the overall pipeline, it is being sent here so admin can still
                        # be aware of it and handle it. activity_event_type         =
                        # settings.ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING
                        activity_event_type = Config_Setting.get_value(
                            setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING",
                            default_or_error_return_value="ETL Warning")
                        activity_description = warning_message
                        self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type,
                                                                        activity_description=activity_description,
                                                                        etl_granule_uuid=Granule_UUID, is_alert=True,
                                                                        additional_json={
                                                                            "warning": warning_message,
                                                                            "is_error": True,
                                                                            "class_name": self.__class__.__name__,
                                                                            "function_name": "execute__Step__Download",
                                                                            "current_object_info": expected_granule
                                                                        })

            except Exception:
                error_counter = error_counter + 1
                error_message = "imerg.execute__Step__Download: Generic Uncaught Error.  At least 1 download failed." \
                                "  System Error Message: " + str(sys.exc_info())
                detail_errors.append(error_message)
                print(error_message)

                # Maybe in here is an error with sending the warning in an earlier step?
            loop_counter = loop_counter + 1

        # Ended, now for reporting
        ret__event_description = "Success.  Completed Step execute__Step__Download by downloading " + str(
            download_counter).strip() + " files."
        #
        return common.get_function_response_object(class_name=self.__class__.__name__,
                                                   function_name=ret__function_name, is_error=ret__is_error,
                                                   event_description=ret__event_description,
                                                   error_description=ret__error_description,
                                                   detail_state_info={
                                                       "class_name": self.__class__.__name__,
                                                       "download_counter": download_counter,
                                                       "error_counter": error_counter,
                                                       "loop_counter": loop_counter,
                                                       "detail_errors": detail_errors
                                                   })

    def execute__Step__Extract(self):
        ret__function_name = "execute__Step__Extract"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        #
        # TODO: Subtype Specific Logic Here
        #

        return common.get_function_response_object(class_name=self.__class__.__name__,
                                                   function_name=ret__function_name, is_error=ret__is_error,
                                                   event_description=ret__event_description,
                                                   error_description=ret__error_description,
                                                   detail_state_info={
                                                       "class_name": self.__class__.__name__,
                                                       "custom_message": "Imerg types do not need to be extracted.  "
                                                                         "The source files are non-compressed Tif "
                                                                         "and Tfw files. "
                                                   })

    def execute__Step__Transform(self):
        ret__function_name = "execute__Step__Transform"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
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

                    # Getting info ready for the current granule.
                    local_extract_path = expected_granules_object['local_extract_path']
                    tif_filename = expected_granules_object['tif_filename']
                    final_nc4_filename = expected_granules_object['final_nc4_filename']
                    expected_full_path_to_local_extracted_tif_file = os.path.join(local_extract_path, tif_filename)

                    ############################################################
                    # Start extracting data and creating output netcdf file.
                    ############################################################

                    # Based on the geotiffFile name, determine the time string elements.
                    # Split elements by period
                    time_string_split = tif_filename.split('.')  # time_string_split = geotiffFile.split('.')
                    time_string = time_string_split[4].split('-')
                    yyyymmdd = time_string[0]
                    hhmmss = time_string[1]
                    # Determine the timestamp for the data.

                    # Determine starting and ending times. # Error with pandas.Timestamp function (Solution,
                    # just use Datetime to parse this) startTime = pd.Timestamp.strptime(yyyymmdd + hhmmss,
                    # '%Y%m%dS%H%M%S')   # pandas can't seem to use datetime to parse timestrings...
                    start_time = datetime.datetime.strptime(yyyymmdd + hhmmss, '%Y%m%dS%H%M%S')
                    end_time = start_time + pd.Timedelta('29M') + pd.Timedelta('59S')  # 4 weeks (i.e. 28 days)

                    ############################################################
                    # beginning extracting data and creating output netcdf file.
                    ############################################################

                    # 1) Read the geotiff data into an xarray data array
                    tiff_data = xr.open_rasterio(expected_full_path_to_local_extracted_tif_file)
                    # Rescale to accumulated precipitation amount
                    tiff_data = tiff_data / 10.0
                    # 2) Convert to a dataset.  (need to assign a name to the data array)
                    imerg = tiff_data.rename('precipitation_amount').to_dataset()
                    # Handle selecting/adding the dimesions
                    imerg = imerg.isel(band=0).reset_coords('band', drop=True)
                    # select the singleton band dimension and drop out the associated coordinate.
                    # Add the time dimension as a new coordinate.
                    imerg = imerg.assign_coords(time=start_time).expand_dims(dim='time', axis=0)
                    # Add an additional variable "time_bnds" for the time boundaries.
                    imerg['time_bnds'] = xr.DataArray(np.array([start_time, end_time]).reshape((1, 2)),
                                                      dims=['time', 'nbnds'])

                    # 3) Rename and add attributes to this dataset.
                    # # Error, 'inplace' has been removed from xarray.
                    # imerg.rename({'y': 'latitude', 'x': 'longitude'}, inplace=True)  # rename lat/lon
                    # Now making the assignment (look like the above ones)
                    imerg = imerg.rename({'y': 'latitude', 'x': 'longitude'})  # rename lat/lon

                    # Lat/Lon/Time dictionaries.
                    # Use Ordered dict]

                    # missing_data/_FillValue , relative time units etc. are handled as
                    # part of the encoding dictionary used in to_netcdf() call.
                    # 'zlib' and 'complevel' are added to generate compression and reduce file size
                    # Set the Attributes
                    imerg.latitude.attrs = OrderedDict([('long_name', 'latitude'),
                                                        ('units', 'degrees_north'),
                                                        ('axis', 'Y')])
                    imerg.longitude.attrs = OrderedDict([('long_name', 'longitude'),
                                                         ('units', 'degrees_east'),
                                                         ('axis', 'X')])
                    imerg.time.attrs = OrderedDict([('long_name', 'time'),
                                                    ('axis', 'T'),
                                                    ('bounds', 'time_bnds')])
                    imerg.time_bnds.attrs = OrderedDict([('long_name', 'time_bounds')])
                    imerg.precipitation_amount.attrs = OrderedDict([('long_name', 'precipitation_amount'),
                                                                    ('units', 'mm'),
                                                                    ('accumulation_interval', '30 minute'),
                                                                    ('comment',
                                                                     'IMERG 30-minute accumulated rainfall,'
                                                                     ' Early Run')])
                    imerg.attrs = OrderedDict(
                        [('Description',
                          'NASA Integrated Multi-satellitE Retrievals for GPM (IMERG) data product, Early Run.'),
                         ('DateCreated', pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%SZ')),
                         ('Contact', 'Lance Gilliland, lance.gilliland@nasa.gov'),
                         ('Source', 'NASA GPM Precipitation Processing System; '
                                    'https://gpm.nasa.gov/data-access/downloads/gpm; '
                                    'ftp://jsimpson.pps.eosdis.nasa.gov:/data/imerg/gis/early'),
                         ('Version', time_string_split[6]),
                         ('Reference', 'G. Huffman, D. Bolvin, D. Braithwaite, K. Hsu, R. Joyce, P. Xie, '
                                       '2014: Integrated Multi-satellitE Retrievals for GPM (IMERG), version 4.4. '
                                       'NASAs Precipitation Processing Center.'),
                         ('RangeStartTime', start_time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                         ('RangeEndTime', end_time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                         ('SouthernmostLatitude', np.min(imerg.latitude.values)),
                         ('NorthernmostLatitude', np.max(imerg.latitude.values)),
                         ('WesternmostLongitude', np.min(imerg.longitude.values)),
                         ('EasternmostLongitude', np.max(imerg.longitude.values)),
                         ('TemporalResolution', '30-minute'), ('SpatialResolution', '0.1deg')])
                    # Set the Endcodings
                    imerg.precipitation_amount.encoding = {
                        '_FillValue': np.uint16(29999),
                        'missing_value': np.uint16(29999),
                        'dtype': np.dtype('uint16'),
                        'scale_factor': 0.1,
                        'add_offset': 0.0,
                        'zlib': True,
                        'complevel': 7}

                    imerg.time.encoding = {
                        'units': 'seconds since 1970-01-01T00:00:00Z',
                        'dtype': np.dtype('int32')
                    }

                    imerg.time_bnds.encoding = {
                        'units': 'seconds since 1970-01-01T00:00:00Z',
                        'dtype': np.dtype('int32')
                    }

                    # 5) Output File outputFile_name_ORIG_SCRIPT = 'NASA-IMERG_EARLY.' + startTime.strftime(
                    # '%Y%m%dT%H%M%SZ') + '.' + regionID + '.nc4' print("READY FOR OUTPUT FILE!: (
                    # outputFile_name_ORIG_SCRIPT): " + str(outputFile_name_ORIG_SCRIPT)) outputFile =
                    # 'NASA-IMERG_EARLY.' + startTime.strftime('%Y%m%dT%H%M%SZ') + '.' + regionID + '.nc4'
                    # imerg.to_netcdf(outputFile, unlimited_dims='time')
                    imerg.to_netcdf(os.path.join(local_extract_path, final_nc4_filename), unlimited_dims='time')

                    pass

                except Exception:
                    Granule_UUID = expected_granules_object['Granule_UUID']

                    error_message = "imerg.execute__Step__Transform: An Error occurred during the Transform step " \
                                    "with ETL_Granule UUID: " + str(Granule_UUID) + ".  System " \
                                    "Error Message: " + str(sys.exc_info())

                    # Individual Transform Granule Error
                    error_counter = error_counter + 1
                    detail_errors.append(error_message)

                    # Update this Granule for Failure (store the error info in the granule also) Granule_UUID =
                    # expected_granules_object['Granule_UUID'] new__granule_pipeline_state =
                    # settings.GRANULE_PIPELINE_STATE__FAILED  # When a granule has a NC4 file in the correct
                    # location, this counts as a Success.
                    new__granule_pipeline_state = Config_Setting.get_value(
                        setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")  #
                    is_error = True
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(
                        granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state,
                        is_error=is_error)
                    new_json_key_to_append = "execute__Step__Transform"
                    self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(
                        granule_uuid=Granule_UUID, new_json_key_to_append=new_json_key_to_append,
                        sub_jsonable_object={
                            "error_message": error_message,
                            "is_update_succeed": is_update_succeed,
                            "is_update_succeed_2": "Current process"
                        })

                pass

        except Exception:
            error_message = "Error: There was an uncaught error when processing the Transform step on all of the " \
                            "expected Granules.  See the additional data and system error message for details on " \
                            "what caused this error.  System Error Message: " + str(str(sys.exc_info()))
            # Exit Here With Error info loaded up
            return common.get_function_response_object(class_name=self.__class__.__name__,
                                                       function_name=ret__function_name, is_error=True,
                                                       event_description=ret__event_description,
                                                       error_description=error_message,
                                                       detail_state_info={
                                                           "error": error_message,
                                                           "is_error": True,
                                                           "class_name": self.__class__.__name__,
                                                           "function_name": "execute__Step__Transform"
                                                       })

        return common.get_function_response_object(class_name=self.__class__.__name__,
                                                   function_name=ret__function_name, is_error=ret__is_error,
                                                   event_description=ret__event_description,
                                                   error_description=ret__error_description,
                                                   detail_state_info={
                                                       "class_name": self.__class__.__name__,
                                                       "error_counter": error_counter,
                                                       "detail_errors": detail_errors
                                                   })

    def execute__Step__Load(self):
        ret__function_name = "execute__Step__Load"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
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
                    expected_full_path_to_local_working_nc4_file = os.path.join(local_extract_path,
                                                                                final_nc4_filename)
                    # Where the NC4 file was generated during the Transform Step
                    expected_full_path_to_local_final_nc4_file = os.path.join(local_final_load_path,
                                                                              final_nc4_filename)
                    # Where the final NC4 file should be placed for THREDDS Server monitoring

                    # Copy the file from the working directory over to the final location for it.  (Where THREDDS
                    # Monitors for it)
                    copyfile(expected_full_path_to_local_working_nc4_file, expected_full_path_to_local_final_nc4_file)
                    Granule_UUID = expected_granules_object['Granule_UUID']
                    # new__granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__SUCCESS # When a granule has a
                    # NC4 file in the correct location, this counts as a Success.
                    new__granule_pipeline_state = Config_Setting.get_value(
                        setting_name="GRANULE_PIPELINE_STATE__SUCCESS", default_or_error_return_value="SUCCESS")  #
                    is_error = False
                    self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(
                        granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state,
                        is_error=is_error)

                    granule_name = final_nc4_filename
                    granule_contextual_information = ""
                    self.etl_parent_pipeline_instance.create_or_update_Available_Granule(
                        granule_name=granule_name,
                        granule_contextual_information=granule_contextual_information,
                        additional_json={
                            "MostRecent__ETL_Granule_UUID": str(Granule_UUID).strip()
                        })

                except Exception:
                    error_message = "Error: There was an error when attempting to copy the current nc4 file to it's " \
                                    "final directory location.  See the additional data and system error message for " \
                                    "details on what caused this error.  System Error " \
                                    "Message: " + str(str(sys.exc_info()))
                    # Update this Granule for Failure (store the error info in the granule also)
                    Granule_UUID = expected_granules_object['Granule_UUID']
                    # new__granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__FAILED  # When a granule has a
                    # NC4 file in the correct location, this counts as a Success.
                    new__granule_pipeline_state = Config_Setting.get_value(
                        setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")
                    self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(
                        granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state,
                        is_error=True)
                    self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(
                        granule_uuid=Granule_UUID, new_json_key_to_append="execute__Step__Load",
                        sub_jsonable_object={
                            "error": error_message,
                            "is_error": True,
                            "class_name": self.__class__.__name__,
                            "function_name": "execute__Step__Load",
                            "expected_full_path_to_local_working_nc4_file": str(
                                expected_full_path_to_local_working_nc4_file).strip(),
                            "expected_full_path_to_local_final_nc4_file": str(
                                expected_full_path_to_local_final_nc4_file).strip()
                        })

            pass
        except Exception:
            error_message = "Error: There was an uncaught error when processing the Load step on all of the expected " \
                            "Granules.  See the additional data and system error message for details on what caused " \
                            "this error.  System Error Message: " + str(str(sys.exc_info()))
            # Exit Here With Error info loaded up
            return common.get_function_response_object(class_name=self.__class__.__name__,
                                                       function_name=ret__function_name, is_error=True,
                                                       event_description=ret__event_description,
                                                       error_description=error_message,
                                                       detail_state_info={
                                                           "is_error": True,
                                                           "error": error_message,
                                                           "class_name": self.__class__.__name__,
                                                           "function_name": "execute__Step__Load"
                                                       })

        return common.get_function_response_object(class_name=self.__class__.__name__,
                                                   function_name=ret__function_name, is_error=ret__is_error,
                                                   event_description=ret__event_description,
                                                   error_description=ret__error_description,
                                                   detail_state_info={
                                                       "class_name": self.__class__.__name__,
                                                       "is_error": False,
                                                       "function_name": "execute__Step__Load"
                                                   })

    def execute__Step__Post_ETL_Custom(self):
        #
        # TODO: Subtype Specific Logic Here
        #
        return common.get_function_response_object(class_name=self.__class__.__name__,
                                                   function_name="execute__Step__Post_ETL_Custom", is_error=False,
                                                   event_description="",
                                                   error_description="",
                                                   detail_state_info={
                                                       "custom_message": "No logic was written here",
                                                       "function_name": "execute__Step__Post_ETL_Custom"
                                                   })

    def execute__Step__Clean_Up(self):
        ret__function_name = "execute__Step__Clean_Up"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        #
        # TODO: Subtype Specific Logic Here
        #
        try:
            temp_working_dir = str(self.temp_working_dir).strip()
            if temp_working_dir == "":

                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                # activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_BLANK
                activity_event_type = Config_Setting.get_value(
                    setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_BLANK",
                    default_or_error_return_value="Temp Working Dir Blank")  #
                activity_description = "Could not remove the temporary working directory.  The value for " \
                                       "self.temp_working_dir was blank. "
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = "imerg"
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type,
                                                                activity_description=activity_description,
                                                                etl_granule_uuid="", is_alert=False,
                                                                additional_json=additional_json)

            else:
                # shutil.rmtree
                rmtree(temp_working_dir)

                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                # activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_REMOVED
                activity_event_type = Config_Setting.get_value(
                    setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_REMOVED",
                    default_or_error_return_value="Temp Working Dir Removed")  #
                activity_description = "Temp Working Directory, " + str(
                    self.temp_working_dir).strip() + ", was removed."
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = "imerg"
                additional_json['temp_working_dir'] = str(temp_working_dir).strip()
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type,
                                                                activity_description=activity_description,
                                                                etl_granule_uuid="", is_alert=False,
                                                                additional_json=additional_json)

        except Exception:
            error_message = "Error: There was an uncaught error when processing the Clean Up step.  This function is " \
                            "supposed to simply remove the working directory.  This means the working directory was " \
                            "not removed.  See the additional data and system error message for details on what " \
                            "caused this error.  System Error Message: " + str(str(sys.exc_info()))
            # Exit Here With Error info loaded up
            return common.get_function_response_object(class_name=self.__class__.__name__,
                                                       function_name=ret__function_name, is_error=True,
                                                       event_description=ret__event_description,
                                                       error_description=error_message,
                                                       detail_state_info={
                                                           "error": error_message,
                                                           "is_error": True,
                                                           "class_name": self.__class__.__name__,
                                                           "function_name": "execute__Step__Clean_Up",
                                                           "self__temp_working_dir": str(self.temp_working_dir).strip()
                                                       })

        return common.get_function_response_object(class_name=self.__class__.__name__,
                                                   function_name=ret__function_name, is_error=ret__is_error,
                                                   event_description=ret__event_description,
                                                   error_description=ret__error_description,
                                                   detail_state_info={
                                                       "class_name": self.__class__.__name__,
                                                       "function_name": "execute__Step__Clean_Up",
                                                       "is_error": False
                                                   })

    @staticmethod
    def test_class_instance():
        print("imerg.test_class_instance: Reached the end.")
