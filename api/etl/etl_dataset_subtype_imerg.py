import datetime, ftplib, os, shutil, sys, time
import xarray as xr
import pandas as pd
import numpy as np
from collections import OrderedDict

from .common import common
from .etl_dataset_subtype_interface import ETL_Dataset_Subtype_Interface

from api.services import Config_SettingService
from ..models import Config_Setting

class ETL_Dataset_Subtype_IMERG(ETL_Dataset_Subtype_Interface):

    # init (Passing a reference from the calling class, so we can callback the error handler)
    def __init__(self, etl_parent_pipeline_instance=None, dataset_subtype=None):
        self.etl_parent_pipeline_instance = etl_parent_pipeline_instance
        self.class_name = self.__class__.__name__
        self._expected_remote_full_file_paths = []
        self._expected_granules = []
        if dataset_subtype == 'imerg_early':
            self.mode = 'EARLY'
        elif dataset_subtype == 'imerg_late':
            self.mode = 'LATE'
        else:
            self.mode = 'LATE'

    # Set default parameters or using default
    def set_optional_parameters(self, params):
        today = datetime.date.today()
        self.YYYY__Year__Start = params.get('YYYY__Year__Start') or today.year
        self.YYYY__Year__End = params.get('YYYY__Year__End') or today.year
        self.MM__Month__Start = params.get('MM__Month__Start') or 1
        self.MM__Month__End = params.get('MM__Month__End') or today.month
        self.DD__Day__Start = params.get('DD__Day__Start') or 1
        self.DD__Day__End = params.get('DD__Day__End') or today.day
        self.NN__30MinIncrement__Start = params.get('NN__30MinIncrement__Start', 0) or 0
        self.NN__30MinIncrement__End = params.get('NN__30MinIncrement__End', 2) or 2

    def execute__Step__Pre_ETL_Custom(self):
        ret__function_name = "execute__Step__Pre_ETL_Custom"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        self.temp_working_dir = self.etl_parent_pipeline_instance.dataset.temp_working_dir
        final_load_dir_path = self.etl_parent_pipeline_instance.dataset.final_load_dir
        current_root_http_path = self.etl_parent_pipeline_instance.dataset.source_url

        # (1) Generate Expected remote file paths
        try:

            # Create the list of Days (From start time to end time)
            start_date = datetime.datetime(self.YYYY__Year__Start, self.MM__Month__Start, self.DD__Day__Start)
            end_date = datetime.datetime(self.YYYY__Year__End, self.MM__Month__End, self.DD__Day__End)

            delta = end_date - start_date
            # print('DELTA: {}'.format(str(delta)))

            for i in range(delta.days + 1):

                current_date = start_date + datetime.timedelta(days=i)
                current_year__YYYY_str = "{:0>4d}".format(current_date.year)
                current_month__MM_str = "{:02d}".format(current_date.month)
                current_day__DD_str = "{:02d}".format(current_date.day)

                # Get the Current Day, Start '30 min' increment
                start_30min_increment = 0
                end_30min_increment = 24 * 2

                if i == 0:
                    # We are on the FIRST day, check the start and end 30 min increments
                    start_30min_increment = self.NN__30MinIncrement__Start
                    # print("FIRST DAY Loop: (i): " + str(i) + ", (start_30min_increment): " + str(start_30min_increment))

                if i == delta.days:
                    # We are on the LAST day, check the start and end 30 min increments
                    end_30min_increment = self.NN__30MinIncrement__End
                    # print("LAST DAY Loop: (i): " + str(i) + ", (end_30min_increment): " + str(end_30min_increment))

                # Analyzing the Dates
                #
                # # April 2nd, 2020, FIRST 30 min item of the day
                # # # 3B-HHR-L.MS.MRG.3IMERG.20200402-S000000-E002959.0000.V06B.30min.tfw
                #
                # # April 2nd, 2020, LAST 30 min item of the day
                # # # 3B-HHR-L.MS.MRG.3IMERG.20200402-S233000-E235959.1410.V06B.30min.tfw

                for j in range(start_30min_increment, end_30min_increment):

                    increment_minute_value = j * 30                                                                 # 1410
                    increment_minute_value_4Char_Str ="{:0>4d}".format(increment_minute_value)                      # "1410"    # Note: A number like 0 gets turned into "0000"
                    increment_minute_value__Div_60 = float(increment_minute_value / 60)                             # 23.5
                    both_hh_str     = "{:0>2d}".format(int(increment_minute_value__Div_60))                         # "23"      # Note: A number like 0 gets turned into "00"
                    hh_remainder    = float(increment_minute_value__Div_60 - int(increment_minute_value__Div_60))   # 0.5
                    start_mm_str    = "00"  # Correct if hh_remainder is NOT 0.5
                    end_mm_str      = "29"  # Correct if hh_remainder is NOT 0.5
                    if hh_remainder == 0.5:
                        start_mm_str    = "30"
                        end_mm_str      = "59"
                    start_ss_str    = "00"
                    end_ss_str      = "59"

                    base_filename = '3B-HHR-{}.MS.MRG.3IMERG.{}{}{}-S{}{}{}-E{}{}{}.V06B.30min'.format(
                        'L' if self.mode == 'LATE' else 'E',
                        current_year__YYYY_str,
                        current_month__MM_str,
                        current_day__DD_str,
                        both_hh_str,
                        start_mm_str,
                        start_ss_str,
                        both_hh_str,
                        end_mm_str,
                        end_ss_str,
                        str(increment_minute_value_4Char_Str)
                    )

                    tfw_filename = '{}.tfw'.format(base_filename)
                    tif_filename = '{}.tif'.format(base_filename)

                    # Create the final nc4 filename
                    # nasa-imerg-late.20200531T000000Z.global.0.1deg.30min.nc4
                    final_nc4_filename = 'nasa-imerg-{}.{}{}{}T{}{}{}Z.global.0.1deg.30min.nc4'.format(
                        'late' if self.mode == 'LATE' else 'early',
                        current_year__YYYY_str,
                        current_month__MM_str,
                        current_day__DD_str,
                        both_hh_str,
                        start_mm_str,
                        start_ss_str
                    )

                    # Now get the remote File Paths (Directory) based on the date infos.
                    # Add the Year and Month to the directory path.
                    remote_directory_path = '{}/{}/{}/'.format(current_root_http_path, current_year__YYYY_str, current_month__MM_str)

                    # Getting full paths
                    remote_full_filepath_tif = str(os.path.join(remote_directory_path, tif_filename)).strip()
                    remote_full_filepath_tfw = str(os.path.join(remote_directory_path, tfw_filename)).strip()
                    local_full_filepath_tif = os.path.join(self.temp_working_dir, tif_filename)
                    local_full_filepath_tfw = os.path.join(self.temp_working_dir, tfw_filename)

                    local_full_filepath_final_nc4_file = os.path.join(final_load_dir_path, current_year__YYYY_str, final_nc4_filename)

                    is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(os.path.join(final_load_dir_path, current_year__YYYY_str))

                    current_obj = {}

                    # Loop and Filename building info
                    current_obj['j'] = j
                    current_obj['increment_minute_value']           = increment_minute_value
                    current_obj['increment_minute_value_4Char_Str'] = increment_minute_value_4Char_Str
                    current_obj['increment_minute_value__Div_60']   = increment_minute_value__Div_60
                    current_obj['both_hh_str']                      = both_hh_str
                    current_obj['start_mm_str']                     = start_mm_str
                    current_obj['end_mm_str']                       = end_mm_str
                    current_obj['start_ss_str']                     = start_ss_str
                    current_obj['end_ss_str']                       = end_ss_str

                    # Date Info (Which Day Is it?)
                    current_obj['date_YYYY']    = current_year__YYYY_str
                    current_obj['date_MM']      = current_month__MM_str
                    current_obj['date_DD']      = current_day__DD_str

                    # Filename and Granule Name info
                    local_extract_path      = self.temp_working_dir  # There is no extract step, so just using the working directory as the local extract path.
                    local_final_load_path   = final_load_dir_path
                    current_obj['local_extract_path']               = local_extract_path        # Download path
                    current_obj['local_final_load_path']            = local_final_load_path     # The path where the final output granule file goes.
                    current_obj['remote_directory_path']            = remote_directory_path
                    current_obj['base_filename']                    = base_filename
                    current_obj['tfw_filename']                     = tfw_filename
                    current_obj['tif_filename']                     = tif_filename
                    current_obj['final_nc4_filename']               = final_nc4_filename
                    current_obj['granule_name']                     = final_nc4_filename

                    # Full Paths
                    current_obj['remote_full_filepath_tif']             = remote_full_filepath_tif
                    current_obj['remote_full_filepath_tfw']             = remote_full_filepath_tfw
                    current_obj['local_full_filepath_tif']              = local_full_filepath_tif
                    current_obj['local_full_filepath_tfw']              = local_full_filepath_tfw
                    current_obj['local_full_filepath_final_nc4_file']   = local_full_filepath_final_nc4_file

                    granule_name = final_nc4_filename
                    granule_contextual_information = ''
                    granule_pipeline_state = Config_SettingService.get_value(setting_name="GRANULE_PIPELINE_STATE__ATTEMPTING", default_or_error_return_value="Attempting")  # settings.GRANULE_PIPELINE_STATE__ATTEMPTING

                    additional_json = current_obj
                    new_granule_uuid = self.etl_parent_pipeline_instance.log_etl_granule(
                        granule_name=granule_name,
                        granule_contextual_information=granule_contextual_information,
                        granule_pipeline_state=granule_pipeline_state,
                        additional_json=additional_json
                    )

                    # Save the Granule's UUID for reference in later steps
                    current_obj['Granule_UUID'] = str(new_granule_uuid).strip()

                    # Add to the granules list
                    self._expected_granules.append(current_obj)

        except:
            sysErrorData = str(sys.exc_info())
            print(sysErrorData)
            error_JSON = {}
            error_JSON['error'] = "Error: There was an error when generating the expected remote filepaths.  See the additional data for details on which expected file caused the error.  System Error Message: " + str(sysErrorData)
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
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(self.temp_working_dir)
        if is_error_creating_directory == True:
            error_JSON = {}
            error_JSON['error'] = "Error: There was an error when the pipeline tried to create a new directory on the filesystem.  The path that the pipeline tried to create was: " + str(self.temp_working_dir) + ".  There should be another error logged just before this one that contains system error info.  That info should give clues to why the directory was not able to be created."
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

        # Connect to FTP
        ftp_host            = Config_Setting.get_value(setting_name="FTP_CREDENTIAL_IMERG__HOST", default_or_error_return_value="error.getting.ftp-host.nasa.gov")
        ftp_username        = Config_Setting.get_value(setting_name="FTP_CREDENTIAL_IMERG__USER", default_or_error_return_value="error_getting_user_name")
        ftp_userpass        = Config_Setting.get_value(setting_name="FTP_CREDENTIAL_IMERG__PASS", default_or_error_return_value="error_getting_user_password")

        # Attempt Making FTP Connection here (if fail, then exit this function with an error
        ftp_connection = None

        # Connect to the FTP Server and download all of the files in the list.
        try:
            ftp_connection = ftplib.FTP_TLS(host=ftp_host, user=ftp_username, passwd=ftp_userpass)
            ftp_connection.prot_p()
            ftp_connection.voidcmd('TYPE I')

            time.sleep(1)

        except Exception as e:
            print(e)
            error_counter = error_counter + 1
            sysErrorData = str(sys.exc_info())
            error_message = "imerg.execute__Step__Download: Error Connecting to FTP.  There was an error when attempting to connect to the Remote FTP Server.  System Error Message: " + str(sysErrorData)
            detail_errors.append(error_message)
            print(error_message)

            # Ended, now for reporting
            ret__detail_state_info['class_name'] = self.__class__.__name__
            ret__detail_state_info['download_counter'] = download_counter
            ret__detail_state_info['error_counter'] = error_counter
            ret__detail_state_info['loop_counter'] = loop_counter
            ret__detail_state_info['detail_errors'] = detail_errors
            # ret__detail_state_info['number_of_expected_remote_full_file_paths'] = str(len(self._expected_remote_full_file_paths)).strip()
            # ret__detail_state_info['number_of_expected_granules'] = str(len(self._expected_granules)).strip()
            ret__event_description = "Error During Step execute__Step__Download by downloading " + str(download_counter).strip() + " files."
            #
            retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
            return retObj

        for expected_granule in expected_granules:

            try:
                if (loop_counter + 1) % modulus_size == 0:
                    event_message = "About to download file: " + str(loop_counter + 1) + " out of " + str(num_of_objects_to_process)
                    print(event_message)
                    activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS", default_or_error_return_value="ETL Download Progress")
                    activity_description = event_message
                    additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                    self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

                # Current Granule to download
                remote_directory_path       = expected_granule['remote_directory_path']
                tfw_filename                = expected_granule['tfw_filename']
                tif_filename                = expected_granule['tif_filename']
                local_full_filepath_tif     = expected_granule['local_full_filepath_tif']
                local_full_filepath_tfw     = expected_granule['local_full_filepath_tfw']

                # Granule info
                Granule_UUID = expected_granule['Granule_UUID']

                print(remote_directory_path)

                # FTP Processes
                # # 1 - Change Directory to the directory path
                ftp_connection.cwd(remote_directory_path)

                # TODO - Fix the problems with checking if a file exists        START
                # # 2 - Check to see if the files exists
                hasFiles = False
                filelist = []  # to store all files
                ftp_connection.retrlines('LIST', filelist.append)  # append to list
                file_found_count = 0
                # Looking for two specific file matches out of the whole list of files in the current remote directory
                for f in filelist:
                    if tfw_filename in f:
                        file_found_count = file_found_count + 1
                    if tif_filename in f:
                        file_found_count = file_found_count + 1

                if file_found_count == 2:
                    hasFiles = True

                if hasFiles == False:
                    print("Could not find both TIF and TFW files in the directory.")

                # # Let's assume the files DO exist on the remote server - until we can get the rest of the stuff working. hasFiles = True

                if hasFiles == True:

                    # Remote paths (where the files are coming from)
                    ftp_PathTo_TIF = tif_filename
                    ftp_PathTo_TWF = tfw_filename
                    # Local Paths (Where the files are being saved)
                    local_FullFilePath_ToSave_Tif = local_full_filepath_tif
                    local_FullFilePath_ToSave_Twf = local_full_filepath_tfw

                    try:
                        # Download the Tif
                        print(local_FullFilePath_ToSave_Tif)
                        fx = open(local_FullFilePath_ToSave_Tif, "wb")
                        fx.close()
                        os.chmod(local_FullFilePath_ToSave_Tif, 0o0777)  # 0777

                        try:
                            with open(local_FullFilePath_ToSave_Tif, "wb") as f:
                                ftp_connection.retrbinary("RETR " + ftp_PathTo_TIF, f.write)  # "RETR %s" % ftp_PathTo_TIF
                        except:
                            os.remove(local_FullFilePath_ToSave_Tif)
                            local_FullFilePath_ToSave_Tif = local_FullFilePath_ToSave_Tif.replace("03E", "04A")
                            ftp_PathTo_TIF = ftp_PathTo_TIF.replace("03E", "04A")
                            fx = open(local_FullFilePath_ToSave_Tif, "wb")
                            fx.close()
                            os.chmod(local_FullFilePath_ToSave_Tif, 0o0777)  # 0777
                            try:
                                with open(local_FullFilePath_ToSave_Tif, "wb") as f:
                                    ftp_connection.retrbinary("RETR " + ftp_PathTo_TIF, f.write)  # "RETR %s" % ftp_PathTo_TIF
                            except:
                                os.remove(local_FullFilePath_ToSave_Tif)
                                ftp_PathTo_TIF = ftp_PathTo_TIF.replace("04A", "04B")
                                local_FullFilePath_ToSave_Tif = local_FullFilePath_ToSave_Tif.replace("04A", "04B")
                                fx = open(local_FullFilePath_ToSave_Tif, "wb")
                                fx.close()
                                os.chmod(local_FullFilePath_ToSave_Tif, 0o0777)  # 0777
                                try:
                                    with open(local_FullFilePath_ToSave_Tif, "wb") as f:
                                        ftp_connection.retrbinary("RETR " + ftp_PathTo_TIF, f.write)  # "RETR %s" % ftp_PathTo_TIF
                                except:
                                    error_counter = error_counter + 1
                                    sysErrorData = str(sys.exc_info())
                                    # print("DEBUG Warn: (WARN LEVEL) (File can not be downloaded).  System Error Message: " + str(sysErrorData))
                                    warn_JSON = {}
                                    warn_JSON['warning'] = "Warning: There was an error when downloading tif file: " + str(tif_filename) + " from FTP directory: " + str(remote_directory_path) + ".  If the System Error message says something like 'nodename nor servname provided, or not known', then one common cause of that error is an unstable or disconnected internet connection.  Double check that the internet connection is working and try again.  System Error Message: " + str(sysErrorData)
                                    warn_JSON['is_error'] = True
                                    warn_JSON['class_name'] = "imerg"
                                    warn_JSON['function_name'] = "execute__Step__Download"
                                    warn_JSON['current_object_info'] = expected_granule  # expected_remote_file_path_object
                                    # Call Error handler right here to send a warning message to ETL log. - Note this warning will not make it back up to the overall pipeline, it is being sent here so admin can still be aware of it and handle it.
                                    activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING", default_or_error_return_value="ETL Warning")
                                    activity_description = warn_JSON['warning']
                                    self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid=Granule_UUID, is_alert=True, additional_json=warn_JSON)

                        # Give the FTP Connection a short break (Server spam protection mitigation)
                        time.sleep(3)

                        # Download the Tfw
                        print(local_FullFilePath_ToSave_Twf)
                        fx = open(local_FullFilePath_ToSave_Twf, "wb")
                        fx.close()
                        os.chmod(local_FullFilePath_ToSave_Twf, 0o0777)  # 0777
                        try:
                            with open(local_FullFilePath_ToSave_Twf, "wb") as f:
                                ftp_connection.retrbinary("RETR " + ftp_PathTo_TWF, f.write)  # "RETR %s" % ftp_PathTo_TIF
                        except:
                            os.remove(local_FullFilePath_ToSave_Twf)
                            local_FullFilePath_ToSave_Twf = local_FullFilePath_ToSave_Twf.replace("03E", "04A")
                            ftp_PathTo_TWF = ftp_PathTo_TWF.replace("03E", "04A")
                            fx = open(local_FullFilePath_ToSave_Twf, "wb")
                            fx.close()
                            os.chmod(local_FullFilePath_ToSave_Twf, 0o0777)  # 0777
                            try:
                                with open(local_FullFilePath_ToSave_Twf, "wb") as f:
                                    ftp_connection.retrbinary("RETR " + ftp_PathTo_TWF, f.write)  # "RETR %s" % ftp_PathTo_TIF
                            except:
                                os.remove(local_FullFilePath_ToSave_Twf)
                                ftp_PathTo_TWF = ftp_PathTo_TWF.replace("04A", "04B")
                                local_FullFilePath_ToSave_Twf = local_FullFilePath_ToSave_Twf.replace("04A", "04B")
                                fx = open(local_FullFilePath_ToSave_Twf, "wb")
                                fx.close()
                                os.chmod(local_FullFilePath_ToSave_Twf, 0o0777)  # 0777
                                try:
                                    with open(local_FullFilePath_ToSave_Twf, "wb") as f:
                                        ftp_connection.retrbinary("RETR " + ftp_PathTo_TWF, f.write)  # "RETR %s" % ftp_PathTo_TIF
                                except:
                                    error_counter = error_counter + 1
                                    sysErrorData = str(sys.exc_info())
                                    # print("DEBUG Warn: (WARN LEVEL) (File can not be downloaded).  System Error Message: " + str(sysErrorData))
                                    warn_JSON = {}
                                    warn_JSON['warning'] = "Warning: There was an error when downloading tfw file: " + str(tfw_filename) + " from FTP directory: " + str(remote_directory_path) + ".  If the System Error message says something like 'nodename nor servname provided, or not known', then one common cause of that error is an unstable or disconnected internet connection.  Double check that the internet connection is working and try again.  System Error Message: " + str(sysErrorData)
                                    warn_JSON['is_error'] = True
                                    warn_JSON['class_name'] = "imerg"
                                    warn_JSON['function_name'] = "execute__Step__Download"
                                    warn_JSON['current_object_info'] = expected_granule  # expected_remote_file_path_object
                                    # Call Error handler right here to send a warning message to ETL log. - Note this warning will not make it back up to the overall pipeline, it is being sent here so admin can still be aware of it and handle it.
                                    activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING", default_or_error_return_value="ETL Warning")
                                    activity_description = warn_JSON['warning']
                                    self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid=Granule_UUID, is_alert=True, additional_json=warn_JSON)

                        # Give the FTP Connection a short break (Server spam protection mitigation)
                        time.sleep(3)

                        # Counting Granule downloads, not individual files (in this case, 1 granule is made up from two files)
                        download_counter = download_counter + 1

                    except:
                        error_counter = error_counter + 1
                        sysErrorData = str(sys.exc_info())
                        # print("DEBUG Warn: (WARN LEVEL) (File can not be downloaded).  System Error Message: " + str(sysErrorData))
                        warn_JSON = {}
                        warn_JSON['warning']                = "Warning: There was an uncaught error when attempting to download 1 of these files (tif or tfw), "+str(tif_filename)+", or "+str(tfw_filename)+" from FTP directory: " +str(remote_directory_path)+ ".  If the System Error message says something like 'nodename nor servname provided, or not known', then one common cause of that error is an unstable or disconnected internet connection.  Double check that the internet connection is working and try again.  System Error Message: " + str(sysErrorData)
                        warn_JSON['is_error']               = True
                        warn_JSON['class_name']             = "imerg"
                        warn_JSON['function_name']          = "execute__Step__Download"
                        warn_JSON['current_object_info']    = expected_granule
                        # Call Error handler right here to send a warning message to ETL log. - Note this warning will not make it back up to the overall pipeline, it is being sent here so admin can still be aware of it and handle it.
                        activity_event_type         = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING", default_or_error_return_value="ETL Warning")
                        activity_description        = warn_JSON['warning']
                        self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid=Granule_UUID, is_alert=True, additional_json=warn_JSON)

            except:
                error_counter = error_counter + 1
                sysErrorData = str(sys.exc_info())
                error_message = "imerg.execute__Step__Download: Generic Uncaught Error.  At least 1 download failed.  System Error Message: " + str(sysErrorData)
                detail_errors.append(error_message)
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

        # For IMERG, there is nothing to extract (we are already downloading TIF and TFW files directly)
        ret__detail_state_info['class_name'] = self.__class__.__name__
        ret__detail_state_info['custom_message'] = "Imerg types do not need to be extracted.  The source files are non-compressed Tif and Tfw files."

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Transform(self):
        ret__function_name = "execute__Step__Transform"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        # error_counter, detail_errors
        error_counter = 0
        detail_errors = []

        try:

            for expected_granules_object in self._expected_granules:
                try:

                    # Getting info ready for the current granule.
                    local_extract_path = expected_granules_object['local_extract_path']
                    tif_filename = expected_granules_object['tif_filename']
                    final_nc4_filename = expected_granules_object['final_nc4_filename']
                    expected_full_path_to_local_extracted_tif_file = os.path.join(local_extract_path, tif_filename)

                    geotiffFile_FullPath = expected_full_path_to_local_extracted_tif_file

                    # Matching to the other script
                    #geotiffFile = tif_filename   # Use Tif Filename for doing string split stuff  # tif_filename
                    #geotiffFile_FullPath
                    # geotiffFile Sometimes is tif_filename
                    # geotiffFile Sometimes is geotiffFile_FullPath

                    ############################################################
                    # Start extracting data and creating output netcdf file.
                    ############################################################

                    # !/usr/bin/env python
                    # Program: Convert NASA IMERG Early 30-min rainfall accumulation geoTiff files into netCDF-4 for storage on the ClimateSERV 2.0 thredds data server.
                    # Calling: imergEarly2netcdf.py geotiffFile
                    # geotiffFile: The inputfile to be processed.
                    #
                    # General Flow:
                    # Determine the date associated with the geoTiff file.
                    # 1) Use  xarray+rasterio to read the geotiff data from a file into a data array.
                    # 2) Convert to a dataset and add an appropriate time dimension
                    # 3) Clean up the dataset: Rename and add dimensions, attributes, and scaling factors as appropriate.
                    # 4) Dump the dataset to a netCDF-4 file with a filename conforming to the ClimateSERV 2.0 TDS conventions.

                    # Set region ID
                    regionID = 'Global'

                    # Based on the geotiffFile name, determine the time string elements.
                    # Split elements by period
                    TimeStrSplit = tif_filename.split('.') #TimeStrSplit = geotiffFile.split('.') 
                    TimeStr = TimeStrSplit[4].split('-')
                    yyyymmdd = TimeStr[0]
                    hhmmss = TimeStr[1]
                    # Set versionID
                    versionID = TimeStrSplit[6]
                    # Determine the timestamp for the data.

                    # Determine starting and ending times.
                    startTime = datetime.datetime.strptime(yyyymmdd + hhmmss, '%Y%m%dS%H%M%S')
                    endTime = startTime + pd.Timedelta('29 minutes 59 seconds')  # 4 weeks (i.e. 28 days)

                    ############################################################
                    # Beging extracting data and creating output netcdf file.
                    ############################################################

                    # 1) Read the geotiff data into an xarray data array
                    tiffData = xr.open_rasterio(geotiffFile_FullPath) # tiffData = xr.open_rasterio(geotiffFile)
                    # Rescale to accumulated precipitation amount
                    tiffData = tiffData / 10.0
                    # 2) Convert to a dataset.  (need to assign a name to the data array)
                    imerg = tiffData.rename('precipitation_amount').to_dataset()
                    # Handle selecting/adding the dimesions
                    imerg = imerg.isel(band=0).reset_coords('band', drop=True)  # select the singleton band dimension and drop out the associated coordinate.
                    # Add the time dimension as a new coordinate.
                    imerg = imerg.assign_coords(time=startTime).expand_dims(dim='time', axis=0)
                    # Add an additional variable "time_bnds" for the time boundaries.
                    imerg['time_bnds'] = xr.DataArray(np.array([startTime, endTime]).reshape((1, 2)), dims=['time', 'nbnds'])

                    # 3) Rename and add attributes to this dataset.
                    # # Error, 'inplace' has been removed from xarray.
                    #imerg.rename({'y': 'latitude', 'x': 'longitude'}, inplace=True)  # rename lat/lon
                    # Now making the assignment (look like the above ones)
                    imerg = imerg.rename({'y': 'latitude', 'x': 'longitude'})  # rename lat/lon

                    # Lat/Lon/Time dictionaries.
                    # Use Ordered dict
                    latAttr = OrderedDict([('long_name', 'latitude'), ('units', 'degrees_north'), ('axis', 'Y')])
                    lonAttr = OrderedDict([('long_name', 'longitude'), ('units', 'degrees_east'), ('axis', 'X')])
                    timeAttr = OrderedDict([('long_name', 'time'), ('axis', 'T'), ('bounds', 'time_bnds')])
                    timeBoundsAttr = OrderedDict([('long_name', 'time_bounds')])
                    precipAttr = OrderedDict([('long_name', 'precipitation_amount'), ('units', 'mm'), ('accumulation_interval', '30 minute'), ('comment', 'IMERG 30-minute accumulated rainfall, Early Run')])
                    fileAttr = OrderedDict(
                        [('Description', 'NASA Integrated Multi-satellitE Retrievals for GPM (IMERG) data product, Early Run.'), ('DateCreated', pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%SZ')), ('Contact', 'Lance Gilliland, lance.gilliland@nasa.gov'), ('Source', 'NASA GPM Precipitation Processing System; https://gpm.nasa.gov/data-access/downloads/gpm; ftp://jsimpson.pps.eosdis.nasa.gov:/data/imerg/gis/early'), ('Version', versionID), ('Reference', 'G. Huffman, D. Bolvin, D. Braithwaite, K. Hsu, R. Joyce, P. Xie, 2014: Integrated Multi-satellitE Retrievals for GPM (IMERG), version 4.4. NASAs Precipitation Processing Center.'), ('RangeStartTime', startTime.strftime('%Y-%m-%dT%H:%M:%SZ')), ('RangeEndTime', endTime.strftime('%Y-%m-%dT%H:%M:%SZ')), ('SouthernmostLatitude', np.min(imerg.latitude.values)), ('NorthernmostLatitude', np.max(imerg.latitude.values)), ('WesternmostLongitude', np.min(imerg.longitude.values)), ('EasternmostLongitude', np.max(imerg.longitude.values)), \
                         ('TemporalResolution', '30-minute'), ('SpatialResolution', '0.1deg')])

                    # missing_data/_FillValue , relative time units etc. are handled as part of the encoding dictionary used in to_netcdf() call.
                    # 'zlib' and 'complevel' are added to generate compression and reduce file size
                    precipEncoding = {'_FillValue': np.uint16(29999), 'missing_value': np.uint16(29999), 'dtype': np.dtype('uint16'), 'scale_factor': 0.1, 'add_offset': 0.0, 'zlib': True, 'complevel': 7}
                    timeEncoding = {'units': 'seconds since 1970-01-01T00:00:00Z','dtype':np.dtype('int32')}
                    timeBoundsEncoding = {'units': 'seconds since 1970-01-01T00:00:00Z','dtype':np.dtype('int32')}
                    # Set the Attributes
                    imerg.latitude.attrs = latAttr
                    imerg.longitude.attrs = lonAttr
                    imerg.time.attrs = timeAttr
                    imerg.time_bnds.attrs = timeBoundsAttr
                    imerg.precipitation_amount.attrs = precipAttr
                    imerg.attrs = fileAttr
                    # Set the Endcodings
                    imerg.precipitation_amount.encoding = precipEncoding
                    imerg.time.encoding = timeEncoding
                    imerg.time_bnds.encoding = timeBoundsEncoding

                    # 5) Output File
                    #outputFile_name_ORIG_SCRIPT = 'NASA-IMERG_EARLY.' + startTime.strftime('%Y%m%dT%H%M%SZ') + '.' + regionID + '.nc4'
                    #print("READY FOR OUTPUT FILE!: (outputFile_name_ORIG_SCRIPT): " + str(outputFile_name_ORIG_SCRIPT))
                    #outputFile = 'NASA-IMERG_EARLY.' + startTime.strftime('%Y%m%dT%H%M%SZ') + '.' + regionID + '.nc4'
                    #imerg.to_netcdf(outputFile, unlimited_dims='time')
                    outputFile_FullPath = os.path.join(local_extract_path, final_nc4_filename)
                    imerg.to_netcdf(outputFile_FullPath, unlimited_dims='time')

                except:

                    sysErrorData = str(sys.exc_info())

                    Granule_UUID = expected_granules_object['Granule_UUID']

                    error_message = "imerg.execute__Step__Transform: An Error occurred during the Transform step with ETL_Granule UUID: " + str(Granule_UUID) + ".  System Error Message: " + str(sysErrorData)

                    #print("DEBUG: PRINT ERROR HERE: (error_message) " + str(error_message))

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
        ret__function_name = "execute__Step__Load"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        try:
            expected_granules = self._expected_granules
            for expected_granules_object in expected_granules:

                expected_full_path_to_local_working_nc4_file = "UNSET"
                expected_full_path_to_local_final_nc4_file = "UNSET"

                # local_extract_path      = expected_granules_object['local_extract_path']
                # local_final_load_path   = expected_granules_object['local_final_load_path']
                # final_nc4_filename      = expected_granules_object['final_nc4_filename']
                # expected_full_path_to_local_working_nc4_file = os.path.join(local_extract_path, final_nc4_filename)  # Where the NC4 file was generated during the Transform Step
                # expected_full_path_to_local_final_nc4_file = os.path.join(local_final_load_path, final_nc4_filename)  # Where the final NC4 file should be placed for THREDDS Server monitoring

                try:
                    local_extract_path = expected_granules_object['local_extract_path']
                    # local_final_load_path = expected_granules_object['local_final_load_path']
                    final_nc4_filename = expected_granules_object['final_nc4_filename']
                    expected_full_path_to_local_working_nc4_file = os.path.join(local_extract_path, final_nc4_filename)  # Where the NC4 file was generated during the Transform Step
                    # expected_full_path_to_local_final_nc4_file = os.path.join(local_final_load_path, final_nc4_filename)  # Where the final NC4 file should be placed for THREDDS Server monitoring
                    expected_full_path_to_local_final_nc4_file = expected_granules_object['local_full_filepath_final_nc4_file']

                    # Copy the file from the working directory over to the final location for it.  (Where THREDDS Monitors for it)
                    shutil.copyfile(expected_full_path_to_local_working_nc4_file, expected_full_path_to_local_final_nc4_file)

                    # Create a new Granule Entry - The first function 'log_etl_granule' is the one that actually creates a new ETL Granule Attempt (There is one granule per dataset per pipeline attempt run in the ETL Granule Table)
                    Granule_UUID                = expected_granules_object['Granule_UUID']
                    new__granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__SUCCESS", default_or_error_return_value="SUCCESS")
                    is_error                    = False
                    is_update_succeed           = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)

                    # Now that the granule is in it's destination location, we can do a create_or_update 'Available Granule' so that the database knows this granule exists in the system (so the client side will know it is available)
                    #
                    # # TODO - Possible Parameter updates needed here.  (As we learn more about what the specific client side needs are)
                    # # def create_or_update_Available_Granule(self, granule_name, granule_contextual_information, etl_pipeline_run_uuid, etl_dataset_uuid, created_by, additional_json):
                    granule_name                    = final_nc4_filename
                    granule_contextual_information  = ""
                    additional_json                 = {}
                    additional_json['MostRecent__ETL_Granule_UUID'] = str(Granule_UUID).strip()
                    # self.etl_parent_pipeline_instance.create_or_update_Available_Granule(granule_name=granule_name, granule_contextual_information=granule_contextual_information, additional_json=additional_json)

                except:
                    sysErrorData = str(sys.exc_info())
                    error_JSON = {}
                    error_JSON['error'] = "Error: There was an error when attempting to copy the current nc4 file to it's final directory location.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
                    error_JSON['is_error'] = True
                    error_JSON['class_name'] = "imerg"
                    error_JSON['function_name'] = "execute__Step__Load"
                    #
                    # Additional infos
                    error_JSON['expected_full_path_to_local_working_nc4_file']  = str(expected_full_path_to_local_working_nc4_file).strip()
                    error_JSON['expected_full_path_to_local_final_nc4_file']    = str(expected_full_path_to_local_final_nc4_file).strip()
                    #

                    # Update this Granule for Failure (store the error info in the granule also)
                    Granule_UUID = expected_granules_object['Granule_UUID']
                    #new__granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__FAILED  # When a granule has a NC4 file in the correct location, this counts as a Success.
                    new__granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")  #
                    is_error = True
                    is_update_succeed   = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)
                    new_json_key_to_append = "execute__Step__Load"
                    is_update_succeed_2 = self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(granule_uuid=Granule_UUID, new_json_key_to_append=new_json_key_to_append, sub_jsonable_object=error_JSON)

                    # # Exit Here With Error info loaded up
                    # # UPDATE - NO - Exiting here would fail the entire pipeline run when only a single granule fails..
                    # ret__is_error = True
                    # ret__error_description = error_JSON['error']
                    # ret__detail_state_info = error_JSON
                    # retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
                    # return retObj

        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON['error'] = "Error: There was an uncaught error when processing the Load step on all of the expected Granules.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = "imerg"
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
        ret__function_name = "execute__Step__Clean_Up"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        try:
            temp_working_dir = str(self.temp_working_dir).strip()
            if temp_working_dir == "":
                # Log an ETL Activity that says that the value of the temp_working_dir was blank
                activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_BLANK", default_or_error_return_value="Temp Working Dir Blank")  #
                activity_description = "Could not remove the temporary working directory.  The value for self.temp_working_dir was blank. "
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = self.__class__.__name__
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
            else:
                shutil.rmtree(temp_working_dir)
                # Log an ETL Activity that says that the value of the temp_working_dir was blank
                activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_REMOVED", default_or_error_return_value="Temp Working Dir Removed")  #
                activity_description = "Temp Working Directory, " + str(self.temp_working_dir).strip() + ", was removed."
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = self.__class__.__name__
                additional_json['temp_working_dir'] = str(temp_working_dir).strip()
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
            # print("execute__Step__Clean_Up: Cleanup is finished.")
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
