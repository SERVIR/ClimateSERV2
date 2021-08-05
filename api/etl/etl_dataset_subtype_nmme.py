import calendar, datetime, glob, os, shutil, sys
import xarray as xr
import pandas as pd
import numpy as np
from collections import OrderedDict

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

        try:

            self.temp_working_dir = self.etl_parent_pipeline_instance.dataset.temp_working_dir
            final_load_dir_path = self.etl_parent_pipeline_instance.dataset.final_load_dir
            current_root_http_path = self.etl_parent_pipeline_instance.dataset.source_url

            print(current_root_http_path)
            print(final_load_dir_path)
            print(self.temp_working_dir)

            # (1) Generate Expected remote file paths

            start_date = datetime.datetime(self.YYYY__Year__Start, self.MM__Month__Start, 1)
            end_monthrange = calendar.monthrange(self.YYYY__Year__Start, self.MM__Month__End)
            end_date = datetime.datetime(self.YYYY__Year__End, self.MM__Month__End, end_monthrange[1])

            filenames, dates, ids, ensambles = [], [], [], []
            for date_dir in os.scandir(current_root_http_path):
                if date_dir.is_dir():
                    date = datetime.datetime.strptime(date_dir.name,'%Y%m')
                    if date >= start_date and date <= end_date:
                        for ensemble_dir in os.scandir(os.path.join(current_root_http_path, date_dir.name)):
                            if ensemble_dir.is_dir():
                                for path in glob.glob(os.path.join(current_root_http_path, date_dir.name, ensemble_dir.name, '*.tif')):
                                    filename = os.path.basename(path)
                                    _, _, _, id = filename.replace('.tif', '').split('_')
                                    filenames.append(filename)
                                    dates.append(date)
                                    ids.append(id)
                                    ensambles.append(ensemble_dir.name)

            for filename, date, id, ensamble in zip(filenames, dates, ids, ensambles):

                current_year__YYYY_str = '{:0>4d}'.format(date.year)
                current_month__MM_str = '{:02d}'.format(date.month)
                current_day__DD_str = '{:02d}'.format(date.day)

                final_nc4_filename = 'nmme-cfs_v2.{}{}{}T000000Z.{}.global.daily.nc4'.format(
                    current_year__YYYY_str,
                    current_month__MM_str,
                    current_day__DD_str,
                    id
                )

                date_str = date.strftime('%Y%m')

                # Granule
                current_obj = {}
                current_obj['local_download_path'] = os.path.join(self.temp_working_dir, date_str, ensamble)
                current_obj['local_final_load_path'] = os.path.join(final_load_dir_path, date_str, ensamble)
                #
                current_obj['tif_filename'] = filename
                current_obj['final_nc4_filename'] = final_nc4_filename
                current_obj['granule_name'] = final_nc4_filename
                #
                current_obj['remote_directory_path'] = current_root_http_path
                current_obj['remote_full_filepath_tif'] = os.path.join(current_root_http_path, date_str, ensamble, filename)
                #
                current_obj['local_full_filepath_download'] = os.path.join(self.temp_working_dir, date_str, ensamble, filename)
                current_obj['working_full_filepath_final_nc4_file'] = os.path.join(self.temp_working_dir, date_str, ensamble, final_nc4_filename)
                current_obj['local_full_filepath_final_nc4_file'] = os.path.join(final_load_dir_path, date_str, ensamble, final_nc4_filename)

                granule_pipeline_state = Config_Setting.get_value(setting_name="GRANULE_PIPELINE_STATE__ATTEMPTING", default_or_error_return_value="Attempting")

                new_Granule_UUID = self.etl_parent_pipeline_instance.log_etl_granule(granule_name=final_nc4_filename, granule_contextual_information='', granule_pipeline_state=granule_pipeline_state, additional_json=current_obj)

                # Save the Granule's UUID for reference in later steps
                current_obj['Granule_UUID'] = str(new_Granule_UUID).strip()

                # Add to the granules list
                self._expected_granules.append(current_obj)

                # Make sure the directories exist
                is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(os.path.join(self.temp_working_dir, date_str, ensamble))
                if is_error_creating_directory == True:
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

                # final_load_dir_path
                is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(os.path.join(final_load_dir_path, date_str, ensamble))
                if is_error_creating_directory == True:
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

        except Exception as e:
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

        # Ended, now for reporting
        ret__detail_state_info['class_name'] = "nmme"
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

        num_of_objects_to_process = len(self._expected_granules)
        num_of_download_activity_events = 4
        modulus_size = int(num_of_objects_to_process / num_of_download_activity_events)
        if modulus_size < 1:
            modulus_size = 1

        # Process each expected granule
        for expected_granule in self._expected_granules:
            try:
                if (loop_counter + 1) % modulus_size == 0:
                    event_message = "About to download file: " + str(loop_counter + 1) + " out of " + str(num_of_objects_to_process)
                    print(event_message)
                    activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS", default_or_error_return_value="ETL Download Progress")  # settings.ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS
                    activity_description = event_message
                    additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                    self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

                # Current Granule to download
                current_url_to_download = expected_granule['remote_full_filepath_tif']
                current_download_destination_local_full_file_path = expected_granule['local_full_filepath_download']

                # Download the file - Actually do the download now
                try:
                    shutil.copyfile(current_url_to_download, current_download_destination_local_full_file_path)
                    download_counter = download_counter + 1
                except Exception as e:
                    print(e)
                    error_counter = error_counter + 1
                    sysErrorData = str(sys.exc_info())
                    warn_JSON = {}
                    warn_JSON['warning'] = "Warning: There was an uncaught error when attempting to download file at URL: " + str(current_url_to_download) + ".  If the System Error message says something like 'nodename nor servname provided, or not known', then one common cause of that error is an unstable or disconnected internet connection.  Double check that the internet connection is working and try again.  System Error Message: " + str(sysErrorData)
                    warn_JSON['is_error'] = True
                    warn_JSON['class_name'] = "esi"
                    warn_JSON['function_name'] = "execute__Step__Download"
                    warn_JSON['current_object_info'] = expected_granule
                    # Call Error handler right here to send a warning message to ETL log. - Note this warning will not make it back up to the overall pipeline, it is being sent here so admin can still be aware of it and handle it.
                    activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING", default_or_error_return_value="ETL Warning")
                    activity_description = warn_JSON['warning']
                    self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=warn_JSON)

            except:
                error_counter = error_counter + 1
                sysErrorData = str(sys.exc_info())
                error_message = "nmme.execute__Step__Download: Generic Uncaught Error.  At least 1 download failed.  System Error Message: " + str(sysErrorData)
                detail_errors.append(error_message)
                print(error_message)

            loop_counter = loop_counter + 1

        # Ended, now for reporting
        ret__detail_state_info['class_name'] = 'nmme'
        ret__detail_state_info['download_counter'] = download_counter
        ret__detail_state_info['error_counter'] = error_counter
        ret__detail_state_info['loop_counter'] = loop_counter
        ret__detail_state_info['detail_errors'] = detail_errors
        # ret__detail_state_info['number_of_expected_remote_full_file_paths'] = str(len(self._expected_remote_full_file_paths)).strip()
        # ret__detail_state_info['number_of_expected_granules'] = str(len(self._expected_granules)).strip()
        ret__event_description = "Success.  Completed Step execute__Step__Download by downloading " + str(download_counter).strip() + " files."

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Extract(self):
        ret__function_name = "execute__Step__Extract"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        ret__detail_state_info['class_name'] = self.class_name
        ret__detail_state_info['custom_message'] = "NMME types do not need to be extracted.  The source files are non-compressed Tif files."

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name, is_error=ret__is_error, event_description=ret__event_description, error_description=ret__error_description, detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Transform(self):
        ret__function_name = "execute__Step__Transform"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        error_counter = 0
        detail_errors = []

        try:
            expected_granules = self._expected_granules
            for expected_granules_object in expected_granules:
                try:

                    # print("A")

                    # Getting info ready for the current granule.
                    local_download_path                                 = expected_granules_object['local_download_path']
                    tif_filename                                        = expected_granules_object['tif_filename']
                    final_nc4_filename                                  = expected_granules_object['final_nc4_filename']
                    expected_full_path_to_local_extracted_tif_file      = os.path.join(local_download_path, tif_filename)
                    local_full_filepath_final_nc4_file                  = expected_granules_object['working_full_filepath_final_nc4_file']

                    geotiffFile_FullPath = expected_full_path_to_local_extracted_tif_file

                    # print("B")

                    # Note there are only 3 lines of differences between the 4wk and 12 wk scripts
                    # TODO: Place those difference variables here.
                    mode_var__pd_timedelta = ""
                    mode_var__attr_composite_interval = ""
                    mode_var__attr_comment = ""
                    mode_var__TemporalResolution = ""

                    # print("C")

                    # Determine starting and ending times
                    _, datestring, _, id = tif_filename.split('_')
                    endTime = datetime.datetime.strptime(datestring,'%Y%m')
                    startTime = datetime.datetime.strptime(datestring,'%Y%m')

                    # print("D")

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

                    # print("E")

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

                    # print("F")

                    # 5) Output File
                    esi.to_netcdf(local_full_filepath_final_nc4_file, unlimited_dims='time')

                    # print("outputFile_FullPath: " + str(outputFile_FullPath))
                    # Can't put ':' in file names...

                    # print("G")

                except Exception as e:
                    print(e)

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

        except Exception as e:
            print(e)

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

        try:

            for expected_granules_object in self._expected_granules:

                try:

                    expected_full_path_to_local_working_nc4_file = expected_granules_object['working_full_filepath_final_nc4_file']
                    expected_full_path_to_local_final_nc4_file = expected_granules_object['local_full_filepath_final_nc4_file']

                    # Copy the file from the working directory over to the final location for it.  (Where THREDDS Monitors for it)
                    shutil.copyfile(expected_full_path_to_local_working_nc4_file, expected_full_path_to_local_final_nc4_file)

                    # Create a new Granule Entry - The first function 'log_etl_granule' is the one that actually creates a new ETL Granule Attempt (There is one granule per dataset per pipeline attempt run in the ETL Granule Table)
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
                    granule_contextual_information = ""
                    additional_json = {}
                    additional_json['MostRecent__ETL_Granule_UUID'] = str(Granule_UUID).strip()
                    # self.etl_parent_pipeline_instance.create_or_update_Available_Granule(granule_name=granule_name, granule_contextual_information=granule_contextual_information, additional_json=additional_json)

                except Exception as e:
                    print(e)
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
                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_BLANK", default_or_error_return_value="Temp Working Dir Blank")  #
                activity_description = "Could not remove the temporary working directory.  The value for self.temp_working_dir was blank. "
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = "esi"
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
            else:
                shutil.rmtree(temp_working_dir)

                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_REMOVED", default_or_error_return_value="Temp Working Dir Removed")  #
                activity_description = "Temp Working Directory, " + str(self.temp_working_dir).strip() + ", was removed."
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = "esi"
                additional_json['temp_working_dir'] = str(temp_working_dir).strip()
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)


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
