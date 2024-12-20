import datetime, os, requests, shutil, sys, urllib
import xarray as xr
import pandas as pd
import numpy as np
from collections import OrderedDict

from .common import common
from .etl_dataset_subtype_interface import ETL_Dataset_Subtype_Interface
from .etl_dataset_subtype import ETL_Dataset_Subtype

from api.services import Config_SettingService
from ..models import Config_Setting

from bs4 import BeautifulSoup


class ETL_Dataset_Subtype_USDA_SMAP(ETL_Dataset_Subtype, ETL_Dataset_Subtype_Interface):

    # init (Passing a reference from the calling class, so we can callback the error handler)
    def __init__(self, etl_parent_pipeline_instance=None, dataset_subtype=None):
        self.etl_parent_pipeline_instance = etl_parent_pipeline_instance
        self.class_name = self.__class__.__name__
        self._expected_remote_full_file_paths = []
        self._expected_granules = []
        self.etl_parent_pipeline_instance = etl_parent_pipeline_instance
        if dataset_subtype == 'smap_10km':
            self.mode = 'smap_10km'
        else:
            self.mode = 'smap_10km'

    # Set default parameters or using default
    def set_optional_parameters(self, params):
        super().set_optional_parameters(params)
        today = datetime.date.today()
        self.YYYY__Year__Start = params.get('YYYY__Year__Start') or today.year
        self.YYYY__Year__End = params.get('YYYY__Year__End') or today.year
        self.MM__Month__Start = params.get('MM__Month__Start') or 1
        self.MM__Month__End = params.get('MM__Month__End') or today.month
        self.DD__Day__Start = params.get('DD__Day__Start') or 1
        self.DD__Day__End = params.get('DD__Day__End') or today.day

    def execute__Step__Pre_ETL_Custom(self, uuid):
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        self.temp_working_dir = self.etl_parent_pipeline_instance.dataset.temp_working_dir
        final_load_dir_path = self.etl_parent_pipeline_instance.dataset.final_load_dir
        current_root_http_path = self.etl_parent_pipeline_instance.dataset.source_url

        # (1) Generate Expected remote file paths
        try:

            start_date = datetime.datetime(self.YYYY__Year__Start, self.MM__Month__Start, self.DD__Day__Start)
            end_date = datetime.datetime(self.YYYY__Year__End, self.MM__Month__End, self.DD__Day__End)

            filenames = []
            dates = []
            response = requests.get(current_root_http_path)
            soup = BeautifulSoup(response.text, 'html.parser')
            for _, link in enumerate(soup.findAll('a')):
                if link.get('href').startswith('NASA_USDA_SMAP_'):
                    _, _, _, start, end = link.get('href').split('_')
                    s_date = datetime.datetime.strptime(start[2:], '%Y%m%d')
                    e_date = datetime.datetime.strptime(end[:-4], '%Y%m%d')
                    if s_date >= start_date and e_date <= end_date:
                        filenames.append(link.get('href'))
                        dates.append(s_date)

            for filename, date in zip(filenames, dates):
                current_year__YYYY_str = "{:0>4d}".format(date.year)
                current_month__MM_str = "{:02d}".format(date.month)
                current_day__DD_str = "{:02d}".format(date.day)

                # usda-smap.20200415T000000Z.global.10km.3dy.nc4
                final_nc4_filename = 'usda-smap.{}{}{}T000000Z.global.{}.{}.nc4'.format(
                    current_year__YYYY_str,
                    current_month__MM_str,
                    current_day__DD_str,
                    '10km',
                    '3dy'
                )

                extracted_tif_filename = filename
                remote_full_filepath_gz_tif = urllib.parse.urljoin(current_root_http_path, filename)
                local_full_filepath_final_nc4_file = os.path.join(final_load_dir_path, final_nc4_filename)

                current_obj = {}

                # Filename and Granule Name info
                local_extract_path = self.temp_working_dir  # We are using the same directory for the download and extract path
                local_final_load_path = final_load_dir_path
                local_full_filepath_download = os.path.join(local_extract_path, extracted_tif_filename)

                # current_obj['local_download_path'] = local_extract_path      # Download path and extract path
                current_obj['local_extract_path'] = local_extract_path  # Download path and extract path
                current_obj[
                    'local_final_load_path'] = local_final_load_path  # The path where the final output granule file goes.
                current_obj['remote_directory_path'] = current_root_http_path
                #
                current_obj['extracted_tif_filename'] = extracted_tif_filename
                current_obj['final_nc4_filename'] = final_nc4_filename
                current_obj['granule_name'] = final_nc4_filename
                #
                current_obj['remote_full_filepath_gz_tif'] = remote_full_filepath_gz_tif  # remote_full_filepath_tif
                current_obj['local_full_filepath_download'] = local_full_filepath_download  # local_full_filepath
                current_obj['local_full_filepath_final_nc4_file'] = local_full_filepath_final_nc4_file

                granule_name = final_nc4_filename
                granule_contextual_information = ""
                granule_pipeline_state = Config_SettingService.get_value(
                    setting_name="GRANULE_PIPELINE_STATE__ATTEMPTING", default_or_error_return_value="Attempting")
                additional_json = current_obj
                new_Granule_UUID = self.etl_parent_pipeline_instance.log_etl_granule(granule_name=granule_name,
                                                                                     granule_contextual_information=granule_contextual_information,
                                                                                     granule_pipeline_state=granule_pipeline_state,
                                                                                     additional_json=additional_json)

                # Save the Granule's UUID for reference in later steps
                current_obj['Granule_UUID'] = str(new_Granule_UUID).strip()

                # Add to the granules list
                self._expected_granules.append(current_obj)

        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON[
                'error'] = "Error: There was an error when generating the expected remote filepaths.  See the additional data for details on which expected file caused the error.  System Error Message: " + str(
                sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.class_name
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Call Error handler right here (If this is commented out, then the info should be bubbling up to the calling function))
            # activity_event_type         = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR
            # self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=error_JSON)
            #
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

        # Make sure the directories exist
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(self.temp_working_dir)
        if is_error_creating_directory == True:
            error_JSON = {}
            error_JSON[
                'error'] = "Error: There was an error when the pipeline tried to create a new directory on the filesystem.  The path that the pipeline tried to create was: " + str(
                self.temp_working_dir) + ".  There should be another error logged just before this one that contains system error info.  That info should give clues to why the directory was not able to be created."
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.class_name
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

        # final_load_dir_path
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(final_load_dir_path)
        if is_error_creating_directory == True:
            error_JSON = {}
            error_JSON[
                'error'] = "Error: There was an error when the pipeline tried to create a new directory on the filesystem.  The path that the pipeline tried to create was: " + str(
                final_load_dir_path) + ".  There should be another error logged just before this one that contains system error info.  That info should give clues to why the directory was not able to be created."
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.class_name
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

        # Ended, now for reporting
        ret__detail_state_info['class_name'] = self.class_name
        # ret__detail_state_info['number_of_expected_remote_full_file_paths'] = str(len(self._expected_remote_full_file_paths)).strip()
        ret__detail_state_info['number_of_expected_granules'] = str(len(self._expected_granules)).strip()
        ret__event_description = "Success.  Completed Step execute__Step__Pre_ETL_Custom by generating " + str(
            len(self._expected_remote_full_file_paths)).strip() + " expected full file paths to download and " + str(
            len(self._expected_granules)).strip() + " expected granules to process."

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                     is_error=ret__is_error, event_description=ret__event_description,
                                                     error_description=ret__error_description,
                                                     detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Download(self, uuid):
        ret__function_name = sys._getframe().f_code.co_name
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

        # Process each expected granule
        for expected_granule in expected_granules:
            try:
                if (loop_counter + 1) % modulus_size == 0:
                    event_message = "About to download file: " + str(loop_counter + 1) + " out of " + str(
                        num_of_objects_to_process)
                    print(event_message)
                    activity_event_type = Config_SettingService.get_value(
                        setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS",
                        default_or_error_return_value="ETL Download Progress")  # settings.ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS
                    activity_description = event_message
                    additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                    self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type,
                                                                    activity_description=activity_description,
                                                                    etl_granule_uuid="", is_alert=False,
                                                                    additional_json=additional_json)

                # Current Granule to download
                current_url_to_download = expected_granule['remote_full_filepath_gz_tif']
                current_download_destination_local_full_file_path = expected_granule[
                    'local_full_filepath_download']  # current_download_destination_local_full_file_path = expected_granule['local_full_filepath']

                # Granule info
                Granule_UUID = expected_granule['Granule_UUID']
                granule_name = expected_granule['granule_name']

                print(current_url_to_download)

                # Download the file - Actually do the download now
                try:
                    r = requests.get(current_url_to_download)
                    with open(current_download_destination_local_full_file_path, 'wb') as outfile:
                        outfile.write(r.content)
                    download_counter = download_counter + 1
                except:
                    error_counter = error_counter + 1
                    sysErrorData = str(sys.exc_info())
                    # print("DEBUG Warn: (WARN LEVEL) (File can not be downloaded).  System Error Message: " + str(sysErrorData))
                    warn_JSON = {}
                    warn_JSON[
                        'warning'] = "Warning: There was an uncaught error when attempting to download file at URL: " + str(
                        current_url_to_download) + ".  If the System Error message says something like 'nodename nor servname provided, or not known', then one common cause of that error is an unstable or disconnected internet connection.  Double check that the internet connection is working and try again.  System Error Message: " + str(
                        sysErrorData)
                    warn_JSON['is_error'] = True
                    warn_JSON['class_name'] = self.class_name
                    warn_JSON['function_name'] = "execute__Step__Download"
                    warn_JSON['current_object_info'] = expected_granule
                    # Call Error handler right here to send a warning message to ETL log. - Note this warning will not make it back up to the overall pipeline, it is being sent here so admin can still be aware of it and handle it.
                    activity_event_type = Config_Setting.get_value(
                        setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING",
                        default_or_error_return_value="ETL Warning")
                    activity_description = warn_JSON['warning']
                    self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type,
                                                                    activity_description=activity_description,
                                                                    etl_granule_uuid="", is_alert=True,
                                                                    additional_json=warn_JSON)

            except:
                error_counter = error_counter + 1
                sysErrorData = str(sys.exc_info())
                error_message = "usda_smap.execute__Step__Download: Generic Uncaught Error.  At least 1 download failed.  System Error Message: " + str(
                    sysErrorData)
                detail_errors.append(error_message)
                print(error_message)

            loop_counter = loop_counter + 1

        # Ended, now for reporting
        ret__detail_state_info['class_name'] = self.class_name
        ret__detail_state_info['download_counter'] = download_counter
        ret__detail_state_info['error_counter'] = error_counter
        ret__detail_state_info['loop_counter'] = loop_counter
        ret__detail_state_info['detail_errors'] = detail_errors
        ret__event_description = "Success.  Completed Step execute__Step__Download by downloading " + str(
            download_counter).strip() + " files."

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                     is_error=ret__is_error, event_description=ret__event_description,
                                                     error_description=ret__error_description,
                                                     detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Extract(self):
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        ret__detail_state_info['class_name'] = self.class_name
        ret__detail_state_info[
            'custom_message'] = "SMAP types do not need to be extracted.  The source files are non-compressed Tif and Tfw files."

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                     is_error=ret__is_error, event_description=ret__event_description,
                                                     error_description=ret__error_description,
                                                     detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Transform(self):
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        # error_counter, detail_errors
        error_counter = 0
        detail_errors = []

        try:
            expected_granules = self._expected_granules
            for expected_granules_object in expected_granules:
                try:

                    # print("A")

                    # Getting info ready for the current granule.
                    local_extract_path = expected_granules_object['local_extract_path']
                    tif_filename = expected_granules_object['extracted_tif_filename']
                    final_nc4_filename = expected_granules_object['final_nc4_filename']
                    expected_full_path_to_local_extracted_tif_file = os.path.join(local_extract_path, tif_filename)

                    geotiffFile_FullPath = expected_full_path_to_local_extracted_tif_file

                    # print("B")

                    # Note there are only 3 lines of differences between the 4wk and 12 wk scripts
                    # TODO: Place those difference variables here.
                    mode_var__pd_timedelta = ""
                    mode_var__attr_composite_interval = ""
                    mode_var__attr_comment = ""
                    mode_var__TemporalResolution = ""

                    # print("C")

                    # Matching to the other script

                    ############################################################
                    # Start extracting data and creating output netcdf file.
                    ############################################################

                    _, _, _, start, end = tif_filename.split('_')
                    startTime = datetime.datetime.strptime(start[2:], '%Y%m%d')
                    endTime = datetime.datetime.strptime(end[:-4], '%Y%m%d')

                    # print("D")

                    ############################################################
                    # Beging extracting data and creating output netcdf file.
                    ############################################################
                    # 1) Read the geotiff data into an xarray data array
                    tiffData = xr.open_rasterio(geotiffFile_FullPath)  # tiffData = xr.open_rasterio(geotiffFile)
                    # 2) Convert to a dataset.  (need to assign a name to the data array)
                    usda = tiffData.rename('smap').to_dataset()
                    # Handle selecting/adding the dimesions
                    ssm = usda.isel(band=0).reset_coords('band', drop=True).rename(
                        {'smap': 'ssm'})  # select the singleton band dimension and drop out the associated coordinate.
                    susm = usda.isel(band=1).reset_coords('band', drop=True).rename({'smap': 'susm'})
                    smp = usda.isel(band=2).reset_coords('band', drop=True).rename({'smap': 'smp'})
                    ssma = usda.isel(band=3).reset_coords('band', drop=True).rename({'smap': 'ssma'})
                    susma = usda.isel(band=4).reset_coords('band', drop=True).rename({'smap': 'susma'})

                    usda = xr.merge([ssm, susm, smp, ssma, susma])
                    centerTime = startTime + pd.Timedelta('36h')
                    # Add the time dimension as a new coordinate.
                    usda = usda.assign_coords(time=centerTime).expand_dims(dim='time', axis=0)
                    # Add an additional variable "time_bnds" for the time boundaries.
                    usda['time_bnds'] = xr.DataArray(np.array([startTime, endTime]).reshape((1, 2)),
                                                     dims=['time', 'nbnds'])
                    # 3) Rename and add attributes to this dataset.
                    usda = usda.rename({'y': 'latitude', 'x': 'longitude'})
                    # 4) Reorder latitude dimension into ascending order
                    if usda.latitude.values[1] - usda.latitude.values[0] < 0:
                        usda = usda.reindex(latitude=usda.latitude[::-1])

                    # print("E")

                    # Lat/Lon/Time dictionaries.
                    # Use Ordered dict
                    latAttr = OrderedDict([('long_name', 'latitude'), ('units', 'degrees_north'), ('axis', 'Y')])
                    lonAttr = OrderedDict([('long_name', 'longitude'), ('units', 'degrees_east'), ('axis', 'X')])
                    timeAttr = OrderedDict([('long_name', 'time'), ('axis', 'T'), ('bounds', 'time_bnds')])
                    timeBoundsAttr = OrderedDict([('long_name', 'time_bounds')])
                    ssmAttr = OrderedDict(
                        [('long_name', 'surface_soil_moisture'), ('units', 'mm'), ('composite_interval', '3 day'),
                         ('comment', '3 day mean composite estimate')])
                    susmAttr = OrderedDict(
                        [('long_name', 'sub_surface_soil_moisture'), ('units', 'mm'), ('composite_interval', '3 day'),
                         ('comment', '3 day mean composite estimate')])
                    smpAttr = OrderedDict(
                        [('long_name', 'soil_moisture_profile'), ('units', '%'), ('composite_interval', '3 day'),
                         ('comment', '3 day mean composite estimate')])
                    ssmaAttr = OrderedDict([('long_name', 'surface_soil_moisture_anomaly'), ('units', 'unitless'),
                                            ('composite_interval', '3 day'),
                                            ('comment', '3 day mean composite estimate')])
                    susmaAttr = OrderedDict([('long_name', 'sub_surface_soil_moisture_anomaly'), ('units', 'unitless'),
                                             ('composite_interval', '3 day'),
                                             ('comment', '3 day mean composite estimate')])

                    fileAttr = OrderedDict([('Description',
                                             'The NASA-USDA Enhanced SMAP Global soil moisture data provides soil moisture information across the globe at 10-km spatial resolution.'), \
                                            ('DateCreated', pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%SZ')), \
                                            ('Contact', 'Lance Gilliland, lance.gilliland@nasa.gov'), \
                                            ('Source',
                                             'NASA Goddard Space Flight Center; John D. Bolten. john.bolten@nasa.gov; https://gimms.gsfc.nasa.gov/SMOS/SMAP/SMAP_10KM_tiff/'), \
                                            ('Version', '1.0'), \
                                            ('Reference',
                                             'Mladenova, I.E., Bolten, J.D., Crow, W., Sazib, N. and Reynolds, C., 2020. Agricultural drought monitoring via the assimilation of SMAP soil moisture retrievals into a global soil water balance model.'), \
                                            ('RangeStartTime', startTime.strftime('%Y-%m-%dT%H:%M:%SZ')), \
                                            ('RangeEndTime', endTime.strftime('%Y-%m-%dT%H:%M:%SZ')), \
                                            ('SouthernmostLatitude', np.min(usda.latitude.values)), \
                                            ('NorthernmostLatitude', np.max(usda.latitude.values)), \
                                            ('WesternmostLongitude', np.min(usda.longitude.values)), \
                                            ('EasternmostLongitude', np.max(usda.longitude.values)), \
                                            ('TemporalResolution', str(mode_var__TemporalResolution)), \
                                            ('SpatialResolution', '10kmx10km')])

                    # missing_data/_FillValue , relative time units etc. are handled as part of the encoding dictionary used in to_netcdf() call.
                    ssmEncoding = {'_FillValue': np.float32(-999.0), 'missing_value': np.float32(-999.0),
                                   'dtype': np.dtype('float32')}
                    timeEncoding = {'units': 'seconds since 1970-01-01T00:00:00Z', 'dtype': np.dtype('int32')}
                    timeBoundsEncoding = {'units': 'seconds since 1970-01-01T00:00:00Z', 'dtype': np.dtype('int32')}
                    # Set the Attributes
                    usda.latitude.attrs = latAttr
                    usda.longitude.attrs = lonAttr
                    usda.time.attrs = timeAttr
                    usda.time_bnds.attrs = timeBoundsAttr
                    usda.ssm.attrs = ssmAttr
                    usda.susm.attrs = susmAttr
                    usda.smp.attrs = smpAttr
                    usda.ssma.attrs = ssmaAttr
                    usda.susma.attrs = susmaAttr
                    usda.attrs = fileAttr
                    # Set the Encodings
                    usda.ssm.encoding = ssmEncoding
                    usda.susm.encoding = ssmEncoding
                    usda.smp.encoding = ssmEncoding
                    usda.ssma.encoding = ssmEncoding
                    usda.susma.encoding = ssmEncoding

                    usda.time.encoding = timeEncoding
                    usda.time_bnds.encoding = timeBoundsEncoding
                    usda.reindex(latitude=usda.latitude[::-1])

                    # 5) Output File
                    outputFile_FullPath = os.path.join(local_extract_path, final_nc4_filename)
                    usda.to_netcdf(outputFile_FullPath, unlimited_dims='time')

                    print(outputFile_FullPath)

                except Exception as e:
                    print(e)
                    ret__is_error = True

                    sysErrorData = str(sys.exc_info())

                    Granule_UUID = expected_granules_object['Granule_UUID']

                    error_message = "usada_smap.execute__Step__Transform: An Error occurred during the Transform step with ETL_Granule UUID: " + str(
                        Granule_UUID) + ".  System Error Message: " + str(sysErrorData)

                    print("DEBUG: PRINT ERROR HERE: (error_message) " + str(error_message))

                    # Individual Transform Granule Error
                    error_counter = error_counter + 1
                    detail_errors.append(error_message)

                    error_JSON = {}
                    error_JSON['error_message'] = error_message

                    # Update this Granule for Failure (store the error info in the granule also)
                    new__granule_pipeline_state = Config_Setting.get_value(
                        setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")  #
                    is_error = True
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(
                        granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state,
                        is_error=is_error)
                    new_json_key_to_append = "execute__Step__Transform"
                    is_update_succeed_2 = self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(
                        granule_uuid=Granule_UUID, new_json_key_to_append=new_json_key_to_append,
                        sub_jsonable_object=error_JSON)

                pass

        except:

            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON[
                'error'] = "Error: There was an uncaught error when processing the Transform step on all of the expected Granules.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(
                sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.class_name
            error_JSON['function_name'] = "execute__Step__Transform"
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

        ret__detail_state_info['class_name'] = self.class_name
        ret__detail_state_info['error_counter'] = error_counter
        ret__detail_state_info['detail_errors'] = detail_errors

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                     is_error=ret__is_error, event_description=ret__event_description,
                                                     error_description=ret__error_description,
                                                     detail_state_info=ret__detail_state_info)
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
                    expected_full_path_to_local_working_nc4_file = os.path.join(local_extract_path,
                                                                                final_nc4_filename)  # Where the NC4 file was generated during the Transform Step
                    expected_full_path_to_local_final_nc4_file = expected_granules_object[
                        'local_full_filepath_final_nc4_file']  # Where the final NC4 file should be placed for THREDDS Server monitoring

                    print(expected_full_path_to_local_final_nc4_file)

                    # Copy the file from the working directory over to the final location for it.  (Where THREDDS Monitors for it)
                    super()._copy_nc4_file(expected_full_path_to_local_working_nc4_file,
                                           expected_full_path_to_local_final_nc4_file)

                    # Create a new Granule Entry - The first function 'log_etl_granule' is the one that actually creates a new ETL Granule Attempt (There is one granule per dataset per pipeline attempt run in the ETL Granule Table)
                    Granule_UUID = expected_granules_object['Granule_UUID']

                    new__granule_pipeline_state = Config_Setting.get_value(
                        setting_name="GRANULE_PIPELINE_STATE__SUCCESS", default_or_error_return_value="SUCCESS")  #
                    is_error = False
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(
                        granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state,
                        is_error=is_error)

                    additional_json = {}
                    additional_json['MostRecent__ETL_Granule_UUID'] = str(Granule_UUID).strip()
                    # self.etl_parent_pipeline_instance.create_or_update_Available_Granule(granule_name=granule_name, granule_contextual_information=granule_contextual_information, additional_json=additional_json)

                except:
                    sysErrorData = str(sys.exc_info())
                    error_JSON = {}
                    error_JSON[
                        'error'] = "Error: There was an error when attempting to copy the current nc4 file to it's final directory location.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(
                        sysErrorData)
                    error_JSON['is_error'] = True
                    error_JSON['class_name'] = self.class_name
                    error_JSON['function_name'] = "execute__Step__Load"
                    #
                    # Additional infos
                    error_JSON['expected_full_path_to_local_working_nc4_file'] = str(
                        expected_full_path_to_local_working_nc4_file).strip()
                    error_JSON['expected_full_path_to_local_final_nc4_file'] = str(
                        expected_full_path_to_local_final_nc4_file).strip()
                    #

                    # Update this Granule for Failure (store the error info in the granule also)
                    Granule_UUID = expected_granules_object['Granule_UUID']
                    # new__granule_pipeline_state = settings.GRANULE_PIPELINE_STATE__FAILED  # When a granule has a NC4 file in the correct location, this counts as a Success.
                    new__granule_pipeline_state = Config_Setting.get_value(
                        setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")  #
                    is_error = True
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(
                        granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state,
                        is_error=is_error)
                    new_json_key_to_append = "execute__Step__Load"
                    is_update_succeed_2 = self.etl_parent_pipeline_instance.etl_granule__Append_JSON_To_Additional_JSON(
                        granule_uuid=Granule_UUID, new_json_key_to_append=new_json_key_to_append,
                        sub_jsonable_object=error_JSON)

            pass
        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON[
                'error'] = "Error: There was an uncaught error when processing the Load step on all of the expected Granules.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(
                sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.class_name
            error_JSON['function_name'] = "execute__Step__Load"
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

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                     is_error=ret__is_error, event_description=ret__event_description,
                                                     error_description=ret__error_description,
                                                     detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Post_ETL_Custom(self):
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        try:
            super().execute__Step__Post_ETL_Custom()
        except Exception as e:
            print(e)

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                     is_error=ret__is_error, event_description=ret__event_description,
                                                     error_description=ret__error_description,
                                                     detail_state_info=ret__detail_state_info)
        return retObj

    def execute__Step__Clean_Up(self):
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        try:
            temp_working_dir = str(self.temp_working_dir).strip()
            if temp_working_dir == "":
                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                activity_event_type = Config_Setting.get_value(
                    setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_BLANK",
                    default_or_error_return_value="Temp Working Dir Blank")  #
                activity_description = "Could not remove the temporary working directory.  The value for self.temp_working_dir was blank. "
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = "usda_smap"
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type,
                                                                activity_description=activity_description,
                                                                etl_granule_uuid="", is_alert=False,
                                                                additional_json=additional_json)
            else:
                shutil.rmtree(temp_working_dir)

                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                activity_event_type = Config_Setting.get_value(
                    setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_REMOVED",
                    default_or_error_return_value="Temp Working Dir Removed")  #
                activity_description = "Temp Working Directory, " + str(
                    self.temp_working_dir).strip() + ", was removed."
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = "usda_smap"
                additional_json['temp_working_dir'] = str(temp_working_dir).strip()
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type,
                                                                activity_description=activity_description,
                                                                etl_granule_uuid="", is_alert=False,
                                                                additional_json=additional_json)

        except:
            sysErrorData = str(sys.exc_info())
            error_JSON = {}
            error_JSON[
                'error'] = "Error: There was an uncaught error when processing the Clean Up step.  This function is supposed to simply remove the working directory.  This means the working directory was not removed.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(
                sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.class_name
            error_JSON['function_name'] = "execute__Step__Clean_Up"
            #
            # Additional info
            error_JSON['self__temp_working_dir'] = str(self.temp_working_dir).strip()
            #
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

        retObj = common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                     is_error=ret__is_error, event_description=ret__event_description,
                                                     error_description=ret__error_description,
                                                     detail_state_info=ret__detail_state_info)
        return retObj
