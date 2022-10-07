import datetime
import os
import shutil
import sys
from collections import OrderedDict
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from api.services import Config_SettingService
from .common import common
from .etl_dataset_subtype import ETL_Dataset_Subtype
from .etl_dataset_subtype_interface import ETL_Dataset_Subtype_Interface
from ..models import Config_Setting


class ETL_Dataset_Subtype_africa_lis(ETL_Dataset_Subtype, ETL_Dataset_Subtype_Interface):

    # init (Passing a reference from the calling class, so we can call the error handler)
    def __init__(self, etl_parent_pipeline_instance=None, dataset_subtype=None):
        super().__init__()
        self.final_load_dir_path = None
        self.temp_working_dir = None
        self.DD__Day__End = None
        self.DD__Day__Start = None
        self.MM__Month__End = None
        self.MM__Month__Start = None
        self.YYYY__Year__End = None
        self.YYYY__Year__Start = None
        self.etl_parent_pipeline_instance = etl_parent_pipeline_instance
        self.class_name = self.__class__.__name__
        self._expected_remote_full_file_paths = []
        self._expected_granules = []
        self.etl_parent_pipeline_instance = etl_parent_pipeline_instance
        if dataset_subtype == 'sport_lis':
            self.mode = 'sport_lis'
        else:
            self.mode = 'sport_lis'

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
        print("execute__Step__Pre_ETL_Custom")
        ret__function_name = "execute__Step__Pre_ETL_Custom"
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        self.temp_working_dir = self.etl_parent_pipeline_instance.dataset.temp_working_dir
        self.final_load_dir_path = os.path.join(self.etl_parent_pipeline_instance.dataset.final_load_dir, '')
        current_domain_path = self.etl_parent_pipeline_instance.dataset.source_url

        # (1) Generate Expected remote file paths
        try:
            start_date = datetime.datetime(self.YYYY__Year__Start, self.MM__Month__Start, self.DD__Day__Start)
            end_date = datetime.datetime(self.YYYY__Year__End, self.MM__Month__End, self.DD__Day__End)
            filenames = []
            dates = []

            # current_domain_path
            for file in Path(current_domain_path).glob('LIS_HIST*'):
                file_time = datetime.datetime.strptime(file.stem.split('_')[-1].split(".")[0][:-4], '%Y%m%d')
                if start_date <= file_time <= end_date:
                    filenames.append(file)
                    dates.append(file_time)

            for filename, date in zip(filenames, dates):
                current_year__YYYY_str = "{:0>4d}".format(date.year)
                current_month__MM_str = "{:02d}".format(date.month)
                current_day__DD_str = "{:02d}".format(date.day)

                final_nc4_filename = 'sport-lis.{}{}{}T000000Z.africa.{}.{}.nc4'.format(
                    current_year__YYYY_str,
                    current_month__MM_str,
                    current_day__DD_str,
                    '0.03deg',
                    'daily'
                )
                domain_file_path = filename
                current_obj = {}
                # Filename and Granule Name info
                local_final_load_path = self.final_load_dir_path

                current_obj[
                    'local_final_load_path'] = local_final_load_path  # Final nc file location.

                current_obj['domain_file_path'] = domain_file_path  # remote_full_filepath_tif
                current_obj['date_year'] = current_year__YYYY_str
                current_obj['date_month'] = current_month__MM_str
                current_obj['date_day'] = current_day__DD_str
                current_obj['final_nc4_filename'] = final_nc4_filename
                granule_name = final_nc4_filename
                granule_contextual_information = ""
                granule_pipeline_state = Config_SettingService.get_value(
                    setting_name="GRANULE_PIPELINE_STATE__ATTEMPTING", default_or_error_return_value="Attempting")
                additional_json = current_obj
                new_Granule_UUID = self.etl_parent_pipeline_instance.log_etl_granule(
                    granule_name=granule_name,
                    granule_contextual_information=granule_contextual_information,
                    granule_pipeline_state=granule_pipeline_state,
                    additional_json=additional_json)

                # Save the Granule's UUID for reference in later steps
                current_obj['Granule_UUID'] = str(new_Granule_UUID).strip()

                # Add to the granules list
                self._expected_granules.append(current_obj)

        except:
            sysErrorData = str(sys.exc_info())
            print(sysErrorData)
            error_JSON = {}
            error_JSON[
                'error'] = "Error: There was an error when generating the expected remote filepaths.  See the " \
                           "additional data for details on which expected file caused the error.  System Error " \
                           "Message: " + str(
                sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.class_name
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            return common.get_function_response_object(class_name=self.class_name,
                                                       function_name=ret__function_name,
                                                       is_error=ret__is_error,
                                                       event_description=ret__event_description,
                                                       error_description=ret__error_description,
                                                       detail_state_info=ret__detail_state_info)

        # Make sure the directories exist
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(self.temp_working_dir)
        if is_error_creating_directory:
            error_JSON = {}
            error_JSON[
                'error'] = "Error: There was an error when the pipeline tried to create a new directory on the " \
                           "filesystem.  The path that the pipeline tried to create was: " + str(
                self.temp_working_dir) + ".  There should be another error logged just before this one that contains " \
                                         "system error info.  That info should give clues to why the directory was " \
                                         "not able to be created. "
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.class_name
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            return common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                       is_error=ret__is_error,
                                                       event_description=ret__event_description,
                                                       error_description=ret__error_description,
                                                       detail_state_info=ret__detail_state_info)

        # final_load_dir_path
        is_error_creating_directory = self.etl_parent_pipeline_instance.create_dir_if_not_exist(
            self.final_load_dir_path)
        if is_error_creating_directory == True:
            error_JSON = {}
            error_JSON[
                'error'] = "Error: There was an error when the pipeline tried to create a new directory on the " \
                           "filesystem.  The path that the pipeline tried to create was: " + str(
                self.final_load_dir_path) + ".  There should be another error logged just before this one that " \
                                            "contains system error info.  That info should give clues to why the " \
                                            "directory was not able to be created. "
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.class_name
            error_JSON['function_name'] = "execute__Step__Pre_ETL_Custom"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            return common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                       is_error=ret__is_error,
                                                       event_description=ret__event_description,
                                                       error_description=ret__error_description,
                                                       detail_state_info=ret__detail_state_info)

        # Ended, now for reporting
        ret__detail_state_info['class_name'] = self.class_name
        ret__detail_state_info['number_of_expected_granules'] = str(len(self._expected_granules)).strip()
        ret__event_description = "Success.  Completed Step execute__Step__Pre_ETL_Custom by generating " + str(
            len(self._expected_remote_full_file_paths)).strip() + " expected full file paths to download and " + str(
            len(self._expected_granules)).strip() + " expected granules to process."

        return common.get_function_response_object(class_name=self.class_name,
                                                   function_name=ret__function_name,
                                                   is_error=ret__is_error,
                                                   event_description=ret__event_description,
                                                   error_description=ret__error_description,
                                                   detail_state_info=ret__detail_state_info)

    def execute__Step__Download(self, uuid):
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {'class_name': self.class_name, 'custom_message': "No download needed"}

        return common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                   is_error=ret__is_error, event_description=ret__event_description,
                                                   error_description=ret__error_description,
                                                   detail_state_info=ret__detail_state_info)

    def execute__Step__Extract(self):
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {'class_name': self.class_name,
                                  'custom_message': "SMAP types do not need to be extracted.  The source files are "
                                                    "non-compressed Tif and Tfw files."}

        return common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                   is_error=ret__is_error, event_description=ret__event_description,
                                                   error_description=ret__error_description,
                                                   detail_state_info=ret__detail_state_info)

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

                    temporal_resolution = 'daily'
                    mode_var__precipAttr_comment = "LIS model output"
                    mode_var__fileAttr_Description = "Land Information System output from the Noah land surface model."
                    mode_var__fileAttr_Version = "1.0"

                    print(expected_granules_object["domain_file_path"])

                    ds = xr.open_dataset(expected_granules_object["domain_file_path"])

                    latVals = np.round(np.round(np.nanmin(ds.lat.values), 3) + 0.03 * np.arange(0, 2231), 3)
                    lonVals = np.round(np.round(np.nanmin(ds.lon.values), 3) + 0.03 * np.arange(0, 2351), 3)

                    ds = ds.rename({
                        'Evap_acc': 'Evapotranspiration',
                        'SoilMoist_tavg': 'Soil Moisture',
                        'Qs_acc': 'Runoff',
                        'Qsb_acc': 'Baseflow'
                    })
                    ds = ds.rename_dims({'north_south': 'latitude', 'east_west': 'longitude'})
                    ds = ds.drop_vars(['lat', 'lon', 'PotEvap_acc'])
                    ds = ds.assign_coords(latitude=latVals)  # something like this, may be dim not dims
                    ds = ds.assign_coords(longitude=lonVals)

                    yearStr = expected_granules_object["date_year"]
                    monthStr = expected_granules_object["date_month"]
                    dayStr = expected_granules_object["date_day"]

                    start_time = pd.Timestamp('{}-{}-{}T00:00:00'.format(yearStr, monthStr, dayStr))
                    end_time = pd.Timestamp('{}-{}-{}T23:59:59'.format(yearStr, monthStr, dayStr))

                    # ds = ds.isel(band=0).reset_coords('band', drop=True)
                    # Add the time dimension as a new coordinate.
                    ds = ds.assign_coords(time=start_time).expand_dims(dim='time', axis=0)
                    # Add a variable "time_bnds" for the time boundaries.
                    ds['time_bnds'] = xr.DataArray(np.array([start_time, end_time]).reshape((1, 2)),
                                                   dims=['time', 'nbnds'])

                    ds.latitude.attrs = OrderedDict(
                        [('long_name', 'latitude'), ('units', 'degrees_north'), ('axis', 'Y')])
                    ds.longitude.attrs = OrderedDict(
                        [('long_name', 'longitude'), ('units', 'degrees_east'), ('axis', 'X')])
                    ds.time.attrs = OrderedDict(
                        [('long_name', 'time'), ('axis', 'T'), ('bounds', 'time_bnds')])
                    ds.time_bnds.attrs = OrderedDict(
                        [('long_name', 'time_bounds')])
                    ds["Evapotranspiration"].attrs = OrderedDict(
                        [('long_name', "Evapotranspiration"),
                         ('units', 'mm'),
                         ('accumulation_interval', temporal_resolution),
                         ('comment', str(mode_var__precipAttr_comment))])

                    ds["Evapotranspiration"].encoding = {
                        '_FillValue': np.float32(-9999.0),
                        'missing_value': np.float32(-9999.0),
                        'dtype': np.dtype('float32'),
                        'chunksizes': (1, 256, 256)
                    }

                    ds["Soil Moisture"].attrs = OrderedDict(
                        [('long_name', "Soil Moisture"),
                         ('units', 'm^3 m-3'),
                         ('comment', str(mode_var__precipAttr_comment) + ". Layers: 0-10, 10-40, 40-100, 100-200 cm")])

                    ds["Soil Moisture"].encoding = {
                        '_FillValue': np.float32(-9999.0),
                        'missing_value': np.float32(-9999.0),
                        'dtype': np.dtype('float32'),
                        'chunksizes': (1, 256, 256)
                    }

                    ds["Runoff"].attrs = OrderedDict(
                        [('long_name', "Surface runoff"),
                         ('units', 'mm'),
                         ('accumulation_interval', temporal_resolution),
                         ('comment', str(mode_var__precipAttr_comment))])

                    ds["Runoff"].encoding = {
                        '_FillValue': np.float32(-9999.0),
                        'missing_value': np.float32(-9999.0),
                        'dtype': np.dtype('float32'),
                        'chunksizes': (1, 256, 256)
                    }

                    ds["Baseflow"].attrs = OrderedDict(
                        [('long_name', "Subsurface runoff"),
                         ('units', 'mm'),
                         ('accumulation_interval', temporal_resolution),
                         ('comment', str(mode_var__precipAttr_comment))])

                    ds["Baseflow"].encoding = {
                        '_FillValue': np.float32(-9999.0),
                        'missing_value': np.float32(-9999.0),
                        'dtype': np.dtype('float32'),
                        'chunksizes': (1, 256, 256)
                    }
                    ds.attrs = OrderedDict([
                        ('Description', str(mode_var__fileAttr_Description)),
                        ('DateCreated', pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%SZ')),
                        ('Contact', 'Lance Gilliland, lance.gilliland@nasa.gov'),
                        ('Source',
                         'NASA Short-Term Prediction Research and Transition (SPoRT) Center; Clay Blankenship, '
                         'clay.blankenship@nasa.gov'),
                        ('Version', str(mode_var__fileAttr_Version)),
                        ('Reference',
                         'Ellenburg, W. L., V. Mishra, J. Roberts, A. Limaye, J. L. Case, C. B. Blankenship, '
                         'and K. Cressman, '
                         '2021:  Detecting desert locust breeding grounds:  A satellite assisted modeling approach.  '
                         'Remote Sensing, '
                         '13(7), 1276. https://doi.org/10.3390/rs13071276'),
                        ('NUM_SOIL_LAYERS', np.int32(4)),
                        ('SOIL_LAYER_THICKNESSES',
                         [np.float32(10.0), np.float32(30.0), np.float32(60.0), np.float32(100.0)]),
                        ('RangeStartTime', start_time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                        ('RangeEndTime', end_time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                        ('SouthernmostLatitude', np.min(ds.latitude.values)),
                        ('NorthernmostLatitude', np.max(ds.latitude.values)),
                        ('WesternmostLongitude', np.min(ds.longitude.values)),
                        ('EasternmostLongitude', np.max(ds.longitude.values)),
                        ('TemporalResolution', temporal_resolution),
                        ('SpatialResolution', '0.03deg')
                    ])

                    ds.time.encoding = {'units': 'seconds since 1970-01-01T00:00:00Z', 'dtype': np.dtype('int32')}
                    ds.time_bnds.encoding = {'units': 'seconds since 1970-01-01T00:00:00Z',
                                             'dtype': np.dtype('int32')}

                    # final_load_dir_path
                    print("self.final_load_dir_path " + self.final_load_dir_path)
                    os.makedirs(os.path.dirname(self.final_load_dir_path), exist_ok=True)
                    ds.to_netcdf(self.final_load_dir_path + expected_granules_object["final_nc4_filename"],
                                 unlimited_dims='time')
                    duplicate_nc4_filepath = self.final_load_dir_path.replace(
                        "mnt/climateserv", "mnt/nvmeclimateserv") + expected_granules_object["final_nc4_filename"]
                    os.makedirs(os.path.dirname(self.final_load_dir_path.replace(
                        "mnt/climateserv", "mnt/nvmeclimateserv")), exist_ok=True)
                    shutil.copyfile(self.final_load_dir_path + expected_granules_object["final_nc4_filename"],
                                    duplicate_nc4_filepath)

                except Exception as e:
                    print(e)
                    ret__is_error = True

                    sysErrorData = str(sys.exc_info())

                    Granule_UUID = expected_granules_object['Granule_UUID']

                    error_message = "usada_smap.execute__Step__Transform: An Error occurred during the Transform " \
                                    "step with ETL_Granule UUID: " + str(
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
                'error'] = "Error: There was an uncaught error when processing the Transform step on all of the " \
                           "expected Granules.  See the additional data and system error message for details on what " \
                           "caused this error.  System Error Message: " + str(
                sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.class_name
            error_JSON['function_name'] = "execute__Step__Transform"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            return common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                       is_error=ret__is_error,
                                                       event_description=ret__event_description,
                                                       error_description=ret__error_description,
                                                       detail_state_info=ret__detail_state_info)

        ret__detail_state_info['class_name'] = self.class_name
        ret__detail_state_info['error_counter'] = error_counter
        ret__detail_state_info['detail_errors'] = detail_errors

        return common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                   is_error=ret__is_error, event_description=ret__event_description,
                                                   error_description=ret__error_description,
                                                   detail_state_info=ret__detail_state_info)

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
                                                                                final_nc4_filename)
                    expected_full_path_to_local_final_nc4_file = expected_granules_object[
                        'local_full_filepath_final_nc4_file']

                    print(expected_full_path_to_local_final_nc4_file)
                    Granule_UUID = expected_granules_object['Granule_UUID']

                    new__granule_pipeline_state = Config_Setting.get_value(
                        setting_name="GRANULE_PIPELINE_STATE__SUCCESS", default_or_error_return_value="SUCCESS")  #
                    is_error = False
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(
                        granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state,
                        is_error=is_error)

                    additional_json = {}
                    additional_json['MostRecent__ETL_Granule_UUID'] = str(Granule_UUID).strip()

                except:
                    sysErrorData = str(sys.exc_info())
                    error_JSON = {}
                    error_JSON[
                        'error'] = "Error: There was an error when attempting to copy the current nc4 file to it's " \
                                   "final directory location.  See the additional data and system error message for " \
                                   "details on what caused this error.  System Error Message: " + str(
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
                'error'] = "Error: There was an uncaught error when processing the Load step on all of the expected " \
                           "Granules.  See the additional data and system error message for details on what caused " \
                           "this error.  System Error Message: " + str(
                sysErrorData)
            error_JSON['is_error'] = True
            error_JSON['class_name'] = self.class_name
            error_JSON['function_name'] = "execute__Step__Load"
            # Exit Here With Error info loaded up
            ret__is_error = True
            ret__error_description = error_JSON['error']
            ret__detail_state_info = error_JSON
            return common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                         is_error=ret__is_error,
                                                         event_description=ret__event_description,
                                                         error_description=ret__error_description,
                                                         detail_state_info=ret__detail_state_info)

        return common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                     is_error=ret__is_error, event_description=ret__event_description,
                                                     error_description=ret__error_description,
                                                     detail_state_info=ret__detail_state_info)

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

        return common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                     is_error=ret__is_error, event_description=ret__event_description,
                                                     error_description=ret__error_description,
                                                     detail_state_info=ret__detail_state_info)

    def execute__Step__Clean_Up(self):
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = "No Clean up needed"
        ret__error_description = "No Clean up needed"
        ret__detail_state_info = {}

        return common.get_function_response_object(class_name=self.class_name, function_name=ret__function_name,
                                                   is_error=ret__is_error, event_description=ret__event_description,
                                                   error_description=ret__error_description,
                                                   detail_state_info=ret__detail_state_info)
