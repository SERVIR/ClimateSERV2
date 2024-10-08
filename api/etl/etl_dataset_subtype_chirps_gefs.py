import datetime, gzip, os, requests, shutil, sys, urllib
import glob

import xarray as xr
import pandas as pd
import numpy as np
from collections import OrderedDict
from copy import deepcopy
from .utils import sendNotification, listFD
from .common import common
from .etl_dataset_subtype_interface import ETL_Dataset_Subtype_Interface
from .etl_dataset_subtype import ETL_Dataset_Subtype

from api.services import Config_SettingService

from bs4 import BeautifulSoup

class ETL_Dataset_Subtype_CHIRPS_GEFS(ETL_Dataset_Subtype, ETL_Dataset_Subtype_Interface):

    # init (Passing a reference from the calling class, so we can callback the error handler)
    def __init__(self, etl_parent_pipeline_instance=None, dataset_subtype=None):
        super().__init__()
        self.etl_parent_pipeline_instance = etl_parent_pipeline_instance
        self.class_name = self.__class__.__name__
        self._expected_remote_full_file_paths = []
        self._expected_granules = []
        self.dates_arr=[]
        self.misc_error = ""
        if dataset_subtype == 'chirps_gefs':
            self.mode = 'chirps_gefs'
        else:
            self.mode = 'chirps_gefs'

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

            start_date = datetime.datetime(year=self.YYYY__Year__Start, month=self.MM__Month__Start, day=self.DD__Day__Start)
            end_date = datetime.datetime(year=self.YYYY__Year__End, month=self.MM__Month__End, day=self.DD__Day__End)

            filenames = []
            dates = []
            new_dates = []
            dates_arr = []
            sd=start_date
            while sd <= end_date:
                new_dates.append(sd)
                sd += datetime.timedelta(days=1)

            response = requests.get(current_root_http_path)
            soup = BeautifulSoup(response.text, 'html.parser')
            for _, link in enumerate(soup.findAll('a')):
                if link.get('href').endswith('.tif'):
                    date_string= link.get('href').split('/')[-1].split('_')
                    l_date_string = link.get('href').split('/')[-1].split('_')[1].split('.')[0]
                    date = datetime.datetime.strptime(l_date_string,'%Y%m%d')
                    if start_date <= date <= end_date:
                        filenames.append(link.get_text())
                        dates.append(date)

            # for ETL alerts
            list_of_files = glob.glob(final_load_dir_path + '/*')
            if len(list_of_files) > 0:
                latest_file = max(list_of_files, key=os.path.getctime)
                date = latest_file.split('.')[2]
                date_part = date.split('T')[0]
                datetime_object = datetime.datetime.strptime(date_part, '%Y%m%d')
            for nd in new_dates:
                # delta =  datetime_object - nd
                delta = datetime.datetime.now() - nd
                file_Date = nd.strftime('%Y%m%d')
                print(delta)
                if (int(delta.days) < int(self.etl_parent_pipeline_instance.dataset.late_after)):
                    self.dates_arr.append(file_Date)
                    self.misc_error =  "There was an issue downloading one or more files."
            if len(self.dates_arr) > 0:
                sendNotification(uuid, self.etl_parent_pipeline_instance.dataset.dataset_name+"-"+self.etl_parent_pipeline_instance.dataset.dataset_subtype, self.dates_arr, int(self.etl_parent_pipeline_instance.dataset.late_after))
                #ret__is_error = True
            for filename, date in zip(filenames, dates):

                current_year__YYYY_str  = "{:0>4d}".format(date.year)
                current_month__MM_str   = "{:02d}".format(date.month)
                current_day__DD_str     = "{:02d}".format(date.day)

                # Create the base filename
                base_filename = filename

                # Create the final nc4 filename
                # ucsb-chirp.20210731T000000Z.global.0.05deg.daily.nc4
                product = 'chirps-gefs'
                temporal_resolution = '10dy'
                final_nc4_filename = 'ucsb-{}.{}{}{}T000000Z.global.0.05deg.{}.nc4'.format(
                    product,
                    current_year__YYYY_str,
                    current_month__MM_str,
                    current_day__DD_str,
                    temporal_resolution
                )

                # Now get the remote File Paths (Directory) based on the date infos.
                # All 3 of these mode products use a year appended to the end of their path.
                # Add the Year to the directory path.
                remote_directory_path = current_root_http_path
                print(remote_directory_path)

                # Getting full paths
                remote_full_filepath_tif            = urllib.parse.urljoin(remote_directory_path, base_filename)
                local_full_filepath_tif             = os.path.join(self.temp_working_dir, base_filename)
                local_full_filepath_final_nc4_file  = os.path.join(final_load_dir_path, final_nc4_filename)

                # Make the current Granule Object
                current_obj = {}

                # Date Info (Which Day Is it?)
                current_obj['date_YYYY']    = current_year__YYYY_str
                current_obj['date_MM']      = current_month__MM_str
                current_obj['date_DD']      = current_day__DD_str

                # Filename and Granule Name info
                local_extract_path      = self.temp_working_dir
                local_final_load_path   = final_load_dir_path
                current_obj['local_extract_path']       = local_extract_path  # Download path
                current_obj['local_final_load_path']    = local_final_load_path  # The path where the final output granule file goes.
                current_obj['remote_directory_path']    = remote_directory_path

                # Filename and Granule Name info
                current_obj['base_filename']            = base_filename
                current_obj['tif_filename']             = base_filename
                current_obj['final_nc4_filename']       = final_nc4_filename
                current_obj['granule_name']             = final_nc4_filename

                # Full Paths
                current_obj['remote_full_filepath_tif'] = remote_full_filepath_tif
                current_obj['local_full_filepath_tif'] = local_full_filepath_tif
                current_obj['local_full_filepath_final_nc4_file'] = local_full_filepath_final_nc4_file

                # Create a new Granule Entry - The first function 'log_etl_granule' is the one that actually creates a new ETL Granule Attempt (There is one granule per dataset per pipeline attempt run in the ETL Granule Table)
                granule_pipeline_state = Config_SettingService.get_value(setting_name="GRANULE_PIPELINE_STATE__ATTEMPTING", default_or_error_return_value="Attempting")
                new_Granule_UUID = self.etl_parent_pipeline_instance.log_etl_granule(granule_name=final_nc4_filename, granule_contextual_information="", granule_pipeline_state=granule_pipeline_state, additional_json=current_obj)

                # Save the Granule's UUID for reference in later steps
                current_obj['Granule_UUID'] = str(new_Granule_UUID).strip()

                # Add to the granules list
                self._expected_granules.append(current_obj)

        except Exception as e:
            print(e)
            sysErrorData = str(sys.exc_info())
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

        # Setting up for the periodic reporting on the terminal
        num_of_objects_to_process = len(self._expected_granules)
        num_of_download_activity_events = 4
        modulus_size = int(num_of_objects_to_process / num_of_download_activity_events)
        if modulus_size < 1:
            modulus_size = 1
        dates_arr = []
        print(num_of_objects_to_process)
        # Loop through each expected granule
        for expected_granule in self._expected_granules:
            try:
                if (loop_counter + 1) % modulus_size == 0:
                    event_message = "About to download file: " + str(loop_counter + 1) + " out of " + str(num_of_objects_to_process)
                    print(event_message)
                    activity_event_type = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__DOWNLOAD_PROGRESS", default_or_error_return_value="ETL Download Progress")
                    activity_description = event_message
                    additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                    self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
                # Current Granule to download
                remote_directory_path       = expected_granule['remote_directory_path']
                tif_filename                = expected_granule['tif_filename']
                local_full_filepath_tif     = expected_granule['local_full_filepath_tif']
                local_full_filepath_tif1    = local_full_filepath_tif.replace('data-mean', 'anomaly-mean')
                print(remote_directory_path)

                # Download the file - Actually do the download now
                try:
                    current_url_to_download = urllib.parse.urljoin(remote_directory_path, tif_filename)
                    print(current_url_to_download)
                    r = requests.get(current_url_to_download)
                    if r.ok:
                        with open(local_full_filepath_tif, 'wb') as outfile:
                            outfile.write(r.content)
                    else:
                        r.raise_for_status()
                    current_url_to_download = urllib.parse.urljoin(current_url_to_download.replace('precip_mean', 'anom_mean'), tif_filename.replace('data-mean', 'anomaly-mean'))
                    print(current_url_to_download)
                    r = requests.get(current_url_to_download)
                    if r.ok:
                        with open(local_full_filepath_tif1, 'wb') as outfile:
                            outfile.write(r.content)
                    else:
                        print('anomaly-mean file not found!')
                    download_counter = download_counter + 1
                except:
                    self.misc_error = "There was an issue downloading one or more files."
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
                    activity_event_type         = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_WARNING", default_or_error_return_value="ETL Warning")
                    activity_description        = warn_JSON['warning']
                    self.etl_parent_pipeline_instance.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=warn_JSON)

            except:
                error_counter = error_counter + 1
                sysErrorData = str(sys.exc_info())
                error_message = "ETL_Dataset_Subtype_CHIRPS.execute__Step__Download: Generic Uncaught Error.  At least 1 download failed.  System Error Message: " + str(sysErrorData)
                detail_errors.append(error_message)
                print(error_message)

            # Increment the loop counter
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
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        # For IMERG, there is nothing to extract (we are already downloading TIF)
        ret__detail_state_info['class_name'] = self.__class__.__name__
        ret__detail_state_info['custom_message'] = "CHIRPS GEFS types do not need to be extracted.  The source files are non-compressed Tif files."

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
                    final_nc4_filename = expected_granules_object['final_nc4_filename']
                    expected_full_path_to_local_extracted_tif_file = os.path.join(local_extract_path, tif_filename)
                    expected_full_path_to_local_extracted_tif_file1 = os.path.join(local_extract_path, tif_filename.replace('data-mean', 'anomaly-mean'))

                    geotiffFile_FullPath = expected_full_path_to_local_extracted_tif_file
                    geotiffFile_FullPath1 = expected_full_path_to_local_extracted_tif_file1

                    print(final_nc4_filename)

                    # print("B")

                    mode_var__precipAttr_comment    = 'chirps_gefs'
                    mode_var__fileAttr_Description  = 'chirps_gefs'
                    mode_var__fileAttr_Version      = 'chirps_gefs'

                    # print("C")

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
                    # geotiffFile: The inputfile to be processed
                    #
                    # General Flow:
                    # Determine the date associated with the geoTiff file
                    # 1) Use xarray+rasterio to read the geotiff data from a file into a data array
                    # 2) Convert to a dataset and add an appropriate time dimension
                    # 3) Clean up the dataset: Rename and add dimensions, attributes, and scaling factors as appropriate
                    # 4) Dump the precipitation dataset to a netCDF-4 file with a filename conforming to the ClimateSERV 2.0 TDS conventions

                    # Based on the geotiffFile name, determine the time string elements.
                    # Split elements by period

                    TimeStrSplit          = tif_filename.split('_')
                    start_yearStr         = TimeStrSplit[1][:4]
                    start_monthStr        = TimeStrSplit[1][4:6]
                    start_dayStr          = TimeStrSplit[1][6:8]
                    end_yearStr           = TimeStrSplit[2][:4]
                    end_monthStr          = TimeStrSplit[2][4:6]
                    end_dayStr            = TimeStrSplit[2][6:8]
                    temporal_resolution = '10 days'

                    # Determine the timestamp for the data.
                    start_time = pd.Timestamp('{}-{}-{}T00:00:00'.format(start_yearStr, start_monthStr, start_dayStr))
                    end_time = pd.Timestamp('{}-{}-{}T23:59:59'.format(end_yearStr, end_monthStr, end_dayStr))

                    # print("D")

                    ############################################################
                    # Beging extracting data and creating output netcdf file.
                    ############################################################

                    # 1) Read the geotiff data into an xarray data array
                    da = xr.open_rasterio(geotiffFile_FullPath)
                    da1 = None
                    if os.path.isfile(geotiffFile_FullPath1):
                        da1 = xr.open_rasterio(geotiffFile_FullPath1)
                    else:
                        da1 = deepcopy(da)
                        da1.values[:] = np.nan
                    # 2) Convert to a dataset.  (need to assign a name to the data array)
                    # ds = da.rename('precipitation_amount').to_dataset()
                    # 2) Merge both into a dataset
                    ds = xr.merge([da.rename(self.etl_parent_pipeline_instance.dataset.dataset_nc4_variable_name), da1.rename('precipitation_anomaly')])
                    # Handle selecting/adding the dimesions
                    ds = ds.isel(band=0).reset_coords('band', drop=True)  # select the singleton band dimension and drop out the associated coordinate.
                    # Add the time dimension as a new coordinate.
                    ds = ds.assign_coords(time=start_time).expand_dims(dim='time', axis=0)
                    # Add an additional variable "time_bnds" for the time boundaries.
                    ds['time_bnds'] = xr.DataArray(np.array([start_time, end_time]).reshape((1, 2)), dims=['time', 'nbnds'])
                    # 3) Rename and add attributes to this dataset.
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
                    ds[self.etl_parent_pipeline_instance.dataset.dataset_nc4_variable_name].attrs = OrderedDict([('long_name', self.etl_parent_pipeline_instance.dataset.dataset_nc4_variable_name), ('units', 'mm'), ('accumulation_interval', temporal_resolution), ('comment', str(mode_var__precipAttr_comment))])
                    if self.mode == "chirps_gefs":
                        ds.precipitation_anomaly.attrs = OrderedDict([('long_name', 'precipitation_anomaly'), ('units', 'mm'), ('accumulation_interval', temporal_resolution), ('comment', 'Ensemble mean GEFS forecast bias corrected and converted into anomaly versus CHIRPS 2.0 climatology')])
                    ds.attrs = OrderedDict([
                        ('Description', str(mode_var__fileAttr_Description)),
                        ('DateCreated', pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%SZ')),
                        ('Contact', 'Lance Gilliland, lance.gilliland@nasa.gov'),
                        ('Source', 'University of California at Santa Barbara; Climate Hazards Group; Pete Peterson, pete@geog.ucsb.edu; https://data.chc.ucsb.edu/products/EWX/data/forecasts/CHIRPS-GEFS_precip_v12/10day/precip_mean/'),
                        ('Version', str(mode_var__fileAttr_Version)),
                        ('Reference', 'Funk, C.C., Peterson, P.J., Landsfeld, M.F., Pedreros, D.H., Verdin, J.P., Rowland, J.D., Romero, B.E., Husak, G.J., Michaelsen, J.C., and Verdin, A.P., 2014, A quasi-global precipitation time series for drought monitoring: U.S. Geological Survey Data Series 832, 4 p., http://dx.doi.org/110.3133/ds832.'),
                        ('RangeStartTime', start_time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                        ('RangeEndTime', end_time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                        ('SouthernmostLatitude', np.min(ds.latitude.values)),
                        ('NorthernmostLatitude', np.max(ds.latitude.values)),
                        ('WesternmostLongitude', np.min(ds.longitude.values)),
                        ('EasternmostLongitude', np.max(ds.longitude.values)),
                        ('TemporalResolution', temporal_resolution),
                        ('SpatialResolution', '0.05deg')
                    ])
                    # Set the Endcodings
                    ds[self.etl_parent_pipeline_instance.dataset.dataset_nc4_variable_name].encoding = {
                        '_FillValue': np.float32(-9999.0),
                        'missing_value': np.float32(-9999.0),
                        'dtype': np.dtype('float32'),
                        'chunksizes': (1, 256, 256)
                    }
                    if self.mode == "chirps_gefs":
                        ds.precipitation_anomaly.encoding = {
                            '_FillValue': np.float32(-9999.0),
                            'missing_value': np.float32(-9999.0),
                            'dtype': np.dtype('float32'),
                            'chunksizes': (1, 256, 256)
                        }
                    ds.time.encoding = {'units': 'seconds since 1970-01-01T00:00:00Z', 'dtype': np.dtype('int32')}
                    ds.time_bnds.encoding = {'units': 'seconds since 1970-01-01T00:00:00Z', 'dtype': np.dtype('int32')}

                    # print("F")

                    # 5) Output File
                    outputFile_FullPath = os.path.join(local_extract_path, final_nc4_filename)
                    ds.to_netcdf(outputFile_FullPath, unlimited_dims='time')

                    # print("G")

                    print(outputFile_FullPath)

                except Exception as e:
                    print(e)
                    ret__is_error = True

                    sysErrorData = str(sys.exc_info())

                    Granule_UUID = expected_granules_object['Granule_UUID']

                    error_message = "ETL_Dataset_Subtype_CHIRPS.execute__Step__Transform: An Error occurred during the Transform step with ETL_Granule UUID: " + str(Granule_UUID) + ".  System Error Message: " + str(sysErrorData)

                    # Individual Transform Granule Error
                    error_counter = error_counter + 1
                    detail_errors.append(error_message)

                    error_JSON = {}
                    error_JSON['error_message'] = error_message

                    # Update this Granule for Failure (store the error info in the granule also)
                    new__granule_pipeline_state = Config_SettingService.get_value(setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")
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
                    expected_full_path_to_local_final_nc4_file = expected_granules_object['local_full_filepath_final_nc4_file']  # Where the final NC4 file should be placed for THREDDS Server monitoring

                    print(expected_full_path_to_local_final_nc4_file)

                    # Copy the file from the working directory over to the final location for it.  (Where THREDDS Monitors for it)
                    super()._copy_nc4_file(expected_full_path_to_local_working_nc4_file, expected_full_path_to_local_final_nc4_file)

                    # Create a new Granule Entry - The first function 'log_etl_granule' is the one that actually creates a new ETL Granule Attempt (There is one granule per dataset per pipeline attempt run in the ETL Granule Table)
                    Granule_UUID = expected_granules_object['Granule_UUID']

                    new__granule_pipeline_state = Config_SettingService.get_value(setting_name="GRANULE_PIPELINE_STATE__SUCCESS", default_or_error_return_value="SUCCESS")
                    is_error = False
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)

                    additional_json = {}
                    additional_json['MostRecent__ETL_Granule_UUID'] = str(Granule_UUID).strip()
                    # self.etl_parent_pipeline_instance.create_or_update_Available_Granule(granule_name=final_nc4_filename, granule_contextual_information="", additional_json=additional_json)

                except:
                    sysErrorData = str(sys.exc_info())
                    error_JSON = {}
                    error_JSON['error'] = "Error: There was an error when attempting to copy the current nc4 file to it's final directory location.  See the additional data and system error message for details on what caused this error.  System Error Message: " + str(sysErrorData)
                    error_JSON['is_error'] = True
                    error_JSON['class_name'] = self.__class__.__name__
                    error_JSON['function_name'] = ret__function_name

                    # Additional infos
                    error_JSON['expected_full_path_to_local_working_nc4_file'] = str(expected_full_path_to_local_working_nc4_file).strip()
                    error_JSON['expected_full_path_to_local_final_nc4_file'] = str(expected_full_path_to_local_final_nc4_file).strip()

                    # Update this Granule for Failure (store the error info in the granule also)
                    Granule_UUID = expected_granules_object['Granule_UUID']
                    new__granule_pipeline_state = Config_SettingService.get_value(setting_name="GRANULE_PIPELINE_STATE__FAILED", default_or_error_return_value="FAILED")
                    is_error = True
                    is_update_succeed = self.etl_parent_pipeline_instance.etl_granule__Update__granule_pipeline_state(granule_uuid=Granule_UUID, new__granule_pipeline_state=new__granule_pipeline_state, is_error=is_error)
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
        ret__function_name = sys._getframe().f_code.co_name
        ret__is_error = False
        ret__event_description = ""
        ret__error_description = ""
        ret__detail_state_info = {}

        try:
            super().execute__Step__Post_ETL_Custom()
        except Exception as e:
            print(e)

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
                activity_event_type = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_BLANK", default_or_error_return_value="Temp Working Dir Blank")  #
                activity_description = "Could not remove the temporary working directory.  The value for self.temp_working_dir was blank. "
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = self.__class__.__name__
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
            else:
                shutil.rmtree(temp_working_dir)
                # Log an ETL Activity that says that the value of the temp_working_dir was blank.
                activity_event_type = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__TEMP_WORKING_DIR_REMOVED", default_or_error_return_value="Temp Working Dir Removed")  #
                activity_description = "Temp Working Directory, " + str(self.temp_working_dir).strip() + ", was removed."
                additional_json = self.etl_parent_pipeline_instance.to_JSONable_Object()
                additional_json['subclass'] = self.__class__.__name__
                additional_json['temp_working_dir'] = str(temp_working_dir).strip()
                self.etl_parent_pipeline_instance.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
            if self.misc_error != "" or len(self.dates_arr)>0:
                self.etl_parent_pipeline_instance.log_etl_error(activity_event_type="Error in ETL run",
                                                                activity_description=self.misc_error,
                                                                etl_granule_uuid="", is_alert=False,
                                                                additional_json=additional_json)
                ret__is_error=True
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
