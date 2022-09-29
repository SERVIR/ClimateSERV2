import glob, os, sys, re
import smtplib
import datetime
import urllib
import requests
import gzip
import shutil
from collections import OrderedDict

import xarray as xr
import pandas as pd
import numpy as np

from django.contrib.auth.models import User
from django.utils import timezone
from django.db.models.functions import Now

from api.services import Config_SettingService, ETL_DatasetService, ETL_GranuleService, ETL_LogService, \
    ETL_PipelineRunService

from ..models import Config_Setting, ETL_Dataset, Storage_Review, Profile

from ..serializers import ETL_DatasetSerializer

from . import etl_exceptions


class ETL_Pipeline():
    dataset = None
    stdout = None
    style = None
    START_YEAR_YYYY = None
    START_MONTH_MM = None
    START_DAY_DD = None
    END_YEAR_YYYY = None
    END_MONTH_MM = None
    END_DAY_YYYY = None
    _expected_granules = []


    # Default Constructor
    def __init__(self, dataset, stdout, style):
        self.dataset = dataset
        self.stdout = stdout
        self.style = style


    # function set_start_date
    # Sets the start date variables START_YEAR_YYY, START_MONTH_MM, and START_DAY_DD to the dates of the last processed
    # file in the final load directory of the dataset
    def set_start_date(self):
        final_load_dir = self.dataset.final_load_dir
        list_of_files = sorted(filter(os.path.isfile, glob.glob(final_load_dir + '/**/*', recursive=True)))
        if len(list_of_files) != 0:
            last_processed_file = list_of_files[-1]
            date = os.path.basename(last_processed_file).split('.')
            if len(date) > 0:
                self.START_YEAR_YYYY = int(date[1][:4])
                self.START_MONTH_MM = int(date[1][4:6])
                self.START_DAY_DD = int(date[1][6:8])


    def set_end_date(self):
        today = datetime.date.today()
        self.END_YEAR_YYYY = today.year
        self.END_MONTH_MM = today.month
        self.END_DAY_YYYY = today.day


    def get_replacement_string(self, key, format_parameters):
        current_date = format_parameters['current_date']
        if key == 'product':
            return self.dataset.tds_product_name
        if key == 'region':
            return self.dataset.tds_region
        if key == 'spatial_resolution':
            return self.dataset.tds_spatial_resolution
        if key == 'temporal_resolution':
            return self.dataset.tds_temporal_resolution
        if 'YYYYMMDD' in key:
            if key == 'YYYYMMDD':
                return "{:0>4d}{:02d}{:02d}".format(current_date.year, current_date.month, current_date.day)
            match = re.search(r'(?<=-)\d+', key)
            if match:
                days_before = int(match.group(0))
                delta_date = current_date - datetime.timedelta(days=days_before)
                return "{:0>4d}{:02d}{:02d}".format(delta_date.year, delta_date.month, delta_date.day)
        if key == 'YYYY':
            return "{:0>4d}".format(current_date.year)
        if key == 'MM':
            return "{:02d}".format(current_date.month)
        if key == 'DD':
            return "{:02d}".format(current_date.day)


    def format_string(self, pre_format_str, format_parameters):
        open_bracket_i = pre_format_str.find('{')
        close_bracket_i = pre_format_str.find('}')
        while open_bracket_i >= 0 and close_bracket_i >= 0:
            key = pre_format_str[open_bracket_i+1:close_bracket_i]
            pre_format_str = pre_format_str[0:open_bracket_i] + self.get_replacement_string(key, format_parameters) + pre_format_str[close_bracket_i+1:]
            open_bracket_i = pre_format_str.find('{')
            close_bracket_i = pre_format_str.find('}')
        return pre_format_str


    def copy_nc4_file(self, working_nc4_filepath, final_nc4_filepath):
        # Copy file over to final directory
        shutil.copyfile(working_nc4_filepath, final_nc4_filepath)

        # Store duplicate copy as backup
        duplicate_nc4_filepath = re.sub('/mnt/climateserv/', '/mnt/nvmeclimateserv/', final_nc4_filepath, 1)
        os.makedirs(os.path.dirname(duplicate_nc4_filepath), exist_ok=True)
        shutil.copyfile(working_nc4_filepath, duplicate_nc4_filepath)


    def construct_granules(self):
        try:
            start_date = datetime.datetime(year=self.START_YEAR_YYYY, month=self.START_MONTH_MM,
                                           day=self.START_DAY_DD)
            end_date = datetime.datetime(year=self.END_YEAR_YYYY, month=self.END_MONTH_MM, day=self.END_DAY_YYYY)
            delta = end_date - start_date
            for i in range(delta.days + 1):
                current_date = start_date + datetime.timedelta(days=i)
                format_parameters = {'current_date' : current_date}
                nc4_filename = self.format_string(self.dataset.final_nc4_filename_format_string, format_parameters)
                base_filename = self.format_string(self.dataset.filename_format_string, format_parameters)
                filename = base_filename + self.dataset.filename_extension

                # Now get the remote url
                remote_directory_path = self.format_string(self.dataset.source_url, format_parameters)

                # Getting full paths
                remote_full_filepath = urllib.parse.urljoin(remote_directory_path, filename)
                local_full_filepath = os.path.join(self.dataset.temp_working_dir, filename)
                local_full_nc4_filepath = os.path.join(self.dataset.final_load_dir, nc4_filename)
                local_working_nc4_filepath = os.path.join(self.dataset.temp_working_dir, nc4_filename)

                # Make the current Granule Object
                expected_granule = {}

                # Date Info (Which Day Is it?)
                expected_granule['date'] = current_date

                # Filename and Granule Name info
                expected_granule['local_extract_path'] = self.dataset.temp_working_dir  # Download path
                expected_granule['local_final_load_path'] = self.dataset.final_load_dir  # The path of the final output granule file.

                # Filename and Granule Name info
                expected_granule['base_filename'] = base_filename
                expected_granule['filename'] = filename
                expected_granule['final_nc4_filename'] = nc4_filename

                # Full Paths
                expected_granule['remote_full_filepath'] = remote_full_filepath
                expected_granule['local_full_filepath'] = local_full_filepath
                expected_granule['local_full_nc4_filepath'] = local_full_nc4_filepath
                expected_granule['local_working_nc4_filepath'] = local_working_nc4_filepath

                filename_list = []
                filepath_list = []
                base_filename_list = []
                remote_full_filepath_list = []
                for preformat_filename, preformat_source_url, ext in zip(self.dataset.filename_format_string_list,
                                                                         self.dataset.source_url_list,
                                                                         self.dataset.filename_extension_list):
                    remote_directory_path = self.format_string(preformat_source_url, format_parameters)
                    base_filename = self.format_string(preformat_filename, format_parameters)
                    filename = base_filename + ext
                    local_full_filepath = os.path.join(self.dataset.temp_working_dir, filename)
                    remote_full_filepath = urllib.parse.urljoin(remote_directory_path, filename)

                    filename_list.append(filename)
                    base_filename_list.append(base_filename)
                    filepath_list.append(local_full_filepath)
                    remote_full_filepath_list.append(remote_full_filepath)

                expected_granule['base_filename_list'] = base_filename_list
                expected_granule['filename_list'] = filename_list
                expected_granule['filepath_list'] = filepath_list
                expected_granule['remote_filepath_list'] = remote_full_filepath_list

                expected_granule['granule_has_error'] = False

                # Add to the granules list
                self._expected_granules.append(expected_granule)

        except Exception as e:
            expected_granule['granule_has_error'] = True

            exc_type, exc_obj, exc_tb = sys.exc_info()
            msg_string = 'Uncaught exception in construct_granules. System error message at line number {}: {}'
            self.stdout.write(self.style.ERROR(msg_string.format(exc_tb.tb_lineno, e)))


    def download_granules(self):
        # Setting up for the periodic reporting on the terminal
        num_of_objects_to_process = len(self._expected_granules)

        # Loop through each expected granule
        for i, expected_granule in enumerate(self._expected_granules):
            try:
                file_list = expected_granule['filename_list']
                path_list = expected_granule['filepath_list']
                url_list = expected_granule['remote_filepath_list']

                msg_string = "Starting download of granule {} out of {}. Files to download: {}"
                self.stdout.write(msg_string.format(i + 1, num_of_objects_to_process, file_list))

                for file, path, url in zip(file_list, path_list, url_list):
                    # Download the file - Actually do the download now
                    r = requests.get(url)
                    if r.ok:
                        with open(path, 'wb') as outfile:
                            outfile.write(r.content)

                        msg_string = "Successfully downloaded file {}"
                        self.stdout.write(msg_string.format(file))
                    else:
                        expected_granule['granule_has_error'] = True

                        msg_string = "Error when downloading file {}. This file may be unavailable at this time."
                        self.stdout.write(self.style.ERROR(msg_string.format(file)))

            except Exception as e:
                expected_granule['granule_has_error'] = True

                exc_type, exc_obj, exc_tb = sys.exc_info()
                msg_string = 'Uncaught exception in download_granules. System error message at line number {}: {}'
                self.stdout.write(self.style.ERROR(msg_string.format(exc_tb.tb_lineno, e)))


    def extract_granules(self):
        for expected_granule in self._expected_granules:
            if expected_granule['granule_has_error']:
                self.stdout.write(self.style.ERROR(
                    'Aborting extraction of file {}.'.format(expected_granule['base_filename'])
                ))
                continue
            try:
                path_list = expected_granule['filepath_list']
                local_extract_path = expected_granule['local_extract_path']
                base_filename_list = expected_granule['base_filename_list']
                extension_list = self.dataset.filename_extension_list
                for (i, (path, base_filename, ext)) in enumerate(zip(path_list, base_filename_list, extension_list)):
                    local_extract_full_filepath = os.path.join(local_extract_path, base_filename + '.tif')

                    if ext == '.tif.gz':
                        self.stdout.write("Extracting file {}".format(base_filename))
                        if os.path.isfile(path):
                            with gzip.open(path, 'rb') as f_in:
                                with open(local_extract_full_filepath, 'wb') as f_out:
                                    shutil.copyfileobj(f_in, f_out)
                                    expected_granule['filepath_list'][i] = local_extract_full_filepath
                        else:
                            expected_granule['granule_has_error'] = True
                            self.stdout.write(self.style.ERROR(
                                'Aborting extraction of file {}. This file may not have been available for download at this time.'.format(
                                    base_filename)
                            ))

            except Exception as e:
                expected_granule['granule_has_error'] = True

                exc_type, exc_obj, exc_tb = sys.exc_info()
                msg_string = 'Uncaught exception in extract_granules. System error message at line number {}: {}'
                self.stdout.write(self.style.ERROR(msg_string.format(exc_tb.tb_lineno, e)))


    def transform_granules(self):
        for expected_granule in self._expected_granules:
            if expected_granule['granule_has_error']:
                self.stdout.write(self.style.ERROR(
                    'Skipping creation of nc4 file for {}.'.format(expected_granule['base_filename'])
                ))
                continue
            try:
                # Getting info ready for the current granule.
                geotiff_filepath = expected_granule['local_full_filepath']
                output_working_path = expected_granule['local_working_nc4_filepath']
                current_date = expected_granule['date']
                base_filename = expected_granule['base_filename']

                if not os.path.isfile(geotiff_filepath):
                    expected_granule['granule_has_error'] = True
                    self.stdout.write(self.style.ERROR(
                        'Skipping creation of nc4 file for {}. This is likely because the remote resource was not available.'.format(base_filename)
                    ))
                    continue
                ############################################################
                # Start extracting data and creating output netcdf file.
                ############################################################

                year = "{:0>4d}".format(current_date.year)
                month = "{:02d}".format(current_date.month)
                day = "{:02d}".format(current_date.day)
                temporal_resolution = self.dataset.tds_temporal_resolution
                spatial_resolution = self.dataset.tds_spatial_resolution
                dataset_attribute_json = self.dataset.nc4_attribute_data
                variable_name = self.dataset.dataset_nc4_variable_name
                end_date = current_date
                if(temporal_resolution == 'daily'):
                    start_date = current_date
                if(temporal_resolution == '10dy'):
                    start_date = current_date - datetime.timedelta(days=9)


                # Determine the timestamp for the data.
                start_time = pd.Timestamp('{:0>4d}-{:02d}-{:02d}T00:00:00'.format(start_date.year, start_date.month, start_date.day))
                end_time = pd.Timestamp('{:0>4d}-{:02d}-{:02d}T23:59:59'.format(end_date.year, end_date.month, end_date.day))

                # 1) Read the geotiff data into an xarray data array
                da = xr.open_rasterio(geotiff_filepath)

                # 2) Convert to a dataset.  (need to assign a name to the data array)
                ds = da.rename(variable_name).to_dataset()

                # Handle selecting/adding the dimesions
                # select the singleton band dimension and drop out the associated coordinate.
                ds = ds.isel(band=0).reset_coords('band', drop=True)

                # Add the time dimension as a new coordinate.
                ds = ds.assign_coords(time=start_time).expand_dims(dim='time', axis=0)

                # Add an additional variable "time_bnds" for the time boundaries.
                ds['time_bnds'] = xr.DataArray(np.array([start_time, end_time]).reshape((1, 2)), dims=['time', 'nbnds'])

                # 3) Rename and add attributes to this dataset.
                ds = ds.rename({'y': 'latitude', 'x': 'longitude'})

                # 4) Reorder latitude dimension into ascending order
                if ds.latitude.values[1] - ds.latitude.values[0] < 0:
                    ds = ds.reindex(latitude=ds.latitude[::-1])

                # Set the Attributes
                ds.latitude.attrs = OrderedDict(
                    [('long_name', 'latitude'), ('units', 'degrees_north'), ('axis', 'Y')])
                ds.longitude.attrs = OrderedDict(
                    [('long_name', 'longitude'), ('units', 'degrees_east'), ('axis', 'X')])
                ds.time.attrs = OrderedDict([('long_name', 'time'), ('axis', 'T'), ('bounds', 'time_bnds')])
                ds.time_bnds.attrs = OrderedDict([('long_name', 'time_bounds')])

                ds[variable_name].attrs = OrderedDict(
                    [('long_name', variable_name),
                     ('units', 'mm'),
                     ('accumulation_interval', temporal_resolution),
                     ('comment', dataset_attribute_json['comment'])])

                ds.attrs = OrderedDict([
                    ('Description', dataset_attribute_json['description']),
                    ('DateCreated', pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%SZ')),
                    ('Contact', dataset_attribute_json['contact']),
                    ('Source', dataset_attribute_json['source']),
                    ('Version', dataset_attribute_json['version']),
                    ('Reference', dataset_attribute_json['reference']),
                    ('RangeStartTime', start_time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                    ('RangeEndTime', end_time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                    ('SouthernmostLatitude', np.min(ds.latitude.values)),
                    ('NorthernmostLatitude', np.max(ds.latitude.values)),
                    ('WesternmostLongitude', np.min(ds.longitude.values)),
                    ('EasternmostLongitude', np.max(ds.longitude.values)),
                    ('TemporalResolution', temporal_resolution),
                    ('SpatialResolution', spatial_resolution)
                ])

                # Set the Encodings
                ds[variable_name].encoding = {
                    '_FillValue': np.float32(-9999.0),
                    'missing_value': np.float32(-9999.0),
                    'dtype': np.dtype('float32'),
                    'chunksizes': (1, 256, 256)
                }
                ds.time.encoding = {'units': 'seconds since 1970-01-01T00:00:00Z', 'dtype': np.dtype('int32')}
                ds.time_bnds.encoding = {'units': 'seconds since 1970-01-01T00:00:00Z',
                                         'dtype': np.dtype('int32')}

                # 5) Output File
                ds.to_netcdf(output_working_path, unlimited_dims='time')

            except Exception as e:
                expected_granule['granule_has_error'] = True

                exc_type, exc_obj, exc_tb = sys.exc_info()
                msg_string = 'Uncaught exception in transform_granules. System error message at line number {}: {}'
                self.stdout.write(self.style.ERROR(msg_string.format(exc_tb.tb_lineno, e)))


    def load_granules(self):
        for expected_granule in self._expected_granules:
            if expected_granule['granule_has_error']:
                self.stdout.write(self.style.ERROR(
                    'Skipping loading of nc4 file for {}.'.format(base_filename)
                ))
                continue
            try:
                nc4_working_path = expected_granule['local_working_nc4_filepath']
                nc4_final_path = expected_granule['local_full_nc4_filepath']

                # Copy the file from the working directory over to the final location for it.  (Where THREDDS
                # Monitors for it)
                self.copy_nc4_file(nc4_working_path, nc4_final_path)

            except Exception as e:
                expected_granule['granule_has_error'] = True

                exc_type, exc_obj, exc_tb = sys.exc_info()
                msg_string = 'Uncaught exception in load_granules. System error message at line number {}: {}'
                self.stdout.write(self.style.ERROR(msg_string.format(exc_tb.tb_lineno, e)))

    def merge_granules(self):
        pass


    def clean_up(self):
        pass


    # function execute_etl_pipeline
    # Initializes an ETL Pipeline run for the dataset that the ETL Pipeline class was initialized with.
    def execute_etl_pipeline(self):
        self.set_start_date()
        self.set_end_date()
        self.construct_granules()
        #self.download_granules()
        self.extract_granules()
        self.transform_granules()
        self.load_granules()
        self.merge_granules()
        self.clean_up()