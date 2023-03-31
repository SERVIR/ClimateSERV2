import calendar
import sys
import datetime
import glob
import gzip
import os
import re
import shutil
import urllib
import zipfile
import json
from collections import OrderedDict

import numpy as np
import pandas as pd
import requests
import xarray as xr

from api.etl.daily_iterator import Daily_Iterator
from api.etl.dekadal_iterator import Dekadal_Iterator
from api.etl.link_iterator import Link_Iterator
from api.etl.imerg_link_iterator import IMERG_Link_Iterator
from api.etl.esi_iterator import ESI_Iterator
from api.etl.lis_iterator import LIS_Iterator


class ETL_Pipeline:
    dataset = None
    stdout = None
    style = None
    start_date = None
    end_date = None
    date_iterator = None
    extract_extensions = ['.tif.gz', '.zip']

    # Default Constructor
    def __init__(self, dataset, stdout, style):
        self.dataset = dataset
        self.stdout = stdout
        self.style = style

    # function set_start_date
    def get_start_date(self):
        final_load_dir = self.dataset.final_load_dir
        list_of_files = sorted(filter(os.path.isfile, glob.glob(final_load_dir + '/**/*', recursive=True)))
        if len(list_of_files) != 0:
            last_processed_file = list_of_files[-1]
            date = os.path.basename(last_processed_file).split('.')
            if len(date) > 0:
                year = int(date[1][:4])
                month = int(date[1][4:6])
                day = int(date[1][6:8])
                self.start_date = datetime.date(year=year, month=month, day=day)
                return self.start_date
            else:
                # TODO catch error
                pass
        else:
            # TODO Change behavior on no files found
            # Setting default for now
            self.start_date = datetime.date(year=2022, month=12, day=1)
            return self.start_date

    def get_end_date(self):
        today = datetime.date.today()
        self.end_date = today
        return today

    def get_replacement_string(self, key, format_params):
        current_date = format_params['current_date']
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
            match = re.search(r'(?<=\+)\d+', key)
            if match:
                days_before = int(match.group(0))
                delta_date = current_date + datetime.timedelta(days=days_before)
                return "{:0>4d}{:02d}{:02d}".format(delta_date.year, delta_date.month, delta_date.day)
        if key == 'YYYY':
            return "{:0>4d}".format(current_date.year)
        if key == 'MM':
            return "{:02d}".format(current_date.month)
        if key == 'DD':
            return "{:02d}".format(current_date.day)
        if key == 'YY':
            return "{:02d}".format(current_date.year % 100)
        if key == 'DK':
            dekad = (current_date.month - 1) * 3
            dekad += min(current_date.day - 1, 29) // 10 + 1
            return str(dekad)
        if key == 'time':
            return format_params['current_time']

    def format_string(self, pre_format_str, format_params):
        open_bracket_i = pre_format_str.find('{')
        close_bracket_i = pre_format_str.find('}')
        formatted_string = pre_format_str
        while open_bracket_i >= 0 and close_bracket_i >= 0:
            key = formatted_string[open_bracket_i+1:close_bracket_i]
            formatted_string = formatted_string[0:open_bracket_i] + self.get_replacement_string(key, format_params) + \
                               formatted_string[close_bracket_i+1:]
            open_bracket_i = formatted_string.find('{')
            close_bracket_i = formatted_string.find('}')
        return formatted_string

    @staticmethod
    def copy_nc4_file(working_nc4_filepath, final_nc4_filepath):
        # Copy file over to final directory
        shutil.copyfile(working_nc4_filepath, final_nc4_filepath)

        # Store duplicate copy as backup
        duplicate_nc4_filepath = re.sub('/mnt/climateserv/', '/mnt/nvmeclimateserv/', final_nc4_filepath, 1)
        os.makedirs(os.path.dirname(duplicate_nc4_filepath), exist_ok=True)
        shutil.copyfile(working_nc4_filepath, duplicate_nc4_filepath)

    def get_date_iterator(self):
        if self.dataset is None or self.start_date is None or self.end_date is None:
            # TODO Catch error
            pass

        iteration_type = self.dataset.dataset_availability
        if iteration_type == "daily":
            return Daily_Iterator(self.start_date, self.end_date)
        elif iteration_type == "dekadal":
            return Dekadal_Iterator(self.start_date, self.end_date)
        elif iteration_type == "link":
            options = self.dataset.dataset_information['iterator_options']
            return Link_Iterator(self.start_date, self.end_date, options)
        elif iteration_type == "imerg":
            options = self.dataset.dataset_information['iterator_options']
            return IMERG_Link_Iterator(self.start_date, self.end_date, options)
        elif iteration_type == "esi":
            options = self.dataset.dataset_information['iterator_options']
            return ESI_Iterator(self.start_date, self.end_date, options)
        elif iteration_type == "lis":
            options = self.dataset.dataset_information['iterator_options']
            return LIS_Iterator(self.start_date, self.end_date, options)
        else:
            # TODO Catch error or provide default
            return None

    def download_handler(self, format_params, file_info, granule):
        working_path = self.dataset.temp_working_dir
        filename = ""

        has_error = False

        ext = file_info['extension']
        requires_extraction = ext in self.extract_extensions

        if "filename" in granule:
            filename = granule['filename']
        else:
            filename = self.format_string(file_info['preformat_filename'], format_params) + ext
        full_path = os.path.join(working_path, filename)

        dataset_information = self.dataset.dataset_information

        if file_info['download_type'] == 'https':
            source = file_info['source']
            if "source" in granule:
                url = granule['source']
            else:
                url = urllib.parse.urljoin(self.format_string(source, format_params), filename)

            r = requests.get(url)
            if r.ok:
                with open(full_path, 'wb') as outfile:
                    outfile.write(r.content)
            else:
                # TODO ERROR could not find remote resource
                has_error = True
                pass
        elif file_info['download_type'] == 'imerg_session':
            source = granule['source']

            session = requests.Session()
            session.auth = (dataset_information['session_user'], dataset_information['session_pass'])

            r = session.get(source)
            if not os.path.exists(os.path.dirname(working_path)):
                os.makedirs(os.path.dirname(working_path))
            with open(full_path, 'wb') as outfile:
                outfile.write(r.content)
        elif file_info['download_type'] == 'filesystem':
            full_path = granule['source']
        else:
            pass

        return_obj = {
            "has_error": has_error,
            "requires_extraction": requires_extraction,
            "path": full_path,
            "filename": filename,
        }

        return return_obj

    def extraction_handler(self, path, file_info, format_params, file_variable):
        working_path = self.dataset.temp_working_dir
        has_error = False

        ext = file_info['extension']

        return_obj = {
            "has_error": has_error,
            "files": {}
        }

        if ext == '.tif.gz':
            with gzip.open(path, 'rb') as f_in:
                tif_path = os.path.splitext(path)[0]
                # GZipped TIF files will extract a file with the same name, simply replacing the extension with .tif
                with open(tif_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    file_info["path"] = tif_path
                    # TODO ERROR check if variable already exists
                    return_obj["files"][file_variable] = file_info
        elif ext == '.zip':
            with zipfile.ZipFile(path, "r") as f_in:
                list_of_files = f_in.namelist()
                zip_files = file_info["contained_files"]
                for file in zip_files.keys():
                    local_file_info = zip_files[file]
                    ext = local_file_info['extension']
                    init = True
                    backup_idx = 0
                    filename = self.format_string(local_file_info['preformat_filename'], format_params) + ext
                    while init or ("backup_filename" in local_file_info and
                                   backup_idx <= len(local_file_info['backup_filename'])):
                        if init:
                            init = False
                        else:
                            backup_idx += 1
                        if filename in list_of_files:
                            # Extract a single file from zip
                            f_in.extract(filename, working_path)
                            local_file_info["path"] = os.path.join(working_path, filename)
                            return_obj["files"][file] = local_file_info
                            break
                        # TODO Keep track of partial download netcdf4 files created like this
                        filename = self.format_string(local_file_info['backup_filename'][backup_idx],
                                                      format_params) + ext

        else:
            # TODO ERROR could not extract (database info not correctly set up)
            pass

        return return_obj

    def format_attributes(self, attributes):
        attribute_obj = OrderedDict()

        insertion_order = attributes['insertion_order']

        # TODO catch database irregularity errors
        for attr in insertion_order:
            attribute_obj[attr] = attributes[attr]

        return attribute_obj

    def format_wildcard(self, wildcard):
        end = wildcard.find('=')
        key = wildcard[1:end]
        value = wildcard[end+1:len(wildcard)-1]
        if key == 'npdtype':
            return np.dtype(value)
        elif key == 'npfloat':
            return np.float32(value)
        elif key == 'npuint16':
            return np.uint16(value)
        elif key == 'npint32':
            return np.int32(value)
        elif key == 'npfloat32array':
            arr = json.loads(value)
            new_arr = []
            for val in arr:
                new_arr.append(np.float32(val))
            return new_arr
        elif key == 'tuple':
            num_elems = value.count(",")
            tuple_vals = value.split(", ")
            if not num_elems == len(tuple_vals):
                # TODO ERROR in database format
                pass
            if len(tuple_vals) == 3:
                return int(tuple_vals[0]), int(tuple_vals[1]), int(tuple_vals[2])
        elif key == 'float':
            return float(value)
        elif key == 'bool':
            return bool(value)
        elif key == 'int':
            return int(value)
        else:
            pass
            # TODO ERROR key not found, not set up properly

    def format_encoding(self, encoding):
        encoding_obj = encoding.copy()

        for key in encoding_obj.keys():
            encoding_obj[key] = self.format_wildcard(encoding_obj[key])

        return encoding_obj

    def execute_etl_pipeline(self):
        try:
            dataset_information = self.dataset.dataset_information

            self.get_start_date()
            self.get_end_date()

            if "adjust_start_date" in dataset_information:
                if dataset_information['adjust_start_date'] == 'lis':
                    source_dir = dataset_information['iterator_options']['source_dir']
                    forecast_dir = os.path.join(source_dir, "forecasts")
                    list_of_files = sorted(filter(os.path.isfile, glob.glob(forecast_dir + '/*.nc', recursive=False)))
                    self.start_date = self.start_date - datetime.timedelta(days=len(list_of_files))

            working_path = self.dataset.temp_working_dir

            date_iterator = self.get_date_iterator()

            self.date_iterator = date_iterator
            granule_list = date_iterator.get_range()

            preformat_nc4_filename = dataset_information['preformat_nc4_filename']
            data_info = dataset_information['data']

            # Loop through list of possible dates
            for granule in granule_list:
                date = granule['date']
                print("Starting to process date:", date)
                if "add_hhmmss" in dataset_information:
                    format_params = {'current_date': datetime.datetime.strptime(date, '%Y%m%d%H%M%S')}
                else:
                    format_params = {'current_date': datetime.datetime.strptime(date, '%Y%m%d')}
                if "time" in granule:
                    format_params['current_time'] = granule['time']
                # Generate final NC4 Filename
                final_nc4_filename = self.format_string(preformat_nc4_filename, format_params)

                downloaded_files = {}
                for file_variable in data_info.keys():
                    file_info = data_info[str(file_variable)].copy()
                    download_result = self.download_handler(format_params, file_info, granule)
                    if not download_result["has_error"]:
                        if download_result["requires_extraction"]:
                            extraction_result = self.extraction_handler(download_result["path"], file_info,
                                                                        format_params, file_variable)
                            downloaded_files.update(extraction_result["files"])
                        else:
                            file_info["path"] = download_result["path"]
                            downloaded_files[file_variable] = file_info
                    else:
                        # TODO ERROR and continue to next date
                        pass

                dataset_by_variable = []
                variable_list = []
                encodings_by_variable = {}
                attributes_by_variable = {}
                final_ds = xr.Dataset()

                for file_variable in downloaded_files.keys():
                    file_info = downloaded_files[file_variable]
                    variables = file_info["variables"]
                    path = file_info["path"]
                    if file_info['extension'] == '.nc':
                        ds = xr.open_dataset(path)
                        for variable in variables.keys():
                            file_variable_data = variables[variable]
                            profile_number = file_variable_data['profile'] if 'profile' in file_variable_data.keys() \
                                                                           else None
                            original_variable = file_variable_data['original_variable']

                            if profile_number is not None:
                                ds = ds.assign(variables={variable: ds[original_variable][profile_number]})
                            else:
                                ds = ds.assign(variables={variable: ds[original_variable]})
                            variable_list.append(variable)

                            # TODO catch ERRORS if database not properly configured
                            encodings_by_variable[variable] = file_info["encodings"][variable]
                            attributes_by_variable[variable] = file_info["attributes"][variable]

                        ds = ds.drop_vars(file_info['vars_to_drop'])
                    else:
                        file = xr.open_rasterio(path)
                        if "rescale_accumulated_precip" in dataset_information:
                            file = file / 10.0
                        file_ds = file.rename(file_variable).to_dataset()
                        for band in variables.keys():
                            variable = variables[band]
                            variable_list.append(variable)

                            variable_ds = file_ds.isel(band=int(band))\
                                                 .reset_coords('band', drop=True)\
                                                 .rename({file_variable: variable})

                            dataset_by_variable.append(variable_ds)
                            # TODO catch ERRORS if database not properly configured
                            encodings_by_variable[variable] = file_info["encodings"][variable]
                            attributes_by_variable[variable] = file_info["attributes"][variable]
                        ds = xr.merge(dataset_by_variable)
                    final_ds = xr.merge([final_ds, ds])

                ds = final_ds

                # Set default timestamps
                start_time = format_params['current_date']
                end_time = start_time
                ds_time_index = start_time

                if dataset_information["nc4_indexed_by"] == "start":
                    file_start_date = format_params['current_date']
                    start_time = file_start_date + datetime.timedelta(**dataset_information["timedelta_start"])
                    end_time = file_start_date + datetime.timedelta(**dataset_information["timedelta_end"])
                    ds_time_index = file_start_date + datetime.timedelta(**dataset_information["timedelta_ds_time_index"])
                if dataset_information["nc4_indexed_by"] == "dekad_start":
                    file_start_date = format_params['current_date']
                    start_time = file_start_date + datetime.timedelta(**dataset_information["timedelta_start"])
                    month = file_start_date.month
                    year = file_start_date.year
                    day = calendar.monthrange(year, month)[1]
                    end_date = min((start_time + datetime.timedelta(days=10)).date(),
                                   datetime.date(year=year, month=month, day=day))
                    end_time = end_date + datetime.timedelta(**dataset_information["timedelta_end"])
                    ds_time_index = start_time

                if "convert_to_ndvi" in dataset_information:
                    ds = (ds - 100.0) / 100.0

                print(ds_time_index)

                # Add the time dimension as a new coordinate.
                ds = ds.assign_coords(time=ds_time_index).expand_dims(dim='time', axis=0)
                ds['time_bnds'] = xr.DataArray(np.array([start_time, end_time]).reshape((1, 2)), dims=['time', 'nbnds'])

                # 3) Rename and add attributes to this dataset.
                if "lis_manual_lat_lon" in dataset_information:
                    lat_vals = np.round(np.round(np.nanmin(ds.lat.values), 3) + 0.03 * np.arange(0, 2231), 3)
                    lon_vals = np.round(np.round(np.nanmin(ds.lon.values), 3) + 0.03 * np.arange(0, 2351), 3)

                    ds = ds.rename_dims({'north_south': 'latitude', 'east_west': 'longitude'})

                    ds = ds.assign_coords(latitude=lat_vals)  # something like this, may be dim not dims
                    ds = ds.assign_coords(longitude=lon_vals)
                    ds = ds.drop_vars(['lat', 'lon'])
                else:
                    ds = ds.rename({'y': 'latitude', 'x': 'longitude'})

                if "round_coords_lat_lon" in dataset_information:
                    ds = ds.assign_coords(latitude=np.around(ds.latitude.values, decimals=6),
                                          longitude=np.around(ds.longitude.values, decimals=6))

                # 4) Reorder latitude dimension into ascending order
                if ds.latitude.values[1] - ds.latitude.values[0] < 0:
                    ds = ds.reindex(latitude=ds.latitude[::-1])

                if "slice_roi" in dataset_information:
                    region_range = dataset_information["slice_roi"]
                    ds = ds.sel(latitude=slice(region_range[0][0], region_range[0][1]),
                                longitude=slice(region_range[1][0], region_range[1][1]))

                # TODO add modifications based on dataset
                lat_attr = OrderedDict([('long_name', 'latitude'), ('units', 'degrees_north'), ('axis', 'Y')])
                lon_attr = OrderedDict([('long_name', 'longitude'), ('units', 'degrees_east'), ('axis', 'X')])

                time_attr = OrderedDict([('long_name', 'time'), ('axis', 'T'), ('bounds', 'time_bnds')])
                time_bounds_attr = OrderedDict([('long_name', 'time_bounds')])

                attribute_dict = dataset_information['metadata']

                # TODO static assignment of latitude possible by dataset
                attribute_dict['south'] = np.min(ds.latitude.values)
                attribute_dict['north'] = np.max(ds.latitude.values)
                attribute_dict['east'] = np.max(ds.longitude.values)
                attribute_dict['west'] = np.min(ds.longitude.values)

                if "add_version_imerg" in dataset_information:
                    attribute_dict['version'] = granule['version']

                # attribute_dict['temporal_resolution'] = None  # TODO
                # attribute_dict['spatial_resolution'] = None  # TODO

                std_file_attr = OrderedDict([('Description', attribute_dict['description']),
                                             ('DateCreated', pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%SZ')),
                                             ('Contact', attribute_dict['contact']),
                                             ('Source', attribute_dict['source']),
                                             ('Version', attribute_dict['version']),
                                             ('Reference', attribute_dict['reference']),
                                             ('RangeStartTime', start_time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                                             ('RangeEndTime', end_time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                                             ('SouthernmostLatitude', attribute_dict['south']),
                                             ('NorthernmostLatitude', attribute_dict['north']),
                                             ('WesternmostLongitude', attribute_dict['west']),
                                             ('EasternmostLongitude', attribute_dict['east']),
                                             ('TemporalResolution', attribute_dict['temporal_resolution']),
                                             ('SpatialResolution', attribute_dict['spatial_resolution'])])

                file_attr = OrderedDict()

                for key, value in std_file_attr.items():
                    file_attr[key] = value
                    if "modify_file_attr" in dataset_information:
                        if key in dataset_information["modify_file_attr"].keys():
                            attr_names = dataset_information["modify_file_attr"][key]["insertion_order"]
                            attr_values = dataset_information["modify_file_attr"][key]["attrs_to_insert"]
                            attr_values = self.format_encoding(attr_values)
                            for added_key in attr_names:
                                file_attr[added_key] = attr_values[added_key]

                time_encoding = {'units': 'seconds since 1970-01-01T00:00:00Z', 'dtype': np.dtype('int32')}
                time_bounds_encoding = {'units': 'seconds since 1970-01-01T00:00:00Z', 'dtype': np.dtype('int32')}

                # Set the Attributes
                ds.latitude.attrs = lat_attr
                ds.longitude.attrs = lon_attr

                ds.time.attrs = time_attr
                ds.time_bnds.attrs = time_bounds_attr

                for variable in variable_list:
                    ds[variable].attrs = self.format_attributes(attributes_by_variable[variable])

                ds.attrs = file_attr

                # Set the Encodings
                for variable in variable_list:
                    ds[variable].encoding = self.format_encoding(encodings_by_variable[variable])

                ds.time.encoding = time_encoding
                ds.time_bnds.encoding = time_bounds_encoding

                if "is_lat_order_reversed" in dataset_information:
                    ds = ds.reindex(latitude=ds.latitude[::-1])

                nc4_working_path = os.path.join(working_path, final_nc4_filename)
                ds.to_netcdf(nc4_working_path, unlimited_dims='time')

                # Add to nc4 directory
                final_load_dir = self.dataset.final_load_dir
                nc4_final_path = os.path.join(final_load_dir, final_nc4_filename)

                # TODO uncomment
                # shutil.copyfile(nc4_working_path, nc4_final_path)

                # TODO Fix this
                # duplicate_nc4_filepath = re.sub('/mnt/climateserv/', '/mnt/nvmeclimateserv/', nc4_final_path, 1)
                # os.makedirs(os.path.dirname(duplicate_nc4_filepath), exist_ok=True)
                # shutil.copyfile(nc4_working_path, duplicate_nc4_filepath)
                # TODO REMOVE THIS PLEASE
                break
            # Merge datasets
            # Clean up
        except Exception as error:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.stdout.write(self.style.ERROR(
                'Uncaught exception at line number {} in file {}. System error message: {}.'.format(exc_tb.tb_lineno,
                                                                                                    fname, error)
            ))
