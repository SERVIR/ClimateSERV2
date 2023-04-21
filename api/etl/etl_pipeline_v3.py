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
import numpy as np
import pandas as pd
import requests
import xarray as xr
import math
from pathlib import Path
from collections import OrderedDict
from dateutil import rrule
from bs4 import BeautifulSoup


class ETL_Pipeline:
    cleanup_paths = []

    # Default Constructor
    def __init__(self, dataset, stdout, style, start_date, end_date):
        self.dataset = dataset
        self.stdout = stdout
        self.style = style
        self.start_date = start_date
        self.end_date = end_date
        self.final_load_dir = dataset.final_load_dir
        self.temp_working_dir = dataset.temp_working_dir
        self.dataset_information = dataset.dataset_information

########################################################################################################################
# FORMATTING FUNCTIONS #################################################################################################
########################################################################################################################

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
                days_after = int(match.group(0))
                delta_date = current_date + datetime.timedelta(days=days_after)
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
            return format_params['time_string'] if 'time_string' in format_params else '000000'

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

    def format_attributes(self, attributes):
        attribute_obj = OrderedDict()

        insertion_order = attributes['insertion_order']

        for attr in insertion_order:
            if attr not in attributes:
                # TODO catch database irregularity errors
                pass
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

    def has_wildcard(self, search_str):
        open_i = search_str.find('{')
        close_i = search_str.find('}')
        return open_i >= 0 and close_i >= 0

    def count_wildcard(self, search_str):
        open_c = search_str.count('{')
        close_c = search_str.count('}')
        if not open_c == close_c:
            # TODO error, wildcards not properly formatted
            pass
        return open_c
        
    def contains_wildcards(self, search_str, wildcards):
        for wildcard in wildcards:
            if wildcard not in search_str:
                return False
        return True

########################################################################################################################
# UTILITY FUNCTIONS ####################################################################################################
########################################################################################################################

    # function set_start_date
    def start_date_from_last_processed(self):
        final_load_dir = self.final_load_dir
        list_of_files = sorted(filter(os.path.isfile, glob.glob(final_load_dir + '/**/*', recursive=True)))
        if len(list_of_files) != 0:
            last_processed_file = list_of_files[-1]
            date = os.path.basename(last_processed_file).split('.')
            if len(date) > 0:
                year = int(date[1][:4])
                month = int(date[1][4:6])
                day = int(date[1][6:8])
                self.start_date = datetime.date(year=year, month=month, day=day)
                self.stdout.write(self.style.WARNING('Pipeline start date was not initialized. Starting from the last '
                                                     'processed date: {}.'.format(self.start_date)))
                return self.start_date
            else:
                self.stdout.write(self.style.ERROR('Improperly formatted nc4 file in final load directory: '
                                                   '{}'.format(last_processed_file)))
                return None
        else:
            return None

    @staticmethod
    def copy_nc4_file(working_nc4_filepath, final_nc4_filepath):
        # Copy file over to final directory
        shutil.copyfile(working_nc4_filepath, final_nc4_filepath)

        # Store duplicate copy as backup
        duplicate_nc4_filepath = re.sub('/mnt/climateserv/', '/mnt/nvmeclimateserv/', final_nc4_filepath, 1)
        os.makedirs(os.path.dirname(duplicate_nc4_filepath), exist_ok=True)
        shutil.copyfile(working_nc4_filepath, duplicate_nc4_filepath)

    def set_dates(self):
        if self.start_date is None:
            if not self.start_date_from_last_processed():
                self.stdout.write(self.style.ERROR('No start date was provided for this ETL Pipeline Run, and the final'
                                                   'load directory is empty. Please provide an adequate start date to'
                                                   'initialize this dataset (run <python manage.py start_etl_pipeline '
                                                   '{ISO_START_DATE}.'))

        if self.end_date is None:
            today = datetime.date.today()
            self.end_date = today
            self.stdout.write(self.style.WARNING('Pipeline end date was not initialized. The last day to be '
                                                 'processed will be today: {}.'.format(self.end_date)))

    def extract_href(self, html_tag):
        return html_tag.get('href')

    def get_begin_date_from_dekad(self, dekad, year):
        dates = [1, 11, 21]
        day = dates[dekad % 3 - 1]
        month = math.ceil(dekad / 3.0)
        return datetime.date(year, month, day)

########################################################################################################################
# GRANULE FUNCTIONS ####################################################################################################
########################################################################################################################

    def merge_granule_data(self, primary_granule, secondary_granule):
        # TODO check overlapping files or different main components to granule (version, time, etc)
        download_information_by_key = primary_granule['download_information_by_key']
        download_information_by_key.update(secondary_granule['download_information_by_key'])
        primary_granule['download_information_by_key'] = download_information_by_key
        return primary_granule

    def initialize_granule(self, date, time_string):
        preformat_nc4_filename = "{product}.{YYYYMMDD}T{time}Z.{region}.{spatial_resolution}.{temporal_resolution}.nc4"
        # TODO different format_params by date string
        format_params = {'current_date': date}
        if time_string:
            format_params['time_string'] = time_string
        nc4_filename = self.format_string(preformat_nc4_filename, format_params)
        granule_data = {
            'nc4_filename': nc4_filename,
            'download_information_by_key': {}
        }

        return granule_data

    def add_time_indices(self, available_dates, available_granules, time_indices_modifications):
        for date in available_dates:
            file_start_date = date
            file_end_date = date
            if "dynamic_end_date" in time_indices_modifications:
                if time_indices_modifications["dynamic_end_date"] == "dekad":
                    month = file_start_date.month
                    year = file_start_date.year
                    last_day_of_month = calendar.monthrange(year, month)[1]
                    file_end_date = min((file_start_date + datetime.timedelta(days=10)).date(),
                                         datetime.date(year=year, month=month, day=last_day_of_month))
            start_time = file_start_date + datetime.timedelta(**time_indices_modifications["timedelta_start"])
            end_time = file_end_date + datetime.timedelta(**time_indices_modifications["timedelta_end"])
            ds_time_index = file_start_date + datetime.timedelta(**time_indices_modifications["timedelta_ds_time_index"])

            available_granules[date]['start_time'] = start_time
            available_granules[date]['end_time'] = end_time
            available_granules[date]['ds_time_index'] = ds_time_index
        return available_granules

    def generate_sources(self, preformat_source):
        list_of_sources = []
        if self.has_wildcard(preformat_source):
            num_wildcards = self.count_wildcard(preformat_source)
            if self.contains_wildcards(preformat_source, ['{YYYY}', '{MM}']) and num_wildcards == 2:
                for date in rrule.rrule(rrule.MONTHLY, dtstart=self.start_date.replace(day=1), until=self.end_date):
                    format_params = {'current_date': date}
                    list_of_sources.append(self.format_string(preformat_source, format_params))
            elif self.contains_wildcards(preformat_source, ['{YYYY}']) and num_wildcards == 1:
                for date in rrule.rrule(rrule.YEARLY, dtstart=self.start_date.replace(day=1), until=self.end_date):
                    format_params = {'current_date': date}
                    list_of_sources.append(self.format_string(preformat_source, format_params))
            else:
                # TODO Has wildcards but current script does not parse them correctly
                pass
            return list_of_sources
        return [preformat_source]

    def extract_match_data(self, match, current_source, pathtype, search_string, path_options, key):
        time_string = None

        if '<time>' in search_string:
            time_string = match.group('time')
        if '<YY>' and '<dekad>' in search_string:
            dekad = int(match.group('dekad'))
            year = 2000 + int(match.group('YY'))
            date = self.get_begin_date_from_dekad(dekad, year)
            format_params = {'current_date': date}
            date_string = self.format_string('{YYYYMMDD}', format_params)
        elif '<YYYY>' and '<MM>' and '<DD>' in search_string:
            year = int(match.group('YYYY'))
            month = int(match.group('MM'))
            day = int(match.group('DD'))
            date = datetime.date(year, month, day)
            format_params = {'current_date': date}
            date_string = self.format_string('{YYYYMMDD}', format_params)
        elif '<delta>' and '<YYYY>' and '<JJJ>' in search_string:
            delta = 28 if match.group('delta') == '4WK' else 84
            year = int(match.group('YYYY'))
            julian_day = int(match.group('JJJ'))
            date = datetime.datetime.strptime('{} {}'.format(julian_day, year), '%j %Y')
            if delta:
                date = date - datetime.timedelta(days=delta)
            format_params = {'current_date': date}
            date_string = self.format_string('{YYYYMMDD}', format_params)
        else:
            date_string = match.group('date')
        if time_string:
            date = datetime.datetime.strptime(date_string + time_string, '%Y%m%d%H%M%S')
        else:
            date = datetime.datetime.strptime(date_string, '%Y%m%d')
        granule_data = self.initialize_granule(date, time_string)
        if time_string:
            granule_data['time_string'] = time_string

        if '<version>' in search_string:
            granule_data['version'] = match.group('version')
            
        download_information = {}
        
        filename = match.group('filename')
        
        if pathtype == 'url':
            source = urllib.parse.urljoin(current_source, filename)
        elif pathtype == 'filesystem':
            source = os.path.join(current_source, filename)
        else:
            # TODO Log error, wrong pathtype
            source = ''

        if '/' in filename:
            download_information['filename'] = filename.split('/')[-1]
        else:
            download_information['filename'] = filename

        download_information['download_type'] = pathtype
        download_information['source'] = source
        
        if 'session_user' and 'session_pass' in path_options:
            download_information['session_user'] = path_options['session_user']
            download_information['session_pass'] = path_options['session_pass']

        granule_data['download_information_by_key'][key] = download_information

        return date, granule_data

    def scrape_html_anchor_matches(self, source, scrape_options):
        match_list = []
        pathtype = 'url'
        path_options = {}

        search_string = scrape_options['search_string']

        if 'session_user' and 'session_pass' in scrape_options:
            requester = requests.Session()
            path_options = {
                'session_user': scrape_options['session_user'],
                'session_pass': scrape_options['session_pass']
            }
            requester.auth = (scrape_options['session_user'], scrape_options['session_pass'])
        else:
            requester = requests
        file_candidates = BeautifulSoup(requester.get(source).text, 'html.parser').findAll('a')
        for file_candidate in map(self.extract_href, file_candidates):
            match = re.search(search_string, file_candidate)
            if match:
                match_list.append(match)

        return match_list, pathtype, search_string, path_options

    def scrape_html_text_matches(self, source, scrape_options):
        match_list = []
        pathtype = 'url'
        path_options = {}

        search_string = scrape_options['search_string']

        if 'session_user' and 'session_pass' in scrape_options:
            requester = requests.Session()
            path_options = {
                'session_user': scrape_options['session_user'],
                'session_pass': scrape_options['session_pass']
            }
            requester.auth = (scrape_options['session_user'], scrape_options['session_pass'])
        else:
            requester = requests
        file_candidates = requester.get(source).text.split()
        for file_candidate in file_candidates:
            match = re.search(search_string, file_candidate)
            if match:
                match_list.append(match)

        return match_list, pathtype, search_string, path_options

    def scrape_filesystem_matches(self, source, scrape_options):
        match_list = []
        pathtype = 'filesystem'
        path_options = {}

        search_string = scrape_options['search_string']

        file_candidates = [f for f in os.listdir(source) if re.search(search_string, f)]
        # TODO check if no found files, may be error

        for file_candidate in file_candidates:
            match = re.search(search_string, file_candidate)
            if match:
                match_list.append(match)

        return match_list, pathtype, search_string, path_options

########################################################################################################################
# ETL FUNCTIONS ########################################################################################################
########################################################################################################################

    def granules_from_file_key(self, file_key):
        scrape_function_by_type = dict(https_anchor=self.scrape_html_anchor_matches,
                                       https_text=self.scrape_html_text_matches,
                                       filesystem=self.scrape_filesystem_matches)

        granule_info = self.dataset_information['granule_info']
        file_info_by_key = granule_info['file_info_by_key']
        file_info = file_info_by_key[file_key]
        # TODO Check JSON formatting (that file_key exists) error if not

        available_dates = []
        available_granules = {}

        preformat_source = file_info['preformat_source']
        source_type = file_info['source_type']
        scrape_options = file_info['scrape_options']

        source_list = self.generate_sources(preformat_source)

        alternate_base_source = None
        if 'alternate_base_source' in file_info:
            alternate_base_source = file_info['alternate_base_source']

        if 'additional_sources' in file_info:
            for additional_source in file_info['additional_sources']:
                source_list += self.generate_sources(additional_source)

        for source in source_list:
            match_list, pathtype, search_string, path_options = scrape_function_by_type[source_type](source,
                                                                                                     scrape_options)
            for match in match_list:
                base_path = alternate_base_source if alternate_base_source else source
                date, granule_data = self.extract_match_data(match, base_path, pathtype, search_string,
                                                             path_options, file_key)
                if date in available_dates:
                    # Possible error, we got the same file for the same date from two places??
                    pass
                if self.start_date <= date.date() <= self.end_date:
                    available_granules[date] = granule_data
                    available_dates.append(date)

        return available_dates, available_granules

    def construct_granules(self):
        granule_info = self.dataset_information['granule_info']

        primary_file_key = granule_info['primary_key']
        time_indices_modifications = granule_info['time_indices_modifications']
        # TODO ensure JSON formatted properly (primary exists, time_modifications)

        available_dates, available_granules = self.granules_from_file_key(primary_file_key)
        available_granules = self.add_time_indices(available_dates, available_granules, time_indices_modifications)

        if 'additional_file_keys' in granule_info:
            for file_key in granule_info['additional_file_keys']:
                dates_from_new_file, granules_from_new_file = self.granules_from_file_key(file_key)
                if not available_dates == dates_from_new_file:
                    # TODO What to do if the new file does not have the same dates available as the primary file?
                    pass
                for date in available_dates:
                    available_granules[date] = self.merge_granule_data(available_granules[date],
                                                                       granules_from_new_file[date])

        # Return output
        return_obj = {
            'available_dates': available_dates,
            'available_granules': available_granules
        }
        return return_obj

    def download_handler(self, download_information):
        working_path = self.temp_working_dir

        has_error = False

        ext = download_information['source'].split('.')[-1]
        requires_extraction = ext in ['zip']

        filename = download_information['filename']
        full_path = os.path.join(working_path, filename)

        if download_information['download_type'] == 'url':
            url = download_information['source']

            if 'session_user' and 'session_pass' in download_information:
                requester = requests.Session()
                requester.auth = (download_information['session_user'], download_information['session_pass'])
            else:
                requester = requests

            r = requester.get(url)
            if r.ok:
                if not os.path.exists(os.path.dirname(working_path)):
                    os.makedirs(os.path.dirname(working_path))
                with open(full_path, 'wb') as outfile:
                    outfile.write(r.content)
                    self.cleanup_paths.append(full_path)
            else:
                # TODO ERROR could not find remote resource
                has_error = True
                pass
        elif download_information['download_type'] == 'filesystem':
            full_path = download_information['source']
        else:
            pass

        if ext == 'gz':
            with gzip.open(full_path, 'rb') as f_in:
                gzip_file_path = os.path.splitext(full_path)[0]
                # GZipped TIF files will extract a file with the same name, simply replacing the extension with .tif
                with open(gzip_file_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    full_path = gzip_file_path
                    self.cleanup_paths.append(full_path)

        return_obj = {
            "has_error": has_error,
            "requires_extraction": requires_extraction,
            "path": full_path,
            "filename": filename,
        }

        return return_obj

    def extraction_handler(self, path, zipped_file_info, format_params):
        working_path = self.temp_working_dir
        has_error = False

        ext = path.split('.')[-1]

        return_obj = {
            "has_error": has_error,
            "files": {}
        }

        if ext == 'zip':
            with zipfile.ZipFile(path, "r") as f_in:
                contained_files = f_in.namelist()
                zip_files = zipped_file_info["contained_files"]
                for file_key in zip_files.keys():
                    contained_file_info = zip_files[file_key]
                    init = True
                    backup_idx = 0
                    filename = self.format_string(contained_file_info['preformat_filename'], format_params)
                    while init or ("backup_filename" in contained_file_info and
                                   backup_idx <= len(contained_file_info['backup_filename'])):
                        if init:
                            init = False
                        else:
                            backup_idx += 1
                        if filename in contained_files:
                            # Extract a single file from zip
                            f_in.extract(filename, working_path)
                            full_path = os.path.join(working_path, filename)
                            contained_file_info["path"] = full_path
                            self.cleanup_paths.append(full_path)

                            return_obj["files"][file_key] = contained_file_info
                            break
                        # TODO Keep track of partial download netcdf4 files created like this
                        filename = self.format_string(zipped_file_info['backup_filename'][backup_idx], format_params)

        else:
            # TODO ERROR could not extract (database info not correctly set up)
            pass

        return return_obj

########################################################################################################################
# MAIN SCRIPT ##########################################################################################################
########################################################################################################################

    def execute_etl_pipeline(self):
        try:
            subroutines = self.dataset_information['subroutines']
            metadata_dict = self.dataset_information['metadata']
            file_data_info = self.dataset_information['data']

            self.set_dates()

            if "adjust_start_date" in subroutines:
                if 'nc_directory_size' in subroutines['adjust_start_date']:
                    list_of_files = sorted(filter(os.path.isfile, glob.glob(subroutines['adjust_start_date'] + '/*.nc',
                                                                            recursive=False)))
                    self.start_date = self.start_date - datetime.timedelta(days=len(list_of_files))

            granules_return_object = self.construct_granules()
            available_dates = granules_return_object['available_dates']
            available_granules = granules_return_object['available_granules']
            available_dates = sorted(available_dates)

            # Loop through list of possible dates
            for nc4_date in available_dates:
                self.cleanup_paths = []
                granule = available_granules[nc4_date]
                print("Starting to process date:", nc4_date)

                format_params = {'current_date': nc4_date}
                if "time_string" in granule:
                    format_params['time_string'] = granule['time_string']

                downloaded_files = {}
                for file_key in file_data_info.keys():
                    file_info = file_data_info[str(file_key)].copy()
                    download_result = self.download_handler(granule['download_information_by_key'][file_key])
                    if not download_result["has_error"]:
                        if download_result["requires_extraction"]:
                            extraction_result = self.extraction_handler(download_result["path"], file_info,
                                                                        format_params)
                            downloaded_files.update(extraction_result["files"])
                        else:
                            file_info["path"] = download_result["path"]
                            downloaded_files[file_key] = file_info
                    else:
                        # TODO ERROR and continue to next date
                        pass

                dataset_by_variable = []
                variable_list = []
                encodings_by_variable = {}
                attributes_by_variable = {}
                final_ds = xr.Dataset()

                for file_key in downloaded_files.keys():
                    file_info = downloaded_files[file_key]
                    variables = file_info["variables"]
                    path = file_info["path"]
                    if Path(path).suffix == '.nc':
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
                        # TODO Populate NAN on CHIRPS GEFS Anomaly if file not found
                        file = xr.open_rasterio(path)
                        if "rescale_accumulated_precip" in subroutines:
                            file = file / 10.0
                        file_ds = file.rename(file_key).to_dataset()
                        for band in variables.keys():
                            variable = variables[band]
                            variable_list.append(variable)

                            variable_ds = file_ds.isel(band=int(band))\
                                                 .reset_coords('band', drop=True)\
                                                 .rename({file_key: variable})

                            dataset_by_variable.append(variable_ds)
                            # TODO catch ERRORS if database not properly configured
                            encodings_by_variable[variable] = file_info["encodings"][variable]
                            attributes_by_variable[variable] = file_info["attributes"][variable]
                        ds = xr.merge(dataset_by_variable)
                    final_ds = xr.merge([final_ds, ds])

                ds = final_ds

                # Set default timestamps
                start_time = granule['start_time']
                end_time = granule['end_time']
                ds_time_index = granule['ds_time_index']

                # Add the time dimension as a new coordinate.
                ds = ds.assign_coords(time=ds_time_index).expand_dims(dim='time', axis=0)
                ds['time_bnds'] = xr.DataArray(np.array([start_time, end_time]).reshape((1, 2)), dims=['time', 'nbnds'])

                # 3) Rename and add attributes to this dataset.
                if "lis_manual_lat_lon" in subroutines:
                    lat_vals = np.round(np.round(np.nanmin(ds.lat.values), 3) + 0.03 * np.arange(0, 2231), 3)
                    lon_vals = np.round(np.round(np.nanmin(ds.lon.values), 3) + 0.03 * np.arange(0, 2351), 3)

                    ds = ds.rename_dims({'north_south': 'latitude', 'east_west': 'longitude'})

                    ds = ds.assign_coords(latitude=lat_vals)  # something like this, may be dim not dims
                    ds = ds.assign_coords(longitude=lon_vals)
                    ds = ds.drop_vars(['lat', 'lon'])
                else:
                    ds = ds.rename({'y': 'latitude', 'x': 'longitude'})

                if "round_coords_lat_lon" in subroutines:
                    ds = ds.assign_coords(latitude=np.around(ds.latitude.values, decimals=6),
                                          longitude=np.around(ds.longitude.values, decimals=6))

                # 4) Reorder latitude dimension into ascending order
                if ds.latitude.values[1] - ds.latitude.values[0] < 0:
                    ds = ds.reindex(latitude=ds.latitude[::-1])

                if "slice_roi" in subroutines:
                    region_range = subroutines["slice_roi"]
                    ds = ds.sel(latitude=slice(region_range[0][0], region_range[0][1]),
                                longitude=slice(region_range[1][0], region_range[1][1]))

                # TODO add modifications based on dataset
                lat_attr = OrderedDict([('long_name', 'latitude'), ('units', 'degrees_north'), ('axis', 'Y')])
                lon_attr = OrderedDict([('long_name', 'longitude'), ('units', 'degrees_east'), ('axis', 'X')])

                time_attr = OrderedDict([('long_name', 'time'), ('axis', 'T'), ('bounds', 'time_bnds')])
                time_bounds_attr = OrderedDict([('long_name', 'time_bounds')])

                # TODO static assignment of latitude possible by dataset
                metadata_dict['south'] = np.min(ds.latitude.values)
                metadata_dict['north'] = np.max(ds.latitude.values)
                metadata_dict['east'] = np.max(ds.longitude.values)
                metadata_dict['west'] = np.min(ds.longitude.values)

                if 'version' in granule:
                    metadata_dict['version'] = granule['version']

                std_file_attr = OrderedDict([('Description', metadata_dict['description']),
                                             ('DateCreated', pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%SZ')),
                                             ('Contact', metadata_dict['contact']),
                                             ('Source', metadata_dict['source']),
                                             ('Version', metadata_dict['version']),
                                             ('Reference', metadata_dict['reference']),
                                             ('RangeStartTime', start_time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                                             ('RangeEndTime', end_time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                                             ('SouthernmostLatitude', metadata_dict['south']),
                                             ('NorthernmostLatitude', metadata_dict['north']),
                                             ('WesternmostLongitude', metadata_dict['west']),
                                             ('EasternmostLongitude', metadata_dict['east']),
                                             ('TemporalResolution', metadata_dict['temporal_resolution']),
                                             ('SpatialResolution', metadata_dict['spatial_resolution'])])

                file_attr = OrderedDict()

                for key, value in std_file_attr.items():
                    file_attr[key] = value
                    if "modify_file_attr" in subroutines:
                        if key in subroutines["modify_file_attr"].keys():
                            modification_info = subroutines["modify_file_attr"][key]
                            attr_names = modification_info["insertion_order"]
                            preformat_attr_values = modification_info["attrs_to_insert"]
                            attr_values = self.format_encoding(preformat_attr_values)
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

                if "reverse_lat_order" in subroutines:
                    ds = ds.reindex(latitude=ds.latitude[::-1])

                final_nc4_filename = granule['nc4_filename']

                nc4_working_path = os.path.join(self.temp_working_dir, final_nc4_filename)

                ds.to_netcdf(nc4_working_path, unlimited_dims='time')

                # Add to nc4 directory
                final_load_dir = self.final_load_dir
                nc4_final_path = os.path.join(final_load_dir, final_nc4_filename)

                self.copy_nc4_file(nc4_working_path, nc4_final_path)

                for cleanup_path in self.cleanup_paths:
                    os.remove(cleanup_path)
            # Merge datasets
        except Exception as error:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.stdout.write(self.style.ERROR(
                'Uncaught exception at line number {} in file {}. System error message: {}.'.format(exc_tb.tb_lineno,
                                                                                                    fname, error)
            ))
