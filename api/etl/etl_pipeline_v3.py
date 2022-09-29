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

    # Default Constructor
    def __init__(self, dataset, stdout, style):
        self.dataset = dataset
        self.stdout = stdout
        self.style = style


    # function set_start_date
    # Sets the start date variables START_YEAR_YYY, START_MONTH_MM, and START_DAY_DD to the dates of the last processed
    # file in the final load directory of the dataset
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
                return datetime.datetime(year=year, month=month, day=day)
            else:
                # TODO error
                pass
        else:
            # TODO error
            pass


    def get_end_date(self):
        today = datetime.date.today()
        return today


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


    def get_date_iterator(self):
        iteration_type = self.dataset.dataset_availability
        if(iteration_type == "daily"):
            pass
        return


    def execute_etl_pipeline(self):
        #start_date = self.get_start_date()
        #end_date = self.get_end_date()

        #date_obj = self.get_date_iterator()

        # Construct list of possible dates
        # Loop through list of possible dates
            # Generate final NC4 Filename
            # Download data files necessary for NC4 dataset
                # Conditional: Extract data files from downloaded sources if necessary
            # Create object to store datasets as dictionary of multiple xarray data arrays (file_data)
            # Loop through downloaded resources
                # Extract xarray data array for each downloaded file
                # Transform variable names and select specified bands for the file
                # Append xarray data array to file_data dictionary object
            # Merge file_data object into a single xarray data array
            # Add NC4 metadata and attributes
            # Export xarray data array as netcdf
            # Store netcdf in predetermined locations (main and fast access paths)
        # Merge datasets
        # Clean up