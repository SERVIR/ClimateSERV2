import os
import shutil
import subprocess
import logging

from datetime import datetime
from django.core.management.base import BaseCommand
from api.etl import etl_exceptions
from api.models import ETL_Dataset
from netCDF4 import Dataset
from os import listdir
from os.path import isfile, join

logger = logging.getLogger("request_processor")


def unix_time(dt):
    return (dt - datetime.utcfromtimestamp(0)).total_seconds()


def get_append_list(pattern_filepath, temp_aggregate_filepath):
    ds = Dataset(
        temp_aggregate_filepath,
        "r", format="NETCDF4")
    ts = ds.variables['time']

    current_files = [f for f in listdir(pattern_filepath) if isfile(join(pattern_filepath, f))]
    split_file = current_files[0].split(".")
    test_files = []
    for file in current_files:
        test_files.append(unix_time(datetime.strptime(file.split(".")[1], '%Y%m%dT%H%M%SZ')))

    new_file_dates = set(test_files)
    filtered_timesteps = list(filter(lambda t: t not in ts[:], new_file_dates))
    returnable_files = []
    for tm in filtered_timesteps:
        split_file[1] = datetime.fromtimestamp(tm).strftime('%Y%m%dT%H%M%SZ')
        returnable_files.append(os.path.join(pattern_filepath, ".".join(split_file)))

    return returnable_files


class Command(BaseCommand):
    help = ''

    # Parsing params
    def add_arguments(self, parser):
        parser.add_argument('--etl_dataset_uuid', required=True, type=str, default='')
        parser.add_argument('--YEAR_YYYY', nargs='?', type=int)
        parser.add_argument('--MONTH_MM', nargs='?', type=int, default=None)

        # Function Handler

    def handle(self, *args, **options):
        print("made it to the merge")
        # Get the dataset uuid input params
        etl_dataset_uuid = options.get('etl_dataset_uuid').strip()
        year_yyyy = str(options.get('YEAR_YYYY'))
        temp_mm = options.get('MONTH_MM')
        if temp_mm:
            month_mm = str(temp_mm).zfill(2)
        else:
            month_mm = None

        try:
            etl_dataset = ETL_Dataset.objects.get(pk=etl_dataset_uuid)
        except ETL_Dataset.DoesNotExist:
            raise etl_exceptions.UnableToReadDatasetException()

        print(f'Merging {etl_dataset.dataset_subtype} - {year_yyyy} - {month_mm}')

        # temp_aggregate_path = etl_dataset.temp_working_dir
        temp_aggregate_filepath = etl_dataset.temp_working_dir
        pattern_filepath = etl_dataset.final_load_dir
        # pattern_filepath = os.path.join(etl_dataset.final_load_dir, "current_run")
        # current_run_path = os.path.join(etl_dataset.final_load_dir, "current_run")
        temp_fast_path = etl_dataset.fast_directory_path  # '/mnt/climateserv/process_tmp/'

        pattern_filename = ''
        aggregate_filename = ''
        ncrcat_options = ''
        if month_mm != 'None' and month_mm is not None:
            if etl_dataset.dataset_subtype.lower() == 'chirp':
                # temp_fast_path = os.path.join(temp_fast_path, 'fast_chirp')
                pattern_filename = 'ucsb-chirp.{}{}*daily.nc4'
                aggregate_filename = 'ucsb_chirp.global.0.05deg.daily.{}{}.nc4'
                ncrcat_options = '-4 -h --cnk_dmn time,16 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            elif etl_dataset.dataset_subtype.lower() == 'chirps_gefs':
                print("got to gefs merge block")
                # temp_fast_path = os.path.join(temp_fast_path, 'fast_chirps_gefs')
                pattern_filename = 'ucsb-chirps-gefs.{}{}*.10dy.nc4'
                aggregate_filename = 'ucsb-chirps-gefs.global.0.05deg.10dy.{}{}.nc4'
                ncrcat_options = '-4 -h --cnk_dmn time,16 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            elif etl_dataset.dataset_subtype.lower() == 'emodis':
                if etl_dataset.tds_region == 'eastafrica':
                    # temp_fast_path = os.path.join(temp_fast_path, 'fast_emodis_eastafrica')
                    pattern_filename = 'emodis-ndvi.{}{}*.eastafrica.250m.10dy.nc4'
                    aggregate_filename = 'emodis-ndvi.eastafrica.250m.10dy.{}{}.nc4'
                    ncrcat_options = '-4 -h --cnk_dmn time,3 --cnk_dmn latitude,256 --cnk_dmn longitude,256 --ppc ' \
                                     'longitude=.5 --ppc latitude=.5 '
                elif etl_dataset.tds_region == 'westafrica':
                    # temp_fast_path = os.path.join(temp_fast_path, 'fast_emodis_westafrica')
                    pattern_filename = 'emodis-ndvi.{}{}*.westafrica.250m.10dy.nc4'
                    aggregate_filename = 'emodis-ndvi.westafrica.250m.10dy.{}{}.nc4'
                    ncrcat_options = '-4 -h --cnk_dmn time,3 --cnk_dmn latitude,256 --cnk_dmn longitude,256'
                elif etl_dataset.tds_region == 'southernafrica':
                    # temp_fast_path = os.path.join(temp_fast_path, 'fast_emodis_southernafrica')
                    pattern_filename = 'emodis-ndvi.{}{}*.southernafrica.250m.10dy.nc4'
                    aggregate_filename = 'emodis-ndvi.southernafrica.250m.10dy.{}{}.nc4'
                    ncrcat_options = '-4 -h --cnk_dmn time,3 --cnk_dmn latitude,256 --cnk_dmn longitude,256 --ppc ' \
                                     'longitude=.5 --ppc latitude=.5 '
                elif etl_dataset.tds_region == 'centralasia':
                    # temp_fast_path = os.path.join(temp_fast_path, 'fast_emodis_centralasia')
                    pattern_filename = 'emodis-ndvi.{}{}*.centralasia.250m.10dy.nc4'
                    aggregate_filename = 'emodis-ndvi.centralasia.250m.10dy.{}{}.nc4'
                    ncrcat_options = '-4 -h --cnk_dmn time,3 --cnk_dmn latitude,256 --cnk_dmn longitude,256'
            else:
                pass
            pattern_filepath = os.path.join(pattern_filepath, pattern_filename.format(year_yyyy, month_mm))

            if not os.path.exists(etl_dataset.temp_working_dir + '/by_month/'):
                os.makedirs(etl_dataset.temp_working_dir + '/by_month/', mode=0o777, exist_ok=True)
            temp_aggregate_path = os.path.join(temp_aggregate_filepath, 'by_month/')
            if not os.path.exists(temp_aggregate_path):
                os.makedirs(temp_aggregate_path, mode=0o777, exist_ok=True)
            temp_aggregate_filepath = os.path.join(temp_aggregate_path, aggregate_filename.format(year_yyyy, month_mm))
        else:
            if etl_dataset.dataset_subtype.lower() == 'chirps':
                print("setting options")
                # temp_fast_path = os.path.join(temp_fast_path, 'fast_chirps')
                pattern_filename = 'ucsb-chirps.{}*daily.nc4'
                aggregate_filename = 'ucsb_chirps.global.0.05deg.daily.{}.nc4'
                ncrcat_options = '-4 -h -L 1 --cnk_dmn time,31 --cnk_dmn latitude,256 --cnk_dmn longitude,256'
                print("should have set options")
            elif etl_dataset.dataset_subtype.lower() == 'esi_12week':
                # temp_fast_path = os.path.join(temp_fast_path, 'fast_sport_esi_12wk')
                pattern_filename = 'sport-esi.{}*.nc4'
                aggregate_filename = 'sport-esi.global.0.05deg.12wk.{}.nc4'
                ncrcat_options = '-4 -h -L 1 --cnk_dmn time,31 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            elif etl_dataset.dataset_subtype.lower() == 'esi_4week':
                # temp_fast_path = os.path.join(temp_fast_path, 'fast_sport_esi_4wk')
                pattern_filename = 'sport-esi.{}*.nc4'
                aggregate_filename = 'sport-esi.global.0.05deg.4wk.{}.nc4'
                ncrcat_options = '-4 -h -L 1 --cnk_dmn time,31 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            elif etl_dataset.dataset_subtype.lower() == 'imerg_early_1dy':
                # temp_fast_path = os.path.join(temp_fast_path, 'fast_nasa_imerg_early_daily')
                pattern_filename = 'nasa-imerg-early.{}*.global.0.1deg.1dy.nc4'
                aggregate_filename = 'nasa-imerg-early.global.0.1deg.1dy.{}.nc4'
                ncrcat_options = '-4 -h -L 7 --cnk_dmn time,31 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            elif etl_dataset.dataset_subtype == 'imerg_late_1dy':
                # temp_fast_path = os.path.join(temp_fast_path, 'fast_nasa_imerg_late_daily')
                pattern_filename = 'nasa-imerg-late.{}*.global.0.1deg.1dy.nc4'
                aggregate_filename = 'nasa-imerg-late.global.0.1deg.1dy.{}.nc4'
                ncrcat_options = '-4 -h -L 7 --cnk_dmn time,31 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            elif etl_dataset.dataset_subtype.lower() == 'usda_smap':
                # temp_fast_path = os.path.join(temp_fast_path, 'fast_usda_smap')
                pattern_filename = 'usda-smap.{}*.nc4'
                aggregate_filename = 'usda-smap.global.10km.3dy.{}.nc4'
                ncrcat_options = '-4 -h --cnk_dmn time,31 --cnk_dmn latitude,256 --cnk_dmn longitude,256'
            elif etl_dataset.dataset_subtype.lower() == 'sport_lis':
                # temp_fast_path = os.path.join(temp_fast_path, 'fast_sport_lis')
                pattern_filename = 'sport-lis.{}*.nc4'
                aggregate_filename = 'sport-lis.africa.0.03deg.daily.{}.nc4'
                ncrcat_options = '-4 -h -L 1 --cnk_dmn time,31 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            elif etl_dataset.dataset_subtype.lower() == 'nsidc-smap-1k':
                # temp_fast_path = os.path.join(temp_fast_path, 'fast_sport_lis')
                pattern_filename = 'nsidc-smap-sentinel.{}*.nc4'
                aggregate_filename = 'nsidc-smap-sentinel.global.1km.daily.{}.nc4'
                ncrcat_options = '-4 -h -L 1 --cnk_dmn time,31 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            elif etl_dataset.dataset_subtype.lower() == 'nsidc-smap-1k-15':
                # temp_fast_path = os.path.join(temp_fast_path, 'fast_sport_lis')
                pattern_filename = 'nsidc_smap_sentinel.{}*.nc4'
                aggregate_filename = 'usda-nsidc-smap.global.1km.15dy.{}.nc4'
                ncrcat_options = '-4 -h -L 1 --cnk_dmn time,31 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            else:
                pass
            pattern_filepath = os.path.join(pattern_filepath, pattern_filename.format(year_yyyy))
            temp_aggregate_path = os.path.join(temp_aggregate_filepath, 'by_year/')
            if not os.path.exists(temp_aggregate_path):
                os.makedirs(temp_aggregate_path, mode=0o777, exist_ok=True)
            temp_aggregate_filepath = os.path.join(temp_aggregate_path, aggregate_filename.format(year_yyyy))


            # temp_aggregate_filepath = os.path.join(temp_fast_path, aggregate_filename.format(year_yyyy))
            # append_list = " ".join(get_append_list(current_run_path, temp_aggregate_filepath))

        if ncrcat_options == '':
            raise Exception()
        command_str = f'sudo ncrcat {ncrcat_options} -O {pattern_filepath} {temp_aggregate_filepath}'
        # command_str = f'sudo ncrcat {ncrcat_options} --rec_apn {pattern_filepath} {temp_aggregate_filepath}'
        if os.name == 'nt':
            command_str = f'ncra -Y {command_str}'

        print("about to run the merge command")
        print(command_str)
        process = subprocess.Popen(command_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        print("merged")
        print(temp_aggregate_filepath)
        if temp_aggregate_filepath:
            _, tail = os.path.split(temp_aggregate_filepath)
            print("temp_fast_path: " + temp_fast_path)
            if not os.path.exists(temp_fast_path):
                os.makedirs(temp_fast_path, mode=0o777, exist_ok=True)
            print("copying from: " + temp_aggregate_filepath + " to: " + os.path.join(temp_fast_path, tail))
            shutil.copyfile(temp_aggregate_filepath, os.path.join(temp_fast_path, tail))
            # this should happen in the cleanup step
            # shutil.rmtree(temp_aggregate_path)

        else:
            print(f'File not found: {temp_aggregate_filepath}')

        return
