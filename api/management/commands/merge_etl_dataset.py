import os, subprocess
from distutils.dir_util import copy_tree
from django.core.management.base import BaseCommand

from api.etl import etl_exceptions
from api.models import ETL_Dataset

class Command(BaseCommand):
    help = ''

    # Parsing params
    def add_arguments(self, parser):
        parser.add_argument('--etl_dataset_uuid', required=True, type=str, default='')
        parser.add_argument('--YEAR_YYYY', nargs='?', type=int)
        parser.add_argument('--MONTH_MM', nargs='?', type=int)

    # Function Handler
    def handle(self, *args, **options):

        # Get the dataset uuid input params
        etl_dataset_uuid = options.get('etl_dataset_uuid').strip()
        YEAR_YYYY  = options.get('YEAR_YYYY')
        MONTH_MM   = options.get('MONTH_MM')
        MONTH_MM   = options.get('MONTH_MM')
        REGION_CODE_XX   = options.get('REGION_CODE_XX')

        try:
            etl_dataset = ETL_Dataset.objects.get(pk=etl_dataset_uuid)
        except:
            raise etl_exceptions.UnableToReadDatasetException()

        print(etl_dataset.dataset_subtype)
        print(YEAR_YYYY)
        print(MONTH_MM)

        temp_aggregate_filepath = etl_dataset.temp_working_dir
        pattern_filepath = etl_dataset.final_load_dir
        temp_fast_path = '/mnt/climateserv/process_tmp//mnt/climateserv/process_tmp/'
        pattern_filename = ''
        aggregate_filename = ''
        ncrcat_options = ''
        if MONTH_MM:
            if etl_dataset.dataset_subtype == 'chirp':
                temp_fast_path = os.path.join('temp_fast_path', 'fast_chirp')
                pattern_filename = 'ucsb-chirp.{}{}*daily.nc4'
                aggregate_filename = 'ucsb_chirp.global.0.05deg.daily.{}{}.nc4'
                ncrcat_options = '-4 -h --cnk_dmn time,16 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            else:
                pass
            pattern_filepath = os.path.join(pattern_filepath, pattern_filename.format(YEAR_YYYY, MONTH_MM))
            temp_aggregate_filepath = os.path.join(temp_aggregate_filepath, 'by_month/', aggregate_filename.format(YEAR_YYYY, MONTH_MM))
        else:
            if etl_dataset.dataset_subtype == 'chirps':
                temp_fast_path = os.path.join('temp_fast_path', 'fast_chirps')
                pattern_filename = 'ucsb-chirps.{}*daily.nc4'
                aggregate_filename = 'ucsb_chirps.global.0.05deg.daily.{}.nc4'
                ncrcat_options = '-4 -h -L 1 --cnk_dmn time,31 --cnk_dmn latitude,256 --cnk_dmn longitude,256'
            elif etl_dataset.dataset_subtype == 'esi_12week':
                temp_fast_path = os.path.join('temp_fast_path', 'fast_sport_esi_12wk')
                pattern_filename = 'sport-esi.{}*.nc4'
                aggregate_filename = 'sport-esi.global.0.05deg.12wk.{}.nc4'
                ncrcat_options = '-4 -h -L 1 --cnk_dmn time,31 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            elif etl_dataset.dataset_subtype == 'esi_4week':
                temp_fast_path = os.path.join('temp_fast_path', 'fast_sport_esi_4wk')
                pattern_filename = 'sport-esi.{}*.nc4'
                aggregate_filename = 'sport-esi.global.0.05deg.4wk.{}.nc4'
                ncrcat_options = '-4 -h -L 1 --cnk_dmn time,31 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            elif etl_dataset.dataset_subtype == 'imerg_early_1dy':
                temp_fast_path = os.path.join('temp_fast_path', 'nasa_imerg_early_daily')
                pattern_filename = 'nasa-imerg-early.{}*.global.0.1deg.1dy.nc4'
                aggregate_filename = 'nasa-imerg-early.global.0.1deg.1dy.{}.nc4'
                ncrcat_options = '-4 -h -L 7 --cnk_dmn time,31 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            elif etl_dataset.dataset_subtype == 'imerg_late_1dy':
                temp_fast_path = os.path.join('temp_fast_path', 'nasa_imerg_lat_daily')
                pattern_filename = 'nasa-imerg-late.{}*.global.0.1deg.1dy.nc4'
                aggregate_filename = 'nasa-imerg-late.global.0.1deg.1dy.{}.nc4'
                ncrcat_options = '-4 -h -L 7 --cnk_dmn time,31 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
            elif etl_dataset.dataset_subtype == 'usda_smap':
                temp_fast_path = os.path.join('temp_fast_path', 'fast_usda_smap')
                pattern_filename = 'usda-smap.{}*.nc4'
                aggregate_filename = 'usda-smap.global.10km.3dy.{}.nc4'
                ncrcat_options = '-4 -h --cnk_dmn time,31 --cnk_dmn latitude,256 --cnk_dmn longitude,256'
            else:
                pass
            pattern_filepath = os.path.join(pattern_filepath, pattern_filename.format(YEAR_YYYY))
            temp_aggregate_filepath = os.path.join(temp_aggregate_filepath, 'by_year/', aggregate_filename.format(YEAR_YYYY))

        if ncrcat_options == '':
            raise Exception()

        command_str = 'ncrcat {} -O {} {}'.format(ncrcat_options, pattern_filepath, temp_aggregate_filepath)
        if os.name == 'nt':
            command_str = 'ncra -Y {}'.format(command_str)
        print(command_str)

        process = subprocess.Popen(command_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        print(stderr)

        copy_tree(temp_aggregate_filepath, temp_fast_path)

        return