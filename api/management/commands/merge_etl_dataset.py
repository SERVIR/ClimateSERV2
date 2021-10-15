import os, subprocess
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
        MONTH_MM  = options.get('MONTH_MM')

        try:
            etl_dataset = ETL_Dataset.objects.get(pk=etl_dataset_uuid)
        except:
            raise etl_exceptions.UnableToReadDatasetException()

        pattern_filename = ''
        aggregate_filename = ''
        ncrcat_options = ''
        print(etl_dataset.dataset_subtype)
        if etl_dataset.dataset_subtype == 'imerg_late_1dy':
            # pattern_filename = 'nasa-imerg-late.{}*.global.0.1deg.1dy.nc4'.format(YEAR_YYYY)
            pattern_filename = 'nasa-imerg-late.20210101T000000Z.global.0.1deg.1dy.nc4'
            aggregate_filename = 'nasa-imerg-late.global.0.1deg.1dy.{}.nc4'.format(YEAR_YYYY)
            ncrat_options = '-4 -h -L 7 --cnk_dmn time,31 --cnk_dmn longitude,256 --cnk_dmn latitude,256'
        pattern_filepath = os.path.join(etl_dataset.final_load_dir, pattern_filename)
        aggregate_filepath = os.path.join(etl_dataset.final_load_dir, 'by_year/', aggregate_filename)
        command_str = 'ncra -Y ncrcat {} -O {} {}'.format(ncrat_options, pattern_filepath, aggregate_filepath)
        print(command_str)
        process = subprocess.Popen(command_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        print(stderr)

        return
