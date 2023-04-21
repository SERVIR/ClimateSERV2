import sys
import os
from django.core.management.base import BaseCommand
from api.models.etl_dataset_model_v3 import ETL_Dataset_V3
from api.etl.etl_pipeline_v3 import ETL_Pipeline
import datetime

class Command(BaseCommand):
    help = ''

    # Parsing params
    def add_arguments(self, parser):
        parser.add_argument('--etl_dataset_uuid', nargs='?', type=str, default=None)
        parser.add_argument('--start_date', nargs='?', type=str, default=None)
        parser.add_argument('--end_date', nargs='?', type=str, default=None)

        # Function Handler

    # Function Handler
    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Initializing ETL pipeline for all pipeline enabled datasets'))
        ds_objects = ETL_Dataset_V3.objects.all()

        uuid = options.get('etl_dataset_uuid')

        start_date = options.get('start_date')
        end_date = options.get('end_date')

        if start_date is not None:
            try:
                start_date = datetime.date.fromisoformat(start_date)
                self.stdout.write(self.style.SUCCESS(
                    'start_etl_pipeline.handle(): Successfully set start_date [{}] ').format(start_date))
            except ValueError:
                self.stdout.write(self.style.ERROR(
                    'start_etl_pipeline.handle(): start_date [{}] is not a valid date. Please try a different '
                    'date. Please ensure that the input follows the following format: YYYY-MM-DD').format(start_date))
                # TODO better exit
                exit()

        if end_date is not None:
            try:
                end_date = datetime.date.fromisoformat(end_date)
                self.stdout.write(self.style.SUCCESS(
                    'start_etl_pipeline.handle(): Successfully set end_date [{}] ').format(end_date))
            except ValueError:
                self.stdout.write(self.style.ERROR(
                    'start_etl_pipeline.handle(): end_date [{}] is not a valid date. Please try a different '
                    'date. Please ensureS that the input follows the following format: YYYY-MM-DD').format(end_date))
                # TODO better exit
                exit()

        for dataset in ds_objects:
            try:
                if uuid and dataset.uuid == uuid:
                    self.stdout.write(self.style.SUCCESS('Initializing ETL pipeline for dataset '
                                                         '[{}] with uuid [{}].'.format(str(dataset), dataset.uuid)))
                    etl_pipeline = ETL_Pipeline(dataset, self.stdout, self.style, start_date, end_date)
                    etl_pipeline.execute_etl_pipeline()
                elif not uuid and dataset.is_pipeline_enabled:
                    self.stdout.write(self.style.SUCCESS('Initializing ETL pipeline for dataset '
                                                         '[{}] with uuid [{}].'.format(str(dataset), dataset.uuid)))
                    etl_pipeline = ETL_Pipeline(dataset, self.stdout, self.style, start_date, end_date)
                    etl_pipeline.execute_etl_pipeline()
                else:
                    self.stdout.write(self.style.ERROR(
                            'start_etl_pipeline.handle(): Dataset [{}], with uuid [{}] is currently not currently enabled. '
                            'If you would like the ETL pipeline to process this dataset, please modify the '
                            'is_pipeline_enabled option for this dataset.'.format(str(dataset), dataset.uuid)))
            except Exception as error:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                self.stdout.write(self.style.ERROR(
                    'Uncaught exception at line number {} in file {}. System error message: {}.'.format(exc_tb.tb_lineno, fname, error)
                ))
            finally:
                self.stdout.write(self.style.SUCCESS("ETL Pipeline step for dataset [{}] with uuid [{}] completed. "
                                                     "Moving on to next dataset.".format(str(dataset), dataset.uuid)))

        self.stdout.write(self.style.SUCCESS('ETL Pipeline completed for all datasets.'))