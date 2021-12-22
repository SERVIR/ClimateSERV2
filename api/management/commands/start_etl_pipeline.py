from django.core.management.base import BaseCommand
from api.services import ETL_DatasetService
from api.etl.etl_pipeline import ETL_Pipeline


class Command(BaseCommand):
    help = ''

    # Parsing params
    def add_arguments(self, parser):
        parser.add_argument('--etl_dataset_uuid', required=True, type=str, default='')
        parser.add_argument('--no_duplicates', action='store_true', default=False)
        parser.add_argument('--from_last_processed', action='store_true', default=False)
        parser.add_argument('--merge_yearly', action='store_true', default=False)
        parser.add_argument('--merge_monthly', action='store_true', default=False)
        parser.add_argument('--START_YEAR_YYYY', nargs='?', type=int)
        parser.add_argument('--END_YEAR_YYYY', nargs='?', type=int)
        parser.add_argument('--START_MONTH_MM', nargs='?', type=int)
        parser.add_argument('--END_MONTH_MM', nargs='?', type=int)
        parser.add_argument('--START_DAY_DD', nargs='?', type=int)
        parser.add_argument('--END_DAY_DD', nargs='?', type=int)

    # Function Handler
    def handle(self, *args, **options):

        # Get the dataset uuid input params
        etl_dataset_uuid = options.get('etl_dataset_uuid').strip()

        # DEBUG
        self.stdout.write(self.style.SUCCESS(
            'start_etl_pipeline.py: Successfully called handle(): with param: (etl_dataset_uuid) {}'.format(
                str(etl_dataset_uuid))))

        # Verify that this uuid does exist
        does_etl_dataset_exist = ETL_DatasetService.does_etl_dataset_exist__by_uuid(etl_dataset_uuid)
        if not does_etl_dataset_exist:
            self.stdout.write(self.style.ERROR(
                'start_etl_pipeline.handle(): Dataset with UUID: {} does not exist.  Try using "python manage.py '
                'list_etl_dataset_uuids" to see a list of all datasets and their uuids.'.format(
                    str(etl_dataset_uuid))))
        else:

            # DEBUG
            self.stdout.write(self.style.SUCCESS(
                'start_etl_pipeline.py: (does_etl_dataset_exist) {}'.format(str(does_etl_dataset_exist))))

            # Create an instance of the pipeline
            etl_pipeline = ETL_Pipeline(etl_dataset_uuid)

            # Get the other optional params
            # Set input params as configuration options on the ETL Pipeline
            etl_pipeline.no_duplicates = options.get('no_duplicates')
            etl_pipeline.from_last_processed = options.get('from_last_processed')
            etl_pipeline.merge_yearly = options.get('merge_yearly')
            etl_pipeline.merge_monthly = options.get('merge_monthly')
            etl_pipeline.START_YEAR_YYYY = options.get('START_YEAR_YYYY')
            etl_pipeline.END_YEAR_YYYY = options.get('END_YEAR_YYYY')
            etl_pipeline.START_MONTH_MM = options.get('START_MONTH_MM')
            etl_pipeline.END_MONTH_MM = options.get('END_MONTH_MM')
            etl_pipeline.START_DAY_DD = options.get('START_DAY_DD')
            etl_pipeline.END_DAY_DD = options.get('END_DAY_DD')

            # Call the actual function to start the pipeline
            etl_pipeline.execute_pipeline_control_function()
