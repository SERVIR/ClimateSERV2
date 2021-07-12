from django.core.management.base import BaseCommand
from api.services import ETL_DatasetService
from api.etl.etl_pipeline import ETL_Pipeline

class Command(BaseCommand):
    help = ''

    # Parsing params
    def add_arguments(self, parser):
        parser.add_argument('--etl_dataset_uuid', required=True, type=str, default='')
        parser.add_argument('--START_YEAR_YYYY', nargs='?', type=int, default=0)
        parser.add_argument('--END_YEAR_YYYY', nargs='?', type=int, default=0)
        parser.add_argument('--START_MONTH_MM', nargs='?', type=int, default=0)
        parser.add_argument('--END_MONTH_MM', nargs='?', type=int, default=0)
        parser.add_argument('--START_DAY_DD', nargs='?', type=int, default=0)
        parser.add_argument('--END_DAY_DD', nargs='?', type=int, default=0)
        parser.add_argument('--START_30MININCREMENT_NN', nargs='?', type=int, default=0)
        parser.add_argument('--END_30MININCREMENT_NN', nargs='?', type=int, default=0)

    # Function Handler
    def handle(self, *args, **options):

        # Get the dataset uuid input params
        etl_dataset_uuid = options.get('etl_dataset_uuid').strip()

        # Get the other optional params
        START_YEAR_YYYY  = options.get('START_YEAR_YYYY')
        END_YEAR_YYYY    = options.get('END_YEAR_YYYY')
        START_MONTH_MM   = options.get('START_MONTH_MM')
        END_MONTH_MM     = options.get('END_MONTH_MM')
        START_DAY_DD     = options.get('START_DAY_DD')
        END_DAY_DD       = options.get('END_DAY_DD')
        START_30MININCREMENT_NN  = options.get('START_30MININCREMENT_NN')
        END_30MININCREMENT_NN    = options.get('START_30MININCREMENT_NN')

        # DEBUG
        self.stdout.write(self.style.SUCCESS('start_etl_pipeline.py: Successfully called handle(): with param: (etl_dataset_uuid) {}'.format(str(etl_dataset_uuid))))

        # Verify that this uuid does exist
        does_etl_dataset_exist = ETL_DatasetService.does_etl_dataset_exist__by_uuid(etl_dataset_uuid)
        if does_etl_dataset_exist == False:
            self.stdout.write(self.style.ERROR('start_etl_pipeline.handle(): Dataset with UUID: {} does not exist.  Try using "python manage.py list_etl_dataset_uuids" to see a list of all datasets and their uuids.'.format(str(etl_dataset_uuid))))
            return

        # DEBUG
        self.stdout.write(self.style.SUCCESS('start_etl_pipeline.py: (does_etl_dataset_exist) {}'.format(str(does_etl_dataset_exist))))

        # Create an instance of the pipeline
        etl_pipeline = ETL_Pipeline(etl_dataset_uuid)

        # Set input params as configuration options on the ETL Pipeline
        etl_pipeline.START_YEAR_YYYY  = START_YEAR_YYYY
        etl_pipeline.END_YEAR_YYYY    = END_YEAR_YYYY
        etl_pipeline.START_MONTH_MM   = START_MONTH_MM
        etl_pipeline.END_MONTH_MM     = END_MONTH_MM
        etl_pipeline.START_DAY_DD     = START_DAY_DD
        etl_pipeline.END_DAY_DD       = END_DAY_DD
        etl_pipeline.START_30MININCREMENT_NN  = START_30MININCREMENT_NN
        etl_pipeline.END_30MININCREMENT_NN    = END_30MININCREMENT_NN

        # Call the actual function to start the pipeline
        etl_pipeline.execute_pipeline_control_function()

        return
