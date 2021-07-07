from django.core.management.base import BaseCommand
from api.services import ETL_DatasetService
from api.etl.etl_pipeline import ETL_Pipeline

class Command(BaseCommand):
    help = ''

    # Parsing params
    def add_arguments(self, parser):
        parser.add_argument('--etl_dataset_uuid', required=True, type=str)

    # Function Handler
    def handle(self, *args, **options):

        # Get the dataset uuid input params
        etl_dataset_uuid = options.get('etl_dataset_uuid', '').strip()

        # Debug
        self.stdout.write(self.style.SUCCESS('start_etl_pipeline.py: Successfully called handle(): with param: (etl_dataset_uuid) ' + str(etl_dataset_uuid)))

        # Verify that this uuid does exist
        does_etl_dataset_exist = ETL_DatasetService.does_etl_dataset_exist__by_uuid(input__uuid=etl_dataset_uuid)
        if does_etl_dataset_exist == False:
            self.stdout.write(self.style.ERROR('start_etl_pipeline.handle(): Dataset with UUID: ' + str() + 'does not exist.  Try using "python manage.py list_etl_dataset_uuids" to see a list of all datasets and their uuids.'))
            return

        # DEBUG
        self.stdout.write(self.style.SUCCESS('start_etl_pipeline.py: (does_etl_dataset_exist) ' + str(does_etl_dataset_exist)))

        # Create an instance of the pipeline
        etl_pipeline = ETL_Pipeline()

        # Set input params as configuration options on the ETL Pipeline
        etl_pipeline.etl_dataset_uuid = etl_dataset_uuid

        # Call the actual function to start the pipeline
        etl_pipeline.execute_pipeline_control_function()

        return
