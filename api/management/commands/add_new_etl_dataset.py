from django.core.management.base import BaseCommand
from api.services import ETL_DatasetService

class Command(BaseCommand):
    help = 'Create a new Dataset from an input name with default parameters.'

    # Parsing params
    def add_arguments(self, parser):
        parser.add_argument('etl_dataset_name', required=True, type=str)

    # Function Handler
    def handle(self, *args, **options):

        # Get the dataset uuid input params
        etl_dataset_name = options.get('etl_dataset_name', '').strip()

        # Check and see if dataset name is already taken
        is_dataset_name_available = ETL_DatasetService.is_datasetname_avalaible(input__datasetname=etl_dataset_name)
        if is_dataset_name_available == False:
            self.stdout.write(self.style.ERROR('add_new_etl_dataset.handle(): Dataset name is not available.  Try another name for this dataset.'))
            return

        # Create the new Dataset
        did_create_dataset, new_etl_dataset_uuid = ETL_DatasetService.create_etl_dataset_from_datasetname_only(input__datasetname=etl_dataset_name, created_by="terminal_manage_py_command__add_new_etl_dataset")

        # Check to see if this was created and then output to the console.
        if did_create_dataset == True:
            self.stdout.write(self.style.SUCCESS('add_new_etl_dataset.py: Successfully created new dataset, ' + str(etl_dataset_name) + ', with UUID: ' + str(new_etl_dataset_uuid)))
        else:
            self.stdout.write(self.style.ERROR('add_new_etl_dataset.handle(): Unknown Error.  Dataset was not created.'))

        return
