from django.core.management.base import BaseCommand, CommandError
from myapi.services import ETL_DatasetService

class Command(BaseCommand):
    help = 'List all the Dataset IDs along with their respective human-readable (human defined) names.'

    # Function Handler
    def handle(self, *args, **options):

        etl_datasets = ETL_DatasetService.get_all_etl_datasets_preview_list()

        self.stdout.write(self.style.SUCCESS(''))
        self.stdout.write(self.style.SUCCESS('UUID,    Dataset Name'))
        for current_etl_dataset in etl_datasets:
            current_uuid            = str(current_etl_dataset['uuid']).strip()
            current_dataset_name    = str(current_etl_dataset['dataset_name']).strip()

            self.stdout.write(self.style.SUCCESS(current_uuid + ',    ' + current_dataset_name))
        self.stdout.write(self.style.SUCCESS(''))

        return
