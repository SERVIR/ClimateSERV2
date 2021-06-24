from django.core.management.base import BaseCommand, CommandError
from myapi.services import ETL_DatasetService

class Command(BaseCommand):
    help = 'List all of the possible ETL Dataset Subtypes that are currently supported by the ETL Pipeline.'

    # Function Handler
    def handle(self, *args, **options):

        etl_subtypes_array = ETL_DatasetService.get_all_subtypes_as_string_array()
        self.stdout.write(self.style.SUCCESS('ETL Dataset Subtypes: ' + str(etl_subtypes_array)))
        self.stdout.write(self.style.SUCCESS(''))

        return
