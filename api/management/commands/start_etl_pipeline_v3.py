import sys
from django.core.management.base import BaseCommand
from api.models.etl_dataset_model_v3 import ETL_Dataset_V3
from api.etl.etl_pipeline_v3 import ETL_Pipeline


class Command(BaseCommand):
    help = ''

    # Function Handler
    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Initializing ETL pipeline for all pipeline enabled datasets'))
        ds_objects = ETL_Dataset_V3.objects.all()
        for dataset in ds_objects:
            try:
                if dataset.is_pipeline_enabled:
                    self.stdout.write(self.style.SUCCESS('Initializing ETL pipeline for dataset '
                                                         '[{}] with uuid [{}].'.format(str(dataset), dataset.uuid)))
                    etl_pipeline = ETL_Pipeline(dataset, self.stdout, self.style)
                    etl_pipeline.execute_etl_pipeline()
                else:
                    self.stdout.write(self.style.ERROR(
                            'start_etl_pipeline.handle(): Dataset [{}], with uuid [{}] is currently not currently enabled. '
                            'If you would like the ETL pipeline to process this dataset, please modify the '
                            'is_pipeline_enabled option for this dataset.'.format(str(dataset), dataset.uuid)))
            except Exception as error:
                self.stdout.write(self.style.ERROR(
                    'Uncaught exception. System error message: {}'.format(error)
                ))
            finally:
                self.stdout.write(self.style.SUCCESS("ETL Pipeline step for dataset [{}] with uuid [{}] completed. "
                                                     "Moving on to next dataset.".format(str(dataset), dataset.uuid)))

        self.stdout.write(self.style.SUCCESS('ETL Pipeline completed for all datasets.'))