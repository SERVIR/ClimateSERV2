import os
import pandas as pd
import re
import shutil
import logging

from django.core.management import call_command

logger = logging.getLogger("request_processor")


class ETL_Dataset_Subtype():

    def __init__(self):
        self.no_duplicates = None
        self.merge_yearly = None
        self.merge_monthly = None

    def set_optional_parameters(self, params):
        self.no_duplicates = params.get('no_duplicates')
        self.merge_yearly = params.get('merge_yearly')
        self.merge_monthly = params.get('merge_monthly')

    def execute__Step__Post_ETL_Custom(self):
        print("Post processing step")
        if self.merge_yearly:
            years = list(range(self.YYYY__Year__Start, self.YYYY__Year__End + 1))
            for year in years:
                print('Merging {}'.format(year))
                call_command('merge_etl_dataset', etl_dataset_uuid=self.etl_parent_pipeline_instance.etl_dataset_uuid,
                             YEAR_YYYY=year, MONTH_MM=None)
        if self.merge_monthly:
            print("in monthly merge option")
            pr = pd.period_range(
                start='{}-{}'.format(self.YYYY__Year__Start, self.MM__Month__Start),
                end='{}-{}'.format(self.YYYY__Year__End, self.MM__Month__End),
                freq='M'
            )
            pr_tuples = tuple([(period.month, period.year) for period in pr])
            for pr_tuple in pr_tuples:
                print('Merging {}-{}'.format(pr_tuple[1], pr_tuple[0]))
                call_command('merge_etl_dataset', etl_dataset_uuid=self.etl_parent_pipeline_instance.etl_dataset_uuid,
                             YEAR_YYYY=pr_tuple[1], MONTH_MM=pr_tuple[0])

    def _copy_nc4_file(self, working_nc4_filepath, final_nc4_filepath):
        shutil.copyfile(working_nc4_filepath, final_nc4_filepath)
        if not self.no_duplicates:
            duplicate_nc4_filepath = re.sub('/mnt/climateserv/', '/mnt/nvmeclimateserv/', final_nc4_filepath, 1)
            os.makedirs(os.path.dirname(duplicate_nc4_filepath), exist_ok=True)
            shutil.copyfile(working_nc4_filepath, duplicate_nc4_filepath)
