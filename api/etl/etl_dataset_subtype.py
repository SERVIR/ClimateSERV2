import os, pandas as pd, shutil

from django.core.management import call_command

class ETL_Dataset_Subtype():

    def set_optional_parameters(self, params):
        self.merge_yearly = params.get('merge_yearly')
        self.merge_monthly = params.get('merge_monthly')

    def execute__Step__Post_ETL_Custom(self):
        if self.merge_yearly:
            years = list(range(self.YYYY__Year__Start, self.YYYY__Year__End + 1))
            for year in years:
                print('Merging {}'.format(year))
                call_command('merge_etl_dataset', etl_dataset_uuid=self.etl_parent_pipeline_instance.etl_dataset_uuid, YEAR_YYYY=year, MONTH_MM=None)
        if self.merge_monthly:
            pr = pd.period_range(
                start='{}-{}'.format(self.YYYY__Year__Start, self.MM__Month__Start),
                end='{}-{}'.format(self.YYYY__Year__End, self.MM__Month__End),
                freq='M'
            )
            pr_tuples = tuple([(period.month, period.year) for period in pr])
            for pr_tuple in pr_tuples:
                print('Merging {}-{}'.format(pr_tuple[1], pr_tuple[0]))
                call_command('merge_etl_dataset', etl_dataset_uuid=self.etl_parent_pipeline_instance.etl_dataset_uuid, YEAR_YYYY=pr_tuple[1], MONTH_MM=pr_tuple[0])
