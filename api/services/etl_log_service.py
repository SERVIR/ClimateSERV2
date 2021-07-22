import json

from ..models import ETL_Dataset, ETL_Granule, ETL_Log, ETL_PipelineRun
from ..etl.utils import get_True_or_False_from_boolish_string

class ETL_LogService():

    @staticmethod
    def create_etl_log_row(activity_event_type, activity_description, etl_pipeline_run_uuid, etl_dataset_uuid, etl_granule_uuid, is_alert, created_by, additional_json):
        ret_uuid = ''
        etl_pipeline_run_uuid = etl_pipeline_run_uuid if etl_pipeline_run_uuid != '' else None
        etl_dataset_uuid = etl_dataset_uuid if etl_dataset_uuid != '' else None
        etl_granule_uuid = etl_granule_uuid if etl_granule_uuid != '' else None
        try:
            is_alert__BOOL = get_True_or_False_from_boolish_string(bool_ish_str_value=is_alert, defaultBoolValue=False)
            additional_json_STR = json.dumps({})
            try:
                additional_json_STR = json.dumps(additional_json)
            except:
                additional_json_STR = json.dumps({})
            try:
                etl_log = ETL_Log()
                etl_log.activity_event_type = str(activity_event_type).strip()
                etl_log.activity_description = str(activity_description).strip()
                etl_log.etl_pipeline_run = ETL_PipelineRun.objects.get(pk=etl_pipeline_run_uuid) if etl_pipeline_run_uuid else etl_pipeline_run_uuid
                etl_log.etl_dataset = ETL_Dataset.objects.get(pk=etl_dataset_uuid) if etl_dataset_uuid else etl_dataset_uuid
                etl_log.etl_granule = ETL_Granule.objects.get(pk=etl_granule_uuid) if etl_granule_uuid else etl_granule_uuid
                etl_log.is_alert = is_alert__BOOL
                etl_log.created_by = str(created_by).strip()
                etl_log.additional_json = additional_json_STR
                etl_log.save()
                ret_uuid = str(etl_log.uuid).strip()
            except Exception as e:
                print(e)
                pass
        except Exception as e:
            print(e)
            pass
        return ret_uuid
