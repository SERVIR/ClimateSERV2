import json

from ..models import ETL_Dataset, ETL_Granule, ETL_Log, ETL_PipelineRun
from ..etl.utils import get_True_or_False_from_boolish_string

class ETL_LogService():

    @staticmethod
    def create_etl_log_row(activity_event_type, activity_description, etl_pipeline_run_uuid, etl_dataset_uuid, etl_granule_uuid, is_alert, created_by, additional_json):
        ret__new_ETL_Log_UUID = ""
        try:
            is_alert__BOOL = get_True_or_False_from_boolish_string(bool_ish_str_value=is_alert, defaultBoolValue=False)
            additional_json_STR = json.dumps({})
            try:
                additional_json_STR = json.dumps(additional_json)
            except:
                additional_json_STR = json.dumps({})
            try:
                new_ETL_Log = ETL_Log()
                new_ETL_Log.activity_event_type     = str(activity_event_type).strip()
                new_ETL_Log.activity_description    = str(activity_description).strip()
                new_ETL_Log.etl_pipeline_run        = ETL_PipelineRun.objects.get(pk=etl_pipeline_run_uuid)
                new_ETL_Log.etl_dataset             = ETL_Dataset.objects.get(pk=etl_dataset_uuid)
                new_ETL_Log.etl_granule             = ETL_Granule.objects.get(pk=etl_granule_uuid)
                new_ETL_Log.is_alert                = is_alert__BOOL
                new_ETL_Log.created_by              = str(created_by).strip()
                new_ETL_Log.additional_json         = additional_json_STR
                new_ETL_Log.save()
                ret__new_ETL_Log_UUID = str(new_ETL_Log.uuid).strip()
            except:
                pass
        except:
            pass
        return ret__new_ETL_Log_UUID
