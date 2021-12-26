from ..etl import utils
from ..models import ETL_Dataset, ETL_Granule, ETL_PipelineRun
import json


class ETL_GranuleService():

    @staticmethod
    def create_new_ETL_Granule_row(granule_name, granule_contextual_information, etl_pipeline_run_uuid,
                                   etl_dataset_uuid, granule_pipeline_state, created_by, additional_json):
        ret_uuid = ''
        etl_pipeline_run_uuid = etl_pipeline_run_uuid if etl_pipeline_run_uuid != '' else None
        etl_dataset_uuid = etl_dataset_uuid if etl_dataset_uuid != '' else None
        try:
            try:
                additional_json_str = json.dumps(additional_json)
            except:
                additional_json_str = json.dumps({})
            try:
                etl_granule = ETL_Granule()
                etl_granule.granule_name = str(granule_name).strip()
                etl_granule.granule_contextual_information = str(granule_contextual_information).strip()
                etl_granule.etl_pipeline_run = ETL_PipelineRun.objects.get(
                    pk=etl_pipeline_run_uuid) if etl_pipeline_run_uuid else etl_pipeline_run_uuid
                etl_granule.etl_dataset = ETL_Dataset.objects.get(
                    pk=etl_dataset_uuid) if etl_dataset_uuid else etl_dataset_uuid
                etl_granule.is_missing = False
                etl_granule.granule_pipeline_state = str(granule_pipeline_state).strip()
                etl_granule.created_by = str(created_by).strip()
                etl_granule.additional_json = additional_json_str
                etl_granule.save()
                ret_uuid = str(etl_granule.uuid).strip()
            except Exception as e:
                print(e)
                pass
        except Exception as e:
            print(e)
            pass
        return ret_uuid

    @staticmethod
    def update_existing_ETL_Granule__granule_pipeline_state(granule_uuid, new__granule_pipeline_state):
        ret__update_is_success = False
        try:
            existing_etl_granule_row = ETL_Granule.objects.filter(uuid=str(granule_uuid).strip())[0]
            existing_etl_granule_row.granule_pipeline_state = str(new__granule_pipeline_state).strip()
            existing_etl_granule_row.save()
            ret__update_is_success = True
        except:
            ret__update_is_success = False
        return ret__update_is_success

    @staticmethod
    def update_existing_ETL_Granule__is_missing_bool_val(granule_uuid, new_is_missing_bool_val):
        try:
            is_missing__bool = utils.get_True_or_False_from_boolish_string(bool_ish_str_value=new_is_missing_bool_val,
                                                                           defaultBoolValue=True)
            existing_etl_granule_row = ETL_Granule.objects.filter(uuid=str(granule_uuid).strip())[0]
            existing_etl_granule_row.is_missing = is_missing__bool
            existing_etl_granule_row.save()
            return True
        except:
            return False

    @staticmethod
    def update_existing_ETL_Granule__Append_To_Additional_JSON(granule_uuid, new_json_key_to_append,
                                                               sub_json_type_object):
        try:
            new_json_key_to_append = str(new_json_key_to_append).strip()
            existing_etl_granule_row = ETL_Granule.objects.filter(uuid=str(granule_uuid).strip())[0]
            current_additional_json = json.loads(existing_etl_granule_row.additional_json)
            current_additional_json[new_json_key_to_append] = sub_json_type_object
            existing_etl_granule_row.additional_json = json.dumps(current_additional_json)
            existing_etl_granule_row.save()
            return True
        except:
            return False
