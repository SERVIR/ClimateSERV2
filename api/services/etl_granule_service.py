from ..models import ETL_Dataset, ETL_Granule, ETL_PipelineRun
import json

class ETL_GranuleService():

    @staticmethod
    def create_new_ETL_Granule_row(granule_name, granule_contextual_information, etl_pipeline_run_uuid, etl_dataset_uuid, granule_pipeline_state, created_by, additional_json):
        ret__new_ETL_Granule_UUID = ""
        try:
            additional_json_STR = json.dumps({})
            try:
                additional_json_STR = json.dumps(additional_json)
            except:
                # Error parsing input as a JSONable python object
                additional_json_STR = json.dumps({})
            try:
                new_ETL_Granule = ETL_Granule()
                new_ETL_Granule.granule_name                   = str(granule_name).strip()
                new_ETL_Granule.granule_contextual_information = str(granule_contextual_information).strip()
                new_ETL_Granule.etl_pipeline_run               = ETL_PipelineRun.objects.get(pk=etl_pipeline_run_uuid)
                new_ETL_Granule.etl_dataset                    = ETL_Dataset.objects.get(pk=etl_dataset_uuid)
                new_ETL_Granule.is_missing                     = False
                new_ETL_Granule.granule_pipeline_state         = str(granule_pipeline_state).strip()
                new_ETL_Granule.created_by                     = str(created_by).strip()
                new_ETL_Granule.additional_json                = additional_json_STR  # try/except of json.dumps( additional_json )   # Use json.loads(self.additional_json) to get data out
                new_ETL_Granule.save()
                ret__new_ETL_Granule_UUID   = str(new_ETL_Granule.uuid).strip()
            except Exception as e:
                print(e)
                # Error Setting Log Entry props and saving
                pass
        except Exception as e:
            print(e)
            # Error Creating a new ETL Granule Row Entry
            pass
        return ret__new_ETL_Granule_UUID

    @staticmethod
    def update_existing_ETL_Granule__granule_pipeline_state(granule_uuid, new__granule_pipeline_state):
        ret__update_is_success = False
        try:
            existing_ETL_Granule_Row = ETL_Granule.objects.filter(uuid=str(granule_uuid).strip())[0]
            existing_ETL_Granule_Row.granule_pipeline_state = str(new__granule_pipeline_state).strip()
            existing_ETL_Granule_Row.save()
            ret__update_is_success = True
        except:
            ret__update_is_success = False
        return ret__update_is_success
