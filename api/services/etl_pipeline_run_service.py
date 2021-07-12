from ..models import ETL_PipelineRun

class ETL_PipelineRunService():

    @staticmethod
    def create_etl_pipeline_run():
        ret_uuid = ''
        ret_created = False
        try:
            new_instance = ETL_PipelineRun()
            new_instance.save()
            ret_uuid = new_instance.uuid
            ret_created = True
        except:
            ret_uuid = ''
            ret_created = False
        return ret_created, ret_uuid
