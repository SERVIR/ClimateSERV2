from django.db import models
import uuid
from .etl_pipeline_run_model import ETL_PipelineRun
from .etl_dataset_model import ETL_Dataset
import json

class ETL_Granule(models.Model):
    uuid = models.CharField(default=uuid.uuid4, editable=False, max_length=40, primary_key=True)    # The Table's Unique ID (Globally Unique String)
    # Additional Columns Here
    granule_name = models.CharField('Granule Name', max_length=250, blank=False, default="Unknown Granule Name", help_text="Most of the time, this may be a filename.  Sometimes it is not.  It should be a name that is unique to the combination of dataset and temporal data value.  This row should be useful in tracking down the specific granule's file/source info.")
    granule_contextual_information = models.TextField('Granule Contextual Information', default="No Additional Information", help_text="A way to capture additional contextual information around a granule, if needed.")
    etl_pipeline_run = models.ForeignKey(ETL_PipelineRun, on_delete=models.SET_NULL, blank=True, null=True, help_text="Each time the ETL Pipeline runs, there is a unique ID generated, this field operates like a way to tag which pipeline this row is attached to")
    etl_dataset = models.ForeignKey(ETL_Dataset, on_delete=models.SET_NULL, blank=True, null=True, help_text="Each Individual Granule has a parent ETL Dataset, storing the association here")
    is_missing = models.BooleanField(  default=False, help_text="Was this expected granule missing at the time of processing?  If this is set to True, that means during an ingest run, there was an expected granule that was either not found at the data source, or had an error that made it unable to be processed and ingested.  The time that this record gets created as compared to ETL Log rows would be a good way to nail down the exact issue as more data is stored about issues in the ETL Log table.")
    granule_pipeline_state = models.CharField('Granule Pipeline State', max_length=20, blank=False, default="UNSET_ATTEMPTED", help_text="Each Individual Granule has a pipeline state.  This lets us easily understand if the Granule succeeded or failed")
    # TODO: More Columns on this table as needed.
    # For now, Only FAILED granule_pipeline_states have additional info stored in this field.
    additional_json = models.TextField('JSON Data', default="{}", help_text="Extra data field.  Please don't touch this!  Messing with this will likely result in broken content elsewhere in the system.")
    created_at = models.DateTimeField('created_at', auto_now_add=True, blank=True)
    created_by = models.CharField('Created By User or Process Name or ID', max_length=90, blank=False, default="Table_Default_Process", help_text="Who or What Process created this record? 90 chars max")
    is_test_object = models.BooleanField(default=False, help_text="Is this Instance meant to be used ONLY for internal platform testing? (Used only for easy cleanup - DO NOT DEPEND ON FOR VALIDATION)")

    def __str__(self):
        return self.uuid

    class Meta:
        verbose_name = 'ETL Granule'
        verbose_name_plural = 'ETL Granules'

    # Creates a brand new ETL Granule Row
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
                new_ETL_Granule.granule_name                    = str(granule_name).strip()
                new_ETL_Granule.granule_contextual_information  = str(granule_contextual_information).strip()
                # new_ETL_Granule.etl_pipeline_run_uuid           = str(etl_pipeline_run_uuid).strip()
                # new_ETL_Granule.etl_dataset_uuid                = str(etl_dataset_uuid).strip()
                new_ETL_Granule.etl_pipeline_run           = ETL_PipelineRun.objects.get(pk=etl_pipeline_run_uuid)
                new_ETL_Granule.etl_dataset                = ETL_Dataset.objects.get(pk=etl_dataset_uuid)
                new_ETL_Granule.is_missing                      = False
                new_ETL_Granule.granule_pipeline_state          = str(granule_pipeline_state).strip()
                new_ETL_Granule.created_by                      = str(created_by).strip()
                new_ETL_Granule.additional_json                 = additional_json_STR  # try/except of json.dumps( additional_json )   # Use json.loads(self.additional_json) to get data out
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

    # Updates the Granule Pipeline State
    # # Example: See settings for: settings.GRANULE_PIPELINE_STATE__ATTEMPTED  (and the other possible states)
    @staticmethod
    def update_existing_ETL_Granule__granule_pipeline_state(granule_uuid, new__granule_pipeline_state):
        ret__update_is_success = False
        try:
            existing_ETL_Granule_Row = ETL_Granule.objects.filter(uuid=str(granule_uuid).strip())[0]
            existing_ETL_Granule_Row.granule_pipeline_state = str(new__granule_pipeline_state).strip()
            existing_ETL_Granule_Row.save()
            ret__update_is_success = True
        except:
            # Error updating the record
            ret__update_is_success = False
        return ret__update_is_success
