from django.db import models
import uuid
from .etl_pipeline_run_model import ETL_PipelineRun
from .etl_dataset_model import ETL_Dataset

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
