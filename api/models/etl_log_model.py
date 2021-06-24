from django.db import models
import uuid

class ETL_Log(models.Model):
    class_name_string = "ETL_Log"
    #
    uuid = models.CharField(default=uuid.uuid4, editable=False, max_length=40, primary_key=True)    # The Table's Unique ID (Globally Unique String)
    # Additional Columns Here
    activity_event_type = models.CharField('Standardized Activity Event Type', max_length=90, blank=False, default="Unknown ETL Activity Event Type", help_text="What is the standardized type for this ETL Activity Event?")
    activity_description = models.TextField('Activity Description', default="No Description", help_text="A field for more detailed information on an ETL Event Activity")
    etl_pipeline_run = models.ForeignKey('ETL_PipelineRun', on_delete=models.SET_NULL, blank=True, null=True)
    etl_dataset = models.ForeignKey('ETL_Dataset', on_delete=models.SET_NULL, blank=True, null=True)
    etl_granule = models.ForeignKey('ETL_Granule', on_delete=models.SET_NULL, blank=True, null=True)
    # etl_pipeline_run_uuid = models.CharField('ETL Pipeline Run UUID', max_length=40, blank=False, default="UNSET_DATASET_UUID", help_text="Each time the ETL Pipeline runs, there is a unique ID generated, this field operates like a way to tag which pipeline this row is attached to.")
    # etl_dataset_uuid = models.CharField('ETL Dataset UUID', max_length=40, blank=False, default="UNSET_DATASET_UUID", help_text="If there is an associated ETL Dataset UUID, it should appear here.  Note: This field may be blank or unset, not all events will have one of these associated items.")
    # etl_granule_uuid = models.CharField('ETL Granule UUID', max_length=40, blank=False, default="UNSET_GRANULE_UUID", help_text="If there is an associated ETL Granule UUID, it should appear here.  Note: This field may be blank or unset, not all events will have one of these associated items.")
    is_alert = models.BooleanField(default=False, help_text="Is this an Item that should be considered an alert?  (by default, all errors and warnings are considered alerts)")
    is_alert_dismissed = models.BooleanField(default=False, help_text="Setting this to True will change the display style for the admin.")
    # More fields will likely be needed.
    additional_json = models.TextField('JSON Data', default="{}", help_text="Extra data field.  Please don't touch this!  Messing with this will likely result in broken content elsewhere in the system.")
    created_at = models.DateTimeField('created_at', auto_now_add=True, blank=True)
    created_by = models.CharField('Created By User or Process Name or ID', max_length=90, blank=False, default="Table_Default_Process", help_text="Who or What Process created this record? 90 chars max")
    is_test_object = models.BooleanField(  default=False, help_text="Is this Instance meant to be used ONLY for internal platform testing? (Used only for easy cleanup - DO NOT DEPEND ON FOR VALIDATION)")
