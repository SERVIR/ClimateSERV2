from django.db import models
import uuid
from .etl_pipeline_run_model import ETL_PipelineRun
from .etl_dataset_model import ETL_Dataset
from .etl_granule_model import ETL_Granule


class ETL_Log(models.Model):
    uuid = models.CharField(default=uuid.uuid4, editable=False, max_length=40, primary_key=True)
    #
    activity_event_type = models.CharField('Standardized Activity Event Type', max_length=255, blank=False,
                                           default="Unknown ETL Activity Event Type",
                                           help_text="What is the standardized type for this ETL Activity Event?")
    activity_description = models.TextField('Activity Description', default="No Description",
                                            help_text="A field for more detailed information on an ETL Event Activity")
    etl_pipeline_run = models.ForeignKey(ETL_PipelineRun, on_delete=models.SET_NULL, blank=True, null=True,
                                         help_text="Each time the ETL Pipeline runs, there is a unique ID generated, "
                                                   "this field operates like a way to tag which pipeline this row is "
                                                   "attached to.")
    etl_dataset = models.ForeignKey(ETL_Dataset, on_delete=models.SET_NULL, blank=True, null=True,
                                    help_text="If there is an associated ETL Dataset UUID, it should appear here.  "
                                              "Note: This field may be blank or unset, not all events will have one "
                                              "of these associated items.")
    etl_granule = models.ForeignKey(ETL_Granule, on_delete=models.SET_NULL, blank=True, null=True,
                                    help_text="If there is an associated ETL Granule UUID, it should appear here.  "
                                              "Note: This field may be blank or unset, not all events will have one "
                                              "of these associated items.")
    is_alert = models.BooleanField(default=False,
                                   help_text="Is this an Item that should be considered an alert?  (by default, "
                                             "all errors and warnings are considered alerts)")
    is_alert_dismissed = models.BooleanField(default=False,
                                             help_text="Setting this to True will change the display style for the "
                                                       "admin.")
    #
    additional_json = models.TextField('JSON Data', default="{}",
                                       help_text="Extra data field.  Please don't touch this!  Messing with this will "
                                                 "likely result in broken content elsewhere in the system.")
    created_at = models.DateTimeField('created_at', auto_now_add=True, blank=True)
    created_by = models.CharField('Created By User or Process Name or ID', max_length=255, blank=False,
                                  default="Table_Default_Process",
                                  help_text="Who or What Process created this record? 90 chars max")
    is_test_object = models.BooleanField(default=False,
                                         help_text="Is this Instance meant to be used ONLY for internal platform "
                                                   "testing? (Used only for easy cleanup - DO NOT DEPEND ON FOR "
                                                   "VALIDATION)")
    status = models.CharField('Status', max_length=255, blank=False,
                                           default="In Progress")
    start_time = models.DateTimeField('start_time', blank=True,null=True)
    end_time = models.DateTimeField('end_time', blank=True,null=True)

    def __str__(self):
        return '{} - {}'.format(self.activity_event_type, self.created_by)

    class Meta:
        verbose_name = 'ETL Log'
        verbose_name_plural = 'ETL Logs'
        ordering = ['-created_at']
