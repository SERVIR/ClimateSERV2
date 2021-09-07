from django.db import models
import uuid

class ETL_PipelineRun(models.Model):
    uuid = models.CharField(default=uuid.uuid4, editable=False, max_length=40, primary_key=True)
    #
    additional_json = models.TextField('JSON Data', default="{}", help_text="Extra data field.  Please don't touch this!  Messing with this will likely result in broken content elsewhere in the system.")
    created_at = models.DateTimeField('created_at', auto_now_add=True, blank=True)
    created_by = models.CharField('Created By User or Process Name or ID', max_length=90, blank=False, default="Table_Default_Process", help_text="Who or What Process created this record? 90 chars max")
    is_test_object = models.BooleanField(default=False, help_text="Is this Instance meant to be used ONLY for internal platform testing? (Used only for easy cleanup - DO NOT DEPEND ON FOR VALIDATION)")

    def __str__(self):
        return self.created_at

    class Meta:
        verbose_name = 'ETL Pipeline Run'
        verbose_name_plural = 'ETL Pipeline Runs'
        ordering = ['-created_at']
