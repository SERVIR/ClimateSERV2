import uuid

from django.db import models


class Storage_Review(models.Model):
    unique_id = models.CharField(default=uuid.uuid4,max_length=50,editable=False)
    directory = models.CharField(max_length=50, default=None,help_text="Path you want to monitor")
    file_size = models.CharField("Size",max_length=50, default="0",editable=False)
    free_space = models.CharField(max_length=50,blank=False,editable=False)
    threshold =  models.IntegerField("Threshold in megabytes(GB)",blank=False,default="50",help_text="Threshold in GB, upon reaching sends an email")
    last_notified_time = models.DateTimeField(null=True, blank=True,editable=False)
    time_to_check =  models.IntegerField(blank=False,default="15",help_text="Time in minutes to check and notify")
    class Meta:
        verbose_name = 'Storage Review'
        verbose_name_plural = 'Storage Review'


