import shutil
import uuid
from pathlib import Path

from django.db import models
from django.db.models.signals import post_save


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
    def update_on_test(sender, instance, **kwargs):
         # custome operation as you want to perform
         stat = shutil.disk_usage(sender.directory)
         free = stat.free
         size = sum(p.stat().st_size for p in Path(sender.directory).rglob('*'))
         for unit in ("B", "K", "M", "G", "T"):
             if free < 1024:
                 break
             free /= 1024
             if size < 1024:
                 break
             size /= 1024
         free_str = str(round(free, 2)) + unit
         used_str = str(round(size, 2)) + unit
         sender.file_size = used_str
         sender.free_space = free_str
         post_save.connect(sender.update_on_test, sender=Storage_Review)


