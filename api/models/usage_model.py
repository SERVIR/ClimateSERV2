from django.db import models
from django.utils import timezone
class Track_Usage(models.Model):
    unique_id = models.CharField(max_length=50)
    originating_IP=models.CharField(max_length=50,default=None)
    time_requested = models.DateTimeField(default=timezone.now)
    AOI = models.JSONField(default=None)
    dataset = models.CharField(max_length=50,default=None)
    start_date = models.DateTimeField(default=timezone.now)
    end_date = models.DateTimeField(default=timezone.now)
    request_type = models.CharField(max_length=50,default=None)
    status = models.CharField(max_length=50,default=None)
    file_size = models.IntegerField(default=0) #for download data


    def __str__(self):
        return f"{self.unique_id}"