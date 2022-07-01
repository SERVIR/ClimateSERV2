from django.db import models
from django.utils import timezone


class Track_Usage(models.Model):
    unique_id = models.CharField(max_length=50, unique=True)
    originating_IP = models.CharField(max_length=50, default=None)
    time_requested = models.DateTimeField(default=timezone.now)
    AOI = models.JSONField(default=None)
    dataset = models.CharField(max_length=50, default=None)
    start_date = models.DateTimeField(default=timezone.now)
    end_date = models.DateTimeField(default=timezone.now)
    calculation = models.CharField(max_length=50, null=True, default=None)
    request_type = models.CharField(max_length=50, default=None)
    status = models.CharField(max_length=50, default=None)
    file_size = models.IntegerField(default=0)  # for download data, get file
    API_call = models.CharField(max_length=250, null=True, blank=True)
    data_retrieved = models.BooleanField(default=False)
    progress = models.CharField(default='0', max_length=50)
    ui_request = models.BooleanField(help_text="Is this request originated through the CS UI?", default=False)
    country_ISO = models.CharField(help_text='ISO Country Code for originating IP', max_length=2, null=False)
    metadata_request = models.BooleanField(help_text="Is this a request for metadata instead of actual data (eg., "
                                                     "information about what data is available)?", default=False,
                                           null=False)

    def __str__(self):
        return f"{self.unique_id}"

    class Meta:
        verbose_name = 'Usage Record'
        verbose_name_plural = 'Usage Records'
        ordering = ['-time_requested']
