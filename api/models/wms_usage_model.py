from django.db import models
from django.utils import timezone


class WMSUsage(models.Model):
    unique_id = models.CharField(max_length=50, unique=True)
    originating_IP = models.CharField(max_length=50, default=None)
    country_ISO = models.CharField(help_text='ISO Country Code for originating IP', max_length=2, null=False)
    time_requested = models.DateTimeField(default=timezone.now)
    ui_id = models.CharField(max_length=200, default=None)

    def __str__(self):
        return f"{self.unique_id}"

    class Meta:
        verbose_name = 'WMS Usage Record'
        verbose_name_plural = 'WMS Usage Records'
        ordering = ['-time_requested']
