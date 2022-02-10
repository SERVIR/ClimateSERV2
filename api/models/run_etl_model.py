from django.db import models

from api.models import ETL_Dataset

class Run_ETL(models.Model):
    datasets = ETL_Dataset.objects.all()
    DS=()
    for x in range(len(datasets)):
        DS=DS+((datasets[x].uuid,datasets[x].dataset_name,),)
    etl = models.CharField(max_length=60, choices=DS, default='CHIRPS')
    start_year = models.CharField(max_length=4, blank=False, default="2020")
    end_year = models.CharField(max_length=4, blank=False,default="2020")
    start_month = models.CharField(max_length=2, blank=False,default="1")
    end_month = models.CharField(max_length=2, blank=False,default="12")
    start_day = models.CharField(max_length=2, blank=False,default="1")
    end_day = models.CharField(max_length=2, blank=False,default="30")
    from_last_processed = models.BooleanField(default=False)
    merge_periodically = models.BooleanField("Merge",default=False)

    class Meta:
        verbose_name = 'ETL Run'
        verbose_name_plural = 'ETL runs'