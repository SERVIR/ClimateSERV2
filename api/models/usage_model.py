from django.db import models

class Track_Usage(models.Model):
    unique_id = models.CharField(max_length=50)
    originating_IP=models.CharField(max_length=50)
    task_ID = models.CharField(max_length=50)
    time_requested = models.DateTimeField()
    AOI = models.JSONField()
    dataset = models.CharField(max_length=50)
    start_date = models.DateTimeField()
    end_date = models.DateTimeField()
    request_type = models.CharField(max_length=50)
    status = models.CharField(max_length=50)
    file_size = models.IntegerField()


    def __str__(self):
        return f"{self.unique_id}"