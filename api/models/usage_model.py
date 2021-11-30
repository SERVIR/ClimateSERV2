from django.db import models
class Track_Usage(models.Model):
    unique_id = models.CharField(max_length=50)
    originating_IP=models.CharField(max_length=50)
    task_ID = models.CharField(max_length=50)
    time_requested = models.CharField(max_length=50)
    AOI = models.CharField(max_length=50)
    dataset = models.CharField(max_length=50)
    time_range = models.CharField(max_length=50)
    request_type = models.CharField(max_length=50)
    status = models.CharField(max_length=50)
    file_size = models.CharField(max_length=50)

    def __str__(self):
        return f"{self.unique_id}"


