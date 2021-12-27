from django.db import models


class Request_Progress(models.Model):
    request_id = models.CharField(max_length=50)
    progress = models.CharField(max_length=50)

    def __str__(self):
        return f"{self.request_id}"


class Request_Log(models.Model):
    unique_id = models.CharField(max_length=50)
    log = models.JSONField()
    log_date = models.CharField(max_length=50)
    log_dat = models.CharField(max_length=50, default="", blank=True)

    def __str__(self):
        return f"{self.unique_id}"
