from django.utils.timezone import now
from django.db import models


# def get_time():
#     return datetime.utcnow().replace(tzinfo=pytz.utc)


class Request_Progress(models.Model):
    request_id = models.CharField(max_length=50)
    progress = models.CharField(max_length=50)
    date_created = models.DateTimeField(default=now, blank=True)

    def __str__(self):
        return f"{self.request_id}"


class Request_Log(models.Model):
    unique_id = models.CharField(max_length=50)
    log = models.JSONField()
    log_date = models.CharField(max_length=50)
    log_dat = models.CharField(max_length=50, default="", blank=True)

    def __str__(self):
        return f"{self.unique_id}"
