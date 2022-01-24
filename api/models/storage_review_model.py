from django.db import models
class Storage_Review(models.Model):
    unique_id = models.CharField(max_length=50)
    directory = models.CharField(max_length=50, default=None)
    file_size = models.CharField(max_length=50, default=None)
    free_space =models.CharField(max_length=50,blank=False)
    class Meta:
        verbose_name = 'Storage Review'
        verbose_name_plural = 'Storage Review'


