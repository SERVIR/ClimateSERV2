from django.db import models
class Storage_Review(models.Model):
    unique_id = models.CharField(max_length=50)
    API_request_txt_files = models.CharField(max_length=50, default=None) #/cserv2/tmp/*.txt
    API_request_zip_files = models.CharField(max_length=50, default=None)#/cserv2/tmp/zipout/Zipfile_Scratch/*.zip
    ingested_datasets = models.CharField(max_length=50, default=None)#/mnt/climateserv/process_tmp/each dataset



