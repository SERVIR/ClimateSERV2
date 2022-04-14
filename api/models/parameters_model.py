from django.db import models


class Parameters(models.Model):
    DEBUG_LIVE = models.BooleanField(default=False)
    logToConsole = models.BooleanField(default=True)
    pythonPath = models.CharField('Python Path', max_length=255, blank=False,
                                  default="/cserv2/python_environments/conda/anaconda3/envs/climateserv2/bin/python")
    serviringestroot = models.CharField('serviringestroot', max_length=255, blank=False,
                                        default="/cserv2/tmp/data/pythonCode/serviringest/")
    requestLog_db_basepath = models.CharField('requestLog_db_basepath', max_length=255, blank=False,
                                              default="/cserv2/tmp/")
    zipFile_ScratchWorkspace_Path = models.CharField('zipFile_ScratchWorkspace_Path', max_length=255, blank=False,
                                                     default="/mnt/cs-temp/request_out/")  # '''/cserv2/tmp/zipout/Zipfile_Scratch/'''
    logfilepath = models.CharField('logfilepath', max_length=255, blank=False, default="/cserv2/tmp/")
    workpath = models.CharField('workpath', max_length=255, blank=False, default="/cserv2/tmp/")
    shapefilepath = models.CharField('shapefilepath', max_length=255, blank=False, default="/cserv2/tmp/mapfiles/")
    ageInDaysToPurgeData = models.IntegerField(default=7)
    nmme_ccsm4_path = models.CharField('nmme_ccsm4_path', max_length=255, blank=False,
                                       default="/mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/")  # '''/cserv2/tmp/data/nmme/'''
    nmme_cfsv2_path = models.CharField('nmme_cfsv2_path', max_length=255, blank=False,
                                       default="/mnt/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/")
    base_data_path = models.CharField('base_data_path', max_length=255, blank=False,
                                      default="/mnt/climateserv/")  # '''/mnt/climateserv/ucsb-chirps/global/0.05deg/daily/'''
    parameters = models.TextField('parameters object',
                                  default="[[0, 'max', 'Max'], [1, 'min', 'Min'], [2, 'median', 'Median'], [3, 'range', 'Range'], [4, 'sum', 'Sum'], [5, 'avg', 'Average'], [6, 'download', 'Download'], [7, 'netcdf', 'NetCDF'], [8, 'csv', 'CSV']]", )
    resultsdir = models.CharField('results dir', max_length=255, blank=False, default="/mnt/cs-temp/request_out/")
    shapefileName = models.TextField('shapefile JSON Data', default="{}", )
    class Meta:
        verbose_name = 'Parameters'
        verbose_name_plural = 'Parameters'
