from django.contrib import admin

# Register your models here.

from .models import Config_Setting
from .models import ETL_Dataset
from .models import ETL_Granule
from .models import ETL_Log
from .models import ETL_PipelineRun
from .models import Request_Progress, Request_Log


admin.site.register(Config_Setting)
admin.site.register(ETL_Dataset)
admin.site.register(ETL_Granule)
admin.site.register(ETL_Log)
admin.site.register(ETL_PipelineRun)
admin.site.register(Request_Progress)

admin.site.register(Request_Log)
