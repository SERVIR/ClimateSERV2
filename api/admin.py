from django.contrib import admin

# Register your models here.

from .models import Config_Setting
from .models import ETL_Dataset
from .models import ETL_Granule
from .models import ETL_Log
from .models import ETL_PipelineRun
from .models import Request_Progress, Request_Log, Track_Usage

admin.site.register(Config_Setting)
admin.site.register(ETL_Dataset)


@admin.register(ETL_Granule)
class ETLGranuleAdmin(admin.ModelAdmin):
    list_display = ('uuid', 'granule_name', 'granule_contextual_information', 'etl_pipeline_run',
                    'etl_dataset', 'is_missing', 'granule_pipeline_state', 'additional_json', 'created_at',
                    'created_by', 'is_test_object')
    search_fields = ('uuid',)


@admin.register(ETL_Log)
class ETLLogAdmin(admin.ModelAdmin):
    list_display = ('uuid', 'etl_pipeline_run',
                    'etl_dataset', 'etl_granule')
    autocomplete_fields = ['etl_granule']
    search_fields = ('etl_granule',)
    date_hierarchy = "created_at"


@admin.register(ETL_PipelineRun)
class ETLPipelineRunAdmin(admin.ModelAdmin):
    list_display = ('uuid', 'created_at')
    date_hierarchy = "created_at"


admin.site.register(Request_Progress)
admin.site.register(Request_Log)


@admin.register(Track_Usage)
class Track_UsageAdmin(admin.ModelAdmin):
    list_display = ('unique_id', 'time_requested', 'originating_IP', 'dataset', 'start_date', 'end_date', 'file_size')
    list_filter = ('dataset', 'calculation', 'request_type')
    search_fields = ('unique_id', 'dataset')
    date_hierarchy = "time_requested"