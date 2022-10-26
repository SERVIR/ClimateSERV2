from django.contrib import admin
from django.utils.html import format_html
from .models import Config_Setting, Storage_Review, Run_ETL, Profile
from .models import ETL_Dataset
from .models import ETL_Dataset_V2
from .models import ETL_Dataset_V3
from .models import ETL_Granule
from .models import ETL_Log
from .models import ETL_PipelineRun
from .models import Request_Progress, Request_Log, Track_Usage
from .models import Parameters
from django_json_widget.widgets import JSONEditorWidget
from django.db import models

admin.site.register(Parameters)
admin.site.register(Config_Setting)
#admin.site.register(ETL_Dataset)

@admin.register(ETL_Dataset)
class ETLDatasetAdmin(admin.ModelAdmin):
    list_display = ('dataset_name','final_load_dir','dataset_nc4_variable_name','late_after', 'contact_info', 'source_url')

@admin.register(ETL_Dataset_V2)
class ETLDatasetAdmin(admin.ModelAdmin):
    list_display = ('dataset_name','final_load_dir','dataset_nc4_variable_name','late_after', 'contact_info', 'source_url')

@admin.register(ETL_Dataset_V3)
class ETLDatasetAdmin(admin.ModelAdmin):
    formfield_overrides = {
        models.JSONField: {'widget': JSONEditorWidget}
    }
    list_display = ('dataset_name','dataset_subtype')

@admin.register(Profile)
class ProfileAdmin(admin.ModelAdmin):
    list_display = ('user','etl_alerts','feedback_alerts','storage_alerts')


@admin.register(ETL_Granule)
class ETLGranuleAdmin(admin.ModelAdmin):
    list_display = ('uuid', 'granule_name', 'granule_contextual_information', 'etl_pipeline_run',
                    'etl_dataset', 'is_missing', 'granule_pipeline_state', 'additional_json', 'created_at',
                    'created_by', 'is_test_object')
    search_fields = ('uuid',)


@admin.register(ETL_Log)
class ETLLogAdmin(admin.ModelAdmin):
    list_display = ('uuid', 'etl_pipeline_run',
                    'etl_dataset', 'etl_granule', 'start_time', 'end_time', 'status')
    autocomplete_fields = ['etl_pipeline_run']
    search_fields = ('etl_pipeline_run__uuid',)
    date_hierarchy = "created_at"


@admin.register(ETL_PipelineRun)
class ETLPipelineRunAdmin(admin.ModelAdmin):
    list_display = ('uuid', 'created_at')
    search_fields = ('uuid',)
    date_hierarchy = "created_at"


admin.site.register(Request_Progress)
admin.site.register(Request_Log)


@admin.register(Track_Usage)
class Track_UsageAdmin(admin.ModelAdmin):
    list_display = (
        'unique_id',
        'time_requested',
        'ip_location',
        'country_ISO',
        'aoi_button',
        'dataset',
        'start_date',
        'end_date',
        'file_size',
        'ui_request')
    list_filter = ('ui_request', 'metadata_request', 'dataset', 'calculation','country_ISO')
    search_fields = ('unique_id', 'dataset', 'originating_IP')
    date_hierarchy = "time_requested"

    def ip_location(self, obj):
        return format_html(
            "<a href='https://www.ip2location.com/demo/{}' target='_blank'>{}</a>",
            obj.originating_IP,
            obj.originating_IP)

    ip_location.admin_order_field = 'originating_IP'

    def aoi_button(self, obj):
        if obj.AOI == '{}':
            return format_html("<span>No AOI</span>")
        else:
            return format_html(
                "<a href='javascript:open_aoi({})'>Display AOI</a>",
                obj.id)

@admin.register(Storage_Review)
class Storage_ReviewAdmin(admin.ModelAdmin):
    list_display_links = None
    list_display = (
        'unique_id',
        'directory',
        'file_size',
        'free_space',
        'last_notified_time',
        'threshold')
    def changelist_view(self, request, extra_context=None):
        extra_context = {'title': 'Storage space statistics'}
        return super(Storage_ReviewAdmin, self).changelist_view(request, extra_context=extra_context)

@admin.register(Run_ETL)
class Run_ETLAdmin(admin.ModelAdmin):
    list_display = (
        'etl','start_year','end_year', 'start_month','end_month','start_day','end_day','from_last_processed','merge_periodically')
    def start_month(self, obj):
        if obj.from_last_processed == "true":
            return ""

    def render_change_form(self, request, context, add=False, change=False, form_url='', obj=None):
        context.update({
            'show_save': False,
            'show_save_and_continue': False,
            'show_save_and_add_another': False,
            'show_delete': False,
            'from_etl':True
        })
        return super().render_change_form(request, context, add, change, form_url, obj)
