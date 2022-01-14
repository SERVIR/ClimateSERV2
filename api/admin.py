# Register your models here.
from django.urls import get_script_prefix
from django.utils.html import format_html

from .models import Config_Setting
from .models import ETL_Dataset
from .models import ETL_Granule
from .models import ETL_Log
from .models import ETL_PipelineRun
from .models import Request_Progress, Request_Log, Track_Usage, Storage_Review, Run_ETL
from django.contrib import admin
from django.contrib.auth.admin import Group
admin.site.register(Config_Setting)
admin.site.register(ETL_Dataset)

from accounts.models import User

@admin.register(User)
class ETLGranuleAdmin(admin.ModelAdmin):
    list_display = ('username','email','first_name','last_name','is_staff','storage_alerts','feedback_alerts','etl_alerts')


@admin.register(ETL_Granule)
class ETLGranuleAdmin(admin.ModelAdmin):
    list_display = ('uuid', 'etl_pipeline_run', 'etl_dataset', 'granule_name',
                    'is_missing', 'granule_pipeline_state', 'created_at', 'created_by')
    list_filter = ('is_missing', 'is_test_object', 'granule_pipeline_state', 'etl_dataset')
    search_fields = ('uuid', 'etl_pipeline_run')
    autocomplete_fields = ['etl_pipeline_run']


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
    list_filter = ('ui_request', 'metadata_request', 'dataset', 'calculation')
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
    list_display = (
        'unique_id',
        'API_request_txt_files',
        'API_request_zip_files')
    def render_change_form(self, request, context, add=False, change=False, form_url='', obj=None):
        context.update({
            'show_save': False,
            'show_save_and_continue': False,
            'show_save_and_add_another': False,
            'show_delete': False
        })
        return super().render_change_form(request, context, add, change, form_url, obj)

@admin.register(Run_ETL)
class Run_ETLAdmin(admin.ModelAdmin):
    list_display = (
        'etl','start_year','end_year', 'start_month','end_month','start_day','end_day','from_last_processed','merge_option')

    def render_change_form(self, request, context, add=False, change=False, form_url='', obj=None):
        context.update({
            'show_save': False,
            'show_save_and_continue': False,
            'show_save_and_add_another': False,
            'show_delete': False,
            'from_etl':True
        })
        return super().render_change_form(request, context, add, change, form_url, obj)

