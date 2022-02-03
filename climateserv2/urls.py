from django.conf import settings
from django.conf.urls import url
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import include

from climateserv2.views import *

urlpatterns = [
    url('', include('frontend.urls')),
    url(r'^admin/', admin.site.urls, name='admin'),
    url(r'^api/', include('api.urls')),
    url(r'^etl_monitor/', include('etl_monitor.urls')),
    url(r'^system_admin/', include('system_admin.urls')),
    url(r'chirps/getParameterTypes/', get_parameter_types),
    url(r'chirps/submitDataRequest/', submit_data_request),
    url(r'chirps/getDataRequestProgress/', get_data_request_progress),
    url(r'chirps/getDataFromRequest/', get_data_from_request),
    url(r'chirps/getFeatureLayers/', get_feature_layers),
    url('chirps/getClimateScenarioInfo/', get_climate_scenario_info),
    url('chirps/getFileForJobID/', get_file_for_job_id),
    url(r'chirps/submitMonthlyRainfallAnalysisRequest/', submit_monthly_rainfall_analysis_request),
    url(r'chirps/getRequestLogs/', get_request_logs),
    url(r'api/getParameterTypes/', get_parameter_types),
    url(r'api/submitDataRequest/', submit_data_request),
    url(r'api/getDataRequestProgress/', get_data_request_progress),
    url(r'api/getDataFromRequest/', get_data_from_request),
    url(r'api/getFeatureLayers/', get_feature_layers),
    url('api/getClimateScenarioInfo/', get_climate_scenario_info),
    url(r'api/getRequestLogs/', get_request_logs),
    url('api/getFileForJobID/', get_file_for_job_id),
    url(r'api/submitMonthlyRainfallAnalysisRequest/', submit_monthly_rainfall_analysis_request),
    url(r'api/restartClimateSERV/', restart_climateserv),
    url(r'api/run_etl/',run_etl),

    # support old script access path
    url(r'chirps/scriptAccess/getParameterTypes/', get_parameter_types),
    url(r'chirps/scriptAccess/submitDataRequest/', submit_data_request),
    url(r'chirps/scriptAccess/getDataRequestProgress/', get_data_request_progress),
    url(r'chirps/scriptAccess/getDataFromRequest/', get_data_from_request),
    url(r'chirps/scriptAccess/getFeatureLayers/', get_feature_layers),
    url('chirps/scriptAccess/getClimateScenarioInfo/', get_climate_scenario_info),
    url('chirps/scriptAccess/getFileForJobID/', get_file_for_job_id),
    url(r'chirps/scriptAccess/submitMonthlyRainfallAnalysisRequest/', submit_monthly_rainfall_analysis_request)
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL,
                          document_root=settings.MEDIA_ROOT)
