from django.conf import settings
from django.conf.urls import url
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import include

from climateserv2.views import *

urlpatterns = [
    url('', include('frontend.urls')),
    url(r'^admin/', admin.site.urls),
    url(r'^api/', include('api.urls')),
    url(r'^etl_monitor/', include('etl_monitor.urls')),
    url(r'^system_admin/', include('system_admin.urls')),
    url(r'chirps/getParameterTypes/', getParameterTypes),
    url(r'chirps/submitDataRequest/', submitDataRequest),
    url(r'chirps/getDataRequestProgress/', getDataRequestProgress),
    url(r'chirps/getDataFromRequest/', getDataFromRequest),
    url(r'chirps/getFeatureLayers/', getFeatureLayers),
    url('chirps/getClimateScenarioInfo/', getClimateScenarioInfo),
    url('chirps/getFileForJobID/', getFileForJobID),
    url(r'chirps/submitMonthlyRainfallAnalysisRequest/', submitMonthlyRainfallAnalysisRequest),
    url(r'chirps/getRequestLogs/', getRequestLogs),
    url(r'api/getParameterTypes/', getParameterTypes),
    url(r'api/submitDataRequest/', submitDataRequest),
    url(r'api/getDataRequestProgress/', getDataRequestProgress),
    url(r'api/getDataFromRequest/', getDataFromRequest),
    url(r'api/getFeatureLayers/', getFeatureLayers),
    url('api/getClimateScenarioInfo/', getClimateScenarioInfo),
    url(r'api/getRequestLogs/', getRequestLogs),
    url('api/getFileForJobID/', getFileForJobID),
    url(r'api/submitMonthlyRainfallAnalysisRequest/', submitMonthlyRainfallAnalysisRequest),

    # support old script access path
    url(r'chirps/scriptAccess/getParameterTypes/', getParameterTypes),
    url(r'chirps/scriptAccess/submitDataRequest/', submitDataRequest),
    url(r'chirps/scriptAccess/getDataRequestProgress/', getDataRequestProgress),
    url(r'chirps/scriptAccess/getDataFromRequest/', getDataFromRequest),
    url(r'chirps/scriptAccess/getFeatureLayers/', getFeatureLayers),
    url('chirps/scriptAccess/getClimateScenarioInfo/', getClimateScenarioInfo),
    url('chirps/scriptAccess/getFileForJobID/', getFileForJobID),
    url(r'chirps/scriptAccess/submitMonthlyRainfallAnalysisRequest/', submitMonthlyRainfallAnalysisRequest)
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL,
                          document_root=settings.MEDIA_ROOT)
