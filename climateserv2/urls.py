"""climateserv2 URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.conf import settings
from django.conf.urls.static import static
from django.conf.urls import url
from django.contrib import admin,admindocs
from django.urls import path, include
from climateserv2.views import *

urlpatterns = [
    #path('admin/', admin.site.urls),
    # Uncomment the next line to enable the admin:
    url('', include('frontend.urls')),
    url(r'^admin/', admin.site.urls),
    url(r'chirps/getParameterTypes/', getParameterTypes),
    # url(r'^/chirps/getRequiredElements/', getRequiredElements),
    url(r'chirps/submitDataRequest/', submitDataRequest),
    url(r'chirps/getDataRequestProgress/', getDataRequestProgress),
    url(r'chirps/getDataFromRequest/', getDataFromRequest),
    url(r'chirps/getFeatureLayers/', getFeatureLayers),
    # url(r'^/chirps/getCapabilitiesForDataset/', getCapabilitiesForDataset),
    url('chirps/getClimateScenarioInfo/', getClimateScenarioInfo),
    url('chirps/getFileForJobID/', getFileForJobID),
    # url(r'^/chirps/submitMonthlyGEFSRainfallAnalysisRequest/', submitMonthlyGEFSRainfallAnalysisRequest),
    # url(r'^/chirps/scriptAccess/', scriptAccess),  # New path for Serverside Script access.
    url(r'chirps/submitMonthlyRainfallAnalysisRequest/', submitMonthlyRainfallAnalysisRequest),
    url(r'chirps/getRequestLogs/', getRequestLogs),  # ks refactor 2015 // New API Hook getRequestLogs

    # api calls

    url(r'api/getParameterTypes/', getParameterTypes),
    # url(r'^/api/getRequiredElements/', getRequiredElements),
    url(r'api/submitDataRequest/', submitDataRequest),
    url(r'api/getDataRequestProgress/', getDataRequestProgress),
    url(r'api/getDataFromRequest/', getDataFromRequest),
    url(r'api/getFeatureLayers/', getFeatureLayers),
    # url(r'^/api/getCapabilitiesForDataset/', getCapabilitiesForDataset),
    url('api/getClimateScenarioInfo/', getClimateScenarioInfo),
    url(r'api/getRequestLogs/', getRequestLogs),  # ks refactor 2015 // New API Hook getRequestLogs
    url('api/getFileForJobID/', getFileForJobID),
    # url(r'^/api/submitMonthlyGEFSRainfallAnalysisRequest/', submitMonthlyGEFSRainfallAnalysisRequest),
    # url(r'^/api/scriptAccess/', scriptAccess),  # New path for Serverside Script access.
    url(r'api/submitMonthlyRainfallAnalysisRequest/', submitMonthlyRainfallAnalysisRequest)
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL,
                          document_root=settings.MEDIA_ROOT)
