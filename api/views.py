from django.contrib.gis.geoip2 import GeoIP2
from django.shortcuts import render
from django.http import JsonResponse
from geoip2.errors import AddressNotFoundError

from rest_framework.viewsets import ModelViewSet
from rest_framework.views import APIView

from .models import Config_Setting, ETL_Dataset, ETL_Granule, ETL_Log, ETL_PipelineRun, Track_Usage
from .serializers import Config_SettingSerializer, ETL_DatasetSerializer, ETL_GranuleSerializer, ETL_LogSerializer, \
    ETL_PipelineRunSerializer
from .services import ETL_DatasetService


class Config_SettingViewSet(ModelViewSet):
    queryset = Config_Setting.objects.all().order_by('setting_name')
    serializer_class = Config_SettingSerializer


class ETL_DatasetViewSet(ModelViewSet):
    queryset = ETL_Dataset.objects.all().order_by('dataset_name')
    serializer_class = ETL_DatasetSerializer


class ETL_GranuleViewSet(ModelViewSet):
    queryset = ETL_Granule.objects.all().order_by('-created_at')
    serializer_class = ETL_GranuleSerializer


class ETL_LogViewSet(ModelViewSet):
    queryset = ETL_Log.objects.all().order_by('-created_at')
    serializer_class = ETL_LogSerializer


class ETL_PipelineRunViewSet(ModelViewSet):
    queryset = ETL_PipelineRun.objects.all().order_by('-created_at')
    serializer_class = ETL_PipelineRunSerializer


class ETL_SubtypesView(APIView):

    def get(self, request, format=None):
        subtypes = ETL_DatasetService.get_all_subtypes_as_string_array()
        return JsonResponse({'subtypes': subtypes})


class Update_Records(APIView):

    def get(self, request, format=None):
        g = GeoIP2()
        usages = Track_Usage.objects.all()
        try:
            for usage in usages:
                try:
                    country_code = g.country_code(usage.originating_IP)
                except AddressNotFoundError:
                    country_code = "ZZ"
                usage.country_ISO = country_code
                usage.save()
            return JsonResponse({'status': 'Updated records successfully'})
        except:
            return JsonResponse({'status': 'There is an error'})
