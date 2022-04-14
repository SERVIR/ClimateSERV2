from django.contrib.gis.geoip2 import GeoIP2
from django.shortcuts import render
from django.http import JsonResponse
from geoip2.errors import AddressNotFoundError
from django.contrib.admin.views.decorators import staff_member_required

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


def Update_nothing(request):
        return JsonResponse({'status': 'Nothing Updated successfully'})


@staff_member_required
def Update_Datasets(request):

        i = 6
        for x in range(1, 10):
            ds = ETL_Dataset(dataset_name='NMME CCSM4 ENS00' + str(x) + ' Air', dataset_subtype='nmme',
                             dataset_nc4_variable_name='air_temperature',
                             number=i, dataset_name_format='nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens00' + str(x),
                             temp_working_dir='/mnt/climateserv/process_tmp/nmme/',
                             final_load_dir='/mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/',
                             source_url='https://geo.nsstc.nasa.gov/ESB/outgoing/jbr/climateserv2/',
                             fast_directory_path='/mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens00' + str(
                                 x) + '.nc4')
            ds.save()
            i = i + 1
            ds = ETL_Dataset(dataset_name='NMME CCSM4 ENS00' + str(x) + ' Prec', dataset_subtype='nmme',
                             dataset_nc4_variable_name='precipitation',
                             number=i, dataset_name_format='nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens00' + str(x),
                             temp_working_dir='/mnt/climateserv/process_tmp/nmme/',
                             final_load_dir='/mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/',
                             source_url='https://geo.nsstc.nasa.gov/ESB/outgoing/jbr/climateserv2/',
                             fast_directory_path='/mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens00' + str(
                                 x) + '.nc4')
            ds.save()
            i = i + 1
            print('NMME CCSM4 ENS00' + str(x))
        x = 10
        ds = ETL_Dataset(dataset_name='NMME CCSM4 ENS0' + str(x) + ' Air', dataset_subtype='nmme',
                         dataset_nc4_variable_name='air_temperature',
                         number=i, dataset_name_format='nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens0' + str(x),
                         temp_working_dir='/mnt/climateserv/process_tmp/nmme/',
                         final_load_dir='/mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/',
                         source_url='https://geo.nsstc.nasa.gov/ESB/outgoing/jbr/climateserv2/',
                         fast_directory_path='/mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens0' + str(
                             x) + '.nc4')
        ds.save()
        i = i + 1
        ds = ETL_Dataset(dataset_name='NMME CCSM4 ENS0' + str(x) + ' Prec', dataset_subtype='nmme',
                         dataset_nc4_variable_name='precipitation',
                         number=i, dataset_name_format='nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens0' + str(x),
                         temp_working_dir='/mnt/climateserv/process_tmp/nmme/',
                         final_load_dir='/mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/',
                         source_url='https://geo.nsstc.nasa.gov/ESB/outgoing/jbr/climateserv2/',
                         fast_directory_path='/mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens0' + str(
                             x) + '.nc4')
        ds.save()
        print('NMME CCSM4 ENS0' + str(x) + " :number  " + str(i))

        i = 42
        for x in range(1, 10):
            ds = ETL_Dataset(dataset_name='NMME CFSV2 ENS00' + str(x) + ' Air', dataset_subtype='nmme_cfsv2',
                             dataset_nc4_variable_name='air_temperature',
                             number=i,
                             dataset_name_format='nmme-cfsv2_bcsd.latest.global.0.5deg.daily.ens00' + str(x),
                             temp_working_dir='/mnt/climateserv/process_tmp/nmme/',
                             final_load_dir='/mnt/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/',
                             source_url='https://geo.nsstc.nasa.gov/ESB/outgoing/jbr/climateserv2/',
                             fast_directory_path='/mnt/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/nmme-cfsv2_bcsd.latest.global.0.5deg.daily.ens00' + str(
                                 x) + '.nc4')
            ds.save()
            i = i + 1
            ds = ETL_Dataset(dataset_name='NMME CFSV2 ENS00' + str(x) + ' Prec', dataset_subtype='nmme_cfsv2',
                             dataset_nc4_variable_name='precipitation',
                             number=i,
                             dataset_name_format='nmme-cfsv2_bcsd.latest.global.0.5deg.daily.ens00' + str(x),
                             temp_working_dir='/mnt/climateserv/process_tmp/nmme/',
                             final_load_dir='/mnt/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/',
                             source_url='https://geo.nsstc.nasa.gov/ESB/outgoing/jbr/climateserv2/',
                             fast_directory_path='/mnt/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/nmme-cfsv2_bcsd.latest.global.0.5deg.daily.ens00' + str(
                                 x) + '.nc4')
            ds.save()
            i = i + 1
        print('NMME CFSV2 ENS00' + str(x)+ " :number  " + str(i))
        i=60
        for x in range(10, 25):
            ds = ETL_Dataset(dataset_name='NMME CFSV2 ENS0' + str(x) + ' Air', dataset_subtype='nmme_cfsv2',
                             dataset_nc4_variable_name='air_temperature',
                             number=i, dataset_name_format='nmme-cfsv2_bcsd.latest.global.0.5deg.daily.ens0' + str(x),
                             temp_working_dir='/mnt/climateserv/process_tmp/nmme/',
                             final_load_dir='/mnt/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/',
                             source_url='https://geo.nsstc.nasa.gov/ESB/outgoing/jbr/climateserv2/',
                             fast_directory_path='/mnt/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/nmme-cfsv2_bcsd.latest.global.0.5deg.daily.ens0' + str(
                                 x) + '.nc4')
            ds.save()
            i = i + 1
            ds = ETL_Dataset(dataset_name='NMME CFSV2 ENS0' + str(x) + ' Prec', dataset_subtype='nmme_cfsv2',
                             dataset_nc4_variable_name='precipitation',
                             number=i, dataset_name_format='nmme-cfsv2_bcsd.latest.global.0.5deg.daily.ens0' + str(x),
                             temp_working_dir='/mnt/climateserv/process_tmp/nmme/',
                             final_load_dir='/mnt/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/',
                             source_url='https://geo.nsstc.nasa.gov/ESB/outgoing/jbr/climateserv2/',
                             fast_directory_path='/mnt/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/nmme-cfsv2_bcsd.latest.global.0.5deg.daily.ens0' + str(
                                 x) + '.nc4')
            ds.save()
            i=i+1
        print('NMME CFSV2 ENS0' + str(x) + " :number  " + str(i))
        return JsonResponse({'status': 'Updated records successfully'})
