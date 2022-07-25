import json

from django.contrib.admin.views.decorators import staff_member_required
from django.shortcuts import render

from climateserv2.file import TDSExtraction
from climateserv2.processtools import uutools
from climateserv2.views import get_climate_datatype_map
from .models import *
from api.models import Track_Usage

from django import template
from django.template.defaultfilters import stringfilter
from django.views.decorators.cache import cache_page
import xarray as xr

register = template.Library()


@cache_page(60 * 15)
def index(request):
    return render(request, 'index.html', context={
        'page': 'menu-home',
        'datasets': DataSet.objects.exclude(featured=False).all(),
        'page_elements': HomePage.objects.first()
    })


def map_app(request):
    try:
        nc_file = xr.open_dataset(
            '/mnt/climateserv/process_tmp/fast_nmme_monthly/nmme-mme_bcsd.latest.global.0.5deg.daily.nc4',
            chunks={'time': 16, 'longitude': 128,
                    'latitude': 128})

        start_date, end_date = TDSExtraction.get_date_range_from_nc_file(nc_file)
        is_error = False
        climate_model_datatype_capabilities_list = [
            {
                "current_Capabilities": {
                    "startDateTime": start_date,
                    "endDateTime": end_date
                }
            }
        ]
        climate_datatype_map = get_climate_datatype_map()
        api_return_object = {
            "unique_id": uutools.getUUID(),
            "RequestName": "getClimateScenarioInfo",
            "climate_DatatypeMap": climate_datatype_map,
            "climate_DataTypeCapabilities": climate_model_datatype_capabilities_list,
            "isError": False
        }
    except:
        with open('/cserv2/django_app/ClimateSERV2/climateserv2/sample_climate_scenario.json', 'r') as climate_scenario:
            api_return_object = json.loads(climate_scenario.read())
            api_return_object["unique_id"] = uutools.getUUID()
    return render(request, 'map.html', context={
        'page': 'menu-map',
        'data_layers': DataLayer.objects.order_by('title').all(),
        'climateModelInfo': json.dumps(api_return_object),
    })


@staff_member_required
def display_aoi(request, usage_id):
    usage = Track_Usage.objects.get(id=usage_id)
    return render(request, 'display-aoi.html', context={
        'aoi': usage.AOI,
    })


@cache_page(60 * 15)
def about(request):
    return render(request, 'about.html', context={'page': 'menu-about'})


@cache_page(60 * 15)
def help_center(request):
    return render(request, 'help.html', context={
        'page': 'menu-help',
        'datasets': DataSet.objects.all()
    })


@register.filter(is_safe=True)
@stringfilter
def trim(value):
    return value.strip()

