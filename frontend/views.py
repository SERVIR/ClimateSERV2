import json
import uuid

import requests
from django.contrib.admin.views.decorators import staff_member_required
from django.db.models import CharField
from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.core import serializers
from django.db.models.functions import Cast
from climateserv2 import settings
from climateserv2.views import get_nmme_info
from .models import *
from api.models import Track_Usage

from django import template
from django.template.defaultfilters import stringfilter
from django.views.decorators.cache import cache_page
import logging

register = template.Library()

logger = logging.getLogger("request_processor")


@cache_page(60 * 15)
def index(request):
    return render(request, 'index.html', context={
        'page': 'menu-home',
        'datasets': DataSet.objects.exclude(featured=False).all(),
        'page_elements': HomePage.objects.first()
    })


def map_app(request):
    try:
        logger.error("loading map")
        my_data_sets = get_datasets()
    except Exception as e:
        logger.error(str(e))
        my_data_sets = None

    try:
        nmme_info = json.dumps(get_nmme_info(str(uuid.uuid4())))
        # nmme_info = json.dumps({})
    except Exception as e:
        logger.error(str(e))
        nmme_info = json.dumps({})

    return render(request, 'map.html', context={
        'page': 'menu-map',
        'data_layers': my_data_sets,

        'climateModelInfo': nmme_info,
    })


@csrf_exempt
def get_map_layers(request):
    my_data_sets = get_datasets()

    callback = request.POST.get("callback", request.GET.get("callback"))
    if callback:
        http_response = HttpResponse(
            callback + "(" + str(serializers.serialize('json', my_data_sets)) + ")",
            content_type="application/json")
    else:
        http_response = HttpResponse(str(serializers.serialize('json', my_data_sets)))

    return http_response


def get_datasets():
    my_data_sets = DataLayer.objects.order_by('title').all()
    for item in my_data_sets:
        if item.isMultiEnsemble:
            my_ensembles = item.datalayer.order_by('title').all()
            for ens in my_ensembles:
                print(ens.ui_id)
                ens.ui_id = ens.ui_id + str(uuid.uuid4())
            print(str(my_ensembles[0].ui_id))
            item.datalayer.set(my_ensembles)
        else:
            item.ui_id = item.ui_id + str(uuid.uuid4())
    return my_data_sets


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
    # data_layers = DataLayer.objects.all()
    # data_layers = sorted(
    #     data_layers,
    #     key=lambda x: (1 if not x.api_id.isdigit() else 0, int(x.api_id) if x.api_id.isdigit() else -666)
    # )

    data_layers = DataLayer.objects.filter(api_id__regex=r'^[0-9]+$').order_by('api_id')

    return render(request, 'help.html', context={
        'page': 'menu-help',
        'datasets': DataSet.objects.all(),
        'data_layers': data_layers
    })


@cache_page(60 * 15)
def dev_api(request):
    return render(request, 'help.html', context={
        'page': 'menu-help',
        'datasets': DataSet.objects.all(),
        'dev_api': True
    })


@csrf_exempt
def confirm_captcha(request):
    version = request.POST.get('version', '')
    if version == '':
        secret = settings.reCAPTCHA_KEY
    else:
        secret = settings.reCAPTCHA_V2_KEY
    verify_data = {
        'response': request.POST.get('g-recaptcha-response'),
        'secret': secret
    }
    resp = requests.post('https://www.google.com/recaptcha/api/siteverify', data=verify_data)
    result_json = resp.json()
    # this is only to test a low first captcha score.  uncomment to use for testing
    # remember to comment back out before using in production.
    # if version == '':
    #     result_json["score"] = .4
    if "score" in result_json.keys():
        print(result_json["score"])

    return HttpResponse(json.dumps(result_json))


@register.filter(is_safe=True)
@stringfilter
def trim(value):
    return value.strip()
