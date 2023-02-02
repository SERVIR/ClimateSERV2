import json
import uuid

import requests
from django.contrib.admin.views.decorators import staff_member_required
from django.http import HttpResponse
from django.shortcuts import render

from climateserv2 import settings
from climateserv2.views import get_nmme_info
from .models import *
from api.models import Track_Usage

from django import template
from django.template.defaultfilters import stringfilter
from django.views.decorators.cache import cache_page

register = template.Library()


@cache_page(60 * 15)
def index(request):
    return render(request, 'index.html', context={
        'page': 'menu-home',
        'datasets': DataSet.objects.exclude(featured=False).all(),
        'page_elements': HomePage.objects.first()
    })


def map_app(request):
    return render(request, 'map.html', context={
        'page': 'menu-map',
        'data_layers': DataLayer.objects.order_by('title').all(),
        'climateModelInfo': json.dumps(get_nmme_info(str(uuid.uuid4()))),
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

    return HttpResponse(json.dumps(result_json))


@register.filter(is_safe=True)
@stringfilter
def trim(value):
    return value.strip()
