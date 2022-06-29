from django.contrib.admin.views.decorators import staff_member_required
from django.shortcuts import render

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
    })


@staff_member_required
def display_aoi(request, usage_id):
    usage = Track_Usage.objects.get(id=usage_id)
    return render(request, 'display-aoi.html', context={
        'aoi': usage.AOI,
    })


def about(request):
    return render(request, 'about.html', context={'page': 'menu-about'})


def help_center(request):
    return render(request, 'help.html', context={
        'page': 'menu-help',
        'datasets': DataSet.objects.all()
    })


@register.filter(is_safe=True)
@stringfilter
def trim(value):
    return value.strip()

