from django.shortcuts import render
from .models import *


def index(request):
    return render(request, 'index.html', context={
        'page': 'menu-home',
        'datasets': DataSet.objects.all(),
    })


def map_app(request):
    return render(request, 'map.html', context={'page': 'menu-map'})


def about(request):
    return render(request, 'about.html', context={'page': 'menu-about'})


def help_center(request):
    return render(request, 'help.html', context={'page': 'menu-help'})
