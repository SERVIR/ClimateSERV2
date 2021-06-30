from django.shortcuts import render


def index(request):
    return render(request, 'index.html', context={'nonya': 'business'})


def map_app(request):
    return render(request, 'map.html', context={'nonya': 'business'})


def about(request):
    return render(request, 'about.html', context={'nonya': 'business'})


def help_center(request):
    return render(request, 'help.html', context={'nonya': 'business'})
