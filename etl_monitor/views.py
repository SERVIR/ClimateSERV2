from django.shortcuts import render

def index(request):
    return render(request, 'etl_monitor_index.html')
