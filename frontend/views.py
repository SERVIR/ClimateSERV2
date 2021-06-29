from django.shortcuts import render


# Create your views here.
def index(request):
    return render(request, 'index.html', context={'nonya': 'business'})

# Create your views here.
def map(request):
    return render(request, 'map.html', context={'nonya': 'business'})
