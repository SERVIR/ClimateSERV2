# Create your views here.
from django.shortcuts import render
from django.contrib.admin.views.decorators import staff_member_required

from .models import *

@staff_member_required
def testing(request):
    print("REQUEST", request)
    return render(request, 'testing.html')