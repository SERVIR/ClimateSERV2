# Create your views here.
from django.shortcuts import render
from django.contrib.admin.views.decorators import staff_member_required
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

from api.models import Track_Usage


@staff_member_required
def testing(request):
    print("REQUEST", request)
    return render(request, 'testing.html')


@staff_member_required
def usage(request):
    paginator = Paginator(Track_Usage.objects.all(), 2)
    page = request.GET.get('page')

    try:
        items = paginator.page(page)
    except PageNotAnInteger:
        items = paginator.page(1)
    except EmptyPage:
        items = paginator.page(paginator.num_pages)

    index = items.number - 1
    max_index = len(paginator.page_range)
    start_index = index - 5 if index >= 5 else 0
    end_index = index + 5 if index <= max_index - 5 else max_index
    page_range = paginator.page_range[start_index:end_index]

    context = {
        'headers': Track_Usage._meta.get_fields(),
        'usage': Track_Usage.objects.all(),
        'page_range': page_range,
        'items': items
    }
    return render(request, 'usage.html', context)
