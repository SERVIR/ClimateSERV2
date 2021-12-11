# Create your views here.
from django.db.models.functions import Lower
from django.shortcuts import render
from django.contrib.admin.views.decorators import staff_member_required
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.template.defaultfilters import register
from django.forms.models import model_to_dict
from api.models import Track_Usage


@staff_member_required
def testing(request):
    print("REQUEST", request)
    return render(request, 'testing.html')


@staff_member_required
def usage(request):
    if request.method == "POST":
        record = Track_Usage.objects.get(id=request.POST["record_id"])
        record.delete()

    order_by = "id"  # request.GET.get('order_by')
    direction = "asc"  # request.GET.get('direction')
    ordering = order_by
    if direction == 'desc':
        ordering = '-{}'.format(ordering)
    paginator = Paginator(Track_Usage.objects.all().order_by(ordering), 2)
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


def key(d, key_name):
    row = model_to_dict(d)
    return row[key_name]


key = register.filter('key', key)
