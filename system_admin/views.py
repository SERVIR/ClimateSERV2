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
    page = 1
    order_by = 'id'
    direction = "asc"
    number_of_items = 2
    if request.method == "POST":
        if "delete_record_id" in request.POST:
            record = Track_Usage.objects.get(id=request.POST["delete_record_id"])
            record.delete()
            page = request.POST["page"]
        if "sort_column" in request.POST:
            order_by = request.POST["sort_column"]
        if "direction" in request.POST:
            direction = 'desc' if request.POST["direction"] == 'asc' else 'asc'
        if "number_of_items" in request.POST:
            Track_Usage.objects.all().count()
            post_count = request.POST["number_of_items"]
            number_of_items = Track_Usage.objects.all().count() if post_count == "all" else post_count

    else:
        page = request.GET.get('page')
        if 'sorted' in request.GET:
            order_by = request.GET.get('sorted')
        if "direction" in request.GET:
            direction = request.GET.get('direction')
        if "number_of_items" in request.GET:
            Track_Usage.objects.all().count()
            get_count = request.GET.get("number_of_items")
            number_of_items = Track_Usage.objects.all().count() if get_count == "all" else get_count
    ordering = order_by
    if direction == 'desc':
        ordering = '-{}'.format(ordering)
    paginator = Paginator(Track_Usage.objects.all().order_by(ordering), number_of_items)

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
        'items': items,
        'page': page,
        'sorted': order_by,
        'direction': direction,
        'number_of_items': number_of_items,
    }
    return render(request, 'usage.html', context)


def key(d, key_name):
    row = model_to_dict(d)
    return row[key_name]


key = register.filter('key', key)
