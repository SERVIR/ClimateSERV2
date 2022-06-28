# Create your views here.
from django.db.models.functions import Lower
from django.shortcuts import render
from django.contrib.admin.views.decorators import staff_member_required
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.template.defaultfilters import register
from django.forms.models import model_to_dict
from django.db.models import Count, Sum
from django.db.models.functions import Trunc
from api.models import Track_Usage
import ast
from datetime import datetime


# sprint-2 init

@staff_member_required
def testing(request):
    print("REQUEST", request)
    return render(request, 'testing.html')


@staff_member_required
def hits(request):
    record_count = 10
    date_filter = {}
    if request.method == "GET":
        filter_date = request.GET.get('date', None)
        if filter_date:
            date_filter['time_requested__date'] = datetime.strptime(filter_date, "%m/%d/%Y")
    if request.method == "POST":
        if "record_count" in request.POST:
            record_count = int(request.POST["record_count"])
    hits_per_country = Track_Usage.objects.values(
        'country_ISO').filter(**date_filter).annotate(
        NumberOfHits=Count('country_ISO')).order_by(
        '-NumberOfHits')[:record_count]

    hits_per_dataset_all = Track_Usage.objects.exclude(
        dataset__contains="Climate Change Scenario:").values(
        'dataset').filter(**date_filter).annotate(
        NumberOfHits=Count('dataset')).order_by(
        '-NumberOfHits')[:record_count]
    hits_per_dataset_nmme = Track_Usage.objects.filter(
        dataset__contains="Climate Change Scenario:").values(
        'dataset').filter(**date_filter).aggregate(
        NumberOfHits=Count('dataset'))
    hits_per_dataset_list = list(hits_per_dataset_all)
    hits_per_dataset_list.append(
        {'dataset': 'Climate Change Scenario', 'NumberOfHits': hits_per_dataset_nmme['NumberOfHits']})

    hits_per_dataset = sorted(hits_per_dataset_list, key=lambda x: x['NumberOfHits'], reverse=True)

    hits_per_day = Track_Usage.objects.values(
        day=Trunc('time_requested', 'day')).filter(**date_filter).annotate(
        NumberOfHits=Count('day')).order_by('-day')

    bytes_per_day = Track_Usage.objects \
        .values(day=Trunc('time_requested', 'day')).filter(**date_filter) \
        .annotate(BytesDownloaded=Sum('file_size')) \
        .order_by('-day')

    context = {
        'hits_per_country': hits_per_country,
        'hits_per_dataset': hits_per_dataset,
        'hits_per_day': hits_per_day,
        'bytes_per_day': bytes_per_day,
        'number_of_items': record_count,
        'total_hits': Track_Usage.objects.filter(**date_filter).count(),
        'filter_date': filter_date,
    }
    return render(request, 'hits.html', context)


@staff_member_required
def usage(request):
    page = 1
    order_by = 'id'
    direction = "asc"
    number_of_items = 50
    filter_key = None
    filter_value = None
    start_range = ''
    end_range = ''
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
        if 'filter_key' in request.POST and request.POST["filter_key"] != 'None':
            filter_key = request.POST["filter_key"]
            filter_value = None
            if filter_key:
                if filter_key == 'time_requested':
                    filter_key += "__range"
                    start_range = request.POST['start_range']
                    end_range = request.POST['end_range']
                    filter_value = [request.POST['start_range'], request.POST['end_range']]
                else:
                    filter_key += "__icontains"
                    filter_value = request.POST["filter_text"]

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
        if 'filter_key' in request.GET and request.GET.get("filter_key") != 'None':
            filter_key = request.GET.get("filter_key")
            filter_value = None
            if filter_key:
                if filter_key == 'time_requested':
                    filter_key += "__range"
                    start_range = request.GET.get('start_range')
                    end_range = request.GET.get('end_range')
                    if start_range and end_range:
                        filter_value = [request.GET.get('start_range'), request.GET.get('end_range')]
                    else:
                        filter_value = ast.literal_eval(request.GET.get("filter_text"))
                        start_range = filter_value[0]
                        end_range = filter_value[1]
                else:
                    filter_key += "__icontains"
                    filter_value = request.GET.get("filter_text")
    ordering = order_by
    if direction == 'desc':
        ordering = '-{}'.format(ordering)
    if filter_key and filter_value:
        paginator = Paginator(
            Track_Usage.objects.filter(**{filter_key: filter_value}).order_by(ordering)
            , number_of_items
        )
    else:
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
    if filter_key:
        filter_key = filter_key.replace('__icontains', '').replace('__range', '')
    context = {
        'headers': Track_Usage._meta.get_fields(),
        'usage': Track_Usage.objects.all(),
        'page_range': page_range,
        'items': items,
        'page': page,
        'sorted': order_by,
        'direction': direction,
        'number_of_items': number_of_items,
        'filter_key': filter_key,
        'filter_text': filter_value,
        'start_range': start_range,
        'end_range': end_range,
    }
    return render(request, 'usage.html', context)


def key(d, key_name):
    row = model_to_dict(d)
    return row[key_name]


key = register.filter('key', key)
