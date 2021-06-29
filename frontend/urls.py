from django.urls import include, path
from . import views


urlpatterns = [
    path('', views.index, name='index'),
    path('map', views.map, name='map'),
    # path('etl_subtypes/', views.ETL_SubtypesView.as_view(), name='etl_subtypes'),
    # path('api-auth/', include('rest_framework.urls', namespace='rest_framework')),
]