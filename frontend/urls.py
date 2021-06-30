from django.urls import include, path
from . import views


urlpatterns = [
    path('', views.index, name='home'),
    path('map', views.map_app, name='map'),
    path('about', views.about, name='about'),
    path('help', views.help_center, name='help'),
]