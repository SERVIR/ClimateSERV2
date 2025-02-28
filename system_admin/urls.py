from django.urls import include, path
from . import views

app_name = "ClimateSERV_SystemAdmin"
urlpatterns = [
    path('testing', views.testing, name='testing'),
    path('usage', views.usage, name='usage'),
    path('hits', views.hits, name='hits'),
]
