from django.urls import include, path
from . import views

app_name = "ClimateSERV_UI"
urlpatterns = [
    path('', views.index, name='home'),
    path('map', views.map_app, name='map'),
    path('about', views.about, name='about'),
    path('feedback', views.feedback, name='feedback'),
    path('help', views.help_center, name='help'),
    path('dev-api', views.dev_api, name='dev_api'),
    path('get-layers', views.get_map_layers, name='get_layers'),
    path('confirm-captcha', views.confirm_captcha, name="confirm_captcha"),
    path('display-aoi/<str:usage_id>', views.display_aoi, name='display-aoi'),
]
