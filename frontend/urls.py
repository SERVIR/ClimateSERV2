from django.urls import include, path
from . import views

app_name = "ClimateSERV_UI"
urlpatterns = [
    path('', views.index, name='home'),
    path('map', views.map_app, name='map'),
    path('about', views.about, name='about'),
    path('help', views.help_center, name='help'),
    path('confirm-captcha', views.confirm_captcha, name="confirm_captcha"),
    path('display-aoi/<str:usage_id>', views.display_aoi, name='display-aoi'),
]
