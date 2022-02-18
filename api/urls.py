from django.urls import include, path
from rest_framework import routers
from . import views

router = routers.DefaultRouter()
router.register(r'config_setting', views.Config_SettingViewSet)
router.register(r'etl_dataset', views.ETL_DatasetViewSet)
router.register(r'etl_granule', views.ETL_GranuleViewSet)
router.register(r'etl_log', views.ETL_LogViewSet)
router.register(r'etl_pipeline_run', views.ETL_PipelineRunViewSet)

# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browsable API.
urlpatterns = [
    path('', include(router.urls)),
    path('get_etl_subtypes/', views.ETL_SubtypesView.as_view(), name='get_etl_subtypes'),
    path('api-auth/', include('rest_framework.urls', namespace='rest_framework')),
    path('backfill/', views.Update_Records.as_view(), name='backfill'),
]
