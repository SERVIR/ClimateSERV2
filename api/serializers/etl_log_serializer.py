from rest_framework import serializers

from ..models import ETL_Log

class ETL_LogSerializer(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = ETL_Log
        fields = (
            'uuid',
            'activity_event_type',
            'activity_description',
            'etl_pipeline_run',
            'etl_dataset',
            'etl_granule',
            'is_alert',
            'is_alert_dismissed',
            'additional_json',
            'created_at',
            'created_by',
            'is_test_object',
        )
