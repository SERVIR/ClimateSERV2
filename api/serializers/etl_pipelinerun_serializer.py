from rest_framework import serializers

from ..models import ETL_PipelineRun

class ETL_PipelineRunSerializer(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = ETL_PipelineRun
        fields = (
            'uuid',
            'additional_json',
            'created_at',
            'created_by',
            'is_test_object',
        )
