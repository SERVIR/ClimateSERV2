from rest_framework import serializers

from ..models import ETL_PipelineRun

class ETL_PipelineRunSerializer(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = ETL_PipelineRun
        fields = ('created_at', 'created_by')
