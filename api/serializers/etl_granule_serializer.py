from rest_framework import serializers

from ..models import ETL_Granule

class ETL_GranuleSerializer(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = ETL_Granule
        fields = (
            'uuid',
            'granule_name',
            'granule_contextual_information',
            'etl_pipeline_run',
            'etl_dataset',
            'is_missing',
            'granule_pipeline_state',
            'created_at',
            'created_by',
            'is_test_object',
        )
