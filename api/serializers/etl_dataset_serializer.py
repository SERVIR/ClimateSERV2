from rest_framework import serializers

from ..models import ETL_Dataset

class ETL_DatasetSerializer(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = ETL_Dataset
        fields = (
            'uuid',
            'dataset_name',
            'dataset_subtype'
        )
