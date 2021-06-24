from rest_framework import serializers

from ..models import ETL_Dataset

class ETL_GranuleSerializer(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = ETL_Dataset
        fields = ('granule_name', 'granule_contextual_information')
