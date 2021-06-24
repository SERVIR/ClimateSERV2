from rest_framework import serializers

from ..models import ETL_Log

class ETL_LogSerializer(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = ETL_Log
        fields = ('activity_event_type', 'activity_description')
