from rest_framework import serializers

from ..models import Config_Setting

class Config_SettingSerializer(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = Config_Setting
        fields = (
            'id',
            'setting_name',
            'setting_value',
            'setting_data_type',
            'created_at'
        )
