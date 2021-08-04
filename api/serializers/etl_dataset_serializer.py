from rest_framework import serializers

from ..models import ETL_Dataset

class ETL_DatasetSerializer(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = ETL_Dataset
        fields = (
            'uuid',
            'dataset_name',
            'dataset_subtype',
            'is_pipeline_enabled',
            'is_pipeline_active',
            'capabilities',
            'temp_working_dir',
            'final_load_dir',
            'source_url',
            'tds_product_name',
            'tds_region',
            'tds_spatial_resolution',
            'tds_temporal_resolution',
            'dataset_legacy_datatype',
            'dataset_nc4_variable_name',
            'is_lat_order_reversed',
            'dataset_base_directory_path',
            'additional_json',
            'created_at',
            'created_by',
            'is_test_object',
        )
