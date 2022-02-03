from ..models import ETL_Dataset
from ..serializers import ETL_DatasetSerializer


class ETL_DatasetService():

    @staticmethod
    def is_datasetname_avalaible(input__datasetname):
        try:
            existing_etl_dataset = ETL_Dataset.objects.filter(dataset_name=str(input__datasetname).strip())[0]
            return False
        except:
            return True


    @staticmethod
    def does_etl_dataset_exist__by_uuid(input__uuid):
        try:
            existing_etl_dataset = ETL_Dataset.objects.filter(uuid=str(input__uuid).strip())[0]
            return True
        except:
            return False

    @staticmethod
    def create_etl_dataset_from_datasetname_only(input__datasetname, created_by="create_dataset_from_datasetname_only"):
        try:
            new_etl_dataset = ETL_Dataset()
            new_etl_dataset.dataset_name = str(input__datasetname).strip()
            new_etl_dataset.created_by = str(created_by).strip()
            new_etl_dataset.save()

            return True, new_etl_dataset.uuid
        except:
            return False, ""

    @staticmethod
    def get_all_etl_datasets_preview_list():
        ret_list = []
        try:
            all_datasets = ETL_Dataset.objects.all()
            for current_dataset in all_datasets:
                ret_list.append(ETL_DatasetSerializer(current_dataset).data)
        except:
            ret_list = []
        return ret_list

    @staticmethod
    def is_a_valid_subtype_string(input__string):
        try:
            input__string = str(input__string).strip()
            valid_subtypes_string_list = ETL_DatasetService.get_all_subtypes_as_string_array()
            if input__string in valid_subtypes_string_list:
                return True
        except:
            return False

    @staticmethod
    def get_all_subtypes_as_string_array():
        return list(
            ETL_Dataset.objects.order_by('dataset_subtype').values_list('dataset_subtype', flat=True).distinct())
